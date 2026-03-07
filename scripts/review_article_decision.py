#!/usr/bin/env python3
"""Set approve/reject decision for one article in a newsletter review draft."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from process_newsletter import (
    Config,
    _apply_source_metadata,
    _assert_required_summary_model,
    _get_source_snapshot,
    _snapshot_has_meaningful_content,
    _api_post,
    article_preview_quality_issues,
    enrich_article,
    rewrite_article_from_source,
    source_snapshot_title_match,
)
from review_apply_manual_source import _rewrite_from_manual_text


LOG = logging.getLogger("review_article_decision")
MIN_AUTO_SOURCE_TEXT_CHARS = 900
REVIEW_DECISION_STATES = {"pending", "approved", "rejected"}


def _resolve_draft_path(newsletter_id: int, explicit_path: str | None, review_dir: str) -> Path:
    if explicit_path:
        return Path(explicit_path)
    return Path(review_dir) / f"newsletter_{newsletter_id}_draft.json"


def _load_draft(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid draft payload")
    articles = payload.get("articles", [])
    if not isinstance(articles, list):
        raise ValueError("invalid draft payload: articles is not a list")
    return payload


def _save_draft(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_source_text(source_text: str, source_text_file: str, read_stdin: bool) -> str:
    if source_text:
        return source_text.strip()
    if source_text_file:
        return Path(source_text_file).read_text(encoding="utf-8").strip()
    if read_stdin:
        return sys.stdin.read().strip()
    return ""


def _decision_counts(articles: list[dict[str, Any]]) -> dict[str, int]:
    out = {"approved": 0, "rejected": 0, "pending": 0}
    for row in articles:
        state = str(row.get("review_decision", "")).strip().lower()
        if state in out:
            out[state] += 1
        else:
            out["pending"] += 1
    return out


def _first_pending_index(articles: list[dict[str, Any]]) -> int | None:
    for idx, row in enumerate(articles, start=1):
        if not isinstance(row, dict):
            continue
        state = _review_state(row)
        if state == "pending":
            return idx
    return None


def _editorial_gate_index(articles: list[dict[str, Any]]) -> int | None:
    for idx, row in enumerate(articles, start=1):
        if not isinstance(row, dict):
            continue
        decision = _review_state(row)
        if decision != "approved":
            continue
        editorial_status = str(row.get("editorial_status", "")).strip().lower()
        if editorial_status in {"", "pending", "revising", "on_hold"}:
            return idx
    return None


def _auto_target_index(articles: list[dict[str, Any]]) -> int | None:
    gate = _editorial_gate_index(articles)
    if gate:
        return gate
    return _first_pending_index(articles)


def _review_state(article: dict[str, Any]) -> str:
    raw = str(article.get("review_decision", "pending")).strip().lower()
    if raw in REVIEW_DECISION_STATES:
        return raw
    return "pending"


def _parse_iso_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _resolve_latest_notified_pending_target(cfg: Config) -> tuple[int, int, Path, dict[str, Any]]:
    review_dir = Path(cfg.review_output_dir)
    candidates = sorted(review_dir.glob("newsletter_*_draft.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    best: tuple[tuple[datetime, float, int, int], int, int, Path, dict[str, Any]] | None = None

    for draft_path in candidates:
        try:
            draft = _load_draft(draft_path)
            draft_mtime = draft_path.stat().st_mtime
        except Exception:
            continue

        try:
            resolved_newsletter_id = int(draft.get("newsletter_id"))
        except (TypeError, ValueError):
            continue
        if resolved_newsletter_id <= 0:
            continue

        articles = draft.get("articles", [])
        if not isinstance(articles, list):
            continue

        for idx, row in enumerate(articles, start=1):
            if not isinstance(row, dict):
                continue
            if _review_state(row) != "pending":
                continue
            notified_at = _parse_iso_datetime(str(row.get("review_notified_at", "")).strip())
            if notified_at is None:
                continue
            key = (notified_at, draft_mtime, resolved_newsletter_id, idx)
            if best is None or key > best[0]:
                best = (key, resolved_newsletter_id, idx, draft_path, draft)

    if best is None:
        raise SystemExit("missing review context; provide Newsletter #<NID> / Article #<AIdx>")
    return best[1], best[2], best[3], best[4]


def _guard_decision_reentry(
    article: dict[str, Any],
    decision: str,
    newsletter_id: int,
    article_index: int,
    articles: list[Any],
    draft_path: Path,
) -> tuple[dict[str, Any], int] | None:
    current_state = _review_state(article)
    if current_state == "pending":
        return None

    requested_state = "approved" if decision == "approve" else "rejected"
    if current_state == requested_state:
        return (
            {
                "newsletter_id": newsletter_id,
                "article_index": article_index,
                "decision": current_state,
                "title": str(article.get("title", "")).strip(),
                "status": "duplicate_ignored",
                "decision_counts": _decision_counts([row for row in articles if isinstance(row, dict)]),
                "draft_file": str(draft_path),
            },
            200,
        )

    return (
        {
            "error": f"article already decided as {current_state}; conflicting decision blocked",
            "newsletter_id": newsletter_id,
            "article_index": article_index,
            "draft_file": str(draft_path),
        },
        409,
    )


def _resolve_target(
    cfg: Config,
    newsletter_id: int | None,
    article_index: int | None,
    draft_file: str | None,
) -> tuple[int, int, Path, dict[str, Any]]:
    explicit_draft = (draft_file or "").strip()
    if explicit_draft:
        draft_path = Path(explicit_draft)
        draft = _load_draft(draft_path)
        raw_newsletter_id = newsletter_id if newsletter_id is not None else draft.get("newsletter_id")
        try:
            resolved_newsletter_id = int(raw_newsletter_id)
        except (TypeError, ValueError):
            raise SystemExit("unable to resolve newsletter id from draft; pass --newsletter-id")
        if resolved_newsletter_id <= 0:
            raise SystemExit("invalid --newsletter-id")
        resolved_article_index = article_index if article_index is not None else _auto_target_index(draft.get("articles", []))
        if not resolved_article_index:
            raise SystemExit("no actionable article found in draft; pass --article-index explicitly")
        return resolved_newsletter_id, int(resolved_article_index), draft_path, draft

    if newsletter_id is not None:
        draft_path = _resolve_draft_path(int(newsletter_id), None, cfg.review_output_dir)
        draft = _load_draft(draft_path)
        resolved_article_index = article_index if article_index is not None else _auto_target_index(draft.get("articles", []))
        if not resolved_article_index:
            raise SystemExit(
                f"no actionable article found for newsletter {newsletter_id}; pass --article-index explicitly"
            )
        return int(newsletter_id), int(resolved_article_index), draft_path, draft

    if article_index is not None:
        raise SystemExit("--newsletter-id is required when --article-index is provided")

    review_dir = Path(cfg.review_output_dir)
    candidates = sorted(review_dir.glob("newsletter_*_draft.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for draft_path in candidates:
        try:
            draft = _load_draft(draft_path)
        except Exception:
            continue
        raw_newsletter_id = draft.get("newsletter_id")
        try:
            resolved_newsletter_id = int(raw_newsletter_id)
        except (TypeError, ValueError):
            continue
        resolved_article_index = _auto_target_index(draft.get("articles", []))
        if resolved_article_index:
            return resolved_newsletter_id, int(resolved_article_index), draft_path, draft
    raise SystemExit("no draft with actionable article found; pass --newsletter-id and --article-index")


def _mark_decision(article: dict[str, Any], decision: str, comment: str) -> dict[str, Any]:
    out = dict(article)
    out["review_decision"] = decision
    out["review_decided_at"] = datetime.now(tz=timezone.utc).isoformat()
    if comment:
        out["review_comment"] = comment
    return out


def _publish_single_article_preview(
    cfg: Config,
    newsletter_id: int,
    article_index: int,
    article: dict[str, Any],
) -> dict[str, Any]:
    payload_article = dict(article)
    payload_article["_article_index"] = article_index
    return _api_post(
        cfg,
        "articles/publish/",
        {
            "newsletter_id": int(newsletter_id),
            "articles": [payload_article],
            "mode": "preview",
            "prune_missing": False,
        },
    )


def _send_editorial_notification(cfg: Config, newsletter_id: int, publish_result: dict[str, Any]) -> dict[str, Any]:
    try:
        from publish_review_draft import _send_editorial_review_notifications  # Lazy import.

        _send_editorial_review_notifications(cfg, int(newsletter_id), publish_result)
        return {"sent": True}
    except Exception as exc:
        LOG.warning("failed editorial telegram notify newsletter_id=%s err=%s", newsletter_id, exc)
        return {"sent": False, "reason": "notify_error", "error": str(exc)}


def _send_next_article_notification(cfg: Config, newsletter_id: int, draft_file: str) -> dict[str, Any]:
    email_meta: dict[str, Any] | None = None
    try:
        headers = {"X-API-Key": cfg.agent_api_key}
        resp = requests.get(
            f"{cfg.portal_api_url.rstrip('/')}/newsletter/{int(newsletter_id)}/raw/",
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        raw_payload = resp.json()
        email_meta = {
            "subject": raw_payload.get("subject"),
            "sender_name": raw_payload.get("sender_name"),
            "sender_email": raw_payload.get("sender_email"),
            "original_sender_name": raw_payload.get("original_sender_name"),
            "original_sender_email": raw_payload.get("original_sender_email"),
            "original_sent_at": raw_payload.get("original_sent_at"),
            "original_sent_at_raw": raw_payload.get("original_sent_at_raw"),
            "received_at": raw_payload.get("received_at"),
        }
    except Exception as exc:
        LOG.warning("failed to resolve newsletter metadata newsletter_id=%s err=%s", newsletter_id, exc)

    try:
        from gmail_monitor import send_next_review_notification

        return send_next_review_notification(
            cfg,
            int(newsletter_id),
            review_file=draft_file,
            include_intro=True,
            email_meta=email_meta,
        )
    except Exception as exc:
        LOG.warning("failed to send next article notification newsletter_id=%s err=%s", newsletter_id, exc)
        return {"sent": False, "reason": "notify_error", "error": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Approve or reject one article in draft")
    parser.add_argument("--newsletter-id", type=int, help="Newsletter id")
    parser.add_argument("--article-index", type=int, help="1-based article index in draft")
    parser.add_argument("--decision", choices=["approve", "reject"], help="Decision to apply")
    parser.add_argument("--draft-file", help="Explicit draft JSON path (optional)")
    parser.add_argument("--comment", default="", help="Optional reviewer comment")
    parser.add_argument("--source-url", default="", help="Optional replacement source URL for approve flow")
    parser.add_argument("--url", default="", help="Alias for source URL")
    parser.add_argument("--portal-source", default="", help="Alias for canonical source URL in portal")
    parser.add_argument("--source-text-file", default="", help="Optional full source text file for approve flow")
    parser.add_argument("--source-text", default="", help="Optional inline full source text for approve flow")
    parser.add_argument("--stdin", action="store_true", help="Read full source text from stdin")
    parser.add_argument(
        "--allow-unresolved-manual",
        action="store_true",
        help="Deprecated safety override (disabled; approvals fail-closed when manual review is required)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = Config()
    canonical_source_url = (args.source_url or args.portal_source or args.url or "").strip()
    source_read_url = (args.url or "").strip()
    source_text = _read_source_text(args.source_text, args.source_text_file, bool(args.stdin))
    manual_source_text_supplied = bool(source_text)
    decision = args.decision or ("approve" if (canonical_source_url or source_text) else "")
    if not decision:
        raise SystemExit("--decision is required unless source URL/text is provided (auto-approve)")

    if decision == "approve" and not source_text:
        source_candidates: list[str] = []
        if source_read_url and source_read_url != canonical_source_url:
            source_candidates.append(source_read_url)
        if canonical_source_url:
            source_candidates.append(canonical_source_url)

        for candidate_url in source_candidates:
            snapshot = _get_source_snapshot(candidate_url, cfg.source_rewrite_max_chars, cfg.source_open_min_chars)
            candidate_text = str(snapshot.get("text", "")).strip()
            if len(candidate_text) >= MIN_AUTO_SOURCE_TEXT_CHARS:
                source_text = candidate_text
                break

    resolved_newsletter_id, resolved_article_index, draft_path, draft = _resolve_target(
        cfg=cfg,
        newsletter_id=args.newsletter_id,
        article_index=args.article_index,
        draft_file=args.draft_file,
    )
    articles = draft.get("articles", [])

    idx = resolved_article_index - 1
    if idx < 0 or idx >= len(articles):
        raise SystemExit(f"article index out of range: {resolved_article_index} (1..{len(articles)})")
    current = articles[idx]
    if not isinstance(current, dict):
        raise SystemExit("selected article is invalid")

    duplicate = _guard_decision_reentry(
        article=current,
        decision=decision,
        newsletter_id=resolved_newsletter_id,
        article_index=resolved_article_index,
        articles=articles,
        draft_path=draft_path,
    )
    if duplicate is not None:
        payload, status = duplicate
        if status >= 400:
            raise SystemExit(str(payload.get("error", "decision blocked")))
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    if decision == "reject":
        updated = _mark_decision(current, "rejected", args.comment.strip())
        articles[idx] = updated
        draft["articles"] = articles
        _save_draft(draft_path, draft)
        next_notification = _send_next_article_notification(
            cfg,
            resolved_newsletter_id,
            str(draft_path),
        )
        print(
            json.dumps(
                {
                    "newsletter_id": resolved_newsletter_id,
                    "article_index": resolved_article_index,
                    "decision": "rejected",
                    "title": str(updated.get("title", "")).strip(),
                    "decision_counts": _decision_counts([row for row in articles if isinstance(row, dict)]),
                    "next_review_notification": next_notification,
                    "draft_file": str(draft_path),
                },
                ensure_ascii=False,
            )
        )
        return 0

    # Approve flow
    updated = dict(current)

    if source_text or canonical_source_url:
        _assert_required_summary_model(cfg)

    if source_text:
        if canonical_source_url:
            updated["original_url"] = canonical_source_url
            updated = _apply_source_metadata(updated)
        if len(source_text) < 300:
            raise SystemExit("source text too short; provide full source text (>=300 chars)")
        updated = _rewrite_from_manual_text(cfg, updated, source_text)
    elif canonical_source_url:
        updated["original_url"] = canonical_source_url
        updated = _apply_source_metadata(updated)
        updated = rewrite_article_from_source(cfg, updated)
        updated = enrich_article(cfg, updated)

    unresolved = bool(updated.get("manual_review_required"))
    if unresolved:
        raise SystemExit(
            "approve blocked: article still requires manual source decision. "
            "Provide full source text or an open source URL. "
            "Safety override --allow-unresolved-manual is disabled."
        )

    quality_issues = article_preview_quality_issues(updated)
    resolved_source_url = str(updated.get("original_url", "")).strip()
    if resolved_source_url:
        snapshot = _get_source_snapshot(
            resolved_source_url,
            cfg.source_rewrite_max_chars,
            cfg.source_open_min_chars,
        )
        if not _snapshot_has_meaningful_content(snapshot, cfg.source_presence_min_chars):
            if not manual_source_text_supplied:
                quality_issues.append("source_url_has_no_meaningful_content")
        else:
            semantic = source_snapshot_title_match(
                str(updated.get("title", "")).strip(),
                snapshot,
                min_overlap=cfg.source_title_overlap_min_ratio,
            )
            if not bool(semantic.get("ok")):
                quality_issues.append("source_url_semantic_mismatch")
    elif not source_text:
        quality_issues.append("missing_source_material")

    if quality_issues:
        raise SystemExit(
            "approve blocked: preview quality gate failed (" + ", ".join(sorted(set(quality_issues))) + ")"
        )

    updated = _mark_decision(updated, "approved", args.comment.strip())
    articles[idx] = updated
    draft["articles"] = articles
    _save_draft(draft_path, draft)

    publish_result = _publish_single_article_preview(cfg, resolved_newsletter_id, resolved_article_index, updated)
    editorial_notify = _send_editorial_notification(cfg, resolved_newsletter_id, publish_result)

    published_rows = publish_result.get("articles", []) if isinstance(publish_result, dict) else []
    if isinstance(published_rows, list) and published_rows:
        row0 = published_rows[0] if isinstance(published_rows[0], dict) else {}
        if row0:
            updated["portal_article_id"] = row0.get("id")
            updated["editorial_status"] = row0.get("editorial_status")
            updated["editorial_preview_path"] = row0.get("preview_path")
            updated["editorial_preview_token"] = row0.get("preview_token")
            articles[idx] = updated
            draft["articles"] = articles
            _save_draft(draft_path, draft)

    print(
        json.dumps(
            {
                "newsletter_id": resolved_newsletter_id,
                "article_index": resolved_article_index,
                "decision": "approved",
                "title": str(updated.get("title", "")).strip(),
                "manual_review_required": bool(updated.get("manual_review_required")),
                "summary_source_mode": str(updated.get("summary_source_mode", "")).strip(),
                "decision_counts": _decision_counts([row for row in articles if isinstance(row, dict)]),
                "preview_publish_result": publish_result,
                "editorial_notification": editorial_notify,
                "next_review_notification": {
                    "sent": False,
                    "reason": "blocked_until_editorial_public_confirmation",
                },
                "draft_file": str(draft_path),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
