#!/usr/bin/env python3
"""Publica manualmente um draft de newsletter gerado em modo review."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from gmail_monitor import _resolve_telegram_config, _send_telegram_message
from process_newsletter import Config, _api_post


LOG = logging.getLogger("publish_review_draft")
EDITORIAL_TELEGRAM_NOTIFY = os.getenv("EDITORIAL_TELEGRAM_NOTIFY", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
OPENCLAW_SINGLE_SPOC = os.getenv("OPENCLAW_SINGLE_SPOC", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PORTAL_PUBLIC_BASE_URL = os.getenv("PORTAL_PUBLIC_BASE_URL", "").strip().rstrip("/")
_PUBLIC_BASE_CACHE: str | None = None
_PREVIEW_TOKEN_RE = re.compile(r"/preview/([0-9a-fA-F-]{36})/?")


def _resolve_draft_path(newsletter_id: int, explicit_path: str | None, review_dir: str) -> Path:
    if explicit_path:
        path = Path(explicit_path)
    else:
        path = Path(review_dir) / f"newsletter_{newsletter_id}_draft.json"
    return path


def _load_payload(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"draft not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_target_draft(
    cfg: Config,
    newsletter_id: int | None,
    explicit_path: str | None,
) -> tuple[int, Path, dict[str, Any]]:
    if explicit_path:
        draft_path = Path(explicit_path)
        payload = _load_payload(draft_path)
        raw_id = newsletter_id if newsletter_id is not None else payload.get("newsletter_id")
        try:
            resolved_id = int(raw_id)
        except Exception:
            raise SystemExit("unable to resolve newsletter id from draft; pass --newsletter-id")
        return resolved_id, draft_path, payload

    if newsletter_id is not None:
        draft_path = _resolve_draft_path(int(newsletter_id), None, cfg.review_output_dir)
        payload = _load_payload(draft_path)
        return int(newsletter_id), draft_path, payload

    review_dir = Path(cfg.review_output_dir)
    candidates = sorted(review_dir.glob("newsletter_*_draft.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for draft_path in candidates:
        try:
            payload = _load_payload(draft_path)
            resolved_id = int(payload.get("newsletter_id"))
        except Exception:
            continue
        articles = payload.get("articles", [])
        if isinstance(articles, list) and articles:
            return resolved_id, draft_path, payload
    raise SystemExit("unable to auto-resolve draft; pass --newsletter-id or --draft-file")


def _manual_review_pending(articles: list[dict]) -> list[dict]:
    pending: list[dict] = []
    for idx, article in enumerate(articles, start=1):
        if not bool(article.get("manual_review_required")):
            continue
        pending.append(
            {
                "article_index": idx,
                "title": str(article.get("title", "")).strip(),
                "original_url": str(article.get("original_url", "")).strip(),
                "summary_source_mode": str(article.get("summary_source_mode", "")).strip(),
                "review_note": str(article.get("review_note", "")).strip(),
            }
        )
    return pending


def _decision_state(article: dict) -> str:
    value = str(article.get("review_decision", "")).strip().lower()
    if value in {"approved", "rejected", "pending"}:
        return value
    return "pending"


def _decision_buckets(articles: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    approved: list[dict] = []
    rejected: list[dict] = []
    pending: list[dict] = []
    for idx, article in enumerate(articles, start=1):
        row = dict(article) if isinstance(article, dict) else {}
        row["_article_index"] = idx
        state = _decision_state(row)
        if state == "approved":
            approved.append(row)
        elif state == "rejected":
            rejected.append(row)
        else:
            pending.append(row)
    return approved, rejected, pending


def _preview_items(articles: list[dict]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(articles, start=1):
        if not isinstance(row, dict):
            continue
        preview_path = str(row.get("editorial_preview_path", "")).strip()
        preview_token = str(row.get("editorial_preview_token", "")).strip()
        if not preview_path and preview_token:
            preview_path = f"/preview/{preview_token}/"
        if not preview_path:
            continue
        preview_card_path = _build_card_preview_path(preview_path=preview_path, preview_token=preview_token)
        out.append(
            {
                "article_index": idx,
                "title": str(row.get("title", "")).strip(),
                "source": str(row.get("original_url", "")).strip(),
                "editorial_status": str(row.get("editorial_status", "")).strip() or "pending",
                "portal_article_id": row.get("portal_article_id"),
                "preview_path": preview_path,
                "preview_url": _build_preview_url(preview_path),
                "preview_card_path": preview_card_path,
                "preview_card_url": _build_preview_url(preview_card_path),
            }
        )
    return out


def _build_preview_url(preview_path: str) -> str:
    path = (preview_path or "").strip()
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    base = _resolve_public_base_url()
    if base:
        return f"{base}{path}"
    return path


def _extract_preview_token(*, preview_path: str, preview_token: str = "") -> str:
    token = str(preview_token or "").strip()
    if token:
        return token
    path = str(preview_path or "").strip()
    if not path:
        return ""
    match = _PREVIEW_TOKEN_RE.search(path)
    if not match:
        return ""
    return str(match.group(1)).strip()


def _build_card_preview_path(*, preview_path: str, preview_token: str = "") -> str:
    token = _extract_preview_token(preview_path=preview_path, preview_token=preview_token)
    if not token:
        return ""
    return f"/preview/card/{token}/"


def _resolve_public_base_url() -> str:
    global _PUBLIC_BASE_CACHE
    if _PUBLIC_BASE_CACHE:
        return _PUBLIC_BASE_CACHE
    if PORTAL_PUBLIC_BASE_URL:
        _PUBLIC_BASE_CACHE = PORTAL_PUBLIC_BASE_URL
        return _PUBLIC_BASE_CACHE
    return ""


def _send_editorial_review_notifications(cfg: Config, newsletter_id: int, result: dict) -> None:
    if not EDITORIAL_TELEGRAM_NOTIFY or OPENCLAW_SINGLE_SPOC:
        return

    token, chat_id = _resolve_telegram_config(cfg)
    if not token or not chat_id:
        LOG.warning("editorial telegram notification skipped: missing bot token/chat id")
        return

    rows = result.get("articles", [])
    if not isinstance(rows, list):
        rows = []

    header = [
        f"Editorial review queue ready: Newsletter #{newsletter_id}",
        f"Articles pending content approval: {len(rows)}",
        "Each article is in private preview (not visible in public feed).",
    ]
    _send_telegram_message(token, chat_id, "\n".join(header))

    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        title = str(row.get("title", "")).strip() or f"Article {idx}"
        source = str(row.get("original_url", "")).strip() or "n/a"
        draft_index = row.get("draft_article_index") or idx
        preview_path = str(row.get("preview_path", "")).strip()
        preview_token = str(row.get("preview_token", "")).strip()
        preview_url = _build_preview_url(preview_path)
        preview_card_path = str(row.get("preview_card_path", "")).strip() or _build_card_preview_path(
            preview_path=preview_path,
            preview_token=preview_token,
        )
        preview_card_url = _build_preview_url(preview_card_path)
        editorial_status = str(row.get("editorial_status", "")).strip() or "pending"

        lines = [
            f"Editorial review required: Newsletter #{newsletter_id} / Article #{draft_index}",
            f"1) Title: {title}",
            f"2) source: {source}",
            f"3) card preview: {preview_card_url or 'n/a'}",
            f"4) detail preview: {preview_url or 'n/a'}",
            f"5) status: {editorial_status}",
        ]
        message = "\n".join(lines).strip()
        if len(message) > 3900:
            message = message[:3890].rstrip() + "\n\n[truncated]"
        _send_telegram_message(token, chat_id, message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish newsletter draft file")
    parser.add_argument("--newsletter-id", type=int, help="Newsletter id")
    parser.add_argument("--draft-file", help="Explicit draft JSON path (optional)")
    parser.add_argument("--publish", action="store_true", help="Actually publish")
    parser.add_argument("--url", default="", help="Compatibility argument (ignored)")
    parser.add_argument("--portal-source", default="", help="Compatibility argument (ignored)")
    parser.add_argument(
        "--mode",
        choices=["preview", "public"],
        default="preview",
        help="preview: private editorial review (default) | public: immediate public release",
    )
    parser.add_argument(
        "--force-unresolved-manual",
        action="store_true",
        help="Allow publish even if articles still have manual_review_required=true",
    )
    args, unknown_args = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if unknown_args:
        LOG.warning("ignoring unknown args: %s", " ".join(unknown_args))

    cfg = Config()
    if not cfg.agent_api_key:
        raise SystemExit("AGENT_API_KEY missing")

    newsletter_id, draft_path, draft = _resolve_target_draft(cfg, args.newsletter_id, args.draft_file)
    articles = draft.get("articles", [])
    if not isinstance(articles, list):
        raise SystemExit("invalid draft: articles must be a list")
    pending_manual = _manual_review_pending(articles)
    approved, rejected, pending_decisions = _decision_buckets(articles)
    preview_rows = _preview_items(articles)
    print(
        json.dumps(
            {
                "newsletter_id": newsletter_id,
                "draft_file": str(draft_path),
                "articles": len(articles),
                "manual_review_pending": len(pending_manual),
                "manual_review_items": pending_manual,
                "preview_items": preview_rows,
                "decision_counts": {
                    "approved": len(approved),
                    "rejected": len(rejected),
                    "pending": len(pending_decisions),
                },
            },
            ensure_ascii=False,
        )
    )

    if not args.publish:
        return 0

    if pending_manual and not args.force_unresolved_manual:
        raise SystemExit(
            "publish blocked: unresolved manual_review_required articles in draft. "
            "Use review_apply_manual_source.py first, or pass --force-unresolved-manual."
        )
    if pending_decisions:
        raise SystemExit(
            "publish blocked: some articles are still pending decision. "
            "Approve or reject each article first."
        )
    publish_articles = [row for row in approved if not bool(row.get("manual_review_required")) or args.force_unresolved_manual]
    if not publish_articles:
        raise SystemExit("publish blocked: no approved articles to publish.")

    started = time.time()
    result = _api_post(
        cfg,
        "articles/publish/",
        {
            "newsletter_id": newsletter_id,
            "articles": publish_articles,
            "mode": args.mode,
            "prune_missing": True,
        },
    )
    published_count = int(result.get("articles_created", len(publish_articles)))
    mode_used = str(result.get("mode", args.mode)).strip() or args.mode
    action_name = "review_publish_preview" if mode_used == "preview" else "review_publish_public"
    _api_post(
        cfg,
        "log/",
        {
            "newsletter_id": newsletter_id,
            "action": action_name,
            "status": "success",
            "message": (
                f"Published from draft {draft_path}; approved={len(approved)} "
                f"rejected={len(rejected)} mode={mode_used} published={published_count}"
            ),
            "duration_seconds": round(time.time() - started, 3),
        },
    )

    if mode_used == "preview":
        _send_editorial_review_notifications(cfg, newsletter_id, result)

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
