#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import quopri
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any

from gmail_monitor import (
    _decode_header,
    _extract_sender,
    _is_imap_connection_error,
    _is_imap_overquota_error,
    _normalize_datetime,
    _normalize_received_at,
    connect_gmail,
)
from newsletter_revh_parser import parse_email_articles
from process_newsletter import (
    _extract_email_anchor_candidates,
    _extract_email_segments_from_html,
    _extract_forwarded_subject_hint,
    clean_html,
)
from runtime_paths import DATA_DIR, PROJECT_ROOT, ensure_runtime_dirs


LOG = logging.getLogger("parser_replay")

CACHE_ROOT_DEFAULT = DATA_DIR / "parser_replay" / "cache"
REPORT_ROOT_DEFAULT = DATA_DIR / "parser_replay" / "reports"
MANIFEST_NAME = "manifest.json"
DIGEST_PATTERNS = (
    "in today's newsletter",
    "today's issue",
    "in this issue",
    "in today's email",
    "here's what happened",
    "knowledge base",
    "in the news",
    "top stories",
    "today we cover",
    "today's topics",
    "today's roundup",
)


def _load_project_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _allowed_forwarders() -> list[str]:
    return [
        item.strip().lower()
        for item in os.getenv("ALLOWED_FORWARDER_EMAILS", "carlos.santos@plutoanalytics.com").split(",")
        if item.strip()
    ]


def _connect_gmail_from_env():
    gmail_address = os.getenv("GMAIL_ADDRESS", "").strip()
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()
    if not gmail_address or not gmail_app_password:
        raise RuntimeError("GMAIL_ADDRESS/GMAIL_APP_PASSWORD missing")
    return connect_gmail(gmail_address, gmail_app_password)


def _uid_chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


def _search_allowed_uids(mail) -> list[str]:
    mail.select("INBOX")
    uid_values: list[bytes] = []
    for sender in _allowed_forwarders():
        status, data = mail.uid("SEARCH", None, "HEADER", "FROM", sender)
        if status != "OK":
            continue
        uid_values.extend((data[0] or b"").split())
    return sorted({uid.decode() for uid in uid_values if uid}, key=int)


def _cache_file(cache_dir: Path, uid: str) -> Path:
    return cache_dir / f"uid_{uid}.json"


def _slim_text(value: str, limit: int = 220) -> str:
    compact = " ".join(str(value or "").split())
    return compact[:limit]


def _looks_like_digest(*, family: str, cleaned_text: str, anchor_count: int, subject: str) -> bool:
    cleaned_low = cleaned_text.lower()
    if any(pattern in cleaned_low for pattern in DIGEST_PATTERNS):
        return True
    if family in {"tldr", "beehiiv", "neuron", "curated"} and anchor_count >= 3:
        return True
    if family == "kit" and anchor_count >= 4 and (
        "issue" in cleaned_low or "newsletter" in cleaned_low or "today" in cleaned_low
    ):
        return True
    subject_low = str(subject or "").lower()
    if any(token in subject_low for token in ("roundup", "digest", "newsletter", "daily dose")) and anchor_count >= 3:
        return True
    return False


# BODYSTRUCTURE is an IMAP s-expression. We only need enough parsing to locate
# the best text/html or text/plain part and avoid downloading inline images.
def _tokenize_bodystructure(value: str) -> list[str]:
    tokens: list[str] = []
    idx = 0
    while idx < len(value):
        char = value[idx]
        if char.isspace():
            idx += 1
            continue
        if char in "()":
            tokens.append(char)
            idx += 1
            continue
        if char == '"':
            idx += 1
            buf: list[str] = []
            while idx < len(value):
                char = value[idx]
                if char == "\\" and idx + 1 < len(value):
                    buf.append(value[idx + 1])
                    idx += 2
                    continue
                if char == '"':
                    idx += 1
                    break
                buf.append(char)
                idx += 1
            tokens.append("".join(buf))
            continue
        end = idx
        while end < len(value) and not value[end].isspace() and value[end] not in "()":
            end += 1
        tokens.append(value[idx:end])
        idx = end
    return tokens


def _parse_bodystructure_tokens(tokens: list[str]) -> Any:
    def _parse_item(position: int) -> tuple[Any, int]:
        token = tokens[position]
        if token == "(":
            out: list[Any] = []
            position += 1
            while tokens[position] != ")":
                item, position = _parse_item(position)
                out.append(item)
            return out, position + 1
        if token.upper() == "NIL":
            return None, position + 1
        return token, position + 1

    parsed, _ = _parse_item(0)
    return parsed


def _bodystructure_is_multipart(node: Any) -> bool:
    return isinstance(node, list) and bool(node) and isinstance(node[0], list)


def _find_text_parts(node: Any, prefix: str = "") -> list[dict[str, str]]:
    if not isinstance(node, list):
        return []

    if _bodystructure_is_multipart(node):
        out: list[dict[str, str]] = []
        for idx, child in enumerate(node):
            if not isinstance(child, list):
                break
            part_number = f"{prefix}.{idx + 1}" if prefix else str(idx + 1)
            out.extend(_find_text_parts(child, part_number))
        return out

    body_type = str(node[0] or "").upper() if len(node) > 0 else ""
    body_subtype = str(node[1] or "").upper() if len(node) > 1 else ""
    params = node[2] if len(node) > 2 else None
    encoding = str(node[5] or "") if len(node) > 5 else ""
    charset = "utf-8"
    if isinstance(params, list):
        for idx in range(0, len(params) - 1, 2):
            if str(params[idx]).upper() == "CHARSET":
                charset = str(params[idx + 1] or "utf-8")
                break
    if body_type == "TEXT" and body_subtype in {"HTML", "PLAIN"}:
        return [{"part": prefix or "1", "type": body_subtype, "encoding": encoding, "charset": charset}]
    return []


def _parse_bodystructure_fetch_header(fetch_header: str) -> tuple[str | None, dict[str, str] | None]:
    uid_match = re.search(r"UID\s+(\d+)", fetch_header)
    uid = uid_match.group(1) if uid_match else None
    if "BODYSTRUCTURE " not in fetch_header:
        return uid, None
    try:
        expr = fetch_header.split("BODYSTRUCTURE ", 1)[1].rsplit(")", 1)[0]
        node = _parse_bodystructure_tokens(_tokenize_bodystructure(expr))
        parts = _find_text_parts(node)
    except Exception:
        return uid, None
    if not parts:
        return uid, None
    html_part = next((part for part in parts if part["type"] == "HTML"), None)
    return uid, html_part or parts[0]


def _decode_part_payload(payload: bytes, encoding: str, charset: str) -> str:
    raw = bytes(payload)
    encoding_value = str(encoding or "").lower()
    if encoding_value == "base64":
        compact = re.sub(rb"\s+", b"", raw)
        compact = compact[: len(compact) - (len(compact) % 4)]
        decoded = base64.b64decode(compact, validate=False)
    elif encoding_value in {"quoted-printable", "quotedprintable"}:
        decoded = quopri.decodestring(raw)
    else:
        decoded = raw
    return decoded.decode(charset or "utf-8", errors="replace")


def _parse_header_fields(payload: bytes) -> dict[str, str]:
    text = payload.decode("utf-8", errors="replace")
    headers: dict[str, str] = {}
    current: str | None = None
    for line in text.splitlines():
        if not line.strip():
            continue
        if line[:1].isspace() and current:
            headers[current] = headers.get(current, "") + " " + line.strip()
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        current = key.strip().lower()
        headers[current] = value.strip()

    sender_name, sender_email = _extract_sender(headers.get("from", ""))
    received_at_raw = headers.get("date", "")
    return {
        "gmail_message_id": (headers.get("message-id", "") or "").strip("<>"),
        "sender_name": sender_name,
        "sender_email": sender_email,
        "subject": _decode_header(headers.get("subject", "(no subject)")),
        "received_at": _normalize_received_at(received_at_raw),
        "original_sender_name": sender_name,
        "original_sender_email": sender_email,
        "original_sent_at": _normalize_datetime(received_at_raw),
    }


def _imap_fetch(mail, uid_set: str, query: str, *, cooldown_seconds: int):
    while True:
        try:
            status, data = mail.uid("FETCH", uid_set, query)
            if status != "OK":
                raise RuntimeError(f"uid fetch failed status={status} query={query}")
            return mail, data
        except Exception as exc:
            if _is_imap_overquota_error(exc):
                LOG.warning("imap overquota on query=%s; sleeping %ss", query, cooldown_seconds)
                time.sleep(max(1, cooldown_seconds))
            elif _is_imap_connection_error(exc):
                LOG.warning("imap reconnect on query=%s err=%s", query, exc)
                time.sleep(5)
            else:
                raise
            try:
                mail.logout()
            except Exception:
                pass
            mail = _connect_gmail_from_env()
            mail.select("INBOX")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_cached_items(cache_dir: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in sorted(cache_dir.glob("uid_*.json"), key=lambda entry: int(entry.stem.split("_", 1)[1])):
        items.append(json.loads(path.read_text(encoding="utf-8")))
    return items


def cache_imap(args: argparse.Namespace) -> int:
    ensure_runtime_dirs()
    cache_dir = Path(args.cache_dir).expanduser().resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    mail = _connect_gmail_from_env()
    all_uids = _search_allowed_uids(mail)
    selected_uids = all_uids[args.start_index : args.start_index + args.limit]
    if not selected_uids:
        print(json.dumps({"status": "empty", "selected": 0}, ensure_ascii=False))
        return 0

    existing = {path.stem.split("_", 1)[1] for path in cache_dir.glob("uid_*.json")}
    pending_uids = selected_uids if args.force else [uid for uid in selected_uids if uid not in existing]

    body_parts: dict[str, dict[str, str]] = {}
    meta_by_uid: dict[str, dict[str, Any]] = {}
    failed_uids: dict[str, str] = {}

    for chunk in _uid_chunks(pending_uids, args.batch_size):
        uid_set = ",".join(chunk)
        try:
            mail, data = _imap_fetch(
                mail,
                uid_set,
                "(BODY.PEEK[HEADER.FIELDS (DATE FROM SUBJECT MESSAGE-ID)] BODYSTRUCTURE)",
                cooldown_seconds=args.cooldown_seconds,
            )
        except Exception as exc:
            for uid in chunk:
                failed_uids[uid] = f"prep {type(exc).__name__}: {exc}"
            continue

        for item in data:
            if not isinstance(item, tuple):
                continue
            fetch_header = item[0].decode(errors="replace") if isinstance(item[0], (bytes, bytearray)) else str(item[0])
            if "BODYSTRUCTURE" not in fetch_header:
                continue
            uid, part = _parse_bodystructure_fetch_header(fetch_header)
            if not uid or not part:
                failed_uids[uid or "unknown"] = "missing_text_part"
                continue
            body_parts[uid] = part
            meta_by_uid[uid] = _parse_header_fields(item[1] if isinstance(item[1], (bytes, bytearray)) else b"")

    part_numbers = sorted({part["part"] for part in body_parts.values()})
    for part_number in part_numbers:
        group_uids = [uid for uid, part in body_parts.items() if part["part"] == part_number]
        for chunk in _uid_chunks(group_uids, args.batch_size):
            uid_set = ",".join(chunk)
            try:
                mail, data = _imap_fetch(
                    mail,
                    uid_set,
                    f"(BODY.PEEK[{part_number}])",
                    cooldown_seconds=args.cooldown_seconds,
                )
            except Exception as exc:
                for uid in chunk:
                    failed_uids[uid] = f"body {type(exc).__name__}: {exc}"
                continue

            for item in data:
                if not isinstance(item, tuple):
                    continue
                fetch_header = item[0].decode(errors="replace") if isinstance(item[0], (bytes, bytearray)) else str(item[0])
                uid_match = re.search(r"UID\s+(\d+)", fetch_header)
                if not uid_match:
                    continue
                uid = uid_match.group(1)
                if uid not in body_parts:
                    continue
                if "BODY[" not in fetch_header:
                    continue
                part = body_parts[uid]
                try:
                    part_text = _decode_part_payload(item[1], part["encoding"], part["charset"])
                except Exception as exc:
                    failed_uids[uid] = f"decode {type(exc).__name__}: {exc}"
                    continue

                raw_html = part_text if part["type"] == "HTML" else f"<html><body><pre>{part_text}</pre></body></html>"
                payload = dict(meta_by_uid.get(uid, {}))
                payload.update(
                    {
                        "gmail_uid": uid,
                        "raw_html": raw_html,
                        "fetched_body_part": dict(part),
                    }
                )
                _write_json(_cache_file(cache_dir, uid), payload)

    try:
        mail.logout()
    except Exception:
        pass

    cached_uids = [uid for uid in selected_uids if _cache_file(cache_dir, uid).exists()]
    manifest = {
        "mode": "cache-imap",
        "selected": len(selected_uids),
        "selected_uids": selected_uids,
        "start_index": args.start_index,
        "limit": args.limit,
        "batch_size": args.batch_size,
        "cooldown_seconds": args.cooldown_seconds,
        "cached": len(cached_uids),
        "cached_uids": cached_uids,
        "skipped_existing": max(0, len(selected_uids) - len(pending_uids)),
        "failed": len(failed_uids),
        "failed_uids": failed_uids,
        "cache_dir": str(cache_dir),
    }
    _write_json(cache_dir / MANIFEST_NAME, manifest)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0 if not failed_uids else 1


def analyze_cache(args: argparse.Namespace) -> int:
    cache_dir = Path(args.cache_dir).expanduser().resolve()
    if not cache_dir.exists():
        raise SystemExit(f"cache dir not found: {cache_dir}")

    items = _load_cached_items(cache_dir)
    if args.limit > 0:
        items = items[: args.limit]
    if not items:
        raise SystemExit("no cached items found")

    metrics = Counter()
    family_counts = Counter()
    article_hist = Counter()
    segment_hist = Counter()
    skip_reason_counts = Counter()
    fallback_counts = Counter()
    parser_note_counts = Counter()
    family_under = Counter()
    family_good = Counter()
    examples: dict[str, list[dict[str, Any]]] = {
        "undersegmented_digest": [],
        "missing_source_link": [],
        "good_multi_article": [],
        "skipped_samples": [],
    }

    for item in items:
        raw_html = str(item.get("raw_html") or "")
        if not raw_html:
            continue
        metrics["processed"] += 1
        cleaned = clean_html(raw_html)
        segments = _extract_email_segments_from_html(raw_html)
        anchors = _extract_email_anchor_candidates(raw_html)
        revh = parse_email_articles(raw_html, email_meta=item)
        subject_hint = _extract_forwarded_subject_hint(raw_html)
        subject = str(item.get("subject") or "")
        family = str(revh.family or "none")
        article_count = len(revh.articles)
        segment_count = len(segments)
        anchor_count = len(anchors)

        family_counts[family] += 1
        article_hist[str(article_count)] += 1
        segment_hist[str(segment_count)] += 1

        metrics["raw_html_chars_total"] += len(raw_html)
        metrics["cleaned_chars_total"] += len(cleaned)
        metrics["anchor_candidates_total"] += anchor_count
        metrics["segments_total"] += segment_count
        metrics["revh_articles_total"] += article_count
        if segment_count > 0:
            metrics["with_segments"] += 1
        if anchor_count > 0:
            metrics["with_anchor_candidates"] += 1
        if subject_hint:
            metrics["with_subject_hint"] += 1
        if revh.skipped:
            metrics["revh_skipped"] += 1
            for note in revh.skip_notes:
                skip_reason_counts[str(note)] += 1
            if len(examples["skipped_samples"]) < 10:
                examples["skipped_samples"].append(
                    {
                        "uid": item.get("gmail_uid", ""),
                        "subject": _slim_text(subject, 180),
                        "family": family,
                        "skip_notes": list(revh.skip_notes),
                    }
                )

        for article in revh.articles:
            if article.title:
                metrics["articles_with_title"] += 1
            if article.source_link:
                metrics["articles_with_source_link"] += 1
            if article.text:
                metrics["articles_with_text"] += 1
            for note in list(getattr(article, "notes", []) or []):
                parser_note_counts[str(note)] += 1
                if str(note).endswith("_fallback"):
                    fallback_counts[str(note)] += 1

        digest_like = _looks_like_digest(
            family=family,
            cleaned_text=cleaned,
            anchor_count=anchor_count,
            subject=subject,
        )
        if digest_like:
            metrics["digest_like"] += 1
            if article_count <= 1:
                metrics["digest_like_undersegmented"] += 1
                family_under[family] += 1
                if len(examples["undersegmented_digest"]) < 12:
                    examples["undersegmented_digest"].append(
                        {
                            "uid": item.get("gmail_uid", ""),
                            "subject": _slim_text(subject, 180),
                            "family": family,
                            "anchor_count": anchor_count,
                            "segment_count": segment_count,
                            "article_count": article_count,
                            "subject_hint": _slim_text(subject_hint, 180),
                            "top_anchor_texts": [_slim_text(row.get("text", ""), 100) for row in anchors[:4]],
                            "parser_notes": [list(getattr(article, "notes", []) or []) for article in revh.articles[:3]],
                        }
                    )
            else:
                metrics["digest_like_multi_article"] += 1
                family_good[family] += 1
                if len(examples["good_multi_article"]) < 10:
                    examples["good_multi_article"].append(
                        {
                            "uid": item.get("gmail_uid", ""),
                            "subject": _slim_text(subject, 180),
                            "family": family,
                            "anchor_count": anchor_count,
                            "segment_count": segment_count,
                            "article_count": article_count,
                            "titles": [_slim_text(article.title, 120) for article in revh.articles[:5]],
                        }
                    )

        if article_count > 0 and anchor_count >= 2 and all(not article.source_link for article in revh.articles):
            metrics["missing_source_link_despite_anchors"] += 1
            if len(examples["missing_source_link"]) < 12:
                examples["missing_source_link"].append(
                    {
                        "uid": item.get("gmail_uid", ""),
                        "subject": _slim_text(subject, 180),
                        "family": family,
                        "anchor_count": anchor_count,
                        "article_titles": [_slim_text(article.title, 120) for article in revh.articles[:4]],
                        "top_anchor_texts": [_slim_text(row.get("text", ""), 100) for row in anchors[:4]],
                        "parser_notes": [list(getattr(article, "notes", []) or []) for article in revh.articles[:3]],
                    }
                )

    processed = max(1, metrics["processed"])
    article_total = max(1, metrics["revh_articles_total"])
    summary = {
        "processed": metrics["processed"],
        "avg_raw_html_chars": round(metrics["raw_html_chars_total"] / processed, 1),
        "avg_cleaned_chars": round(metrics["cleaned_chars_total"] / processed, 1),
        "avg_anchor_candidates": round(metrics["anchor_candidates_total"] / processed, 2),
        "avg_segments": round(metrics["segments_total"] / processed, 2),
        "avg_revh_articles": round(metrics["revh_articles_total"] / processed, 2),
        "with_segments_pct": round(metrics["with_segments"] * 100 / processed, 1),
        "with_anchor_candidates_pct": round(metrics["with_anchor_candidates"] * 100 / processed, 1),
        "with_subject_hint_pct": round(metrics["with_subject_hint"] * 100 / processed, 1),
        "revh_skipped_pct": round(metrics["revh_skipped"] * 100 / processed, 1),
        "digest_like_pct": round(metrics["digest_like"] * 100 / processed, 1),
        "digest_like_undersegmented_pct": round(
            metrics["digest_like_undersegmented"] * 100 / max(1, metrics["digest_like"]), 1
        ),
        "digest_like_multi_article_pct": round(
            metrics["digest_like_multi_article"] * 100 / max(1, metrics["digest_like"]), 1
        ),
        "articles_with_title_pct": round(metrics["articles_with_title"] * 100 / article_total, 1),
        "articles_with_source_link_pct": round(metrics["articles_with_source_link"] * 100 / article_total, 1),
        "articles_with_text_pct": round(metrics["articles_with_text"] * 100 / article_total, 1),
        "missing_source_link_despite_anchors_pct": round(
            metrics["missing_source_link_despite_anchors"] * 100 / processed, 1
        ),
        "family_counts": dict(family_counts.most_common()),
        "article_hist": dict(sorted(article_hist.items(), key=lambda row: int(row[0]))),
        "segment_hist": dict(sorted(segment_hist.items(), key=lambda row: int(row[0]))),
        "skip_reason": dict(skip_reason_counts.most_common()),
        "fallback": dict(fallback_counts.most_common()),
        "family_under": dict(family_under.most_common()),
        "family_good": dict(family_good.most_common()),
        "parser_note_counts_top20": dict(parser_note_counts.most_common(20)),
    }

    report = {
        "cache_dir": str(cache_dir),
        "manifest": json.loads((cache_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
        if (cache_dir / MANIFEST_NAME).exists()
        else None,
        "summary": summary,
        "examples": examples,
    }

    report_file = Path(args.report_file).expanduser().resolve()
    _write_json(report_file, report)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline parser replay for cached mailbox newsletters")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    subparsers = parser.add_subparsers(dest="command", required=True)

    cache_parser = subparsers.add_parser("cache-imap", help="Cache email bodies from IMAP into local JSON files")
    cache_parser.add_argument("--cache-dir", default=str(CACHE_ROOT_DEFAULT))
    cache_parser.add_argument("--start-index", type=int, default=0)
    cache_parser.add_argument("--limit", type=int, default=1000)
    cache_parser.add_argument("--batch-size", type=int, default=50)
    cache_parser.add_argument("--cooldown-seconds", type=int, default=180)
    cache_parser.add_argument("--force", action="store_true")
    cache_parser.set_defaults(func=cache_imap)

    analyze_parser = subparsers.add_parser("analyze-cache", help="Run parser analysis over cached JSON items")
    analyze_parser.add_argument("--cache-dir", default=str(CACHE_ROOT_DEFAULT))
    analyze_parser.add_argument("--limit", type=int, default=0, help="Limit analysis to first N cached items")
    analyze_parser.add_argument("--report-file", default=str(REPORT_ROOT_DEFAULT / "parser_replay_report.json"))
    analyze_parser.set_defaults(func=analyze_cache)

    return parser


def main() -> int:
    _load_project_env()
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
