#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request


APPROVAL_PATTERNS = (
    r"\bapprove\b",
    r"\bapproved\b",
    r"\baprova\b",
    r"\baprovar\b",
    r"\baprovado\b",
    r"\baprovada\b",
    r"\bpublica\b",
    r"\bpublicar\b",
    r"\bpublicado\b",
    r"\bpublicada\b",
    r"\bpublish\b",
    r"\bgo live\b",
    r"\bsegue para publicar\b",
)

REJECTION_PATTERNS = (
    r"\breject\b",
    r"\brejected\b",
    r"\brejeita\b",
    r"\brejeitar\b",
    r"\breprov",
    r"\bdiscard\b",
    r"\bdescarta\b",
)

AMBIGUOUS_ADVANCE_PATTERNS = (
    r"\bnext\b",
    r"\bpróxim",
    r"\bproxim",
    r"\bseguinte\b",
    r"\bcontinue\b",
    r"\bcontinua\b",
    r"\bresume\b",
    r"\bretoma\b",
    r"\badvance\b",
    r"\bavança\b",
    r"\benvia a próxima\b",
    r"\bsend the next\b",
    r"\bnext newsletter\b",
    r"\bnext article\b",
)


def _base_url() -> str:
    return os.getenv("PLUTONIA_OPS_API_BASE", "http://ainews-gmail-monitor:8001").rstrip("/")


def _headers() -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    token = os.getenv("OPS_API_TOKEN", "").strip()
    if token:
        headers["X-Ops-Token"] = token
    return headers


def _post_action(payload: dict) -> dict:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{_base_url()}/api/editorial/action",
        headers=_headers(),
        data=body,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _payload_from_args(args: argparse.Namespace) -> dict:
    payload: dict[str, object] = {"action": args.action}
    if args.article_id is not None:
        payload["article_id"] = args.article_id
    if args.content_profile:
        payload["content_profile"] = args.content_profile
    if args.source_mode:
        payload["source_mode"] = args.source_mode
    if args.manual_url:
        payload["manual_url"] = args.manual_url
    if args.url:
        payload["url"] = args.url
    if args.instructions:
        payload["instructions"] = args.instructions
    if args.text:
        payload["text"] = args.text
    if args.source_text:
        payload["source_text"] = args.source_text
    if args.max_source_chars is not None:
        payload["max_source_chars"] = args.max_source_chars
    return payload


def _matches_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _validate_explicit_intent(args: argparse.Namespace) -> str | None:
    user_request = (args.user_request or "").strip()
    if args.action not in {"approve_preview", "reject_article"}:
        return None
    if not user_request:
        return (
            f"Refusing {args.action}: --user-request is mandatory for irreversible editorial actions."
        )
    if _matches_any(user_request, AMBIGUOUS_ADVANCE_PATTERNS) and not _matches_any(
        user_request, APPROVAL_PATTERNS if args.action == "approve_preview" else REJECTION_PATTERNS
    ):
        return (
            f"Refusing {args.action}: the user request is operational/ambiguous, not explicit editorial intent."
        )
    if args.action == "approve_preview" and not _matches_any(user_request, APPROVAL_PATTERNS):
        return (
            "Refusing approve_preview: the latest user message does not explicitly approve publication."
        )
    if args.action == "reject_article" and not _matches_any(user_request, REJECTION_PATTERNS):
        return "Refusing reject_article: the latest user message does not explicitly reject the article."
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Plutonia editorial action")
    parser.add_argument("action")
    parser.add_argument("--article-id", type=int)
    parser.add_argument("--content-profile", choices=["news", "resource"])
    parser.add_argument("--source-mode", choices=["process", "manual"])
    parser.add_argument("--manual-url", default="")
    parser.add_argument("--url", default="")
    parser.add_argument("--instructions", default="")
    parser.add_argument("--text", default="")
    parser.add_argument("--source-text", default="")
    parser.add_argument(
        "--user-request",
        default="",
        help="Latest raw user message. Required for approve/reject actions.",
    )
    parser.add_argument("--max-source-chars", type=int)
    parser.add_argument("--text-only", action="store_true", help="Print the main action/session message only")
    args = parser.parse_args()

    validation_error = _validate_explicit_intent(args)
    if validation_error:
        print(validation_error, file=sys.stderr)
        return 2

    try:
        payload = _post_action(_payload_from_args(args))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(body or str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.text_only:
        message = str(payload.get("message", "")).strip()
        if not message and isinstance(payload.get("next_session"), dict):
            message = str(payload["next_session"].get("prompt", "")).strip()
        if not message:
            message = str(payload.get("prompt", "")).strip()
        print(message)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
