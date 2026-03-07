#!/usr/bin/env python3
"""Generate HMAC signature fields for Review API decisions."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path

import requests


BOOL_TRUE = {"1", "true", "yes", "on"}


def _normalize_article_decision(value: str) -> str:
    aliases = {
        "approve": "approve",
        "approved": "approve",
        "accept": "approve",
        "accepted": "approve",
        "yes": "approve",
        "sim": "approve",
        "ok": "approve",
        "aprovar": "approve",
        "aprovado": "approve",
        "aceitar": "approve",
        "aceite": "approve",
        "reject": "reject",
        "rejected": "reject",
        "decline": "reject",
        "declined": "reject",
        "no": "reject",
        "nao": "reject",
        "não": "reject",
        "rejeitar": "reject",
        "rejeitado": "reject",
        "recusar": "reject",
    }
    return aliases.get(value.strip().lower(), value.strip().lower())


def _normalize_content_decision(value: str) -> str:
    decision = value.strip().lower()
    aliases = {"reject": "request_changes", "changes": "request_changes"}
    return aliases.get(decision, decision)


def _normalize_resource_decision(value: str) -> str:
    action = value.strip().lower()
    aliases = {
        "submit": "submit",
        "publish": "submit",
        "create": "submit",
        "add": "submit",
        "resource": "submit",
        "approve": "approve",
        "approved": "approve",
        "accept": "approve",
        "accepted": "approve",
        "yes": "approve",
        "sim": "approve",
        "ok": "approve",
        "aprovar": "approve",
        "aprovado": "approve",
        "aceitar": "approve",
        "aceite": "approve",
        "reject": "reject",
        "rejected": "reject",
        "decline": "reject",
        "declined": "reject",
        "no": "reject",
        "nao": "reject",
        "não": "reject",
        "rejeitar": "reject",
        "rejeitado": "reject",
        "recusar": "reject",
    }
    return aliases.get(action, action)


def _sign(secret: str, prefix: str, ts: int, nonce: str) -> str:
    canonical = f"{prefix}|{int(ts)}|{nonce}"
    return hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def _post_json(api_base: str, endpoint: str, payload: dict) -> tuple[int, dict]:
    resp = requests.post(
        f"{api_base}{endpoint}",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    return resp.status_code, body


def _is_no_pending_articles_response(http_status: int, result_body: dict) -> bool:
    if not (200 <= int(http_status) < 300):
        return False
    if not isinstance(result_body, dict):
        return False

    candidates = [result_body]
    nested = result_body.get("result")
    if isinstance(nested, dict):
        candidates.append(nested)

    for node in candidates:
        status = str(node.get("status", "")).strip().lower()
        reason = str(node.get("reason", "")).strip().lower()
        message = str(node.get("message", "")).strip().lower()
        if status == "no_pending_context":
            return True
        if reason == "no_pending_articles":
            return True
        if "no pending article" in message:
            return True
    return False


def _load_secret_from_dotenv() -> str:
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if not env_file.exists():
        return ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        row = line.strip()
        if not row or row.startswith("#") or "=" not in row:
            continue
        key, val = row.split("=", 1)
        if key.strip() != "REVIEW_SIGNATURE_SECRET":
            continue
        return val.strip().strip("'\"")
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate signature payload for Review API")
    parser.add_argument("--kind", choices=["article", "content", "resource"], required=True, help="Decision endpoint kind")
    parser.add_argument("--decision", required=True, help="Decision keyword")
    parser.add_argument("--newsletter-id", type=int, help="Newsletter id (article decisions)")
    parser.add_argument("--article-index", type=int, help="Article index (article decisions)")
    parser.add_argument("--article-id", type=int, help="Portal article id (content decisions)")
    parser.add_argument("--resource-url", default="", help="Resource URL (resource submit)")
    parser.add_argument("--resource-id", type=int, help="Resource id (resource approve/reject)")
    parser.add_argument("--title", default="", help="Optional title (resource submit)")
    parser.add_argument("--description", default="", help="Optional description (resource submit)")
    parser.add_argument("--image-url", default="", help="Optional image URL (resource submit)")
    parser.add_argument("--section", default="", help="Optional section override (resource submit)")
    parser.add_argument("--category", default="", help="Optional category override (resource submit)")
    parser.add_argument("--subcategory", default="", help="Optional subcategory override (resource submit)")
    parser.add_argument("--featured", action="store_true", help="Flag resource as featured (resource submit)")
    parser.add_argument("--source-url", default="", help="Optional source URL (article decisions)")
    parser.add_argument(
        "--apply-api",
        action="store_true",
        help="Apply the decision immediately via Review API (atomic sign+apply)",
    )
    parser.add_argument(
        "--api-base",
        default=os.getenv("REVIEW_API_BASE", "http://127.0.0.1:8001"),
        help="Review API base URL used with --apply-api or SIGN_REVIEW_AUTO_APPLY=true",
    )
    parser.add_argument("--secret", default=os.getenv("REVIEW_SIGNATURE_SECRET", ""), help="HMAC secret")
    parser.add_argument("--ts", type=int, default=int(time.time()), help="Unix timestamp (seconds)")
    parser.add_argument("--nonce", default="", help="Nonce (auto-generated when omitted)")
    parser.add_argument(
        "--telegram-text",
        action="store_true",
        help="Print a ready-to-send Telegram decision text instead of JSON payload",
    )
    args = parser.parse_args()

    secret = args.secret.strip() or _load_secret_from_dotenv()
    if not secret:
        raise SystemExit("missing signature secret (pass --secret or set REVIEW_SIGNATURE_SECRET)")

    nonce = args.nonce.strip() or secrets.token_urlsafe(12)
    ts = int(args.ts)

    if args.kind == "article":
        decision = _normalize_article_decision(args.decision)
        if decision not in {"approve", "reject"}:
            raise SystemExit("article decision must normalize to approve/reject")
        nid = "" if args.newsletter_id is None else str(int(args.newsletter_id))
        aidx = "" if args.article_index is None else str(int(args.article_index))
        source_url = args.source_url.strip()
        source_url_hash = hashlib.sha256(source_url.encode("utf-8")).hexdigest() if source_url else ""
        prefix = f"article|{decision}|{nid}|{aidx}|{source_url_hash}"
        payload: dict[str, object] = {"decision": decision}
        if args.newsletter_id is not None:
            payload["newsletter_id"] = int(args.newsletter_id)
        if args.article_index is not None:
            payload["article_index"] = int(args.article_index)
        if source_url:
            payload["source_url"] = source_url
    elif args.kind == "content":
        decision = _normalize_content_decision(args.decision)
        valid = {"approve", "revise", "hold", "request_changes"}
        if decision not in valid:
            raise SystemExit(f"content decision must normalize to one of {sorted(valid)}")
        aid = "" if args.article_id is None else str(int(args.article_id))
        prefix = f"content|{decision}|{aid}"
        payload = {"decision": decision}
        if args.article_id is not None:
            payload["article_id"] = int(args.article_id)
    else:
        action = _normalize_resource_decision(args.decision)
        if action == "submit":
            resource_url = args.resource_url.strip()
            if not resource_url.startswith(("http://", "https://")):
                raise SystemExit("--resource-url is required for --kind resource decision=submit (http:// or https://)")
            resource_hash = hashlib.sha256(resource_url.encode("utf-8")).hexdigest()
            prefix = f"resource|submit|{resource_hash}"
            payload = {"resource_url": resource_url}
            if args.title.strip():
                payload["title"] = args.title.strip()
            if args.description.strip():
                payload["description"] = args.description.strip()
            if args.image_url.strip():
                payload["image_url"] = args.image_url.strip()
            if args.section.strip():
                payload["section"] = args.section.strip()
            if args.category.strip():
                payload["category"] = args.category.strip()
            if args.subcategory.strip():
                payload["subcategory"] = args.subcategory.strip()
            if args.featured:
                payload["is_featured"] = True
        elif action in {"approve", "reject"}:
            rid = "" if args.resource_id is None else str(int(args.resource_id))
            prefix = f"resource|{action}|{rid}"
            payload = {"decision": action}
            if args.resource_id is not None:
                payload["resource_id"] = int(args.resource_id)
        else:
            raise SystemExit("resource decision must normalize to submit|approve|reject")

    sig = _sign(secret, prefix, ts, nonce)
    payload.update({"sig_ts": ts, "sig_nonce": nonce, "sig": sig})

    auto_apply_env = os.getenv("SIGN_REVIEW_AUTO_APPLY", "").strip().lower() in BOOL_TRUE
    apply_now = bool(args.apply_api or auto_apply_env)

    if apply_now:
        api_base = args.api_base.strip().rstrip("/") or "http://127.0.0.1:8001"
        endpoint = ""
        apply_payload = dict(payload)

        if args.kind == "article":
            endpoint = "/api/review/article-decision"
        elif args.kind == "content":
            endpoint = "/api/review/content-decision"
        else:
            if action not in {"submit", "approve", "reject"}:
                raise SystemExit("auto-apply for --kind resource supports submit|approve|reject")
            endpoint = "/api/review/resource-submit" if action == "submit" else "/api/review/resource-decision"

        status, result = _post_json(api_base, endpoint, apply_payload)
        body: dict[str, object] = {
            "http_status": status,
            "applied_via": endpoint,
            "payload": apply_payload,
            "result": result,
        }

        fallback_attempted = False
        fallback_ok = False
        if (
            args.kind == "article"
            and args.newsletter_id is None
            and args.article_index is None
            and decision in {"approve", "reject"}
            and _is_no_pending_articles_response(status, body)
        ):
            fallback_attempted = True
            phase2_decision = "approve" if decision == "approve" else "request_changes"
            fallback_ts = int(time.time())
            fallback_nonce = secrets.token_urlsafe(12)
            fallback_prefix = f"content|{phase2_decision}|"
            fallback_payload = {
                "decision": phase2_decision,
                "sig_ts": fallback_ts,
                "sig_nonce": fallback_nonce,
                "sig": _sign(secret, fallback_prefix, fallback_ts, fallback_nonce),
            }
            fb_status, fb_result = _post_json(api_base, "/api/review/content-decision", fallback_payload)
            fallback_ok = 200 <= fb_status < 300
            body["phase2_fallback"] = {
                "attempted": True,
                "http_status": fb_status,
                "applied_via": "/api/review/content-decision",
                "payload": fallback_payload,
                "result": fb_result,
            }

        if not fallback_attempted:
            body["phase2_fallback"] = {"attempted": False}

        print(json.dumps(body, ensure_ascii=False))
        if fallback_attempted:
            return 0 if fallback_ok else 1
        return 0 if 200 <= status < 300 else 1

    if args.telegram_text:
        if args.kind == "article" and args.newsletter_id is not None and args.article_index is not None:
            prefix = f"Newsletter #{int(args.newsletter_id)} / Article #{int(args.article_index)} "
        elif args.kind == "resource":
            prefix = "resource "
        else:
            prefix = ""
        if args.kind == "resource":
            action_word = action
        else:
            action_word = decision
        print(f"{prefix}{action_word} sig_ts={ts} sig_nonce={nonce} sig={sig}")
    else:
        print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
