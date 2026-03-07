#!/usr/bin/env python3
"""Submit one resource URL for classification and publication."""

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


def _normalize_url(value: str) -> str:
    url = str(value or "").strip()
    if url.startswith(("http://", "https://")):
        return url
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit one resource to Review API")
    parser.add_argument("--resource-url", required=True, help="Resource URL")
    parser.add_argument("--title", default="", help="Optional title override")
    parser.add_argument("--description", default="", help="Optional description override")
    parser.add_argument("--image-url", default="", help="Optional image URL override")
    parser.add_argument("--section", default="", help="Optional section override")
    parser.add_argument("--category", default="", help="Optional category override")
    parser.add_argument("--subcategory", default="", help="Optional subcategory override")
    parser.add_argument("--featured", action="store_true", help="Mark as featured")
    parser.add_argument("--inactive", action="store_true", help="Mark as inactive")
    parser.add_argument("--auto-classify", default="true", choices=["true", "false"], help="Enable LLM classification")
    parser.add_argument("--review-required", default="true", choices=["true", "false"], help="Queue Telegram approval before public release")
    parser.add_argument(
        "--api-url",
        default="http://ainews-gmail-monitor:8001/api/review/resource-submit",
        help="Review API endpoint",
    )
    parser.add_argument("--secret", default=os.getenv("REVIEW_SIGNATURE_SECRET", ""), help="HMAC secret")
    parser.add_argument("--ts", type=int, default=int(time.time()), help="Unix timestamp")
    parser.add_argument("--nonce", default="", help="Nonce (auto when omitted)")
    args = parser.parse_args()

    resource_url = _normalize_url(args.resource_url)
    if not resource_url:
        raise SystemExit("--resource-url must start with http:// or https://")

    secret = args.secret.strip() or _load_secret_from_dotenv()
    if not secret:
        raise SystemExit("missing signature secret (pass --secret or set REVIEW_SIGNATURE_SECRET)")

    ts = int(args.ts)
    nonce = args.nonce.strip() or secrets.token_urlsafe(12)
    resource_hash = hashlib.sha256(resource_url.encode("utf-8")).hexdigest()
    canonical = f"resource|submit|{resource_hash}|{ts}|{nonce}"
    sig = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    payload: dict[str, object] = {
        "resource_url": resource_url,
        "auto_classify": args.auto_classify == "true",
        "review_required": args.review_required == "true",
        "sig_ts": ts,
        "sig_nonce": nonce,
        "sig": sig,
    }
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
    if args.inactive:
        payload["is_active"] = False

    resp = requests.post(
        args.api_url,
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=45,
    )

    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    print(json.dumps(body, ensure_ascii=False, indent=2))
    return 0 if 200 <= resp.status_code < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())
