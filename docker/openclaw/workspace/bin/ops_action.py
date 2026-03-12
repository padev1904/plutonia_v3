#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any


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


def _post_action(payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{_base_url()}/api/ops/action",
        headers=_headers(),
        data=body,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {"action": args.action}
    if args.reason:
        payload["reason"] = args.reason
    if args.requested_by:
        payload["requested_by"] = args.requested_by
    if args.mode:
        payload["mode"] = args.mode
    if args.newsletter_id is not None:
        payload["newsletter_id"] = args.newsletter_id
    if args.review_file:
        payload["review_file"] = args.review_file
    if args.payload:
        payload.update(json.loads(args.payload))
    return payload


def _print_text(payload: dict[str, Any]) -> None:
    status = str(payload.get("status", "")).strip()
    action = str(payload.get("action", "")).strip()
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    lines = [f"Acao: {action or '(sem ação)'}", f"Estado: {status or 'ok'}"]
    if result:
        reason = str(result.get("reason", "")).strip()
        if reason:
            lines.append(f"Motivo: {reason}")
    print("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Plutonia ops action")
    parser.add_argument("action")
    parser.add_argument("--reason", default="")
    parser.add_argument("--requested-by", default="")
    parser.add_argument("--mode", default="")
    parser.add_argument("--newsletter-id", type=int)
    parser.add_argument("--review-file", default="")
    parser.add_argument("--payload", default="", help="Extra JSON payload to merge into the request body")
    parser.add_argument("--text-only", action="store_true", help="Print a concise human summary")
    args = parser.parse_args()

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
        _print_text(payload)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
