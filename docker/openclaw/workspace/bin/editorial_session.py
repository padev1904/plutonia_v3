#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def _base_url() -> str:
    return os.getenv("PLUTONIA_OPS_API_BASE", "http://ainews-gmail-monitor:8001").rstrip("/")


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    token = os.getenv("OPS_API_TOKEN", "").strip()
    if token:
        headers["X-Ops-Token"] = token
    return headers


def _fetch_session() -> dict:
    req = urllib.request.Request(f"{_base_url()}/api/editorial/session", headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _print_text(payload: dict) -> None:
    print(str(payload.get("prompt", "")).strip() or "No active editorial session.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch current Plutonia editorial session")
    parser.add_argument("--text", action="store_true", help="Print only the human prompt")
    args = parser.parse_args()

    try:
        payload = _fetch_session()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(body or str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.text:
        _print_text(payload)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
