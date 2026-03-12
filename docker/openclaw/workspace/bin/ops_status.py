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
    headers = {"Accept": "application/json"}
    token = os.getenv("OPS_API_TOKEN", "").strip()
    if token:
        headers["X-Ops-Token"] = token
    return headers


def _fetch_status() -> dict[str, Any]:
    req = urllib.request.Request(f"{_base_url()}/api/ops/status", headers=_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _print_text(payload: dict[str, Any]) -> None:
    pipeline = payload.get("pipeline") if isinstance(payload, dict) else {}
    if not isinstance(pipeline, dict):
        pipeline = {}
    counts = pipeline.get("counts") if isinstance(pipeline.get("counts"), dict) else {}
    active = pipeline.get("active_editorial_session") if isinstance(pipeline.get("active_editorial_session"), dict) else {}
    blockers = payload.get("watchdog") if isinstance(payload, dict) else {}

    review = int(counts.get("review", 0) or 0)
    pending = int(counts.get("pending", 0) or 0)
    processing = int(counts.get("processing", 0) or 0)
    completed = int(counts.get("completed", 0) or 0)
    errors = int(counts.get("error", 0) or 0)

    lines = [
        "Estado atual da pipeline:",
        f"- review={review} pending={pending} processing={processing} completed={completed} error={errors}",
    ]
    if active:
        title = str(active.get("title", "")).strip() or "(sem título)"
        lines.append(f"- sessão editorial ativa: {title}")
    watchdog_blockers = blockers.get("blockers") if isinstance(blockers, dict) else []
    if isinstance(watchdog_blockers, list) and watchdog_blockers:
        lines.append(f"- watchdog blockers={len(watchdog_blockers)}")
    else:
        lines.append("- watchdog blockers=0")
    print("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch current Plutonia ops status")
    parser.add_argument("--text-only", action="store_true", help="Print a concise human summary")
    args = parser.parse_args()

    try:
        payload = _fetch_status()
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
