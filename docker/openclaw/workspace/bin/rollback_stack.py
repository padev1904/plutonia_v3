#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


OPS_RUNNER_BASE_URL = os.getenv("OPS_RUNNER_BASE_URL", "http://ops-runner:8011").rstrip("/")


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    token = os.getenv("OPS_RUNNER_TOKEN", "").strip()
    if token:
        headers["X-Ops-Runner-Token"] = token
    return headers


def _request(path: str, payload: dict) -> dict:
    req = urllib.request.Request(
        f"{OPS_RUNNER_BASE_URL}{path}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=_headers(),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=1800) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body or str(exc)) from exc
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def _collect_services(values: list[str]) -> list[str]:
    services: list[str] = []
    for raw in values:
        for item in raw.split(","):
            name = item.strip()
            if name and name not in services:
                services.append(name)
    if not services:
        raise RuntimeError("at least one --service is required")
    return services


def main() -> int:
    parser = argparse.ArgumentParser(description="Rollback selected services through the internal ops-runner")
    parser.add_argument("--ref", required=True, help="Git ref to roll back to")
    parser.add_argument("--service", action="append", required=True, help="Service name; repeat or use comma-separated values")
    parser.add_argument("--reason", default="openclaw rollback")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        payload = _request(
            "/rollback",
            {
                "ref": args.ref.strip(),
                "services": _collect_services(args.service),
                "build": not args.no_build,
                "reason": args.reason.strip(),
            },
        )
        if args.text_only:
            services = ", ".join(payload.get("services", []))
            after = payload.get("after", {}) if isinstance(payload, dict) else {}
            print(f"Rolled back {services} to {after.get('head', args.ref.strip())}")
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        if args.text_only:
            print(f"ERROR: {exc}", file=sys.stderr)
        else:
            print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
