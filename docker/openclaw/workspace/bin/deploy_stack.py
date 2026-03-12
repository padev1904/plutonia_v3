#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path


def _resolve_repo_dir() -> Path:
    configured = Path(os.getenv("OPENCLAW_BOOTSTRAP_REPO_DIR", "/workspace/repo")).resolve()
    if configured.is_dir():
        return configured
    return Path(__file__).resolve().parents[4]


REPO_DIR = _resolve_repo_dir()
OPS_RUNNER_BASE_URL = os.getenv("OPS_RUNNER_BASE_URL", "http://ops-runner:8011").rstrip("/")


def _ensure_safe_directory() -> None:
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", str(REPO_DIR)],
        text=True,
        capture_output=True,
        check=False,
    )


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    _ensure_safe_directory()
    result = subprocess.run(
        ["git", *args],
        cwd=str(REPO_DIR),
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "git command failed")
    return result


def _repo_status() -> dict:
    head = _git("rev-parse", "HEAD").stdout.strip()
    dirty = bool(_git("status", "--porcelain", check=False).stdout.strip())
    branch = _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    upstream_ref = ""
    upstream_head = ""
    head_pushed = False
    try:
        upstream_ref = _git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}").stdout.strip()
        upstream_head = _git("rev-parse", "@{u}").stdout.strip()
        head_pushed = bool(head and upstream_head and head == upstream_head)
    except Exception:
        pass
    return {
        "branch": branch,
        "head": head,
        "dirty": dirty,
        "upstream_ref": upstream_ref,
        "upstream_head": upstream_head,
        "head_pushed": head_pushed,
    }


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    token = os.getenv("OPS_RUNNER_TOKEN", "").strip()
    if token:
        headers["X-Ops-Runner-Token"] = token
    return headers


def _request(method: str, path: str, payload: dict | None = None) -> dict:
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(f"{OPS_RUNNER_BASE_URL}{path}", data=data, headers=_headers(), method=method)
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
    parser = argparse.ArgumentParser(description="Deploy selected services through the internal ops-runner")
    parser.add_argument("--service", action="append", required=True, help="Service name; repeat or use comma-separated values")
    parser.add_argument("--ref", default="", help="Git ref to deploy; defaults to local HEAD")
    parser.add_argument("--reason", default="openclaw deploy")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        local = _repo_status()
        if local["dirty"]:
            raise RuntimeError("workspace repo is dirty; commit and push before deploy")
        ref = args.ref.strip() or local["head"]
        if not args.ref.strip() and not local["head_pushed"]:
            raise RuntimeError("local HEAD is not pushed upstream; run repo_commit_push.py first")
        payload = _request(
            "POST",
            "/deploy",
            {
                "services": _collect_services(args.service),
                "ref": ref,
                "build": not args.no_build,
                "reason": args.reason.strip(),
            },
        )
        if args.text_only:
            services = ", ".join(payload.get("services", []))
            after = payload.get("after", {}) if isinstance(payload, dict) else {}
            print(f"Deployed {services} at {after.get('head', ref)}")
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
