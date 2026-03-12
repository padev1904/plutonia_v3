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


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
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


def _local_status() -> dict:
    head = _git("rev-parse", "HEAD").stdout.strip()
    branch = _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    dirty = bool(_git("status", "--porcelain", check=False).stdout.strip())
    remote = _git("remote", "get-url", "origin", check=False).stdout.strip()
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
        "repo_dir": str(REPO_DIR),
        "branch": branch,
        "head": head,
        "dirty": dirty,
        "remote": remote,
        "upstream_ref": upstream_ref,
        "upstream_head": upstream_head,
        "head_pushed": head_pushed,
    }


def _runner_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    token = os.getenv("OPS_RUNNER_TOKEN", "").strip()
    if token:
        headers["X-Ops-Runner-Token"] = token
    return headers


def _runner_status() -> dict | None:
    req = urllib.request.Request(f"{OPS_RUNNER_BASE_URL}/status", headers=_runner_headers(), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"error": exc.read().decode("utf-8", errors="replace") or str(exc)}
    except Exception as exc:
        return {"error": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Show local workspace git status and live deploy status")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    payload = {
        "local": _local_status(),
        "runner": _runner_status(),
    }
    if args.text_only:
        local = payload["local"]
        runner = payload.get("runner") or {}
        lines = [
            f"Local branch: {local.get('branch')}",
            f"Local head: {local.get('head')}",
            f"Local dirty: {local.get('dirty')}",
            f"Local pushed: {local.get('head_pushed')}",
        ]
        live_repo = runner.get("repo") if isinstance(runner, dict) else None
        if isinstance(live_repo, dict):
            lines.extend(
                [
                    f"Live branch: {live_repo.get('branch')}",
                    f"Live head: {live_repo.get('head')}",
                    f"Live dirty: {live_repo.get('dirty')}",
                    f"Live pushed: {live_repo.get('head_pushed')}",
                ]
            )
        elif isinstance(runner, dict) and runner.get("error"):
            lines.append(f"Runner: {runner['error']}")
        print("\n".join(lines))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
