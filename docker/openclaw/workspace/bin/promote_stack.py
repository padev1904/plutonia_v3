#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path


WORKSPACE_BIN = Path("/workspace/bin")


def _run_json(script_name: str, *args: str) -> dict:
    script_path = WORKSPACE_BIN / script_name
    cmd = ["python3", str(script_path), *args]
    result = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"{script_name} failed"
        raise RuntimeError(detail)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{script_name} returned invalid JSON") from exc


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


def _healthcheck(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            parsed = None
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = body
            return {"url": url, "status": "ok", "http_status": resp.status, "body": parsed}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"url": url, "status": "error", "http_status": exc.code, "body": body}
    except Exception as exc:
        return {"url": url, "status": "error", "error": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Commit, push, deploy, and verify a production change")
    parser.add_argument("--message", required=True, help="Commit message")
    parser.add_argument("--service", action="append", required=True, help="Service name; repeat or use comma-separated values")
    parser.add_argument("--path", action="append", default=[], help="Optional path to stage; repeat for multiple paths")
    parser.add_argument("--reason", default="openclaw promote")
    parser.add_argument("--health-url", action="append", default=[], help="Optional health URL to verify after deploy")
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        services = _collect_services(args.service)
        commit_cmd = ["--message", args.message.strip()]
        for path in args.path:
            commit_cmd.extend(["--path", path])
        if args.allow_empty:
            commit_cmd.append("--allow-empty")
        commit_result = _run_json("repo_commit_push.py", *commit_cmd)
        commit_head = str(commit_result.get("head") or "").strip()
        if not commit_head:
            raise RuntimeError("repo_commit_push.py did not return a commit head")

        deploy_cmd = ["--ref", commit_head, "--reason", args.reason.strip()]
        if args.no_build:
            deploy_cmd.append("--no-build")
        for service in services:
            deploy_cmd.extend(["--service", service])
        deploy_result = _run_json("deploy_stack.py", *deploy_cmd)

        health_results = [_healthcheck(url.strip()) for url in args.health_url if url.strip()]
        health_ok = all(item.get("status") == "ok" for item in health_results) if health_results else True

        payload = {
            "status": "ok" if health_ok else "degraded",
            "commit": commit_result,
            "deploy": deploy_result,
            "health": health_results,
        }
        if args.text_only:
            health_state = "ok" if health_ok else "degraded"
            print(f"Promoted {commit_head} to {', '.join(services)}; health={health_state}")
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if health_ok else 1
    except Exception as exc:
        if args.text_only:
            print(f"ERROR: {exc}", file=sys.stderr)
        else:
            print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
