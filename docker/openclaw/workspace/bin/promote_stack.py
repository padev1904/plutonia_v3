#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
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


def _wait_for_health(urls: list[str], timeout: int, interval: int) -> list[dict]:
    if not urls:
        return []
    deadline = time.time() + max(1, timeout)
    last_results: list[dict] = []
    while time.time() <= deadline:
        last_results = [_healthcheck(url) for url in urls]
        if all(item.get("status") == "ok" for item in last_results):
            return last_results
        time.sleep(max(1, interval))
    return last_results


def _rollback(services: list[str], rollback_ref: str, no_build: bool) -> dict:
    rollback_cmd = ["--ref", rollback_ref, "--reason", "automatic rollback after failed promotion"]
    if no_build:
        rollback_cmd.append("--no-build")
    for service in services:
        rollback_cmd.extend(["--service", service])
    return _run_json("rollback_stack.py", *rollback_cmd)


def main() -> int:
    parser = argparse.ArgumentParser(description="Commit, push, deploy, and verify a production change")
    parser.add_argument("--message", required=True, help="Commit message")
    parser.add_argument("--service", action="append", required=True, help="Service name; repeat or use comma-separated values")
    parser.add_argument("--path", action="append", default=[], help="Optional path to stage; repeat for multiple paths")
    parser.add_argument("--reason", default="openclaw promote")
    parser.add_argument("--health-url", action="append", default=[], help="Optional health URL to verify after deploy")
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--health-timeout", type=int, default=180)
    parser.add_argument("--health-interval", type=int, default=5)
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        services = _collect_services(args.service)
        baseline_status = _run_json("repo_status.py")
        runner = baseline_status.get("runner") if isinstance(baseline_status, dict) else {}
        runner_repo = runner.get("repo") if isinstance(runner, dict) else {}
        rollback_ref = str(runner_repo.get("head") or "").strip()
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
        rollback_result = None
        try:
            deploy_result = _run_json("deploy_stack.py", *deploy_cmd)
        except Exception as exc:
            if rollback_ref:
                try:
                    rollback_result = _rollback(services, rollback_ref, args.no_build)
                except Exception as rollback_exc:
                    raise RuntimeError(f"deploy failed: {exc}; rollback failed: {rollback_exc}") from rollback_exc
            raise RuntimeError(f"deploy failed: {exc}; rollback={'ok' if rollback_result else 'not-run'}") from exc

        health_results = _wait_for_health(
            [url.strip() for url in args.health_url if url.strip()],
            timeout=args.health_timeout,
            interval=args.health_interval,
        )
        health_ok = all(item.get("status") == "ok" for item in health_results) if health_results else True
        if not health_ok and rollback_ref:
            rollback_result = _rollback(services, rollback_ref, args.no_build)

        payload = {
            "status": "ok" if health_ok else "degraded",
            "commit": commit_result,
            "deploy": deploy_result,
            "health": health_results,
            "rollback": rollback_result,
        }
        if args.text_only:
            if health_ok:
                print(f"Promoted {commit_head} to {', '.join(services)}; health=ok")
            else:
                print(
                    f"Promoted {commit_head} to {', '.join(services)}; health=degraded; "
                    f"rollback={'ok' if rollback_result else 'not-run'}"
                )
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
