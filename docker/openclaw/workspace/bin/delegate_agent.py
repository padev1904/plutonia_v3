#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path


DEFAULT_THINKING = {
    "main": "minimal",
    "coder": "minimal",
    "reviewer": "minimal",
    "editorial": "low",
    "router": "off",
}

DEFAULT_TIMEOUT = {
    "main": 900,
    "coder": 1800,
    "reviewer": 1200,
    "editorial": 900,
    "router": 300,
}


def _task_text(args: argparse.Namespace) -> str:
    if args.task_file:
        return Path(args.task_file).read_text(encoding="utf-8")
    if args.task:
        return args.task
    data = sys.stdin.read()
    if data.strip():
        return data
    raise RuntimeError("missing delegated task text")


def _run(agent: str, task: str, thinking: str, timeout_seconds: int, session_prefix: str) -> dict[str, str]:
    session_id = f"{session_prefix}-{agent}-{uuid.uuid4().hex[:12]}"
    result = subprocess.run(
        [
            "openclaw",
            "agent",
            "--local",
            "--agent",
            agent,
            "--session-id",
            session_id,
            "--thinking",
            thinking,
            "--timeout",
            str(timeout_seconds),
            "--message",
            task,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "delegated agent failed"
        raise RuntimeError(detail)
    return {
        "status": "ok",
        "agent": agent,
        "session_id": session_id,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a delegated OpenClaw sub-agent turn with a fresh session")
    parser.add_argument("--agent", required=True, help="Agent id to run")
    parser.add_argument("--task", help="Task text")
    parser.add_argument("--task-file", help="Path to a UTF-8 task file")
    parser.add_argument("--thinking", default="", help="Thinking level override")
    parser.add_argument("--timeout", type=int, default=0, help="Timeout override in seconds")
    parser.add_argument("--session-prefix", default="delegate", help="Session id prefix")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        agent = str(args.agent).strip()
        if not agent:
            raise RuntimeError("agent id is required")
        task = _task_text(args).strip()
        thinking = str(args.thinking or "").strip() or DEFAULT_THINKING.get(agent, "minimal")
        timeout_seconds = int(args.timeout or 0) or DEFAULT_TIMEOUT.get(agent, 900)
        payload = _run(agent, task, thinking, timeout_seconds, str(args.session_prefix).strip() or "delegate")
        if args.text_only:
            print(payload["stdout"])
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
