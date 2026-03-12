#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from typing import Sequence

from gmail_monitor_healthcheck import evaluate_health


LOG = logging.getLogger("gmail_monitor_supervisor")


def _setup_logging() -> None:
    level_name = str(os.getenv("LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _terminate_child(child: subprocess.Popen[bytes | str], *, force_after_seconds: int) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    deadline = time.time() + max(1, force_after_seconds)
    while time.time() < deadline:
        if child.poll() is not None:
            return
        time.sleep(0.25)
    if child.poll() is None:
        child.kill()


def _default_command() -> list[str]:
    return [sys.executable, "/app/scripts/gmail_monitor.py", "--mode", "monitor"]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Supervise gmail_monitor and force container restart on stale watchdog state.")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    _setup_logging()

    command = list(args.command or [])
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        command = _default_command()

    poll_seconds = max(5, int(os.getenv("MONITOR_SUPERVISOR_POLL_SECONDS", "15")))
    startup_grace_seconds = max(30, int(os.getenv("MONITOR_HEALTHCHECK_STARTUP_GRACE_SECONDS", "180")))
    failures_before_restart = max(1, int(os.getenv("MONITOR_HEALTHCHECK_FAILURES_BEFORE_RESTART", "2")))
    terminate_grace_seconds = max(5, int(os.getenv("MONITOR_SUPERVISOR_TERMINATE_GRACE_SECONDS", "15")))

    child = subprocess.Popen(command)
    startup_deadline = time.time() + startup_grace_seconds
    consecutive_failures = 0
    stop_requested = False

    def _handle_signal(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True
        LOG.info("signal received signum=%s; stopping child", signum)
        _terminate_child(child, force_after_seconds=terminate_grace_seconds)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while True:
        return_code = child.poll()
        if return_code is not None:
            return int(return_code)
        if stop_requested:
            return 0

        if time.time() >= startup_deadline:
            healthy, reason = evaluate_health()
            if healthy:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                LOG.warning(
                    "gmail-monitor healthcheck failed count=%s/%s reason=%s",
                    consecutive_failures,
                    failures_before_restart,
                    reason,
                )
                if consecutive_failures >= failures_before_restart:
                    LOG.error("gmail-monitor unhealthy; terminating child for Docker restart")
                    _terminate_child(child, force_after_seconds=terminate_grace_seconds)
                    return 1

        time.sleep(poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
