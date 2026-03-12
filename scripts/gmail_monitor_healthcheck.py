#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _heartbeat_age_seconds(value: Any) -> float | None:
    dt = _parse_iso_datetime(value)
    if dt is None:
        return None
    return max(0.0, (datetime.now(tz=UTC) - dt).total_seconds())


def evaluate_health() -> tuple[bool, str]:
    watchdog_status_file = os.getenv("WATCHDOG_STATUS_FILE", "/review/ops_watchdog_status.json").strip() or "/review/ops_watchdog_status.json"
    status_path = Path(watchdog_status_file)
    watchdog_interval = max(10, int(os.getenv("PIPELINE_WATCHDOG_INTERVAL_SECONDS", "30")))
    worker_stale_seconds = max(45, int(os.getenv("PIPELINE_WATCHDOG_WORKER_STALE_SECONDS", "90")))
    ingest_interval_seconds = max(10, int(os.getenv("INGEST_INTERVAL_SECONDS", "60")))
    imap_reconnect_seconds = max(5, int(os.getenv("IMAP_RECONNECT_SECONDS", "15")))
    review_gating_sleep_seconds = max(5, int(os.getenv("REVIEW_GATING_SLEEP_SECONDS", "30")))
    file_stale_seconds = max(
        300,
        int(os.getenv("MONITOR_HEALTHCHECK_STALE_SECONDS", "0") or 0),
        watchdog_interval * 10,
        worker_stale_seconds + 180,
        ingest_interval_seconds + imap_reconnect_seconds + 180,
    )
    heartbeat_stale_seconds = max(
        180,
        int(os.getenv("MONITOR_HEARTBEAT_STALE_SECONDS", "0") or 0),
        worker_stale_seconds + 30,
        watchdog_interval * 6,
        ingest_interval_seconds + imap_reconnect_seconds + 90,
        review_gating_sleep_seconds + 90,
    )

    if not status_path.exists():
        return False, f"missing watchdog status file: {status_path}"

    file_age_seconds = max(0.0, time.time() - status_path.stat().st_mtime)
    if file_age_seconds > file_stale_seconds:
        return False, f"watchdog status stale: age={round(file_age_seconds, 1)}s limit={file_stale_seconds}s"

    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"invalid watchdog status payload: {exc}"

    runtime = payload.get("runtime", {}) if isinstance(payload, dict) else {}
    heartbeats = runtime.get("worker_heartbeats", {}) if isinstance(runtime, dict) else {}
    counts = payload.get("counts", {}) if isinstance(payload, dict) else {}
    pending_count = int(counts.get("pending", 0) or 0) if isinstance(counts, dict) else 0
    processing_count = int(counts.get("processing", 0) or 0) if isinstance(counts, dict) else 0
    review_count = int(counts.get("review", 0) or 0) if isinstance(counts, dict) else 0

    required = ("watchdog", "ingest")
    if pending_count > 0 or processing_count > 0 or review_count > 0:
        required = ("watchdog", "ingest", "process")

    for name in required:
        entry = heartbeats.get(name, {}) if isinstance(heartbeats, dict) else {}
        age = _heartbeat_age_seconds(entry.get("at") if isinstance(entry, dict) else None)
        if age is None:
            return False, f"missing heartbeat: {name}"
        state = str(entry.get("state", "")).strip() if isinstance(entry, dict) else ""
        if name == "process" and review_count > 0 and state == "review_wait":
            continue
        if age > heartbeat_stale_seconds:
            return False, f"heartbeat stale: {name} age={round(age, 1)}s state={state or '-'}"

    return True, "ok"


def main() -> int:
    healthy, reason = evaluate_health()
    stream = sys.stdout if healthy else sys.stderr
    print(reason, file=stream)
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
