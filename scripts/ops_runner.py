#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


LOG = logging.getLogger("ops_runner")

HOST = os.getenv("OPS_RUNNER_HOST", "0.0.0.0").strip() or "0.0.0.0"
PORT = int(os.getenv("OPS_RUNNER_PORT", "8011"))
TOKEN = os.getenv("OPS_RUNNER_TOKEN", "").strip()
REPO_DIR = Path(os.getenv("OPS_RUNNER_REPO_DIR", "/srv/live-repo")).resolve()
COMPOSE_FILE = Path(os.getenv("OPS_RUNNER_COMPOSE_FILE", str(REPO_DIR / "docker-compose.yml"))).resolve()
LOG_DIR = Path(os.getenv("OPS_RUNNER_LOG_DIR", "/logs")).resolve()
EVENT_LOG_PATH = LOG_DIR / "ops_runner_events.jsonl"
LIVE_BRANCH = os.getenv("OPS_RUNNER_LIVE_BRANCH", "main").strip() or "main"
REQUIRE_PUSHED_HEAD = os.getenv("OPS_RUNNER_REQUIRE_PUSHED_HEAD", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ALLOWED_SERVICES = {
    item.strip()
    for item in os.getenv(
        "OPS_RUNNER_ALLOWED_SERVICES",
        "portal,gmail-monitor,openclaw-ops,openclaw,nginx,litellm,searxng",
    ).split(",")
    if item.strip()
}
DEPLOY_TIMEOUT_SECONDS = int(os.getenv("OPS_RUNNER_DEPLOY_TIMEOUT_SECONDS", "1800"))
GIT_TIMEOUT_SECONDS = int(os.getenv("OPS_RUNNER_GIT_TIMEOUT_SECONDS", "120"))


class ConflictError(RuntimeError):
    pass


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
        env=os.environ.copy(),
    )
    if check and result.returncode != 0:
        joined = " ".join(shlex.quote(part) for part in cmd)
        raise RuntimeError(
            f"command failed ({result.returncode}): {joined}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}".strip()
        )
    return result


def _ensure_safe_directory() -> None:
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", str(REPO_DIR)],
        text=True,
        capture_output=True,
        check=False,
    )


def _git(*args: str, timeout: int = GIT_TIMEOUT_SECONDS, check: bool = True) -> subprocess.CompletedProcess[str]:
    _ensure_safe_directory()
    return _run(["git", *args], cwd=REPO_DIR, timeout=timeout, check=check)


def _compose(*args: str, timeout: int = DEPLOY_TIMEOUT_SECONDS, check: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    return _run(cmd, cwd=REPO_DIR, timeout=timeout, check=check)


def _ensure_paths() -> None:
    if not REPO_DIR.exists():
        raise RuntimeError(f"repo dir not found: {REPO_DIR}")
    if not COMPOSE_FILE.exists():
        raise RuntimeError(f"compose file not found: {COMPOSE_FILE}")
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _repo_status() -> dict[str, Any]:
    head = _git("rev-parse", "HEAD").stdout.strip()
    branch = _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    dirty = bool(_git("status", "--porcelain", check=False).stdout.strip())
    remote = _git("remote", "get-url", "origin", check=False).stdout.strip()
    upstream_ref = ""
    upstream_head = ""
    remote_branch_head = ""
    in_sync = False
    try:
        upstream_ref = _git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}").stdout.strip()
        upstream_head = _git("rev-parse", "@{u}").stdout.strip()
    except Exception:
        upstream_ref = ""
        upstream_head = ""
    if branch and branch != "HEAD":
        remote_branch_head = _remote_branch_head(branch)
    if remote_branch_head:
        in_sync = bool(head and head == remote_branch_head)
    elif upstream_head:
        in_sync = bool(head and head == upstream_head)
    return {
        "repo_dir": str(REPO_DIR),
        "compose_file": str(COMPOSE_FILE),
        "branch": branch,
        "head": head,
        "dirty": dirty,
        "remote": remote,
        "upstream_ref": upstream_ref,
        "upstream_head": upstream_head,
        "remote_branch_head": remote_branch_head,
        "head_pushed": in_sync,
    }


def _compose_ps() -> list[dict[str, Any]]:
    result = _compose("ps", "--format", "json", timeout=120, check=False)
    payload = (result.stdout or "").strip()
    if not payload:
        return []
    try:
        parsed = json.loads(payload)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except Exception:
        pass
    return [{"raw": payload}]


def _record_event(action: str, payload: dict[str, Any], result: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    row = {
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "payload": payload,
        "result": result,
    }
    with EVENT_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _validate_services(raw: Any) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("services must be a non-empty list")
    services = []
    for item in raw:
        name = str(item or "").strip()
        if not name:
            continue
        if name not in ALLOWED_SERVICES:
            raise ValueError(f"service not allowed: {name}")
        if name not in services:
            services.append(name)
    if not services:
        raise ValueError("no valid services requested")
    return services


def _git_fetch() -> None:
    _git("fetch", "--tags", "--prune", "origin")


def _remote_branch_head(branch: str) -> str:
    result = _git("ls-remote", "origin", f"refs/heads/{branch}", check=False)
    if result.returncode != 0:
        return ""
    parts = result.stdout.strip().split()
    return parts[0] if parts else ""


def _ensure_ref_exists(ref: str) -> str:
    target = str(ref or "").strip()
    if not target:
        raise RuntimeError("ref is required")
    resolved = _git("rev-parse", "--verify", target).stdout.strip()
    if not resolved:
        raise RuntimeError(f"unable to resolve ref: {target}")
    return resolved


def _checkout_ref(ref: str) -> None:
    target = str(ref or "").strip()
    if not target:
        return
    _git_fetch()
    resolved = _ensure_ref_exists(target)
    _git("checkout", "--force", LIVE_BRANCH)
    _git("reset", "--hard", resolved)
    _git("clean", "-fd")


def _require_clean_and_pushed(*, require_upstream_head: bool = True) -> dict[str, Any]:
    status = _repo_status()
    if status["dirty"]:
        raise ConflictError("live repo is dirty; refusing deploy")
    if REQUIRE_PUSHED_HEAD and require_upstream_head and not status["head_pushed"]:
        raise ConflictError("live repo HEAD is not synchronized with upstream; refusing deploy")
    return status


def _handle_status() -> tuple[dict[str, Any], int]:
    payload = {
        "status": "ok",
        "repo": _repo_status(),
        "compose": {"services": _compose_ps()},
        "allowed_services": sorted(ALLOWED_SERVICES),
    }
    return payload, 200


def _handle_deploy(
    data: dict[str, Any],
    *,
    action: str = "deploy",
    require_upstream_head: bool = True,
) -> tuple[dict[str, Any], int]:
    services = _validate_services(data.get("services"))
    ref = str(data.get("ref", "")).strip()
    build = bool(data.get("build", True))
    reason = str(data.get("reason", "")).strip()

    before = _repo_status()
    if ref:
        _checkout_ref(ref)
    after_checkout = _require_clean_and_pushed(require_upstream_head=require_upstream_head)
    _compose("config", "--quiet", timeout=120)
    compose_args = ["up", "-d"]
    if build:
        compose_args.append("--build")
    compose_args.extend(services)
    _compose(*compose_args, timeout=DEPLOY_TIMEOUT_SECONDS)
    after = _repo_status()
    result = {
        "status": "ok",
        "action": action,
        "reason": reason,
        "services": services,
        "build": build,
        "before": before,
        "after": after,
        "compose": {"services": _compose_ps()},
    }
    _record_event(action, {"services": services, "ref": ref, "build": build, "reason": reason}, result)
    return result, 200


def _handle_rollback(data: dict[str, Any]) -> tuple[dict[str, Any], int]:
    ref = str(data.get("ref", "")).strip()
    if not ref:
        return {"error": "ref is required"}, 400
    services = _validate_services(data.get("services"))
    reason = str(data.get("reason", "")).strip() or "rollback"
    result, status = _handle_deploy(
        {
            "services": services,
            "ref": ref,
            "build": bool(data.get("build", True)),
            "reason": reason,
        },
        action="rollback",
        require_upstream_head=False,
    )
    return result, status


class Handler(BaseHTTPRequestHandler):
    server_version = "PlutoniaOpsRunner/1.0"

    def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        parsed = json.loads(raw.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}

    def _auth_failed(self) -> bool:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            return False
        if not TOKEN:
            return False
        provided = self.headers.get("X-Ops-Runner-Token", "").strip()
        if provided == TOKEN:
            return False
        self._write_json({"error": "invalid ops runner token"}, 403)
        return True

    def do_GET(self) -> None:  # noqa: N802
        if self._auth_failed():
            return
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/healthz":
                self._write_json({"ok": True, "service": "ops-runner"})
                return
            if parsed.path == "/status":
                payload, status = _handle_status()
                self._write_json(payload, status)
                return
            self._write_json({"error": "not found", "path": parsed.path}, 404)
        except Exception as exc:
            LOG.exception("GET %s failed", parsed.path)
            self._write_json({"error": str(exc)}, 500)

    def do_POST(self) -> None:  # noqa: N802
        if self._auth_failed():
            return
        parsed = urlparse(self.path)
        try:
            data = self._read_json()
            if parsed.path == "/deploy":
                payload, status = _handle_deploy(data)
                self._write_json(payload, status)
                return
            if parsed.path == "/rollback":
                payload, status = _handle_rollback(data)
                self._write_json(payload, status)
                return
            self._write_json({"error": "not found", "path": parsed.path}, 404)
        except ValueError as exc:
            self._write_json({"error": str(exc)}, 400)
        except ConflictError as exc:
            self._write_json({"error": str(exc)}, 409)
        except Exception as exc:
            LOG.exception("POST %s failed", parsed.path)
            self._write_json({"error": str(exc)}, 500)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        LOG.info("%s - %s", self.address_string(), format % args)


def main() -> int:
    logging.basicConfig(
        level=os.getenv("OPS_RUNNER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _ensure_paths()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    LOG.info("ops runner listening on %s:%s repo=%s compose=%s", HOST, PORT, REPO_DIR, COMPOSE_FILE)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOG.info("ops runner interrupted")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
