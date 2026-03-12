#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse


def _resolve_repo_dir() -> Path:
    configured = Path(os.getenv("OPENCLAW_BOOTSTRAP_REPO_DIR", "/workspace/repo")).resolve()
    if configured.is_dir():
        return configured
    return Path(__file__).resolve().parents[4]


REPO_DIR = _resolve_repo_dir()
DEFAULT_REMOTE = os.getenv("OPENCLAW_GIT_PUSH_REMOTE", "origin").strip() or "origin"
DEFAULT_BRANCH = os.getenv("OPENCLAW_GIT_PUSH_BRANCH", "").strip()
DEFAULT_AUTHOR_NAME = os.getenv("OPENCLAW_GIT_AUTHOR_NAME", "OpenClaw Ops").strip() or "OpenClaw Ops"
DEFAULT_AUTHOR_EMAIL = os.getenv("OPENCLAW_GIT_AUTHOR_EMAIL", "openclaw-ops@plutonia.local").strip() or "openclaw-ops@plutonia.local"
DEFAULT_PUSH_URL = os.getenv("OPENCLAW_GIT_PUSH_URL", "").strip()
DEFAULT_PUSH_TOKEN = (
    os.getenv("OPENCLAW_GITHUB_PUSH_TOKEN", "").strip()
    or os.getenv("OPENCLAW_GIT_PUSH_TOKEN", "").strip()
    or os.getenv("GITHUB_TOKEN", "").strip()
)
DEFAULT_PUSH_USERNAME = os.getenv("OPENCLAW_GITHUB_PUSH_USERNAME", "x-access-token").strip() or "x-access-token"


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


def _current_branch() -> str:
    branch = _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    if branch == "HEAD":
        if DEFAULT_BRANCH:
            return DEFAULT_BRANCH
        raise RuntimeError("repository is in detached HEAD state; set OPENCLAW_GIT_PUSH_BRANCH or use a branch checkout")
    return DEFAULT_BRANCH or branch


def _repo_status() -> dict:
    head = _git("rev-parse", "HEAD").stdout.strip()
    branch = _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    dirty = bool(_git("status", "--porcelain", check=False).stdout.strip())
    remote = _git("remote", "get-url", DEFAULT_REMOTE, check=False).stdout.strip()
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


def _set_identity(name: str, email: str) -> None:
    _git("config", "user.name", name)
    _git("config", "user.email", email)


def _normalize_remote_url(remote_url: str) -> str:
    if not remote_url:
        raise RuntimeError(f"remote '{DEFAULT_REMOTE}' is not configured")
    if remote_url.startswith("git@github.com:"):
        repo = remote_url.split(":", 1)[1]
        return f"https://github.com/{repo}"
    return remote_url


def _push_target(remote_name: str, remote_url: str, username: str, token: str) -> str:
    explicit_push_url = DEFAULT_PUSH_URL or remote_url
    if not token:
        return remote_name
    parsed = urlparse(_normalize_remote_url(explicit_push_url))
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError("token-based push requires an https GitHub remote or OPENCLAW_GIT_PUSH_URL")
    auth = f"{quote(username, safe='')}:{quote(token, safe='')}"
    return urlunparse((parsed.scheme, f"{auth}@{parsed.netloc}", parsed.path, parsed.params, parsed.query, parsed.fragment))


def _stage_changes(paths: list[str]) -> None:
    if paths:
        _git("add", "--", *paths)
    else:
        _git("add", "--all")


def _commit(message: str, allow_empty: bool) -> str:
    status_before = _git("status", "--porcelain", check=False).stdout.strip()
    if not status_before and not allow_empty:
        raise RuntimeError("working tree has no staged or unstaged changes")
    commit_args = ["commit", "-m", message]
    if allow_empty:
        commit_args.append("--allow-empty")
    _git(*commit_args)
    return _git("rev-parse", "HEAD").stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage, commit, and push workspace repo changes to GitHub")
    parser.add_argument("--message", required=True, help="Commit message")
    parser.add_argument("--path", action="append", default=[], help="Optional path to stage; repeat for multiple paths")
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        branch = _current_branch()
        remote_url = _git("remote", "get-url", DEFAULT_REMOTE).stdout.strip()
        _set_identity(DEFAULT_AUTHOR_NAME, DEFAULT_AUTHOR_EMAIL)
        _stage_changes(args.path)
        commit_head = _commit(args.message.strip(), args.allow_empty)
        push_target = _push_target(DEFAULT_REMOTE, remote_url, DEFAULT_PUSH_USERNAME, DEFAULT_PUSH_TOKEN)
        _git("push", push_target, f"HEAD:{branch}")
        payload = {
            "status": "ok",
            "branch": branch,
            "head": commit_head,
            "remote": DEFAULT_REMOTE,
            "remote_url": remote_url,
            "head_pushed": True,
            "repo": _repo_status(),
        }
        if args.text_only:
            print(f"Committed and pushed {commit_head} to {DEFAULT_REMOTE}/{branch}")
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
