#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request


PORTAL_INTERNAL_BASE = os.getenv("PLUTONIA_PORTAL_INTERNAL_BASE", "http://portal:8000").rstrip("/")
PORTAL_PUBLIC_BASE = os.getenv("PORTAL_PUBLIC_BASE_URL", "").strip().rstrip("/")
EDITORIAL_SESSION_URL = os.getenv("PLUTONIA_OPS_API_BASE", "http://ainews-gmail-monitor:8001").rstrip("/") + "/api/editorial/session"

ARTICLE_PATH_RE = re.compile(r'href=["\'](/article/(\d+)/)["\']', re.IGNORECASE)


def _get_json(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "PlutoniaOpenClaw/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _build_public_url(path: str) -> str:
    clean = str(path or "").strip()
    if not clean:
        return ""
    if clean.startswith(("http://", "https://")):
        return clean
    if not clean.startswith("/"):
        clean = f"/{clean}"
    if PORTAL_PUBLIC_BASE:
        return f"{PORTAL_PUBLIC_BASE}{clean}"
    return clean


def _find_latest_public_article_path() -> tuple[str, int] | tuple[str, None]:
    html = _get_text(f"{PORTAL_INTERNAL_BASE}/", timeout=20)
    match = ARTICLE_PATH_RE.search(html)
    if not match:
        return "", None
    return str(match.group(1)).strip(), int(match.group(2))


def _current_editorial_article_id() -> int | None:
    try:
        payload = _get_json(EDITORIAL_SESSION_URL, timeout=20)
    except Exception:
        return None
    article = payload.get("article") if isinstance(payload, dict) else None
    if not isinstance(article, dict):
        return None
    try:
        article_id = int(article.get("id") or 0)
    except Exception:
        return None
    if article_id <= 0:
        return None
    if article.get("public_visible") is True or str(article.get("editorial_status", "")).strip().lower() == "approved":
        return article_id
    return None


def _resolve_paths(explicit_article_id: int | None) -> dict:
    article_id = explicit_article_id or _current_editorial_article_id()
    if article_id:
        detail_path = f"/article/{article_id}/"
        return {
            "status": "ok",
            "source": "article_id",
            "article_id": article_id,
            "detail_path": detail_path,
            "card_path": f"/article/{article_id}/card/",
            "home_path": "/",
        }

    latest_path, latest_article_id = _find_latest_public_article_path()
    if latest_path:
        return {
            "status": "ok",
            "source": "portal_home",
            "article_id": latest_article_id,
            "detail_path": latest_path,
            "card_path": f"{latest_path}card/",
            "home_path": "/",
        }

    return {
        "status": "error",
        "error": "no_public_article_found",
        "home_path": "/",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve public portal links")
    parser.add_argument("--article-id", type=int)
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    try:
        resolved = _resolve_paths(args.article_id)
    except urllib.error.HTTPError as exc:
        resolved = {"status": "error", "error": f"http_error:{exc.code}"}
    except Exception as exc:
        resolved = {"status": "error", "error": str(exc)}

    if PORTAL_PUBLIC_BASE:
        resolved["public_base_url"] = PORTAL_PUBLIC_BASE
    else:
        resolved["public_base_url"] = ""
        resolved["warning"] = "PORTAL_PUBLIC_BASE_URL not configured"

    if resolved.get("status") == "ok":
        resolved["home_url"] = _build_public_url(str(resolved.get("home_path", "/")))
        resolved["detail_url"] = _build_public_url(str(resolved.get("detail_path", "")))
        resolved["card_url"] = _build_public_url(str(resolved.get("card_path", "")))

    if args.text_only:
        if resolved.get("status") != "ok":
            text = "Nao consegui resolver o link publico do portal."
            if resolved.get("warning"):
                text += f" {resolved['warning']}."
            print(text)
            return 1
        lines = []
        if resolved.get("home_url"):
            lines.append(f"Portal: {resolved['home_url']}")
        if resolved.get("card_url"):
            lines.append(f"Card: {resolved['card_url']}")
        if resolved.get("detail_url"):
            lines.append(f"Artigo: {resolved['detail_url']}")
        print("\n".join(lines).strip())
        return 0

    print(json.dumps(resolved, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
