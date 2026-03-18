#!/usr/bin/env python3
"""
email_watcher.py — Watcher contínuo de emails de newsletters.

Comportamento:
  1. No primeiro arranque, começa a partir do email mais recente na caixa.
  2. Corre em loop: a cada POLL_INTERVAL segundos verifica se chegaram novos emails.
  3. Processa cada email com o newsletter_revh_parser e guarda os resultados em
     data/emails/YYYY-MM-DD_HH:MM_UID/.

Estrutura por email:
  meta.json       — metadados (sender, subject, datas, is_forwarded, …)
  articles.json   — artigos parseados (título, source_link, image_urls, …)
  raw.html        — HTML bruto do email
  article_N/
    text.txt      — texto do artigo extraído do email
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import parser_replay as _pr
from link_resolver import resolve_article_links
from newsletter_revh_parser import enrich_with_images, parse_email_articles
from runtime_paths import DATA_DIR, ensure_runtime_dirs

SEARXNG_URL = os.getenv("SEARXNG_URL", "")

LOG = logging.getLogger("email_watcher")

EMAILS_DIR = DATA_DIR / "emails"
STATE_FILE = EMAILS_DIR / ".state.json"
POLL_INTERVAL = 5 * 60  # segundos


# ---------------------------------------------------------------------------
# Estado
# ---------------------------------------------------------------------------

def _load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"last_uid": None}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Nomeação de pastas
# ---------------------------------------------------------------------------

def _folder_name(received_at: str, uid: str) -> str:
    """YYYY-MM-DD_HH:MM_UID a partir de um ISO datetime string."""
    try:
        dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
        return f"{dt.strftime('%Y-%m-%d_%H-%M')}_{uid}"
    except Exception:
        return f"unknown_{uid}"


def _email_folder(received_at: str, uid: str) -> Path:
    return EMAILS_DIR / _folder_name(received_at, uid)


# ---------------------------------------------------------------------------
# Persistência local
# ---------------------------------------------------------------------------

def _save_email(uid: str, raw_html: str, result) -> Path:
    received_at = result.received_at or ""
    folder = _email_folder(received_at, uid)
    folder.mkdir(parents=True, exist_ok=True)

    # meta.json
    meta = {
        "uid": uid,
        "received_at": result.received_at,
        "sender_email": result.sender_email,
        "is_forwarded": result.is_forwarded,
        "original_sent_at": result.original_sent_at,
        "original_sender_email": result.original_sender_email,
        "original_sender_name": result.original_sender_name,
        "original_subject": result.original_subject,
        "family": result.family,
        "skipped": result.skipped,
        "skip_notes": result.skip_notes,
        "article_count": len(result.articles),
    }
    (folder / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # raw.html
    (folder / "raw.html").write_text(raw_html, encoding="utf-8")

    # articles.json + article_N/text.txt
    articles_out = []
    for idx, article in enumerate(result.articles, start=1):
        articles_out.append(
            {
                "uid": uid,
                "index": idx,
                "article_id": f"{uid}_{idx}",
                "title": article.title,
                "source_link": article.source_link,
                "source_link_final": article.source_link_final,
                "published_date": article.published_date,
                "published_date_source": article.published_date_source,
                "confidence": round(article.confidence, 3),
                "family": article.family,
                "notes": article.notes,
                "image_urls": article.images,
                "text_file": f"article_{idx}/text.txt",
            }
        )
        art_dir = folder / f"article_{idx}"
        art_dir.mkdir(exist_ok=True)
        (art_dir / "text.txt").write_text(article.text, encoding="utf-8")

    (folder / "articles.json").write_text(
        json.dumps(articles_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return folder


# ---------------------------------------------------------------------------
# Fetch IMAP de um único UID
# ---------------------------------------------------------------------------

def _fetch_uid_from_imap(mail, uid: str) -> dict[str, Any] | None:
    """Obtém headers + body de um UID via IMAP. Devolve payload ou None."""
    # 1. BODYSTRUCTURE
    try:
        mail, data = _pr._imap_fetch(
            mail,
            uid,
            "(BODY.PEEK[HEADER.FIELDS (DATE FROM SUBJECT MESSAGE-ID)] BODYSTRUCTURE)",
            cooldown_seconds=30,
        )
    except Exception as exc:
        LOG.error("uid=%s header fetch failed: %s", uid, exc)
        return None

    body_part: dict | None = None
    imap_meta: dict = {}
    for item in data:
        if not isinstance(item, tuple):
            continue
        raw_hdr = (
            item[0].decode(errors="replace")
            if isinstance(item[0], (bytes, bytearray))
            else str(item[0])
        )
        if "BODYSTRUCTURE" not in raw_hdr:
            continue
        uid_found, part = _pr._parse_bodystructure_fetch_header(raw_hdr)
        if not uid_found or not part:
            continue
        body_part = part
        imap_meta = _pr._parse_header_fields(
            item[1] if isinstance(item[1], (bytes, bytearray)) else b""
        )
        break

    if not body_part:
        LOG.warning("uid=%s: no body part found", uid)
        return None

    # 2. BODY
    try:
        mail, data = _pr._imap_fetch(
            mail,
            uid,
            f"(BODY.PEEK[{body_part['part']}])",
            cooldown_seconds=30,
        )
    except Exception as exc:
        LOG.error("uid=%s body fetch failed: %s", uid, exc)
        return None

    for item in data:
        if not isinstance(item, tuple):
            continue
        raw_hdr = (
            item[0].decode(errors="replace")
            if isinstance(item[0], (bytes, bytearray))
            else str(item[0])
        )
        if "BODY[" not in raw_hdr:
            continue
        try:
            part_text = _pr._decode_part_payload(
                item[1], body_part["encoding"], body_part["charset"]
            )
        except Exception as exc:
            LOG.error("uid=%s decode failed: %s", uid, exc)
            return None

        raw_html = (
            part_text
            if body_part["type"] == "HTML"
            else f"<html><body><pre>{part_text}</pre></body></html>"
        )
        payload = dict(imap_meta)
        payload["gmail_uid"] = uid
        payload["raw_html"] = raw_html
        return payload

    return None


# ---------------------------------------------------------------------------
# Processamento de um email
# ---------------------------------------------------------------------------

def _process_uid(mail, uid: str) -> bool:
    """Fetch → parse → enrich → save. Devolve True em caso de sucesso."""
    LOG.info("Processing uid=%s", uid)

    payload = _fetch_uid_from_imap(mail, uid)
    if not payload:
        return False

    raw_html = payload.get("raw_html", "")
    email_meta = {
        "received_at": payload.get("received_at", ""),
        "sender_email": payload.get("sender_email", ""),
        "subject": payload.get("subject", ""),
        "original_sender_email": payload.get("original_sender_email", ""),
        "original_sender_name": payload.get("original_sender_name", ""),
        "original_sent_at": payload.get("original_sent_at", ""),
    }

    result = parse_email_articles(raw_html, email_meta)
    enrich_with_images(result)
    if SEARXNG_URL and not result.skipped:
        resolve_article_links(result, searxng_url=SEARXNG_URL)

    folder = _save_email(uid, raw_html, result)
    LOG.info(
        "Saved uid=%s → %s (%d articles%s)",
        uid,
        folder.name,
        len(result.articles),
        ", skipped" if result.skipped else "",
    )
    return True


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

def run(poll_interval: int = POLL_INTERVAL) -> None:
    ensure_runtime_dirs()
    EMAILS_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    state = _load_state()
    LOG.info("Starting. last_uid=%s", state.get("last_uid"))

    mail = _pr._connect_gmail_from_env()

    while True:
        # ---- descobrir UIDs disponíveis ----
        try:
            all_uids = _pr._search_allowed_uids(mail)
        except Exception as exc:
            LOG.warning("IMAP search failed (%s) — reconnecting in 30s", exc)
            try:
                mail.logout()
            except Exception:
                pass
            time.sleep(30)
            try:
                mail = _pr._connect_gmail_from_env()
            except Exception as conn_exc:
                LOG.error("Reconnect failed: %s", conn_exc)
            continue

        if not all_uids:
            LOG.info("No emails found. Sleeping %ds.", poll_interval)
            time.sleep(poll_interval)
            continue

        last_uid = state.get("last_uid")

        if last_uid is None:
            # Primeiro arranque: começa no email mais recente
            anchor = max(all_uids, key=int)
            LOG.info("First run — starting from most recent uid=%s", anchor)
            pending = [anchor]
        else:
            # Polls seguintes: tudo o que chegou depois do último processado
            pending = sorted(
                [u for u in all_uids if int(u) > int(last_uid)], key=int
            )

        if pending:
            LOG.info("%d email(s) to process: %s", len(pending), pending)
        else:
            LOG.info("No new emails. Sleeping %ds.", poll_interval)

        for uid in pending:
            success = _process_uid(mail, uid)
            if success:
                if last_uid is None or int(uid) > int(state.get("last_uid") or 0):
                    state["last_uid"] = uid
                    _save_state(state)
            time.sleep(1)  # pausa entre emails para não sobrecarregar IMAP

        LOG.info("Cycle done. Sleeping %ds.", poll_interval)
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Utilitário: processar a partir de ficheiros JSON de cache local
# ---------------------------------------------------------------------------

def process_from_cache(cache_paths: list[Path]) -> None:
    """Processa emails a partir de ficheiros JSON de cache (sem IMAP)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    EMAILS_DIR.mkdir(parents=True, exist_ok=True)

    for path in cache_paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        uid = str(data.get("gmail_uid", path.stem.replace("uid_", "")))
        raw_html = data.get("raw_html", "")
        email_meta = {
            "received_at": data.get("received_at", ""),
            "sender_email": data.get("sender_email", ""),
            "subject": data.get("subject", ""),
            "original_sender_email": data.get("original_sender_email", ""),
            "original_sender_name": data.get("original_sender_name", ""),
            "original_sent_at": data.get("original_sent_at", ""),
        }
        result = parse_email_articles(raw_html, email_meta)
        enrich_with_images(result)
        if SEARXNG_URL and not result.skipped:
            resolve_article_links(result, searxng_url=SEARXNG_URL)
        folder = _save_email(uid, raw_html, result)
        LOG.info(
            "Cache uid=%s → %s (%d articles%s)",
            uid,
            folder.name,
            len(result.articles),
            ", skipped" if result.skipped else "",
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse

    _pr._load_project_env()

    ap = argparse.ArgumentParser(description="Continuous newsletter email watcher")
    ap.add_argument(
        "--from-cache",
        metavar="JSON",
        nargs="+",
        help="Process from local cache JSON files instead of IMAP (useful for testing)",
    )
    ap.add_argument(
        "--poll-interval",
        type=int,
        default=POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {POLL_INTERVAL})",
    )
    args = ap.parse_args()

    if args.from_cache:
        process_from_cache([Path(p) for p in args.from_cache])
        return 0

    try:
        run(poll_interval=args.poll_interval)
    except KeyboardInterrupt:
        LOG.info("Interrupted — shutting down.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
