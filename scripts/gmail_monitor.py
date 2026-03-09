#!/usr/bin/env python3
"""Monitor Gmail DB-driven para pipeline de newsletters.

Arquitetura canónica (Atlas):
- Gmail é fonte de leitura apenas (nunca move/label/delete)
- DB é a fonte de verdade de estado/ordem
- Ingestão separada do processamento
"""

from __future__ import annotations

import argparse
import email
import imaplib
import json
import logging
import os
import re
import shlex
import threading
import time
import requests
from datetime import UTC, datetime
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from process_newsletter import Config, process_single_newsletter
from review_api import start_review_api_server

LOG = logging.getLogger("gmail_monitor")

INGEST_INTERVAL_SECONDS = int(os.getenv("INGEST_INTERVAL_SECONDS", "60"))
INGEST_MAX_NEW_PER_CYCLE = int(os.getenv("INGEST_MAX_NEW_PER_CYCLE", "1"))
REVIEW_GATING_SLEEP_SECONDS = int(os.getenv("REVIEW_GATING_SLEEP_SECONDS", "30"))
IMAP_RECONNECT_SECONDS = int(os.getenv("IMAP_RECONNECT_SECONDS", "15"))
IMAP_OVERQUOTA_SLEEP_SECONDS = int(os.getenv("IMAP_OVERQUOTA_SLEEP_SECONDS", "180"))
MIN_RAW_HTML_PROCESS_CHARS = int(os.getenv("MIN_RAW_HTML_PROCESS_CHARS", "200"))
PARALLEL_INGEST_BATCH_SIZE = int(os.getenv("PARALLEL_INGEST_BATCH_SIZE", "1"))
PARALLEL_INGEST_ACTIVE_SLEEP_SECONDS = int(os.getenv("PARALLEL_INGEST_ACTIVE_SLEEP_SECONDS", "2"))
PARALLEL_PROCESS_IDLE_SLEEP_SECONDS = int(os.getenv("PARALLEL_PROCESS_IDLE_SLEEP_SECONDS", "3"))
PARALLEL_MAIN_HEARTBEAT_SECONDS = int(os.getenv("PARALLEL_MAIN_HEARTBEAT_SECONDS", "5"))
GMAIL_EXCLUDED_LABELS = [
    item.strip()
    for item in os.getenv("GMAIL_EXCLUDED_LABELS", "Pending,Published,Partial,Rejected").split(",")
    if item.strip()
]
ALLOWED_FORWARDER_EMAILS = [
    item.strip().lower()
    for item in os.getenv("ALLOWED_FORWARDER_EMAILS", "carlos.santos@plutoanalytics.com").split(",")
    if item.strip()
]
REVIEW_TELEGRAM_NOTIFY = os.getenv("REVIEW_TELEGRAM_NOTIFY", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
REVIEW_TELEGRAM_BOT_TOKEN = os.getenv("REVIEW_TELEGRAM_BOT_TOKEN", "").strip()
REVIEW_TELEGRAM_CHAT_ID = os.getenv("REVIEW_TELEGRAM_CHAT_ID", "").strip()
OPENCLAW_CONFIG_PATH = os.getenv("OPENCLAW_CONFIG_PATH", "/home/python/.openclaw/openclaw.json")

MANAGED_GMAIL_LABELS = ("Pending", "Published", "Partial", "Rejected")
MANAGED_GMAIL_LABEL_KEYS = {label.casefold() for label in MANAGED_GMAIL_LABELS}

# Cache apenas resultados válidos; tentativas falhadas devem voltar a resolver no ciclo seguinte.
_TELEGRAM_CONFIG_CACHE: tuple[str, str] | None = None


def _is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _setup_debug_breakpoint_on_start() -> None:
    if not _is_truthy(os.getenv("DEBUGPY_ENABLE")):
        return

    host = os.getenv("DEBUGPY_HOST", "0.0.0.0").strip() or "0.0.0.0"
    port = int(os.getenv("DEBUGPY_PORT", "5679"))
    wait_for_client = _is_truthy(os.getenv("DEBUGPY_WAIT_FOR_CLIENT", "1"))
    break_on_start = _is_truthy(os.getenv("DEBUGPY_BREAK_ON_START", "1"))

    try:
        import debugpy  # type: ignore
    except Exception as exc:
        raise SystemExit(f"DEBUGPY_ENABLE=true but debugpy is not available: {exc}") from exc

    try:
        debugpy.listen((host, port))
    except RuntimeError as exc:
        if "already been started" not in str(exc).lower():
            raise

    LOG.warning(
        "debugpy enabled host=%s port=%s wait_for_client=%s break_on_start=%s",
        host,
        port,
        wait_for_client,
        break_on_start,
    )
    if wait_for_client:
        LOG.warning("debugpy waiting for VS Code client attach...")
        debugpy.wait_for_client()
    if break_on_start:
        debugpy.breakpoint()


def _decode_header(value: str) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    out: list[str] = []
    for part, enc in parts:
        if isinstance(part, bytes):
            out.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(part))
    return "".join(out).strip()


def _uid_to_int(uid: bytes) -> int:
    try:
        return int(uid.decode())
    except Exception:
        return -1


def _normalize_gmail_label(label: str) -> str:
    value = str(label or "").strip().strip('"').strip("'")
    if value.startswith("\\"):
        value = value[1:]
    return value.casefold()


def _parse_x_gm_labels(fetch_text: str) -> set[str]:
    raw = str(fetch_text or "")
    if not raw:
        return set()
    match = re.search(r"X-GM-LABELS\s+\((.*?)\)", raw, re.IGNORECASE)
    if not match:
        return set()
    labels_blob = match.group(1).strip()
    if not labels_blob:
        return set()
    try:
        tokens = shlex.split(labels_blob)
    except Exception:
        tokens = labels_blob.split()
    out: set[str] = set()
    for token in tokens:
        norm = _normalize_gmail_label(token)
        if norm:
            out.add(norm)
    return out


def _fetch_uid_labels(mail: imaplib.IMAP4_SSL, uid: bytes) -> set[str]:
    status, data = mail.uid("FETCH", uid, "(X-GM-LABELS)")
    if status != "OK" or not data:
        LOG.warning("uid label fetch failed uid=%s status=%s", uid.decode(errors="ignore"), status)
        return set()

    labels: set[str] = set()
    for row in data:
        if isinstance(row, tuple):
            for part in row:
                if isinstance(part, (bytes, bytearray)):
                    labels |= _parse_x_gm_labels(part.decode("utf-8", errors="replace"))
        elif isinstance(row, (bytes, bytearray)):
            labels |= _parse_x_gm_labels(bytes(row).decode("utf-8", errors="replace"))
    return labels


def _uid_has_excluded_label(
    mail: imaplib.IMAP4_SSL,
    uid: bytes,
    *,
    excluded_labels: set[str],
) -> bool:
    if not excluded_labels:
        return False
    labels = _fetch_uid_labels(mail, uid)
    return bool(labels & excluded_labels)


def _imap_label_expr(labels: list[str]) -> str:
    escaped_labels: list[str] = []
    for label in labels:
        value = str(label or "").strip()
        if not value:
            continue
        value = value.replace("\\", "\\\\").replace('"', '\\"')
        escaped_labels.append(f'"{value}"')
    return "(" + " ".join(escaped_labels) + ")"


def _ensure_gmail_label(mail: imaplib.IMAP4_SSL, label: str) -> None:
    value = str(label or "").strip()
    if not value:
        return
    try:
        mail.create(value)
    except Exception:
        return


def _apply_managed_gmail_label(mail: imaplib.IMAP4_SSL, uid: bytes, target_label: str) -> dict[str, Any]:
    target_value = str(target_label or "").strip()
    target_norm = _normalize_gmail_label(target_value) if target_value else ""
    current_labels = _fetch_uid_labels(mail, uid)
    current_norms = {_normalize_gmail_label(label) for label in current_labels}

    labels_to_remove = [
        label
        for label in MANAGED_GMAIL_LABELS
        if _normalize_gmail_label(label) in current_norms and _normalize_gmail_label(label) != target_norm
    ]
    labels_to_add = []
    if target_value and target_norm not in current_norms:
        labels_to_add.append(target_value)

    if labels_to_remove:
        status, _ = mail.uid("STORE", uid, "-X-GM-LABELS", _imap_label_expr(labels_to_remove))
        if status != "OK":
            raise RuntimeError(f"failed to remove gmail labels uid={uid.decode(errors='ignore')} labels={labels_to_remove}")

    if labels_to_add:
        for label in labels_to_add:
            _ensure_gmail_label(mail, label)
        status, _ = mail.uid("STORE", uid, "+X-GM-LABELS", _imap_label_expr(labels_to_add))
        if status != "OK":
            raise RuntimeError(f"failed to add gmail labels uid={uid.decode(errors='ignore')} labels={labels_to_add}")

    return {
        "current_labels": sorted(current_labels),
        "target_label": target_value,
        "labels_removed": labels_to_remove,
        "labels_added": labels_to_add,
        "changed": bool(labels_to_remove or labels_to_add),
    }


def _normalize_datetime(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""

    # Traduzir datas em Português geradas por Outlook/Gmail
    pt_months = {
        " de ": " ", "janeiro": "Jan", "fevereiro": "Feb", 
        "março": "Mar", "marco": "Mar", "abril": "Apr", 
        "maio": "May", "junho": "Jun", "julho": "Jul", 
        "agosto": "Aug", "setembro": "Sep", "outubro": "Oct", 
        "novembro": "Nov", "dezembro": "Dec"
    }
    
    clean_raw = raw.lower()
    for pt, en in pt_months.items():
        clean_raw = clean_raw.replace(pt, en)

    try:
        dt = parsedate_to_datetime(clean_raw)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    return ""


def _normalize_received_at(value: str) -> str:
    normalized = _normalize_datetime(value)
    if normalized:
        return normalized
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _extract_sender(from_header: str) -> tuple[str, str]:
    sender = _decode_header(from_header)
    name = sender
    email_addr = "unknown@example.com"
    if "<" in sender and ">" in sender:
        name = sender.split("<", 1)[0].strip().strip('"') or sender
        email_addr = sender.split("<", 1)[1].split(">", 1)[0].strip() or email_addr
    elif "@" in sender:
        email_addr = sender.strip().strip('"')
    return name, email_addr


def _get_best_html(msg: Message) -> str:
    html_parts: list[str] = []
    text_parts: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            ctype = part.get_content_type()
            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if ctype == "text/html":
                html_parts.append(text)
            elif ctype == "text/plain":
                text_parts.append(text)
    else:
        payload = msg.get_payload(decode=True) or b""
        charset = msg.get_content_charset() or "utf-8"
        text = payload.decode(charset, errors="replace")
        if msg.get_content_type() == "text/html":
            html_parts.append(text)
        else:
            text_parts.append(text)

    if html_parts:
        return max(html_parts, key=len)
    if text_parts:
        plain = "\n\n".join(text_parts)
        return f"<html><body><pre>{plain}</pre></body></html>"
    return ""


def _looks_like_plaintext_wrapper_html(value: str) -> bool:
    html = (value or "").strip().lower()
    if not html:
        return False
    compact = re.sub(r"\s+", "", html)
    return compact.startswith("<html><body><pre>") and compact.endswith("</pre></body></html>")


def _extract_forwarded_message(msg: Message) -> Message | None:
    # Estratégia 1: forward como anexo (message/rfc822)
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() != "message/rfc822":
                continue
            payload = part.get_payload()
            if isinstance(payload, list) and payload and isinstance(payload[0], Message):
                return payload[0]
            if isinstance(payload, Message):
                return payload

    # Estratégia 2: forward inline (Outlook/Gmail — cabeçalho no body de texto)
    # Procura padrão: "From: Nome <email>\r\nSent: ..." ou "From: Nome <email>\r\nDate: ..."
    body_text = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                charset = part.get_content_charset() or "utf-8"
                try:
                    body_text = part.get_payload(decode=True).decode(charset, errors="replace")
                    break
                except Exception:
                    continue
    else:
        if msg.get_content_type() == "text/plain":
            charset = msg.get_content_charset() or "utf-8"
            try:
                body_text = msg.get_payload(decode=True).decode(charset, errors="replace")
            except Exception:
                pass

    if body_text:
        # Padrão Outlook: From: ... \n Sent: ...
        # Padrão Gmail:   From: ... \n Date: ...
        # Padrão Outlook PT: De: ... \n Enviado: ...
        match = re.search(
            r"(?:From|De):\s*(.+?)\r?\n(?:Sent|Date|Enviado):\s*(.+?)\r?\n",
            body_text,
            re.IGNORECASE,
        )
        if match:
            synthetic = Message()
            synthetic["From"] = match.group(1).strip()
            synthetic["Date"] = match.group(2).strip()
            # Preserve inline forwarded body so downstream extraction still has full content.
            synthetic.set_type("text/plain")
            synthetic.set_payload(body_text)
            return synthetic

    return None


def connect_gmail(address: str, app_password: str) -> imaplib.IMAP4_SSL:
    mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
    mail.login(address, app_password)
    mail.select("INBOX")  # Seleciona sempre a caixa de entrada para permitir extrações
    return mail


def _is_imap_connection_error(exc: BaseException) -> bool:
    text = str(exc).strip().lower()
    if any(token in text for token in ("broken pipe", "socket error", "connection closed", "connection reset", "timed out")):
        return True
    return isinstance(exc, (imaplib.IMAP4.abort, TimeoutError, ConnectionError, OSError))


def _is_imap_overquota_error(exc: BaseException) -> bool:
    text = str(exc).strip().upper()
    return "OVERQUOTA" in text or "BANDWIDTH LIMITS" in text


def fetch_all_inbox_emails(mail: imaplib.IMAP4_SSL, *, min_uid: int = 0, limit: int = 0) -> list[bytes]:
    mail.select("INBOX")
    uid_values: list[bytes] = []
    min_uid_int = max(0, int(min_uid))
    start_uid = max(1, min_uid_int + 1)
    uid_range = f"{start_uid}:*"

    if ALLOWED_FORWARDER_EMAILS:
        for sender in ALLOWED_FORWARDER_EMAILS:
            status, data = mail.uid("SEARCH", None, "UID", uid_range, "HEADER", "FROM", sender)
            if status != "OK":
                continue
            uid_values.extend((data[0] or b"").split())
    else:
        status, data = mail.uid("SEARCH", None, "UID", uid_range)
        if status != "OK":
            return []
        uid_values = (data[0] or b"").split()

    # Defensive guard:
    # Some IMAP servers (observed on Gmail) may return the boundary UID again
    # even when querying with UID <cursor+1>:*. Keep only strictly newer UIDs.
    dedup = sorted({uid for uid in uid_values if uid}, key=_uid_to_int)
    filtered = [uid for uid in dedup if _uid_to_int(uid) > min_uid_int]

    excluded_labels = {_normalize_gmail_label(label) for label in GMAIL_EXCLUDED_LABELS if _normalize_gmail_label(label)}
    if excluded_labels:
        kept: list[bytes] = []
        skipped = 0
        for uid in filtered:
            if _uid_has_excluded_label(mail, uid, excluded_labels=excluded_labels):
                skipped += 1
                continue
            kept.append(uid)
            if limit > 0 and len(kept) >= limit:
                break
        if skipped:
            LOG.info(
                "ingest skipped %s message(s) due excluded gmail labels=%s",
                skipped,
                ",".join(sorted(excluded_labels)),
            )
        return kept

    if limit > 0:
        return filtered[:limit]
    return filtered


def _fetch_uid_header_bytes(mail: imaplib.IMAP4_SSL, uid: bytes) -> bytes:
    status, data = mail.uid("FETCH", uid, "(BODY.PEEK[HEADER])")
    if status != "OK" or not data:
        raise RuntimeError("uid header fetch failed")
    for item in data:
        if isinstance(item, tuple) and len(item) > 1 and isinstance(item[1], (bytes, bytearray)):
            return bytes(item[1])
    raise RuntimeError("uid header payload missing")


def extract_email_header_data(mail: imaplib.IMAP4_SSL, uid: bytes) -> dict[str, Any]:
    raw_header = _fetch_uid_header_bytes(mail, uid)
    msg = email.message_from_bytes(raw_header)

    message_id = (msg.get("Message-ID") or f"uid-{uid.decode()}").strip("<>")
    sender_name, sender_email = _extract_sender(msg.get("From", ""))
    subject = _decode_header(msg.get("Subject", "(no subject)"))
    received_at_raw = msg.get("Date", "")
    received_at = _normalize_received_at(received_at_raw)

    # Header-only ingest: preserve minimum metadata; full source/body is fetched on-demand at processing time.
    return {
        "gmail_uid": uid.decode(),
        "gmail_message_id": message_id,
        "sender_name": sender_name,
        "sender_email": sender_email,
        "subject": subject,
        "received_at": received_at,
        "original_sender_name": sender_name,
        "original_sender_email": sender_email,
        "original_sent_at_raw": received_at_raw,
        "original_sent_at": _normalize_datetime(received_at_raw),
        "raw_html": "",
    }


def extract_email_data(mail: imaplib.IMAP4_SSL, uid: bytes) -> dict[str, Any]:
    status, data = mail.uid("FETCH", uid, "(RFC822)")
    if status != "OK" or not data or not data[0]:
        raise RuntimeError("uid fetch failed")

    raw = data[0][1]
    msg = email.message_from_bytes(raw)
    nested = _extract_forwarded_message(msg)

    message_id = (msg.get("Message-ID") or f"uid-{uid.decode()}").strip("<>")
    sender_name, sender_email = _extract_sender(msg.get("From", ""))
    subject = _decode_header(msg.get("Subject", "(no subject)"))
    received_at_raw = msg.get("Date", "")

    original_msg = nested or msg
    original_sender_name, original_sender_email = _extract_sender(original_msg.get("From", ""))
    original_sent_at_raw = original_msg.get("Date", "")
    original_sent_at = _normalize_datetime(original_sent_at_raw)

    original_html = _get_best_html(original_msg)
    parent_html = _get_best_html(msg)
    if nested and _looks_like_plaintext_wrapper_html(original_html) and parent_html:
        raw_html = parent_html
    else:
        raw_html = original_html or parent_html

    return {
        "gmail_uid": uid.decode(),
        "gmail_message_id": message_id,
        "sender_name": sender_name,
        "sender_email": sender_email,
        "subject": subject,
        "received_at": _normalize_received_at(received_at_raw),
        "original_sender_name": original_sender_name,
        "original_sender_email": original_sender_email,
        "original_sent_at_raw": original_sent_at_raw,
        "original_sent_at": original_sent_at,
        "raw_html": raw_html,
    }


def _api_get(cfg: Config, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"X-API-Key": cfg.agent_api_key}
    resp = requests.get(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _api_post(cfg: Config, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json", "X-API-Key": cfg.agent_api_key}
    resp = requests.post(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def register_newsletter(cfg: Config, payload: dict[str, Any]) -> dict[str, Any]:
    return _api_post(cfg, "newsletter/register/", payload)


def _load_ingest_cursor(cfg: Config) -> int:
    try:
        payload = _api_get(cfg, "newsletter/ingest-cursor/", {})
        return max(0, int(payload.get("max_gmail_uid", 0)))
    except Exception as exc:
        LOG.warning("failed to load ingest cursor; fallback full scan err=%s", exc)
        return 0


def ingest_inbox(mail: imaplib.IMAP4_SSL, cfg: Config, *, limit: int | None = None) -> dict[str, int]:
    cursor_uid = _load_ingest_cursor(cfg)
    fetch_limit = INGEST_MAX_NEW_PER_CYCLE if limit is None else max(0, int(limit))
    uids = fetch_all_inbox_emails(
        mail,
        min_uid=cursor_uid,
        limit=(fetch_limit if fetch_limit > 0 else 0),
    )

    if uids:
        LOG.info(
            "ingest window: cursor_uid=%s candidates=%s first_uid=%s last_uid=%s",
            cursor_uid,
            len(uids),
            uids[0].decode(errors="ignore"),
            uids[-1].decode(errors="ignore"),
        )

    created = 0
    duplicate = 0
    failed = 0

    for uid in uids:
        try:
            payload = extract_email_header_data(mail, uid)
            result = register_newsletter(cfg, payload)
            status = str(result.get("status", "")).strip().lower()
            if status == "created":
                created += 1
            else:
                duplicate += 1
        except Exception as exc:
            failed += 1
            LOG.warning("ingest failed uid=%s err=%s", uid.decode(errors="ignore"), exc)
            if _is_imap_connection_error(exc):
                LOG.warning("imap connection dropped during ingest; forcing reconnect")
                raise

    return {
        "scanned": len(uids),
        "available": len(uids),
        "cursor_uid": cursor_uid,
        "created": created,
        "duplicate": duplicate,
        "failed": failed,
    }


def _extract_telegram_token_chat_id(payload: dict[str, Any]) -> tuple[str, str]:
    # Preferencial/documentado: integrations.telegram
    for root_key in ("integrations", "channels"):
        root = payload.get(root_key, {})
        if not isinstance(root, dict):
            continue
        tg = root.get("telegram", {})
        if not isinstance(tg, dict):
            continue

        token = str(tg.get("botToken", "")).strip()
        chat_id = ""
        allow = tg.get("allowFrom", [])
        if isinstance(allow, list) and allow:
            chat_id = str(allow[0]).strip()
        if not chat_id:
            chat_id = str(tg.get("chatId", "")).strip()

        if token and chat_id:
            return token, chat_id

    return "", ""


def _resolve_telegram_config(cfg: Config) -> tuple[str, str]:
    global _TELEGRAM_CONFIG_CACHE

    if _TELEGRAM_CONFIG_CACHE and _TELEGRAM_CONFIG_CACHE[0] and _TELEGRAM_CONFIG_CACHE[1]:
        return _TELEGRAM_CONFIG_CACHE

    if REVIEW_TELEGRAM_BOT_TOKEN and REVIEW_TELEGRAM_CHAT_ID:
        _TELEGRAM_CONFIG_CACHE = (REVIEW_TELEGRAM_BOT_TOKEN, REVIEW_TELEGRAM_CHAT_ID)
        return _TELEGRAM_CONFIG_CACHE

    path = Path(OPENCLAW_CONFIG_PATH)
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            token, chat_id = _extract_telegram_token_chat_id(payload)
            if token and chat_id:
                _TELEGRAM_CONFIG_CACHE = (token, chat_id)
                return _TELEGRAM_CONFIG_CACHE
        except Exception as exc:
            LOG.warning("failed to parse openclaw config path=%s err=%s", path, exc)

    try:
        import docker  # Lazy import to avoid hard dependency when notifications are disabled.

        client = docker.from_env()
        container = client.containers.get(cfg.openclaw_container_name)
        exec_result = container.exec_run(
            ["cat", "/root/.openclaw/openclaw.json"],
            stdout=True,
            stderr=True,
            demux=False,
            tty=False,
        )
        if isinstance(exec_result, tuple):
            exit_code, raw_output = exec_result
        else:
            exit_code = int(getattr(exec_result, "exit_code", 1))
            raw_output = getattr(exec_result, "output", b"")
        if exit_code == 0:
            content = raw_output.decode("utf-8", errors="replace") if isinstance(raw_output, bytes) else str(raw_output)
            payload = json.loads(content)
            token, chat_id = _extract_telegram_token_chat_id(payload)
            if token and chat_id:
                _TELEGRAM_CONFIG_CACHE = (token, chat_id)
                return _TELEGRAM_CONFIG_CACHE
    except Exception as exc:
        LOG.warning("failed to resolve telegram config via container=%s err=%s", cfg.openclaw_container_name, exc)

    # Não cachear vazio para permitir retries em ciclos seguintes.
    return "", ""


def _send_telegram_message(token: str, chat_id: str, text: str) -> None:
    resp = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    resp.raise_for_status()


def _is_paywall_article(article: dict[str, Any]) -> bool:
    if bool(article.get("manual_review_required")):
        return True
    mode = str(article.get("summary_source_mode", "")).strip().lower()
    return mode == "manual_review_required"


def _decision_state(article: dict[str, Any]) -> str:
    state = str(article.get("review_decision", "")).strip().lower()
    if state in {"approved", "rejected", "pending"}:
        return state
    return "pending"


def _next_pending_article(articles: list[Any]) -> tuple[int, dict[str, Any]] | tuple[None, None]:
    for idx, article in enumerate(articles, start=1):
        if not isinstance(article, dict):
            continue
        if _decision_state(article) != "pending":
            continue
        # Gating estrito: se o primeiro pending já foi notificado, aguardar decisão.
        if article.get("review_notified_at"):
            return None, None
        return idx, article
    return None, None


def _email_meta_lines(email_meta: dict[str, Any] | None) -> list[str]:
    if not isinstance(email_meta, dict):
        return []

    raw_subject = str(email_meta.get("subject", "")).strip()
    # Remove prefixos como "FW:", "Fwd:", "RE:" para mostrar apenas o assunto original
    clean_subject = re.sub(r"^(?:\s*(?:Fwd|FW|FWD|Re|RE)\s*:\s*)+", "", raw_subject, flags=re.IGNORECASE).strip()
    
    received_at = str(email_meta.get("received_at", "")).strip()
    orig_name = str(email_meta.get("original_sender_name", "")).strip()
    orig_email = str(email_meta.get("original_sender_email", "")).strip()
    sent_raw = str(email_meta.get("original_sent_at_raw", "")).strip()
    sent_norm = str(email_meta.get("original_sent_at", "")).strip()

    lines: list[str] = []
    if clean_subject:
        lines.append(f"Email subject: {clean_subject}")
    if received_at:
        lines.append(f"Received at: {received_at}")
    sender_label = ""
    if orig_name and orig_email:
        sender_label = f"{orig_name} <{orig_email}>"
    elif orig_email:
        sender_label = orig_email
    elif orig_name:
        sender_label = orig_name
    if sender_label:
        lines.append(f"Original sender: {sender_label}")
    if sent_raw or sent_norm:
        lines.append(f"Original sent at: {sent_raw or sent_norm}")
    return lines


def _build_article_review_message(
    newsletter_id: int,
    article_index: int,
    article: dict[str, Any],
    *,
    email_meta: dict[str, Any] | None = None,
) -> str:
    title = str(article.get("title", "")).strip() or f"Article {article_index}"
    source_url = str(article.get("original_url", "")).strip() or "n/a"
    source_origin = str(article.get("source_origin", "")).strip().lower()
    if source_origin == "direct":
        source_label = "source (direct)"
    elif source_origin == "inferred":
        source_label = "source (discovered)"
    else:
        source_label = "source"
    paywall = _is_paywall_article(article)

    lines = [f"Review required: Newsletter #{newsletter_id} / Article #{article_index}"]
    lines.extend(_email_meta_lines(email_meta))
    lines.extend(
        [
            f"1) Title: {title}",
            f"2) {source_label}: {source_url}",
            f"3) paywall: {'yes' if paywall else 'no'}",
        ]
    )

    source_missing = source_url == "n/a"
    if paywall:
        lines.append("4) helper links:")
        lines.append("   - https://smry.ai/pt")
        lines.append("   - https://removepaywalls.com/")
    lines.append("")
    if paywall or source_missing:
        lines.append("If you decide to approve, provide the link or text of the full article")
    else:
        lines.append("If you decide to approve, reply 'approved'")

    message = "\n".join(lines).strip()
    if len(message) > 3900:
        message = message[:3890].rstrip() + "\n\n[truncated]"
    return message


def send_next_review_notification(
    cfg: Config,
    newsletter_id: int,
    *,
    review_file: str | None = None,
    include_intro: bool = False,
    email_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not REVIEW_TELEGRAM_NOTIFY:
        return {"sent": False, "reason": "disabled"}

    draft_file = str(review_file or "").strip() or str(Path(cfg.review_output_dir) / f"newsletter_{newsletter_id}_draft.json")

    draft = json.loads(Path(draft_file).read_text(encoding="utf-8"))
    articles = draft.get("articles", []) if isinstance(draft, dict) else []
    if not isinstance(articles, list):
        articles = []

    decision_rows = [row for row in articles if isinstance(row, dict)]
    pending_count = sum(1 for row in decision_rows if _decision_state(row) == "pending")
    if pending_count == 0:
        approved_count = sum(1 for row in decision_rows if _decision_state(row) == "approved")
        rejected_count = sum(1 for row in decision_rows if _decision_state(row) == "rejected")

        status_sync = {"ok": True}
        try:
            _api_post(
                cfg,
                "newsletter/status/",
                {
                    "newsletter_id": newsletter_id,
                    "status": "completed",
                    "error_message": "",
                },
            )
        except Exception as exc:
            status_sync = {"ok": False, "error": str(exc)}
            LOG.warning(
                "failed to finalize newsletter status newsletter_id=%s err=%s",
                newsletter_id,
                exc,
            )

        return {
            "sent": False,
            "reason": "no_pending_articles",
            "pending_count": 0,
            "decision_counts": {
                "approved": approved_count,
                "rejected": rejected_count,
                "pending": 0,
            },
            "newsletter_status_sync": status_sync,
            "review_file": draft_file,
        }

    token, chat_id = _resolve_telegram_config(cfg)
    if not token or not chat_id:
        LOG.warning("telegram review notification skipped: missing bot token/chat id")
        return {"sent": False, "reason": "missing_telegram_config", "review_file": draft_file}

    next_idx, next_article = _next_pending_article(articles)
    if next_idx is None or next_article is None:
        return {
            "sent": False,
            "reason": "waiting_for_decision",
            "pending_count": pending_count,
            "review_file": draft_file,
        }

    # Intro header disabled by UX request:
    # send only the actionable "Review required: Newsletter #N / Article #M" message.

    text = _build_article_review_message(newsletter_id, next_idx, next_article, email_meta=email_meta)
    _send_telegram_message(token, chat_id, text)

    now_iso = datetime.now(tz=UTC).isoformat()
    next_article["review_notified_at"] = now_iso
    attempts = next_article.get("review_notification_attempts", 0)
    try:
        attempts_int = int(attempts)
    except Exception:
        attempts_int = 0
    next_article["review_notification_attempts"] = attempts_int + 1
    draft["articles"] = articles
    draft["updated_at"] = now_iso

    persisted = True
    try:
        Path(draft_file).write_text(json.dumps(draft, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        persisted = False
        LOG.warning("failed to persist review notification state draft=%s err=%s", draft_file, exc)

    return {
        "sent": True,
        "newsletter_id": newsletter_id,
        "article_index": next_idx,
        "pending_count": pending_count,
        "review_file": draft_file,
        "state_persisted": persisted,
    }


def _send_review_notification(
    cfg: Config,
    newsletter_id: int,
    *,
    review_file: str,
    email_meta: dict[str, Any] | None,
) -> None:
    if not REVIEW_TELEGRAM_NOTIFY:
        return

    try:
        send_next_review_notification(
            cfg,
            newsletter_id,
            review_file=review_file,
            include_intro=True,
            email_meta=email_meta,
        )
    except Exception as exc:
        LOG.warning("telegram review notification failed newsletter_id=%s err=%s", newsletter_id, exc)


def _fetch_newsletters_by_status(
    cfg: Config,
    status: str,
    limit: int = 50,
    *,
    mode: str = "oldest",
) -> list[dict[str, Any]]:
    payload = _api_get(cfg, "newsletter/pending/", {"status": status, "limit": limit, "mode": mode})
    rows = payload.get("newsletters", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _fetch_newsletter_workflow_state(cfg: Config, newsletter_id: int) -> dict[str, Any]:
    payload = _api_get(cfg, f"newsletter/{newsletter_id}/workflow-state/")
    newsletter = payload.get("newsletter", {}) if isinstance(payload, dict) else {}
    return newsletter if isinstance(newsletter, dict) else {}


def _has_active_newsletter_work(cfg: Config) -> bool:
    for status in ("review", "processing", "pending"):
        if _fetch_newsletters_by_status(cfg, status, limit=1):
            return True
    return False


def _sync_newsletter_gmail_label(mail: imaplib.IMAP4_SSL, cfg: Config, newsletter_id: int) -> dict[str, Any]:
    workflow = _fetch_newsletter_workflow_state(cfg, newsletter_id)
    uid_raw = str(workflow.get("gmail_uid", "")).strip()
    if not uid_raw:
        return {"newsletter_id": newsletter_id, "changed": False, "reason": "missing_gmail_uid"}
    sync_result = _apply_managed_gmail_label(mail, uid_raw.encode(), str(workflow.get("gmail_label", "")).strip())
    sync_result["newsletter_id"] = newsletter_id
    sync_result["gmail_uid"] = uid_raw
    sync_result["workflow_status"] = str(workflow.get("status", "")).strip()
    sync_result["workflow_label"] = str(workflow.get("gmail_label", "")).strip()
    sync_result["subject"] = str(workflow.get("subject", "")).strip()
    return sync_result


def _sync_single_status_gmail_label(
    mail: imaplib.IMAP4_SSL,
    cfg: Config,
    *,
    status: str,
    mode: str = "oldest",
) -> dict[str, Any] | None:
    summary = _sync_status_gmail_labels(cfg, status=status, limit=1, mode=mode)
    rows = summary.get("results", []) if isinstance(summary, dict) else []
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    return first if isinstance(first, dict) else None


def _get_gmail_credentials() -> tuple[str, str]:
    gmail_address = os.getenv("GMAIL_ADDRESS", "").strip()
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()
    if not gmail_address or not gmail_app_password:
        raise RuntimeError("GMAIL_ADDRESS/GMAIL_APP_PASSWORD missing")
    return gmail_address, gmail_app_password


def _sync_status_gmail_labels(
    cfg: Config,
    *,
    status: str,
    mode: str = "oldest",
    limit: int = 50,
) -> dict[str, Any]:
    rows = _fetch_newsletters_by_status(cfg, status, limit=limit, mode=mode)
    if not rows:
        return {
            "status": status,
            "mode": mode,
            "newsletter_count": 0,
            "synced": 0,
            "changed": 0,
            "results": [],
            "errors": [],
        }

    gmail_address, gmail_app_password = _get_gmail_credentials()
    mail: imaplib.IMAP4_SSL | None = None
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    synced = 0
    changed = 0

    def _connect() -> imaplib.IMAP4_SSL:
        session = connect_gmail(gmail_address, gmail_app_password)
        try:
            status_sel, _ = session.select("INBOX")
        except Exception:
            _safe_logout(session)
            raise
        if status_sel != "OK":
            _safe_logout(session)
            raise RuntimeError(f"failed to select inbox for gmail label sync status={status_sel}")
        return session

    try:
        mail = _connect()
        for row in rows:
            try:
                newsletter_id = int(row.get("id") or 0)
            except Exception:
                newsletter_id = 0
            if newsletter_id <= 0:
                errors.append({"newsletter_id": row.get("id"), "error": "invalid_newsletter_id"})
                continue

            for attempt in range(2):
                try:
                    result = _sync_newsletter_gmail_label(mail, cfg, newsletter_id)
                    results.append(result)
                    synced += 1
                    if bool(result.get("changed")):
                        changed += 1
                    break
                except Exception as exc:
                    last_attempt = attempt == 1
                    if _is_imap_connection_error(exc) and not last_attempt:
                        LOG.warning(
                            "gmail label sync reconnect newsletter_id=%s status=%s err=%s",
                            newsletter_id,
                            status,
                            exc,
                        )
                        _safe_logout(mail)
                        mail = _connect()
                        continue
                    errors.append({"newsletter_id": newsletter_id, "error": str(exc)})
                    LOG.warning(
                        "gmail label sync failed newsletter_id=%s status=%s err=%s",
                        newsletter_id,
                        status,
                        exc,
                    )
                    break
    finally:
        _safe_logout(mail)

    return {
        "status": status,
        "mode": mode,
        "newsletter_count": len(rows),
        "synced": synced,
        "changed": changed,
        "results": results,
        "errors": errors,
    }


def _review_draft_path(cfg: Config, newsletter_id: int) -> Path:
    return Path(cfg.review_output_dir) / f"newsletter_{newsletter_id}_draft.json"


def _recover_review_gate_startup(cfg: Config, *, limit: int = 200) -> dict[str, int]:
    """
    Recover invalid/stale review gate states at startup.

    Cases handled:
    - review status with missing/invalid draft file -> requeue as pending.
    - review status with draft but no pending article decisions -> mark completed.
    - review status with pending decisions and no notification marker -> send first notification.
    """
    rows = _fetch_newsletters_by_status(cfg, "review", limit=limit)
    stats = {"review_rows": len(rows), "requeued_pending": 0, "completed": 0, "renotified": 0, "errors": 0}
    if not rows:
        return stats

    for row in rows:
        newsletter_id_raw = row.get("id")
        try:
            newsletter_id = int(newsletter_id_raw)
        except Exception:
            stats["errors"] += 1
            continue
        if newsletter_id <= 0:
            stats["errors"] += 1
            continue

        draft_file = _review_draft_path(cfg, newsletter_id)
        if not draft_file.exists():
            try:
                _api_post(
                    cfg,
                    "newsletter/status/",
                    {
                        "newsletter_id": newsletter_id,
                        "status": "pending",
                        "error_message": "Auto-recovered: missing review draft file.",
                    },
                )
                stats["requeued_pending"] += 1
            except Exception as exc:
                stats["errors"] += 1
                LOG.warning(
                    "startup review recovery failed to requeue newsletter_id=%s err=%s",
                    newsletter_id,
                    exc,
                )
            continue

        try:
            draft = json.loads(draft_file.read_text(encoding="utf-8"))
        except Exception as exc:
            try:
                _api_post(
                    cfg,
                    "newsletter/status/",
                    {
                        "newsletter_id": newsletter_id,
                        "status": "pending",
                        "error_message": f"Auto-recovered: invalid review draft ({exc}).",
                    },
                )
                stats["requeued_pending"] += 1
            except Exception as inner_exc:
                stats["errors"] += 1
                LOG.warning(
                    "startup review recovery failed invalid draft newsletter_id=%s err=%s",
                    newsletter_id,
                    inner_exc,
                )
            continue

        articles = draft.get("articles", []) if isinstance(draft, dict) else []
        if not isinstance(articles, list):
            articles = []
        pending_rows = [a for a in articles if isinstance(a, dict) and _decision_state(a) == "pending"]

        if not pending_rows:
            try:
                _api_post(
                    cfg,
                    "newsletter/status/",
                    {
                        "newsletter_id": newsletter_id,
                        "status": "completed",
                        "error_message": "",
                    },
                )
                stats["completed"] += 1
            except Exception as exc:
                stats["errors"] += 1
                LOG.warning(
                    "startup review recovery failed complete newsletter_id=%s err=%s",
                    newsletter_id,
                    exc,
                )
            continue

        has_pending_notified = any(str(row_item.get("review_notified_at", "")).strip() for row_item in pending_rows)
        if has_pending_notified:
            continue

        email_meta = {
            "subject": row.get("subject"),
            "sender_name": row.get("sender_name"),
            "sender_email": row.get("sender_email"),
            "original_sender_name": row.get("original_sender_name"),
            "original_sender_email": row.get("original_sender_email"),
            "original_sent_at": row.get("original_sent_at"),
            "original_sent_at_raw": row.get("original_sent_at_raw"),
            "received_at": row.get("received_at"),
        }
        try:
            notif = send_next_review_notification(
                cfg,
                newsletter_id,
                review_file=str(draft_file),
                include_intro=False,
                email_meta=email_meta,
            )
            if bool(notif.get("sent")):
                stats["renotified"] += 1
        except Exception as exc:
            stats["errors"] += 1
            LOG.warning(
                "startup review recovery notification failed newsletter_id=%s err=%s",
                newsletter_id,
                exc,
            )

    return stats


def _has_review_pending(cfg: Config) -> bool:
    rows = _fetch_newsletters_by_status(cfg, "review", limit=1)
    return bool(rows)


def _fetch_next_pending(cfg: Config) -> dict[str, Any] | None:
    rows = _fetch_newsletters_by_status(cfg, "pending", limit=1)
    if not rows:
        return None
    return rows[0]


def _fetch_newsletter_raw(cfg: Config, newsletter_id: int) -> dict[str, Any]:
    return _api_get(cfg, f"newsletter/{newsletter_id}/raw/")


def _hydrate_newsletter_raw_html(
    mail: imaplib.IMAP4_SSL,
    cfg: Config,
    row: dict[str, Any],
    newsletter_id: int,
) -> dict[str, Any]:
    uid_raw = str(row.get("gmail_uid", "")).strip()
    if not uid_raw:
        raise RuntimeError(f"missing gmail_uid for newsletter_id={newsletter_id}")
    payload = extract_email_data(mail, uid_raw.encode())
    # Keep known DB message id if parser fallback produced synthetic id.
    known_message_id = str(row.get("gmail_message_id", "")).strip()
    if known_message_id:
        payload["gmail_message_id"] = known_message_id
    register_newsletter(cfg, payload)
    refreshed = _fetch_newsletter_raw(cfg, newsletter_id)
    return refreshed


def process_next_pending(cfg: Config, mail: imaplib.IMAP4_SSL | None = None) -> dict[str, Any]:
    if _has_review_pending(cfg):
        return {"status": "blocked_review"}

    row = _fetch_next_pending(cfg)
    if not row:
        return {"status": "idle"}

    newsletter_id = int(row["id"])
    _api_post(cfg, "newsletter/status/", {"newsletter_id": newsletter_id, "status": "processing", "error_message": ""})
    raw_payload = _fetch_newsletter_raw(cfg, newsletter_id)
    raw_html = str(raw_payload.get("raw_html", "")).strip()

    if len(raw_html) < MIN_RAW_HTML_PROCESS_CHARS:
        if mail is None:
            _api_post(
                cfg,
                "newsletter/status/",
                {
                    "newsletter_id": newsletter_id,
                    "status": "error",
                    "error_message": "raw_html not hydrated and no IMAP session available",
                },
            )
            return {"status": "error", "newsletter_id": newsletter_id, "error": "raw_html not hydrated"}
        try:
            raw_payload = _hydrate_newsletter_raw_html(mail, cfg, row, newsletter_id)
            raw_html = str(raw_payload.get("raw_html", "")).strip()
            LOG.info(
                "hydrated newsletter raw_html newsletter_id=%s chars=%s uid=%s",
                newsletter_id,
                len(raw_html),
                str(row.get("gmail_uid", "")).strip(),
            )
        except Exception as exc:
            if _is_imap_connection_error(exc):
                _api_post(cfg, "newsletter/status/", {"newsletter_id": newsletter_id, "status": "pending", "error_message": ""})
                raise
            _api_post(
                cfg,
                "newsletter/status/",
                {
                    "newsletter_id": newsletter_id,
                    "status": "error",
                    "error_message": f"raw_html hydration failed: {exc}",
                },
            )
            return {"status": "error", "newsletter_id": newsletter_id, "error": str(exc)}

    if len(raw_html) < MIN_RAW_HTML_PROCESS_CHARS:
        _api_post(
            cfg,
            "newsletter/status/",
            {
                "newsletter_id": newsletter_id,
                "status": "error",
                "error_message": f"raw_html too short after hydration ({len(raw_html)} chars)",
            },
        )
        return {"status": "error", "newsletter_id": newsletter_id, "error": "raw_html too short"}

    try:
        result = process_single_newsletter(cfg, newsletter_id, raw_html)
    except Exception as exc:
        LOG.exception("processing crash newsletter_id=%s", newsletter_id)
        _api_post(
            cfg,
            "newsletter/status/",
            {
                "newsletter_id": newsletter_id,
                "status": "error",
                "error_message": f"processing crash: {exc}",
            },
        )
        return {"status": "error", "newsletter_id": newsletter_id, "error": str(exc)}
    result_status = str(result.get("status", "")).strip().lower()

    if result_status == "review":
        review_file = str(result.get("review_file", "")).strip() or str(
            Path(cfg.review_output_dir) / f"newsletter_{newsletter_id}_draft.json"
        )
        email_meta = {
            "subject": raw_payload.get("subject"),
            "sender_name": raw_payload.get("sender_name"),
            "sender_email": raw_payload.get("sender_email"),
            "original_sender_name": raw_payload.get("original_sender_name"),
            "original_sender_email": raw_payload.get("original_sender_email"),
            "original_sent_at": raw_payload.get("original_sent_at"),
            "original_sent_at_raw": raw_payload.get("original_sent_at_raw"),
            "received_at": raw_payload.get("received_at"),
        }
        _send_review_notification(cfg, newsletter_id, review_file=review_file, email_meta=email_meta)
        return {"status": "review", "newsletter_id": newsletter_id}

    if result_status == "error":
        return {"status": "error", "newsletter_id": newsletter_id, "error": result.get("error", "")}

    return {
        "status": "processed",
        "newsletter_id": newsletter_id,
        "result_status": result_status or "success",
    }


def run_backlog_mode(mail: imaplib.IMAP4_SSL, cfg: Config) -> None:
    if not _has_active_newsletter_work(cfg):
        try:
            _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
        except Exception as exc:
            LOG.warning("gmail label sync failed before backlog ingest err=%s", exc)
        stats = ingest_inbox(mail, cfg, limit=1)
    else:
        stats = {"scanned": 0, "created": 0, "duplicate": 0, "failed": 0}
    LOG.info("ingest stats: scanned=%s created=%s duplicate=%s failed=%s", stats["scanned"], stats["created"], stats["duplicate"], stats["failed"])

    while True:
        step = process_next_pending(cfg, mail=mail)
        status = step.get("status")

        if status == "processed":
            try:
                _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
            except Exception as exc:
                LOG.warning("gmail label sync failed after backlog processed err=%s", exc)
            continue
        if status == "review" or status == "blocked_review":
            try:
                _sync_status_gmail_labels(cfg, status="review", mode="oldest", limit=1)
            except Exception as exc:
                LOG.warning("gmail label sync failed during backlog review err=%s", exc)
            time.sleep(REVIEW_GATING_SLEEP_SECONDS)
            continue
        if status == "idle":
            if _has_review_pending(cfg):
                time.sleep(REVIEW_GATING_SLEEP_SECONDS)
                continue
            try:
                _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
            except Exception as exc:
                LOG.warning("gmail label sync failed at backlog completion err=%s", exc)
            LOG.info("backlog complete")
            return

        # erro transitório: regista e continua
        LOG.warning("backlog step returned status=%s details=%s", status, step)
        time.sleep(2)


def run_monitor_mode(mail: imaplib.IMAP4_SSL, cfg: Config) -> None:
    while True:
        try:
            if _has_active_newsletter_work(cfg):
                stats = {"scanned": 0, "created": 0, "duplicate": 0, "failed": 0}
            else:
                try:
                    _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
                except Exception as exc:
                    LOG.warning("gmail label sync failed before monitor ingest err=%s", exc)
                stats = ingest_inbox(mail, cfg, limit=1)
            LOG.info(
                "ingest stats: scanned=%s created=%s duplicate=%s failed=%s",
                stats["scanned"],
                stats["created"],
                stats["duplicate"],
                stats["failed"],
            )

            # Drena pending até ficar bloqueado por review ou sem trabalho.
            while True:
                step = process_next_pending(cfg, mail=mail)
                status = step.get("status")
                if status == "processed":
                    try:
                        _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
                    except Exception as exc:
                        LOG.warning("gmail label sync failed after monitor processed err=%s", exc)
                    continue
                if status in {"review", "blocked_review", "idle"}:
                    if status in {"review", "blocked_review"}:
                        try:
                            _sync_status_gmail_labels(cfg, status="review", mode="oldest", limit=1)
                        except Exception as exc:
                            LOG.warning("gmail label sync failed during monitor review err=%s", exc)
                    elif status == "idle":
                        try:
                            _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
                        except Exception as exc:
                            LOG.warning("gmail label sync failed during monitor idle err=%s", exc)
                    break
                LOG.warning("monitor step status=%s details=%s", status, step)
                break

        except Exception as exc:
            if _is_imap_connection_error(exc):
                raise
            LOG.exception("monitor loop error; continue in 10s")
            time.sleep(10)

        time.sleep(INGEST_INTERVAL_SECONDS)


def _sleep_with_stop(stop_event: threading.Event, seconds: int) -> None:
    try:
        timeout = max(0, int(seconds))
    except Exception:
        timeout = 0
    stop_event.wait(timeout=timeout)


def _safe_logout(mail: imaplib.IMAP4_SSL | None) -> None:
    if mail is None:
        return
    try:
        mail.logout()
    except Exception:
        pass


def _parallel_ingest_worker(
    stop_event: threading.Event,
    cfg: Config,
    gmail_address: str,
    gmail_app_password: str,
) -> None:
    mail: imaplib.IMAP4_SSL | None = None
    while not stop_event.is_set():
        if mail is None:
            try:
                mail = connect_gmail(gmail_address, gmail_app_password)
            except Exception as exc:
                if _is_imap_overquota_error(exc):
                    LOG.exception("parallel ingest: gmail overquota; cooldown %ss", IMAP_OVERQUOTA_SLEEP_SECONDS)
                    _sleep_with_stop(stop_event, IMAP_OVERQUOTA_SLEEP_SECONDS)
                    continue
                LOG.exception("parallel ingest: gmail connection failed; retry in %ss", IMAP_RECONNECT_SECONDS)
                _sleep_with_stop(stop_event, IMAP_RECONNECT_SECONDS)
                continue

        try:
            if _has_active_newsletter_work(cfg):
                _sleep_with_stop(stop_event, REVIEW_GATING_SLEEP_SECONDS)
                continue
            try:
                _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
            except Exception as exc:
                LOG.warning("parallel ingest: gmail label sync failed err=%s", exc)
            stats = ingest_inbox(mail, cfg, limit=1)
            LOG.info(
                "parallel ingest stats: scanned=%s created=%s duplicate=%s failed=%s",
                stats["scanned"],
                stats["created"],
                stats["duplicate"],
                stats["failed"],
            )
            if stats["scanned"] > 0:
                _sleep_with_stop(stop_event, PARALLEL_INGEST_ACTIVE_SLEEP_SECONDS)
            else:
                _sleep_with_stop(stop_event, INGEST_INTERVAL_SECONDS)
        except Exception as exc:
            if _is_imap_overquota_error(exc):
                LOG.exception("parallel ingest: gmail overquota; cooldown %ss", IMAP_OVERQUOTA_SLEEP_SECONDS)
                _safe_logout(mail)
                mail = None
                _sleep_with_stop(stop_event, IMAP_OVERQUOTA_SLEEP_SECONDS)
                continue
            if _is_imap_connection_error(exc):
                LOG.exception("parallel ingest: connection dropped; reconnect in %ss", IMAP_RECONNECT_SECONDS)
                _safe_logout(mail)
                mail = None
                _sleep_with_stop(stop_event, IMAP_RECONNECT_SECONDS)
                continue
            LOG.exception("parallel ingest: worker error; retry in 10s")
            _sleep_with_stop(stop_event, 10)

    _safe_logout(mail)


def _parallel_process_worker(
    stop_event: threading.Event,
    cfg: Config,
    gmail_address: str,
    gmail_app_password: str,
) -> None:
    mail: imaplib.IMAP4_SSL | None = None
    while not stop_event.is_set():
        try:
            if _has_review_pending(cfg):
                if mail is None:
                    mail = connect_gmail(gmail_address, gmail_app_password)
                try:
                    _sync_status_gmail_labels(cfg, status="review", mode="oldest", limit=1)
                except Exception as exc:
                    LOG.warning("parallel process: gmail label sync failed err=%s", exc)
                _sleep_with_stop(stop_event, REVIEW_GATING_SLEEP_SECONDS)
                continue

            if not _fetch_next_pending(cfg):
                try:
                    _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
                except Exception as exc:
                    LOG.warning("parallel process: completed gmail label sync failed on idle err=%s", exc)
                _sleep_with_stop(stop_event, PARALLEL_PROCESS_IDLE_SLEEP_SECONDS)
                continue

            if mail is None:
                mail = connect_gmail(gmail_address, gmail_app_password)

            step = process_next_pending(cfg, mail=mail)
            status = step.get("status")

            if status == "processed":
                try:
                    _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
                except Exception as exc:
                    LOG.warning("parallel process: completed gmail label sync failed after processed err=%s", exc)
                continue
            if status in {"review", "blocked_review"}:
                try:
                    _sync_status_gmail_labels(cfg, status="review", mode="oldest", limit=1)
                except Exception as exc:
                    LOG.warning("parallel process: review gmail label sync failed after decision err=%s", exc)
                _sleep_with_stop(stop_event, REVIEW_GATING_SLEEP_SECONDS)
                continue
            if status == "idle":
                try:
                    _sync_status_gmail_labels(cfg, status="completed", mode="oldest", limit=100)
                except Exception as exc:
                    LOG.warning("parallel process: completed gmail label sync failed on idle status err=%s", exc)
                _sleep_with_stop(stop_event, PARALLEL_PROCESS_IDLE_SLEEP_SECONDS)
                continue

            LOG.warning("parallel process step status=%s details=%s", status, step)
            _sleep_with_stop(stop_event, 2)
        except Exception as exc:
            if _is_imap_overquota_error(exc):
                LOG.exception("parallel process: gmail overquota; cooldown %ss", IMAP_OVERQUOTA_SLEEP_SECONDS)
                _safe_logout(mail)
                mail = None
                _sleep_with_stop(stop_event, IMAP_OVERQUOTA_SLEEP_SECONDS)
                continue
            if _is_imap_connection_error(exc):
                LOG.exception("parallel process: connection dropped; reconnect in %ss", IMAP_RECONNECT_SECONDS)
                _safe_logout(mail)
                mail = None
                _sleep_with_stop(stop_event, IMAP_RECONNECT_SECONDS)
                continue
            LOG.exception("parallel process: worker error; retry in 10s")
            _sleep_with_stop(stop_event, 10)

    _safe_logout(mail)

def cleanup_zombie_tasks():
    """Limpa tarefas que ficaram presas em 'processing' no arranque do serviço."""
    LOG.info("Iniciando limpeza de segurança de tarefas zombie no arranque...")
    # Removido o filtro de tempo por falta do campo updated_at; 
    # No arranque, qualquer 'processing' é um resíduo de um crash anterior.
    cmd = (
        "python manage.py shell -c \""
        "from news.models import Newsletter; "
        "count = Newsletter.objects.filter(status='processing')"
        ".update(status='error', error_message='Auto-recovered: Startup cleanup.'); "
        "print(f'Zombies limpos: {count}')\" > /dev/null 2>&1"
    )
    os.system(cmd)

def run_monitor_parallel_mode(cfg: Config, gmail_address: str, gmail_app_password: str) -> None:
    
    cleanup_zombie_tasks()
    start_review_api_server()

    # --- Telegram bot with inline buttons (migrated from local agent, Fase 4) ---
    telegram_bot_thread = None
    telegram_bot_active = False
    try:
        from telegram_bot import run_telegram_bot_thread, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            telegram_bot_thread = run_telegram_bot_thread(cfg)
            telegram_bot_active = True
            LOG.info("telegram bot thread started")
        else:
            LOG.info("telegram bot disabled (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set)")
    except ImportError:
        LOG.warning("telegram_bot module not available; inline button UX disabled")
    except Exception as exc:
        LOG.warning("telegram bot startup failed: %s", exc)

    # When the inline-button bot is active, disable the old plain-text notification
    # system to avoid duplicate messages for the same articles.
    if telegram_bot_active:
        global REVIEW_TELEGRAM_NOTIFY
        REVIEW_TELEGRAM_NOTIFY = False
        LOG.info("old plain-text telegram notifications disabled (inline bot is active)")

    recovery = _recover_review_gate_startup(cfg)
    if any(recovery.get(key, 0) for key in ("requeued_pending", "completed", "renotified", "errors")):
        LOG.info("startup review recovery stats=%s", recovery)
    
    stop_event = threading.Event()
    ingest_thread = threading.Thread(
        target=_parallel_ingest_worker,
        name="gmail-ingest-worker",
        args=(stop_event, cfg, gmail_address, gmail_app_password),
        daemon=True,
    )
    process_thread = threading.Thread(
        target=_parallel_process_worker,
        name="gmail-process-worker",
        args=(stop_event, cfg, gmail_address, gmail_app_password),
        daemon=True,
    )
    ingest_thread.start()
    process_thread.start()

    try:
        while True:
            if not ingest_thread.is_alive():
                raise RuntimeError("ingest worker stopped unexpectedly")
            if not process_thread.is_alive():
                raise RuntimeError("process worker stopped unexpectedly")
            if telegram_bot_thread and not telegram_bot_thread.is_alive():
                LOG.warning("telegram bot thread died; inline button UX unavailable")
                telegram_bot_thread = None
            time.sleep(PARALLEL_MAIN_HEARTBEAT_SECONDS)
    except KeyboardInterrupt:
        stop_event.set()
        ingest_thread.join(timeout=5)
        process_thread.join(timeout=5)
        raise
    except Exception:
        stop_event.set()
        ingest_thread.join(timeout=5)
        process_thread.join(timeout=5)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Gmail monitor for AI newsletter pipeline")
    parser.add_argument("--mode", choices=["backlog", "monitor", "monitor-serial"], default="monitor")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    _setup_debug_breakpoint_on_start()

    gmail_address = os.getenv("GMAIL_ADDRESS", "").strip()
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()
    if not gmail_address or not gmail_app_password:
        raise SystemExit("GMAIL_ADDRESS/GMAIL_APP_PASSWORD missing")

    cfg = Config()
    if not cfg.agent_api_key:
        raise SystemExit("AGENT_API_KEY missing")

    if args.mode == "monitor":
        while True:
            try:
                run_monitor_parallel_mode(cfg, gmail_address, gmail_app_password)
            except KeyboardInterrupt:
                return 0
            except Exception as exc:
                if _is_imap_overquota_error(exc):
                    LOG.exception("gmail overquota; cooling down for %ss", IMAP_OVERQUOTA_SLEEP_SECONDS)
                    time.sleep(IMAP_OVERQUOTA_SLEEP_SECONDS)
                    continue
                LOG.exception("parallel monitor failed; retry in %ss", IMAP_RECONNECT_SECONDS)
                time.sleep(IMAP_RECONNECT_SECONDS)

    while True:
        try:
            mail = connect_gmail(gmail_address, gmail_app_password)
            if args.mode == "backlog":
                run_backlog_mode(mail, cfg)
                return 0
            run_monitor_mode(mail, cfg)
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            if _is_imap_overquota_error(exc):
                LOG.exception("gmail overquota; cooling down for %ss", IMAP_OVERQUOTA_SLEEP_SECONDS)
                time.sleep(IMAP_OVERQUOTA_SLEEP_SECONDS)
                continue
            LOG.exception("gmail connection failed; retry in %ss", IMAP_RECONNECT_SECONDS)
            time.sleep(IMAP_RECONNECT_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
