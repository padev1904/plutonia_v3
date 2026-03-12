#!/usr/bin/env python3
"""Telegram bot with inline buttons for article triage (migrated from local agent).

Runs as a thread inside gmail-monitor. Provides rich UX with:
- Inline keyboard buttons for approve/reject/resource/edit
- Link validation info display
- Portal preview links
- Edit flow for title/summary/image

Communicates with Django via the agent API.
"""

from __future__ import annotations

import asyncio
import html as html_module
import json
import logging
import os
import re
import threading
import time
import unicodedata
from pathlib import Path
from typing import Any

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from process_newsletter import (
    Config,
    _assert_required_summary_model,
    _get_source_snapshot,
    _llm_generate,
    _openclaw_generate,
    _normalize_article_body_text,
    _normalize_keywords,
    _normalize_summary_text,
    _safe_json_object,
)
from link_validator import run_link_validation

LOG = logging.getLogger("telegram_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORTAL_PUBLIC_BASE_URL = os.getenv("PORTAL_PUBLIC_BASE_URL", "").rstrip("/")
TELEGRAM_TRIAGE_INTERVAL = int(os.getenv("TELEGRAM_TRIAGE_INTERVAL", "15"))
TELEGRAM_TRIAGE_NEWSLETTER_ID = os.getenv("TELEGRAM_TRIAGE_NEWSLETTER_ID", "").strip()
REVIEW_OUTPUT_DIR = os.getenv("REVIEW_OUTPUT_DIR", "/review").strip() or "/review"
WATCHDOG_STATUS_FILE = os.getenv("WATCHDOG_STATUS_FILE", "/review/ops_watchdog_status.json").strip() or "/review/ops_watchdog_status.json"

# Per-task model configuration (Fase 6)
OLLAMA_MODEL_TITLE = os.getenv("OLLAMA_MODEL_TITLE", "").strip()
OLLAMA_MODEL_LINK_VALIDATION = os.getenv("OLLAMA_MODEL_LINK_VALIDATION", "").strip()
_PUBLIC_BASE_CACHE: str | None = None


def _esc(text: str) -> str:
    return html_module.escape(str(text)) if text else ""


def _resolve_public_base_url() -> str:
    global _PUBLIC_BASE_CACHE
    if _PUBLIC_BASE_CACHE:
        return _PUBLIC_BASE_CACHE
    if PORTAL_PUBLIC_BASE_URL:
        _PUBLIC_BASE_CACHE = PORTAL_PUBLIC_BASE_URL
        return _PUBLIC_BASE_CACHE
    return ""


def _build_public_preview_url(path: str) -> str:
    raw = str(path or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if not raw.startswith("/"):
        raw = f"/{raw}"
    base = _resolve_public_base_url()
    if not base:
        return raw
    return f"{base}{raw}"


def _is_authorized(chat_id: int | None) -> bool:
    if not TELEGRAM_CHAT_ID or chat_id is None:
        return False
    return str(chat_id) == str(TELEGRAM_CHAT_ID)


def _api_get(cfg: Config, path: str, params: dict | None = None) -> dict:
    headers = {"X-API-Key": cfg.agent_api_key}
    resp = requests.get(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _api_post(cfg: Config, path: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json", "X-API-Key": cfg.agent_api_key}
    resp = requests.post(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _watchdog_status_path(cfg: Config) -> Path:
    raw = str(WATCHDOG_STATUS_FILE or "").strip()
    if not raw:
        return Path(cfg.review_output_dir) / "ops_watchdog_status.json"
    path = Path(raw)
    if path.is_absolute():
        return path
    return Path(cfg.review_output_dir) / raw


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _looks_like_status_query(user_text: str) -> bool:
    raw = str(user_text or "").strip()
    if not raw:
        return False
    if raw.startswith("http://") or raw.startswith("https://"):
        return False
    normalized = " ".join(_strip_accents(raw).lower().split())
    if len(normalized) > 80:
        return False
    exact = {
        "status",
        "estado",
        "pipeline",
        "estado do pipeline",
        "novo status",
        "da me novo status",
        "ponto de situacao",
        "como esta",
        "como esta o pipeline",
        "algum bloqueio",
        "ha algum bloqueio",
    }
    if normalized in exact:
        return True
    starts = (
        "status ",
        "pipeline ",
        "como esta",
        "ha algum bloqueio",
        "algum bloqueio",
        "ponto de situacao",
        "da me novo status",
    )
    return normalized.startswith(starts)


def _load_watchdog_status(cfg: Config) -> dict[str, Any] | None:
    path = _watchdog_status_path(cfg)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("failed to read watchdog status file=%s err=%s", path, exc)
        return None
    return payload if isinstance(payload, dict) else None


def _live_pipeline_status(cfg: Config) -> dict[str, Any]:
    statuses = ("pending", "processing", "review", "completed", "error")
    counts: dict[str, int] = {}
    current: dict[str, list[dict[str, Any]]] = {}
    for status in statuses:
        payload = _api_get(cfg, "newsletter/pending/", {"status": status, "limit": 3, "mode": "oldest"})
        rows = payload.get("newsletters", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        rows = [row for row in rows if isinstance(row, dict)]
        counts[status] = int(payload.get("pending_count", len(rows)) if isinstance(payload, dict) else len(rows))
        current[status] = rows

    article_payload = _api_get(cfg, "articles/editorial-pending/", {"mode": "oldest", "exclude_tg_triaged": "true"})
    article = article_payload.get("article") if isinstance(article_payload, dict) else None
    return {
        "observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "counts": counts,
        "current": current,
        "active_article": article if isinstance(article, dict) else None,
        "editorial_queue": {
            "status": str(article_payload.get("status", "")) if isinstance(article_payload, dict) else "",
            "reason": str(article_payload.get("reason", "")) if isinstance(article_payload, dict) else "",
        },
        "blockers": [],
        "runtime": {"last_actions": []},
        "health": {"review_api_ok": True},
    }


def _format_status_row(row: dict[str, Any]) -> str:
    newsletter_id = row.get("id") or row.get("newsletter_id") or "?"
    uid = row.get("gmail_uid") or "-"
    subject = str(row.get("subject", "")).strip() or str(row.get("title", "")).strip() or "sem assunto"
    if len(subject) > 90:
        subject = subject[:87].rstrip() + "..."
    return f"#{newsletter_id} uid={uid} {subject}"


def _format_action_timestamp(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "n/a"
    if "T" in raw:
        raw = raw.split("T", 1)[1]
    raw = raw.split("+", 1)[0].split("Z", 1)[0]
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw or "n/a"


def _format_pipeline_status_message(payload: dict[str, Any]) -> str:
    observed_at = str(payload.get("observed_at", "")).strip() or "n/a"
    counts = payload.get("counts", {}) if isinstance(payload, dict) else {}
    current = payload.get("current", {}) if isinstance(payload, dict) else {}
    blockers = payload.get("blockers", []) if isinstance(payload, dict) else []
    runtime = payload.get("runtime", {}) if isinstance(payload, dict) else {}
    active_article = payload.get("active_article") if isinstance(payload, dict) else None
    editorial_queue = payload.get("editorial_queue", {}) if isinstance(payload, dict) else {}
    health = payload.get("health", {}) if isinstance(payload, dict) else {}

    lines = [
        "Estado do pipeline",
        f"Observado em: {observed_at}",
        "",
        "Counts:",
        f"- pending: {counts.get('pending', 0)}",
        f"- processing: {counts.get('processing', 0)}",
        f"- review: {counts.get('review', 0)}",
        f"- completed: {counts.get('completed', 0)}",
        f"- error: {counts.get('error', 0)}",
    ]

    for status in ("processing", "review", "pending", "error"):
        rows = current.get(status, []) if isinstance(current, dict) else []
        if not isinstance(rows, list) or not rows:
            continue
        lines.append("")
        lines.append(f"{status.capitalize()} atuais:")
        for row in rows[:2]:
            if isinstance(row, dict):
                lines.append(f"- {_format_status_row(row)}")

    if isinstance(active_article, dict) and active_article:
        title = str(active_article.get("title", "")).strip() or "sem titulo"
        if len(title) > 90:
            title = title[:87].rstrip() + "..."
        lines.append("")
        lines.append("Artigo ativo:")
        lines.append(
            f"- #{active_article.get('id', '?')} newsletter={active_article.get('newsletter_id', '?')} "
            f"status={active_article.get('telegram_triage_status', '-') or '-'} {title}"
        )
    elif str(editorial_queue.get("reason", "")).strip() == "active_triage_exists":
        lines.append("")
        lines.append("Artigo ativo: triagem em curso")

    if isinstance(blockers, list) and blockers:
        lines.append("")
        lines.append("Bloqueios:")
        for blocker in blockers[:3]:
            if not isinstance(blocker, dict):
                continue
            parts = [str(blocker.get("type", "bloqueio"))]
            if blocker.get("newsletter_id"):
                parts.append(f"newsletter={blocker.get('newsletter_id')}")
            if blocker.get("gmail_uid"):
                parts.append(f"uid={blocker.get('gmail_uid')}")
            if blocker.get("age_seconds") is not None:
                parts.append(f"age={blocker.get('age_seconds')}s")
            if blocker.get("message"):
                parts.append(str(blocker.get("message")))
            lines.append(f"- {' | '.join(parts)}")

    actions = runtime.get("last_actions", []) if isinstance(runtime, dict) else []
    if isinstance(actions, list) and actions:
        lines.append("")
        lines.append("Ultimas acoes:")
        for action in actions[-3:]:
            if not isinstance(action, dict):
                continue
            lines.append(
                f"- {_format_action_timestamp(action.get('at'))} "
                f"{action.get('type', 'acao')}: {action.get('message', '')}"
            )

    if isinstance(health, dict) and health.get("disk_free_pct") is not None:
        lines.append("")
        lines.append(f"Disco livre review: {health.get('disk_free_pct')}%")

    return "\n".join(lines)


def _build_status_reply(cfg: Config) -> str:
    payload = _load_watchdog_status(cfg)
    if payload is None:
        payload = _live_pipeline_status(cfg)
    return _format_pipeline_status_message(payload)


def _openclaw_frontend_prompt(user_text: str) -> str:
    text = str(user_text or "").strip()
    return (
        "You are responding behind the unified Telegram frontend for @plutonai_news_bot.\n"
        "Reply in the same language as the user's message.\n"
        "Do not tell the user to contact another bot.\n"
        "The inline editorial triage buttons are handled by the frontend; do not ask the user to switch bots.\n"
        "If the user asks about the current editorial item, inspect the internal APIs and answer directly.\n"
        "Be concise.\n\n"
        f"Latest Telegram message:\n{text}"
    )


def _query_openclaw_frontend(cfg: Config, user_text: str) -> str:
    original_backend = cfg.llm_backend
    original_timeout = cfg.openclaw_timeout_seconds
    try:
        cfg.llm_backend = "openclaw"
        frontend_timeout = int(os.getenv("OPENCLAW_FRONTEND_TIMEOUT_SECONDS", "600") or "600")
        cfg.openclaw_timeout_seconds = max(45, min(max(int(original_timeout or 180), frontend_timeout), 1800))
        return _openclaw_generate(cfg, _openclaw_frontend_prompt(user_text)).strip()
    finally:
        cfg.llm_backend = original_backend
        cfg.openclaw_timeout_seconds = original_timeout


def _recover_pending_input_from_editorial_state(cfg: Config) -> dict[str, Any] | None:
    try:
        payload = _api_get(cfg, "articles/editorial-pending/", {"mode": "oldest", "exclude_tg_triaged": "true"})
    except Exception as exc:
        LOG.warning("failed to recover pending input context err=%s", exc)
        return None

    if not isinstance(payload, dict):
        return None
    if str(payload.get("reason", "")).strip() != "active_triage_exists":
        return None

    article = payload.get("article")
    if not isinstance(article, dict):
        return None

    try:
        article_id = int(article.get("id") or 0)
    except Exception:
        article_id = 0
    if article_id <= 0:
        return None

    tg_status = str(article.get("telegram_triage_status", "")).strip().lower()
    if tg_status == "waiting_edit":
        return {"article_id": article_id, "action": "requestchanges", "recovered": True}
    return None



# ---------------------------------------------------------------------------
# Title generation (Fase 3 - mandatory LLM title)
# ---------------------------------------------------------------------------

TITLE_PROPOSAL_PROMPT = """You are a title generation assistant.
Generate a single concise and engaging title for the content below.
The title must be based only on the text provided.
Ignore newsletter wrapper text, email headers, sender names, author bylines, and phrases such as
"posted new notes", "is now available", "highlighted in newsletter", or similar digest framing.
Focus on the main subject of the article itself.
Do not include the author/publication name unless the article is actually about that person/publication.
Prefer concrete topic nouns over generic wording like newsletter, update, note, roundup, or developments.

Output ONLY raw JSON with this exact structure:
{{
  "title": "..."
}}

Profile: {content_profile}
Text:
{article_text}
"""

PREVIEW_REVISION_PROMPT = """You are revising an editorial preview for a portal article.
Use the editor instructions as strict requirements.
If source material is incomplete, prefer the current preview facts and do not invent details.

CURRENT ARTICLE:
- title: {title}
- summary: {summary}
- article_body: {article_body}
- source_url: {source_url}
- section: {section}
- category: {category}
- subcategory: {subcategory}
- categories: {categories}

EDITOR INSTRUCTIONS:
{instructions}

SOURCE OR PREVIEW CONTEXT:
---
{source_text}
---

Return STRICT JSON only:
{{
  "title": "...",
  "summary": "...",
  "article_body": "...",
  "section": "...",
  "category": "...",
  "subcategory": "...",
  "categories": ["..."]
}}

Rules:
- `summary` is for the card preview: 1-2 concise sentences, specific and teaser-like, max 220 characters.
- `article_body` is the long-form preview article: 5-8 short paragraphs separated by blank lines.
- Remove irrelevant bylines, sender names, editor names, recipient names, and references not required by the editor.
- Keep factual tone. No markdown. No bullet lists.
- Focus on the main source topic only. Ignore tangents, side notes, and unrelated mentions.
- section/category/subcategory in English.
- categories: 3-10 concise keywords.
"""


def _clean_preview_revision_context(text: str) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    if not cleaned:
        return ""
    cut_markers = (
        "on a little tangent:",
        "on a side note:",
        "separately,",
        "meanwhile,",
    )
    lower = cleaned.lower()
    cut_positions = []
    for marker in cut_markers:
        pos = lower.find(marker)
        if pos > 0:
            cut_positions.append(pos)
    if cut_positions:
        cleaned = cleaned[: min(cut_positions)].strip()
    cleaned = re.sub(
        r"^(?:[A-Z][A-Za-z0-9&.,'/-]+\s+){2,10}(?=(?:How|Why|What|When|Reinforcement|Claude|BuildML|OpenAI|Anthropic|Google|Meta|NVIDIA|Mistral|Agent|Training|Reasoning|Ch\s+\d+|Chapter\s+\d+))",
        "",
        cleaned,
    ).strip()
    return cleaned


def _load_draft_revision_context(article: dict, *, max_chars: int) -> str:
    newsletter_id = article.get("newsletter_id")
    draft_index = article.get("draft_article_index")
    if newsletter_id is None:
        return ""
    try:
        draft_path = Path(REVIEW_OUTPUT_DIR) / f"newsletter_{int(newsletter_id)}_draft.json"
        payload = json.loads(draft_path.read_text(encoding="utf-8"))
        articles = payload.get("articles") or []
    except Exception as exc:
        LOG.warning("failed to load draft revision context newsletter_id=%s article_index=%s err=%s", newsletter_id, draft_index, exc)
        return ""

    article_data = None
    article_id = article.get("id")
    original_url = str(article.get("original_url", "")).strip()
    title = str(article.get("title", "")).strip()
    for candidate in articles:
        if article_id and int(candidate.get("portal_article_id") or 0) == int(article_id):
            article_data = candidate
            break
        if original_url and str(candidate.get("original_url", "")).strip() == original_url:
            article_data = candidate
            break
        if title and str(candidate.get("title", "")).strip() == title:
            article_data = candidate
            break

    if article_data is None and draft_index is not None:
        try:
            idx = int(draft_index)
            if 0 <= idx < len(articles):
                article_data = articles[idx]
            elif 1 <= idx <= len(articles):
                article_data = articles[idx - 1]
        except Exception:
            article_data = None

    if not isinstance(article_data, dict):
        return ""
    for key in ("raw_email_segment_text", "enrichment_context", "summary"):
        value = str(article_data.get(key, "")).strip()
        if value:
            return value[:max_chars]
    return ""


def _fallback_revision_source_text(article: dict, *, max_chars: int) -> str:
    parts: list[str] = []
    for key in ("title", "summary", "article_body"):
        value = str(article.get(key, "")).strip()
        if value:
            parts.append(value)
    categories = [str(c).strip() for c in article.get("categories", []) if str(c).strip()]
    if categories:
        parts.append("Categories: " + ", ".join(categories))
    for key in ("section", "category", "subcategory"):
        value = str(article.get(key, "")).strip()
        if value:
            parts.append(f"{key}: {value}")
    return "\n\n".join(parts)[:max_chars].strip()


def _fit_card_summary(text: str, *, max_chars: int = 220) -> str:
    summary = _normalize_summary_text(text)
    if len(summary) <= max_chars:
        return summary
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", summary) if part.strip()]
    fitted: list[str] = []
    for sentence in sentences:
        candidate = " ".join(fitted + [sentence]).strip()
        if len(candidate) <= max_chars:
            fitted.append(sentence)
        else:
            break
    if fitted:
        return " ".join(fitted).strip()
    return summary[:max_chars].rstrip()


def _build_preview_revision_payload(cfg: Config, article: dict, instructions: str) -> dict[str, Any]:
    source_url = str(article.get("original_url", "")).strip()
    source_text = _load_draft_revision_context(article, max_chars=cfg.source_rewrite_max_chars)
    if len(source_text) < 300 and source_url:
        try:
            snapshot = _get_source_snapshot(
                source_url,
                cfg.source_rewrite_max_chars,
                max(120, cfg.source_open_min_chars),
            )
            snapshot_text = str(snapshot.get("text", "")).strip()
            if len(snapshot_text) > len(source_text):
                source_text = snapshot_text
        except Exception as exc:
            LOG.warning("preview revision source snapshot failed article_id=%s err=%s", article.get("id"), exc)
    if len(source_text) < 300:
        fallback_text = _fallback_revision_source_text(article, max_chars=cfg.source_rewrite_max_chars)
        if len(fallback_text) > len(source_text):
            source_text = fallback_text
    cleaned_source_text = _clean_preview_revision_context(source_text)
    if len(cleaned_source_text) >= 180:
        source_text = cleaned_source_text
    source_text = source_text[:cfg.source_rewrite_max_chars]
    if len(source_text) < 180:
        raise RuntimeError("preview context too short for revision")

    prompt_summary = str(article.get("summary", "")).strip()
    prompt_article_body = str(article.get("article_body", "")).strip()
    if len(cleaned_source_text) >= 180:
        prompt_summary = ""
        prompt_article_body = ""

    _assert_required_summary_model(cfg)
    prompt = PREVIEW_REVISION_PROMPT.format(
        title=str(article.get("title", "")).strip()[:500],
        summary=prompt_summary,
        article_body=prompt_article_body,
        source_url=source_url or "unknown",
        section=str(article.get("section", "")).strip(),
        category=str(article.get("category", "")).strip(),
        subcategory=str(article.get("subcategory", "")).strip(),
        categories=", ".join([str(c).strip() for c in article.get("categories", []) if str(c).strip()]) or "None",
        instructions=instructions.strip(),
        source_text=source_text,
    )
    response = _llm_generate(cfg, prompt)
    parsed = _safe_json_object(response)
    if not parsed:
        raise RuntimeError("revision failed: model did not return valid JSON")

    categories = _normalize_keywords(parsed.get("categories", []), limit=10)
    return {
        "decision": "revise",
        "title": str(parsed.get("title", "")).strip()[:500] or str(article.get("title", "")).strip()[:500],
        "summary": _fit_card_summary(str(parsed.get("summary", ""))) or _fit_card_summary(str(article.get("summary", "")).strip()),
        "article_body": _normalize_article_body_text(str(parsed.get("article_body", ""))) or str(article.get("article_body", "")).strip(),
        "section": str(parsed.get("section", "")).strip()[:120] or str(article.get("section", "")).strip()[:120],
        "category": str(parsed.get("category", "")).strip()[:120] or str(article.get("category", "")).strip()[:120],
        "subcategory": str(parsed.get("subcategory", "")).strip()[:120] or str(article.get("subcategory", "")).strip()[:120],
        "categories": categories or article.get("categories", []),
        "comment": f"Revision requested: {instructions.strip()}",
    }


def propose_title(cfg: Config, text: str, content_profile: str = "news") -> str | None:
    """Ask LLM to propose a title. Returns None on failure (triggers block)."""
    truncated = (text or "").strip()[:3000]
    if not truncated:
        return None

    model_override = OLLAMA_MODEL_TITLE or cfg.ollama_model_title or ""
    prompt = TITLE_PROPOSAL_PROMPT.format(
        content_profile=content_profile,
        article_text=truncated,
    )
    try:
        response = _llm_generate(cfg, prompt, model_override=model_override)
        parsed = _safe_json_object(response)
        title = str(parsed.get("title", "")).strip()
        if title and title.lower() not in {"", "sem titulo", "untitled", "none", "null", "n/a"}:
            return title
    except Exception as e:
        LOG.warning("Title proposal failed: %s", e)
    return None


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _format_triage_preview(
    article: dict,
    selection_state: dict[str, Any],
    *,
    proposed_title: str = "",
    newsletter_meta: dict | None = None,
) -> str:
    """Build message 1 with explicit type/source selections."""
    title = _esc(article.get("title", "Sem titulo"))
    process_url = str(selection_state.get("process_url", "")).strip()
    manual_url = str(selection_state.get("manual_url", "")).strip()
    process_origin = str(selection_state.get("process_origin", article.get("source_link_origin", "email"))).strip().lower()
    current_profile = str(selection_state.get("content_profile", "news")).strip().lower()
    current_source = str(selection_state.get("source_mode", "process")).strip().lower()
    summary = str(article.get("summary", "")).strip()
    first_words = " ".join(summary.split()[:25]) if summary else "(sem resumo)"
    proposed = proposed_title or str(article.get("proposed_title", "")).strip() or str(article.get("title", "")).strip()

    lines = ["<b>Triagem editorial</b>"]

    if newsletter_meta:
        subject = str(newsletter_meta.get("subject", "")).strip()
        sender = str(newsletter_meta.get("original_sender_name", "")).strip() or str(newsletter_meta.get("original_sender_email", "")).strip()
        if subject:
            lines.append(f"Email: <b>{_esc(subject)}</b>")
        if sender:
            lines.append(f"Sender: {_esc(sender)}")

    lines.extend([
        f"Titulo original: <b>{title}</b>",
        f"Titulo (modelo): <b>{_esc(proposed)}</b>",
        f"Tipo de conteudo atual: <b>{'Resource' if current_profile == 'resource' else 'News'}</b>",
        f"Source Link atual: <b>{'Manual' if current_source == 'manual' else 'Processo'}</b>",
    ])

    process_origin_label = "inferido" if process_origin == "search" else "direto"
    if process_url and "://" in process_url:
        lines.append(f"Link do processo ({process_origin_label}): {_esc(process_url)}")
    else:
        lines.append(f"Link do processo ({process_origin_label}): por definir")

    if manual_url and "://" in manual_url:
        lines.append(f"Link manual: {_esc(manual_url)}")
    else:
        lines.append("Link manual: por definir")

    status = str(article.get("link_validation_status", "not_checked")).strip()
    confidence = float(article.get("link_validation_confidence", 0.0) or 0.0)
    reason = str(article.get("link_validation_reason", "")).strip()
    status_labels = {
        "valid": "validado",
        "invalid": "invalido",
        "uncertain": "incerto",
        "not_checked": "nao verificado",
    }
    lines.append("")
    lines.append(f"Validacao link: <b>{_esc(status_labels.get(status, status or 'nao verificado'))}</b>")
    lines.append(f"Confianca: {confidence:.0%}")
    if reason:
        lines.append(f"Motivo: <i>{_esc(reason[:180])}</i>")

    lines.extend([
        "",
        f"Resumo: <i>{_esc(first_words)}...</i>",
        "",
        "Ajuste o tipo e o source link, depois carregue em <b>Avancar para preview</b>.",
    ])
    return "\n".join(lines)



def _format_structured_preview(
    article: dict,
    content_profile: str = "news",
) -> str:
    """Build second-step message with portal preview links."""
    profile_label = "RECURSO" if content_profile == "resource" else "NOTICIA"
    section = _esc(str(article.get("section", "")))
    category = _esc(str(article.get("category", "")))
    subcategory = _esc(str(article.get("subcategory", "")))

    taxonomy = " &gt; ".join(filter(None, [section, category, subcategory])) or "Sem categoria"
    preview_token = article.get("preview_token", "")
    source_url = article.get("original_url", "")

    lines = [
        f"<b>Preview privado [{profile_label}]</b>",
        f"<b>{_esc(article.get('title', ''))}</b>",
        f"<b>{taxonomy}</b>",
        "",
    ]

    card_url = _build_public_preview_url(article.get("preview_card_path", "")) if article.get("preview_card_path") else ""
    if preview_token and not card_url:
        card_url = _build_public_preview_url(f"/preview/card/{preview_token}/")

    if card_url:
        lines.append(f'Card: <a href="{_esc(card_url)}">{_esc(card_url)}</a>')
    elif preview_token:
        lines.append(f"Preview token: {preview_token}")

    if source_url and "://" in source_url:
        lines.append(f'Fonte: <a href="{_esc(source_url)}">{_esc(source_url)}</a>')

    lines.extend(["", "Aprovar publicacao no portal?"])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Inline keyboards
# ---------------------------------------------------------------------------

def _triage_buttons(article_id: int, selection_state: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Tipo: News", callback_data=f"m1type_news_{article_id}"),
            InlineKeyboardButton("Tipo: Resource", callback_data=f"m1type_resource_{article_id}"),
        ],
        [
            InlineKeyboardButton("Source: Processo", callback_data=f"m1source_process_{article_id}"),
            InlineKeyboardButton("Source: Manual", callback_data=f"m1source_manual_{article_id}"),
        ],
        [InlineKeyboardButton("Avancar para preview", callback_data=f"m1confirm_{article_id}")],
        [InlineKeyboardButton("Rejeitar", callback_data=f"reject_{article_id}")],
    ])



def _approval_buttons(article_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Aprovar preview", callback_data=f"publish_{article_id}")],
        [InlineKeyboardButton("Pedir alteracoes", callback_data=f"requestchanges_{article_id}")],
    ])
# ---------------------------------------------------------------------------
# Globals for config sharing between threads
# ---------------------------------------------------------------------------

_CFG: Config | None = None


def _get_cfg() -> Config:
    global _CFG
    if _CFG is None:
        _CFG = Config()
    return _CFG


_MESSAGE1_STATE: dict[int, dict[str, Any]] = {}
_MESSAGE1_STATE_LOCK = threading.Lock()


def _default_message1_state(article: dict, *, process_url: str = "", process_origin: str = "") -> dict[str, Any]:
    resolved_process_url = str(process_url or article.get("original_url", "")).strip()
    resolved_origin = str(process_origin or article.get("source_link_origin", "email")).strip().lower() or "email"
    if resolved_origin == "user":
        resolved_origin = "email"
    return {
        "content_profile": "news",
        "source_mode": "process",
        "process_url": resolved_process_url,
        "manual_url": "",
        "process_origin": resolved_origin,
    }


def _reset_message1_state(article: dict, *, process_url: str = "", process_origin: str = "") -> dict[str, Any]:
    article_id = int(article.get("id") or 0)
    state = _default_message1_state(article, process_url=process_url, process_origin=process_origin)
    if article_id <= 0:
        return state
    with _MESSAGE1_STATE_LOCK:
        _MESSAGE1_STATE[article_id] = dict(state)
    return dict(state)


def _ensure_message1_state(article: dict) -> dict[str, Any]:
    article_id = int(article.get("id") or 0)
    fallback = _default_message1_state(article)
    if article_id <= 0:
        return fallback
    with _MESSAGE1_STATE_LOCK:
        state = _MESSAGE1_STATE.setdefault(article_id, dict(fallback))
        return dict(state)


def _update_message1_state(article_id: int, **fields: Any) -> dict[str, Any]:
    with _MESSAGE1_STATE_LOCK:
        state = dict(_MESSAGE1_STATE.get(article_id, {}))
        state.update(fields)
        _MESSAGE1_STATE[article_id] = state
        return dict(state)


def _pop_message1_state(article_id: int) -> dict[str, Any] | None:
    with _MESSAGE1_STATE_LOCK:
        state = _MESSAGE1_STATE.pop(article_id, None)
        return dict(state) if isinstance(state, dict) else None


def _load_newsletter_meta(cfg: Config, newsletter_id: int | None) -> dict[str, Any] | None:
    if not newsletter_id:
        return None
    try:
        nl_data = _api_get(cfg, f"newsletter/{int(newsletter_id)}/raw/")
    except Exception:
        return None
    return {
        "subject": nl_data.get("subject", ""),
        "original_sender_name": nl_data.get("original_sender_name", ""),
        "original_sender_email": nl_data.get("original_sender_email", ""),
    }


async def _refresh_triage_message(query, article_id: int, *, article: dict | None = None) -> None:
    cfg = _get_cfg()
    article_data = article or _load_editorial_article(cfg, article_id)
    if not article_data:
        if query.message is not None:
            await query.message.reply_text("Artigo nao encontrado.")
        return
    selection_state = _ensure_message1_state(article_data)
    newsletter_meta = _load_newsletter_meta(cfg, article_data.get("newsletter_id"))
    preview = _format_triage_preview(
        article_data,
        selection_state,
        proposed_title=str(article_data.get("proposed_title", "")).strip(),
        newsletter_meta=newsletter_meta,
    )
    try:
        await query.edit_message_text(
            text=preview,
            parse_mode="HTML",
            reply_markup=_triage_buttons(article_id, selection_state),
            disable_web_page_preview=True,
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            raise


# ---------------------------------------------------------------------------
# Callback handlers
# ---------------------------------------------------------------------------

def _load_editorial_article(cfg: Config, article_id: int) -> dict[str, Any]:
    result = _api_get(cfg, "articles/editorial-data/", {"article_id": str(article_id)})
    article = result.get("article", {}) if isinstance(result, dict) else {}
    return article if isinstance(article, dict) else {}


def _set_triage_status(cfg: Config, article_id: int, **fields: Any) -> None:
    payload = {"article_id": article_id}
    payload.update(fields)
    _api_post(cfg, "articles/link-validation/", payload)


def _public_card_url_for_article(article: dict[str, Any]) -> str:
    article_id = article.get("id")
    if not article_id:
        return ""
    try:
        return _build_public_preview_url(f"/article/{int(article_id)}/card/")
    except Exception:
        return ""


def _public_detail_url_for_resource(resource: dict[str, Any]) -> str:
    resource_id = resource.get("id")
    if not resource_id:
        return ""
    try:
        return _build_public_preview_url(f"/resources/item/{int(resource_id)}/")
    except Exception:
        return ""


def _publish_resource_from_article(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    original_date = (
        str(article.get("newsletter_original_sent_at", "")).strip()
        or str(article.get("newsletter_received_at", "")).strip()
        or str(article.get("published_at", "")).strip()
    )
    payload = {
        "resource_url": str(article.get("original_url", "")).strip(),
        "title": str(article.get("title", "")).strip(),
        "summary": str(article.get("summary", "")).strip(),
        "article_body": str(article.get("article_body", "")).strip(),
        "image_url": str(article.get("image_url", "")).strip(),
        "section": str(article.get("section", "")).strip(),
        "category": str(article.get("category", "")).strip(),
        "subcategory": str(article.get("subcategory", "")).strip(),
        "review_status": "approved",
        "is_active": True,
        "published_at": original_date,
        "source_published_at": original_date,
    }
    result = _api_post(cfg, "resources/publish/", payload)
    resource = result.get("resource", {}) if isinstance(result, dict) else {}
    if not isinstance(resource, dict) or not resource:
        raise RuntimeError("resource publish returned no resource payload")
    return resource


async def _disable_action_buttons(query) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass


async def _send_preview_message(message, article_id: int, article: dict[str, Any]) -> None:
    content_profile = article.get("content_profile", "news")
    preview = _format_structured_preview(article, content_profile)
    try:
        await message.reply_text(
            text=preview,
            parse_mode="HTML",
            reply_markup=_approval_buttons(article_id),
            disable_web_page_preview=False,
        )
    except Exception:
        plain = re.sub(r"</?[^>]+>", "", preview)
        await message.reply_text(text=plain, reply_markup=_approval_buttons(article_id), disable_web_page_preview=False)


async def _send_publication_confirmation(message, article: dict[str, Any], resource: dict[str, Any] | None = None) -> None:
    title = str(article.get("title", "")).strip() or "Sem titulo"
    content_profile = str(article.get("content_profile", "news")).strip()
    lines = [
        "<b>Publicacao aprovada</b>",
        f"Titulo: <b>{_esc(title)}</b>",
    ]
    if content_profile == "resource" and resource:
        resource_url = _public_detail_url_for_resource(resource)
        if resource_url:
            lines.append(f'Recurso: <a href="{_esc(resource_url)}">{_esc(resource_url)}</a>')
        else:
            lines.append("Recurso: link publico indisponivel")
    else:
        card_url = _public_card_url_for_article(article)
        if card_url:
            lines.append(f'Card: <a href="{_esc(card_url)}">{_esc(card_url)}</a>')
        else:
            lines.append("Card: link publico indisponivel")
    text = "\n".join(lines)
    try:
        await message.reply_text(text=text, parse_mode="HTML", disable_web_page_preview=False)
    except Exception:
        plain = re.sub(r"</?[^>]+>", "", text)
        await message.reply_text(text=plain, disable_web_page_preview=False)


async def _send_next_article_if_ready(bot) -> None:
    if bot is None:
        return
    try:
        await _send_next_for_triage(bot, _get_cfg())
    except Exception as exc:
        LOG.warning("failed to send next article after decision: %s", exc)


async def _prompt_manual_source_input(query, article_id: int, *, message_text: str) -> None:
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _set_pending_input(chat_id, article_id, "manualsource")
    try:
        _set_triage_status(_get_cfg(), article_id, telegram_triage_status="waiting_user_input")
    except Exception:
        pass
    if query.message is not None:
        await query.message.reply_text(
            text=message_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Rejeitar", callback_data=f"reject_{article_id}")]
            ]),
            disable_web_page_preview=True,
        )


async def _handle_select_type(query, article_id: int, profile: str) -> None:
    cfg = _get_cfg()
    article = _load_editorial_article(cfg, article_id)
    if not article:
        if query.message is not None:
            await query.message.reply_text("Artigo nao encontrado.")
        return
    _ensure_message1_state(article)
    _update_message1_state(article_id, content_profile=profile)
    await _refresh_triage_message(query, article_id, article=article)


async def _handle_select_source(query, article_id: int, source_mode: str) -> None:
    cfg = _get_cfg()
    article = _load_editorial_article(cfg, article_id)
    if not article:
        if query.message is not None:
            await query.message.reply_text("Artigo nao encontrado.")
        return
    _ensure_message1_state(article)
    _update_message1_state(article_id, source_mode=source_mode)
    await _refresh_triage_message(query, article_id, article=article)


async def _handle_confirm_selection(query, article_id: int, bot=None) -> None:
    cfg = _get_cfg()
    article = _load_editorial_article(cfg, article_id)
    if not article:
        if query.message is not None:
            await query.message.reply_text("Artigo nao encontrado.")
        return

    selection_state = _ensure_message1_state(article)
    selected_profile = str(selection_state.get("content_profile", "news")).strip().lower() or "news"
    selected_source_mode = str(selection_state.get("source_mode", "process")).strip().lower() or "process"
    process_url = str(selection_state.get("process_url", "")).strip()
    manual_url = str(selection_state.get("manual_url", "")).strip()
    process_origin = str(selection_state.get("process_origin", article.get("source_link_origin", "email"))).strip().lower() or "email"
    if process_origin == "user":
        process_origin = "email"

    if selected_source_mode == "manual":
        if not manual_url:
            await _prompt_manual_source_input(
                query,
                article_id,
                message_text=(
                    "<b>Source Link manual</b>\n\n"
                    "Envie o URL manual a usar neste artigo.\n"
                    "Assim que o receber, avanco automaticamente para o preview."
                ),
            )
            return
        source_url = manual_url
        source_origin = "user"
        validation_status = "valid"
        validation_confidence = 1.0
        validation_reason = "Link manual fornecido pelo utilizador."
    else:
        source_url = process_url
        if not source_url or "://" not in source_url:
            await _prompt_manual_source_input(
                query,
                article_id,
                message_text=(
                    "<b>Link do processo indisponivel</b>\n\n"
                    "Envie um URL manual para este artigo.\n"
                    "Assim que o receber, avanco automaticamente para o preview."
                ),
            )
            return
        source_origin = process_origin if process_origin in {"email", "search"} else "email"
        validation_status = str(article.get("link_validation_status", "not_checked")).strip() or "not_checked"
        validation_confidence = float(article.get("link_validation_confidence", 0.0) or 0.0)
        validation_reason = str(article.get("link_validation_reason", "")).strip()

    _set_triage_status(
        cfg,
        article_id,
        telegram_triage_status="pending_approval",
        content_profile=selected_profile,
        original_url=source_url,
        source_link_origin=source_origin,
        link_validation_status=validation_status,
        link_validation_confidence=validation_confidence,
        link_validation_reason=validation_reason,
    )
    article = _load_editorial_article(cfg, article_id)
    if article:
        article["content_profile"] = selected_profile
        article["original_url"] = source_url
        article["source_link_origin"] = source_origin
        article["link_validation_status"] = validation_status
        article["link_validation_confidence"] = validation_confidence
        article["link_validation_reason"] = validation_reason
    _pop_message1_state(article_id)
    await _disable_action_buttons(query)
    if query.message is not None and article:
        await _send_preview_message(query.message, article_id, article)


async def _handle_publish(query, article_id: int, bot=None):
    """Approve preview, publish article, and send confirmation message 3."""
    cfg = _get_cfg()
    try:
        article_before = _load_editorial_article(cfg, article_id)
        result = _api_post(cfg, "articles/editorial-decision/", {
            "article_id": article_id,
            "decision": "approve",
        })
        article = result.get("article", {}) if isinstance(result, dict) else {}
        if not isinstance(article, dict) or not article:
            article = article_before

        resource_payload = None
        if str(article.get("content_profile", article_before.get("content_profile", "news"))).strip() == "resource":
            resource_payload = _publish_resource_from_article(cfg, article or article_before)

        try:
            _set_triage_status(cfg, article_id, telegram_triage_status="approved")
        except Exception:
            pass
        _pop_message1_state(article_id)
        with _TRIAGE_SENT_LOCK:
            _TRIAGE_SENT_IDS.discard(article_id)
        await _disable_action_buttons(query)
        if query.message is not None:
            await _send_publication_confirmation(query.message, article if isinstance(article, dict) else {}, resource_payload)
        await _send_next_article_if_ready(bot)
    except Exception as e:
        if query.message is not None:
            await query.message.reply_text(
                text=f"Erro na publicacao: {_esc(str(e))}",
                parse_mode="HTML",
                reply_markup=_approval_buttons(article_id),
            )


async def _handle_reject(query, article_id: int, bot=None):
    """Final rejection at message 1, then move to next article."""
    cfg = _get_cfg()
    try:
        _api_post(cfg, "articles/editorial-decision/", {
            "article_id": article_id,
            "decision": "reject",
        })
        try:
            _set_triage_status(cfg, article_id, telegram_triage_status="rejected")
        except Exception:
            pass
        _pop_message1_state(article_id)
        with _TRIAGE_SENT_LOCK:
            _TRIAGE_SENT_IDS.discard(article_id)
        await _disable_action_buttons(query)
        if query.message is not None:
            await query.message.reply_text("Artigo rejeitado. Vou avancar para o proximo.")
        await _send_next_article_if_ready(bot)
    except Exception as e:
        if query.message is not None:
            await query.message.reply_text(
                text=f"Erro ao rejeitar: {_esc(str(e))}",
                parse_mode="HTML",
                reply_markup=_triage_buttons(article_id, _ensure_message1_state(_load_editorial_article(cfg, article_id) or {"id": article_id})),
            )


async def _handle_process(query, article_id: int, bot=None):
    """Legacy path: use default News + Processo and continue to preview."""
    cfg = _get_cfg()
    article = _load_editorial_article(cfg, article_id)
    if not article:
        if query.message is not None:
            await query.message.reply_text("Artigo nao encontrado.")
        return
    _reset_message1_state(article, process_url=str(article.get("original_url", "")).strip(), process_origin=str(article.get("source_link_origin", "email")).strip())
    await _handle_confirm_selection(query, article_id, bot)


async def _handle_resource(query, article_id: int, bot=None):
    """Legacy path: force Resource + Processo and continue to preview."""
    cfg = _get_cfg()
    article = _load_editorial_article(cfg, article_id)
    if not article:
        if query.message is not None:
            await query.message.reply_text("Artigo nao encontrado.")
        return
    _reset_message1_state(article, process_url=str(article.get("original_url", "")).strip(), process_origin=str(article.get("source_link_origin", "email")).strip())
    _update_message1_state(article_id, content_profile="resource")
    await _handle_confirm_selection(query, article_id, bot)


async def _handle_request_changes(query, article_id: int, bot=None):
    chat_id = query.message.chat_id if query.message else None
    cfg = _get_cfg()
    try:
        _set_triage_status(cfg, article_id, telegram_triage_status="waiting_edit")
    except Exception:
        pass
    if chat_id:
        _set_pending_input(chat_id, article_id, "requestchanges")
    await _disable_action_buttons(query)
    if query.message is not None:
        await query.message.reply_text(
            text=(
                "<b>Alteracoes ao preview</b>\n\n"
                "Envie as instrucoes concretas para a nova versao do preview. "
                "Exemplo: corrigir titulo, encurtar resumo, mudar angulo editorial, ajustar taxonomia.\n\n"
                "Depois disso, envio um novo preview para aprovacao."
            ),
            parse_mode="HTML",
        )


# Track pending user input: {chat_id: {"article_id": int, "action": str}}
_PENDING_INPUT: dict[int, dict[str, Any]] = {}
_PENDING_INPUT_LOCK = threading.Lock()


def _set_pending_input(chat_id: int, article_id: int, action: str):
    with _PENDING_INPUT_LOCK:
        _PENDING_INPUT[chat_id] = {"article_id": article_id, "action": action}


def _pop_pending_input(chat_id: int) -> dict[str, Any] | None:
    with _PENDING_INPUT_LOCK:
        return _PENDING_INPUT.pop(chat_id, None)


async def _handle_needtext(query, article_id: int, bot=None):
    """Ask user for full text or link."""
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _set_pending_input(chat_id, article_id, "needtext")
    try:
        _set_triage_status(_get_cfg(), article_id, telegram_triage_status="waiting_user_input")
    except Exception:
        pass
    msg = (
        "<b>Texto/link completo necessario</b>\n\n"
        "Responda com:\n"
        "- um URL para o artigo completo, ou\n"
        "- o texto integral do artigo."
    )
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Rejeitar artigo", callback_data=f"reject_{article_id}")]
    ])
    await query.edit_message_text(text=msg, parse_mode="HTML", reply_markup=reply_markup)


async def _handle_edit_title(query, article_id: int, bot=None):
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _set_pending_input(chat_id, article_id, "edittitle")
    await query.edit_message_text(
        text="<b>Editar titulo</b>\n\nEnvie uma mensagem com o novo titulo.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancelar", callback_data=f"cancel_{article_id}")]
        ]),
    )


async def _handle_edit_summary(query, article_id: int, bot=None):
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _set_pending_input(chat_id, article_id, "editsummary")
    await query.edit_message_text(
        text="<b>Editar resumo</b>\n\nEnvie o novo resumo.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Cancelar", callback_data=f"cancel_{article_id}")]
        ]),
    )


async def _handle_cancel(query, article_id: int, bot=None):
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _pop_pending_input(chat_id)
    await _handle_process(query, article_id, bot)


async def _handle_m1type_news(query, article_id: int, bot=None):
    await _handle_select_type(query, article_id, "news")


async def _handle_m1type_resource(query, article_id: int, bot=None):
    await _handle_select_type(query, article_id, "resource")


async def _handle_m1source_process(query, article_id: int, bot=None):
    await _handle_select_source(query, article_id, "process")


async def _handle_m1source_manual(query, article_id: int, bot=None):
    await _handle_select_source(query, article_id, "manual")


async def _handle_m1confirm(query, article_id: int, bot=None):
    await _handle_confirm_selection(query, article_id, bot)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    chat_id = query.message.chat_id if query.message else None
    if not _is_authorized(chat_id):
        await query.answer("Chat nao autorizado.", show_alert=True)
        return

    await query.answer()
    data = query.data or ""

    if chat_id:
        _pop_pending_input(chat_id)

    handlers = {
        "m1type_news_": _handle_m1type_news,
        "m1type_resource_": _handle_m1type_resource,
        "m1source_process_": _handle_m1source_process,
        "m1source_manual_": _handle_m1source_manual,
        "m1confirm_": _handle_m1confirm,
        "process_": _handle_process,
        "resource_": _handle_resource,
        "publish_": _handle_publish,
        "reject_": _handle_reject,
        "needtext_": _handle_needtext,
        "requestchanges_": _handle_request_changes,
        "regen_": _handle_process,
        "edittitle_": _handle_edit_title,
        "editsummary_": _handle_edit_summary,
        "cancel_": _handle_cancel,
    }

    for prefix, handler in handlers.items():
        if data.startswith(prefix):
            try:
                article_id = int(data[len(prefix):])
                await handler(query, article_id, context.bot)
            except ValueError:
                LOG.warning("Invalid article_id in callback: %s", data)
            return

    LOG.warning("Unknown callback: %s", data)


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages for revision flows and user link/text input."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not _is_authorized(chat_id):
        return

    if not update.message or not update.message.text:
        return

    user_text = update.message.text.strip()
    if not user_text:
        return

    cfg = _get_cfg()
    pending = _pop_pending_input(chat_id) if chat_id else None
    if not pending:
        pending = _recover_pending_input_from_editorial_state(cfg)
    if not pending:
        try:
            loop = asyncio.get_running_loop()
            reply_text = await loop.run_in_executor(None, _query_openclaw_frontend, cfg, user_text)
        except Exception as e:
            LOG.warning("OpenClaw frontend request failed: %s", e)
            await update.message.reply_text(
                "Nao consegui obter resposta do OpenClaw neste momento. Tenta novamente.",
                disable_web_page_preview=True,
            )
            return

        if reply_text:
            await update.message.reply_text(reply_text, disable_web_page_preview=True)
        return

    article_id = pending["article_id"]
    action = pending["action"]

    try:
        if action == "edittitle":
            _api_post(cfg, "articles/editorial-decision/", {
                "article_id": article_id,
                "decision": "revise",
                "title": user_text[:500],
            })
            article = _load_editorial_article(cfg, article_id)
            if article:
                await _send_preview_message(update.message, article_id, article)
            else:
                await update.message.reply_text(
                    f"Titulo atualizado para: <b>{_esc(user_text[:500])}</b>",
                    parse_mode="HTML",
                    reply_markup=_approval_buttons(article_id),
                )

        elif action == "editsummary":
            _api_post(cfg, "articles/editorial-decision/", {
                "article_id": article_id,
                "decision": "revise",
                "summary": user_text,
            })
            article = _load_editorial_article(cfg, article_id)
            if article:
                await _send_preview_message(update.message, article_id, article)
            else:
                await update.message.reply_text(
                    "Resumo atualizado.",
                    reply_markup=_approval_buttons(article_id),
                )

        elif action == "manualsource":
            if not (user_text.startswith("http://") or user_text.startswith("https://")):
                if chat_id:
                    _set_pending_input(chat_id, article_id, "manualsource")
                await update.message.reply_text(
                    "URL invalido. Envie um link completo a comecar por http:// ou https://.",
                    disable_web_page_preview=True,
                )
                return

            article = _load_editorial_article(cfg, article_id)
            if not article:
                raise RuntimeError("artigo nao encontrado para source manual")

            selection_state = _ensure_message1_state(article)
            selected_profile = str(selection_state.get("content_profile", "news")).strip().lower() or "news"
            manual_url = user_text.strip()
            _update_message1_state(article_id, source_mode="manual", manual_url=manual_url)
            _set_triage_status(
                cfg,
                article_id,
                original_url=manual_url,
                source_link_origin="user",
                link_validation_status="valid",
                link_validation_confidence=1.0,
                link_validation_reason="Link manual fornecido pelo utilizador.",
                content_profile=selected_profile,
                telegram_triage_status="pending_approval",
            )
            article = _load_editorial_article(cfg, article_id)
            if article:
                article["content_profile"] = selected_profile
                article["original_url"] = manual_url
                article["source_link_origin"] = "user"
                article["link_validation_status"] = "valid"
                article["link_validation_confidence"] = 1.0
                article["link_validation_reason"] = "Link manual fornecido pelo utilizador."
            _pop_message1_state(article_id)
            await update.message.reply_text("Link manual recebido. Vou avancar para o preview.")
            if article:
                await _send_preview_message(update.message, article_id, article)
            else:
                await update.message.reply_text("Nao consegui carregar o preview deste artigo.")

        elif action == "needtext":
            if user_text.startswith("http://") or user_text.startswith("https://"):
                article = _load_editorial_article(cfg, article_id)
                if article:
                    _reset_message1_state(article, process_url=user_text, process_origin="user")
                _set_triage_status(
                    cfg,
                    article_id,
                    original_url=user_text,
                    source_link_origin="user",
                    link_validation_status="valid",
                    link_validation_confidence=1.0,
                    link_validation_reason="Link fornecido manualmente pelo utilizador.",
                    telegram_triage_status="awaiting_triage",
                )
                await update.message.reply_text(
                    text=f"Link recebido: {_esc(user_text)}\n\nAprovar processamento?",
                    parse_mode="HTML",
                    reply_markup=_triage_buttons(
                        article_id,
                        _ensure_message1_state(article or {"id": article_id, "original_url": user_text, "source_link_origin": "user"}),
                    ),
                    disable_web_page_preview=True,
                )
            else:
                _api_post(cfg, "articles/editorial-decision/", {
                    "article_id": article_id,
                    "decision": "revise",
                    "article_body": user_text,
                })
                _set_triage_status(cfg, article_id, telegram_triage_status="awaiting_triage")
                article = _load_editorial_article(cfg, article_id)
                await update.message.reply_text(
                    f"Texto recebido ({len(user_text)} chars).\n\nAprovar processamento?",
                    reply_markup=_triage_buttons(article_id, _ensure_message1_state(article or {"id": article_id})),
                )

        elif action == "requestchanges":
            article = _load_editorial_article(cfg, article_id)
            if not article:
                raise RuntimeError("artigo nao encontrado para revisao")

            def _build_revision() -> dict[str, Any]:
                return _build_preview_revision_payload(cfg, article, user_text)

            loop = asyncio.get_running_loop()
            revision_payload = await loop.run_in_executor(None, _build_revision)
            revision_payload["article_id"] = article_id
            result = _api_post(cfg, "articles/editorial-decision/", revision_payload)
            try:
                _set_triage_status(cfg, article_id, telegram_triage_status="pending_approval")
            except Exception:
                pass
            updated_article = result.get("article", {}) if isinstance(result, dict) else {}
            if not isinstance(updated_article, dict) or not updated_article:
                updated_article = _load_editorial_article(cfg, article_id)
            await update.message.reply_text("Preview atualizado. Vou reenviar a mensagem de preview.")
            if updated_article:
                await _send_preview_message(update.message, article_id, updated_article)
            else:
                await update.message.reply_text(
                    "Preview atualizado, mas nao consegui reconstruir a mensagem de preview.",
                    reply_markup=_approval_buttons(article_id),
                )
    except BaseException as e:
        if chat_id and action == "requestchanges":
            _set_pending_input(chat_id, article_id, "requestchanges")
        elif chat_id and action == "manualsource":
            _set_pending_input(chat_id, article_id, "manualsource")
        elif chat_id and action == "needtext":
            _set_pending_input(chat_id, article_id, "needtext")
        await update.message.reply_text(f"Erro: {_esc(str(e))}", parse_mode="HTML")


# ---------------------------------------------------------------------------
# Triage job - sends next pending article for Telegram review
# ---------------------------------------------------------------------------

# Set of article IDs already sent for triage in this session (prevents infinite resend)
_TRIAGE_SENT_IDS: set[int] = set()
_TRIAGE_SENT_LOCK = threading.Lock()


async def _send_next_for_triage(bot, cfg: Config):
    """Poll Django for the next article needing Telegram review."""
    article_id = None
    try:
        params: dict[str, Any] = {
            "mode": "oldest",
            "exclude_tg_triaged": "true",
        }
        if TELEGRAM_TRIAGE_NEWSLETTER_ID:
            params["newsletter_id"] = TELEGRAM_TRIAGE_NEWSLETTER_ID
        result = _api_get(cfg, "articles/editorial-pending/", params)
        if result.get("status") != "ok":
            return

        article = result.get("article")
        if not article:
            return

        article_id = article.get("id")
        if not article_id:
            return

        with _TRIAGE_SENT_LOCK:
            if article_id in _TRIAGE_SENT_IDS:
                return
            _TRIAGE_SENT_IDS.add(article_id)

        title = article.get("title", "")
        summary = article.get("summary", "")
        source_url = article.get("original_url", "")
        newsletter_meta = _load_newsletter_meta(cfg, article.get("newsletter_id"))

        validation_result = None
        link_model = OLLAMA_MODEL_LINK_VALIDATION or cfg.ollama_model_link_validation or cfg.ollama_model
        if source_url and "://" in source_url:
            try:
                loop = asyncio.get_running_loop()
                validation_result = await loop.run_in_executor(
                    None,
                    run_link_validation,
                    cfg.ollama_url,
                    link_model,
                    cfg.searxng_url,
                    newsletter_meta.get("subject", "") if newsletter_meta else title,
                    summary[:500],
                    source_url,
                )
            except Exception as e:
                LOG.warning("Link validation error: %s", e)
                validation_result = {
                    "status": "uncertain",
                    "confidence": 0.0,
                    "reason": f"Erro: {str(e)[:100]}",
                    "origin": "email",
                    "final_link": source_url,
                }

        proposed_title = str(article.get("proposed_title", "")).strip() or title
        if not str(article.get("proposed_title", "")).strip():
            try:
                loop = asyncio.get_running_loop()
                llm_title = await loop.run_in_executor(None, propose_title, cfg, summary or title, "news")
                if llm_title:
                    proposed_title = llm_title
            except Exception as e:
                LOG.warning("Title proposal failed: %s", e)

        process_url = source_url
        process_origin = str(article.get("source_link_origin", "email")).strip().lower() or "email"
        if validation_result:
            final_link = str(validation_result.get("final_link", "")).strip()
            if final_link and final_link != "NEEDS_USER_LINK":
                process_url = final_link
            process_origin = str(validation_result.get("origin", process_origin)).strip().lower() or process_origin
            article["link_validation_status"] = validation_result.get("status", "uncertain")
            article["link_validation_confidence"] = validation_result.get("confidence", 0.0)
            article["link_validation_reason"] = validation_result.get("reason", "")
            article["source_link_origin"] = validation_result.get("origin", process_origin)

        article["proposed_title"] = proposed_title
        selection_state = _reset_message1_state(article, process_url=process_url, process_origin=process_origin)
        preview = _format_triage_preview(
            article,
            selection_state,
            proposed_title=proposed_title,
            newsletter_meta=newsletter_meta,
        )
        reply_markup = _triage_buttons(article_id, selection_state)

        sent = False
        try:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=preview,
                parse_mode="HTML",
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            sent = True
        except Exception as e:
            LOG.warning("HTML send failed, trying plain: %s", e)
            try:
                plain = re.sub(r"</?[^>]+>", "", preview)
                await bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=plain,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                sent = True
            except Exception as e2:
                LOG.error("Total send failure: %s", e2)

        if sent:
            persist_payload: dict[str, Any] = {
                "article_id": article_id,
                "telegram_triage_status": "awaiting_triage",
                "proposed_title": proposed_title,
            }
            if validation_result:
                persist_payload.update({
                    "link_validation_status": validation_result.get("status", "uncertain"),
                    "link_validation_confidence": validation_result.get("confidence", 0.0),
                    "link_validation_reason": validation_result.get("reason", ""),
                    "source_link_origin": validation_result.get("origin", "email"),
                })
                final_link = validation_result.get("final_link", "")
                if final_link and final_link != source_url and final_link != "NEEDS_USER_LINK":
                    persist_payload["original_url"] = final_link

            try:
                _api_post(cfg, "articles/link-validation/", persist_payload)
            except Exception as e:
                LOG.warning("Failed to persist triage/validation state: %s", e)

            LOG.info("Triage sent for article_id=%s", article_id)
        else:
            _pop_message1_state(article_id)
            with _TRIAGE_SENT_LOCK:
                _TRIAGE_SENT_IDS.discard(article_id)

    except requests.exceptions.HTTPError as e:
        if article_id:
            _pop_message1_state(article_id)
            with _TRIAGE_SENT_LOCK:
                _TRIAGE_SENT_IDS.discard(article_id)
        if e.response and e.response.status_code == 404:
            return
        LOG.error("Triage poll error: %s", e)
    except Exception as e:
        if article_id:
            _pop_message1_state(article_id)
            with _TRIAGE_SENT_LOCK:
                _TRIAGE_SENT_IDS.discard(article_id)
        LOG.error("Triage error: %s", e, exc_info=True)


async def _triage_job(context: ContextTypes.DEFAULT_TYPE):
    cfg = _get_cfg()
    await _send_next_for_triage(context.bot, cfg)


# ---------------------------------------------------------------------------
# Bot lifecycle
# ---------------------------------------------------------------------------

def create_application(cfg: Config) -> Application:
    """Create and configure the Telegram bot application."""
    global _CFG
    _CFG = cfg

    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID not set")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))
    return app


def run_telegram_bot_thread(cfg: Config) -> threading.Thread:
    """Start the Telegram bot in a daemon thread."""

    def _worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            app = create_application(cfg)

            job_queue = app.job_queue
            job_queue.run_repeating(
                _triage_job,
                interval=TELEGRAM_TRIAGE_INTERVAL,
                first=10,
                name="telegram_triage",
            )

            LOG.info(
                "Telegram bot starting (triage every %ds)",
                TELEGRAM_TRIAGE_INTERVAL,
            )
            app.run_polling(
                allowed_updates=["callback_query", "message"],
                stop_signals=None,
                close_loop=False,
            )
        except Exception as e:
            LOG.exception("Telegram bot crashed: %s", e)
        finally:
            try:
                loop.close()
            except Exception:
                pass

    thread = threading.Thread(target=_worker, name="telegram-bot", daemon=True)
    thread.start()
    return thread
