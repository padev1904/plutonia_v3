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
import logging
import os
import re
import threading
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

from process_newsletter import Config, _llm_generate, _safe_json_object
from link_validator import run_link_validation

LOG = logging.getLogger("telegram_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORTAL_PUBLIC_BASE_URL = os.getenv("PORTAL_PUBLIC_BASE_URL", "").rstrip("/")
TELEGRAM_TRIAGE_INTERVAL = int(os.getenv("TELEGRAM_TRIAGE_INTERVAL", "15"))

# Per-task model configuration (Fase 6)
OLLAMA_MODEL_TITLE = os.getenv("OLLAMA_MODEL_TITLE", "").strip()
OLLAMA_MODEL_LINK_VALIDATION = os.getenv("OLLAMA_MODEL_LINK_VALIDATION", "").strip()


def _esc(text: str) -> str:
    return html_module.escape(str(text)) if text else ""


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


# ---------------------------------------------------------------------------
# Title generation (Fase 3 — mandatory LLM title)
# ---------------------------------------------------------------------------

TITLE_PROPOSAL_PROMPT = """You are a title generation assistant.
Generate a single concise and engaging title for the content below.
The title must be based only on the text provided.

Output ONLY raw JSON with this exact structure:
{{
  "title": "..."
}}

Profile: {content_profile}
Text:
{article_text}
"""


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
    proposed_title: str,
    validation_result: dict | None = None,
    newsletter_meta: dict | None = None,
) -> str:
    """Build first-step triage preview message with link validation info."""
    title = _esc(article.get("title", "Sem título"))
    source_url = article.get("original_url", "")
    summary = article.get("summary", "")
    first_words = " ".join(summary.split()[:25]) if summary else "(sem resumo)"

    lines = [
        "<b>Triagem editorial</b>",
    ]

    if newsletter_meta:
        subject = newsletter_meta.get("subject", "")
        sender = newsletter_meta.get("original_sender_name", "")
        if subject:
            lines.append(f"Email: <b>{_esc(subject)}</b>")
        if sender:
            lines.append(f"Sender: {_esc(sender)}")

    lines.extend([
        f"Titulo original: <b>{title}</b>",
        f"Titulo (modelo): <b>{_esc(proposed_title)}</b>",
    ])

    if source_url and "://" in source_url:
        lines.append(f"Link: {_esc(source_url)}")
    else:
        lines.append("Link: desconhecido")

    # Link validation block
    if validation_result:
        status = validation_result.get("status", "not_checked")
        confidence = validation_result.get("confidence", 0.0)
        reason = validation_result.get("reason", "")
        origin = validation_result.get("origin", "email")

        status_labels = {
            "valid": "✅ validado",
            "invalid": "❌ inválido",
            "uncertain": "⚠️ incerto",
        }
        lines.append("")
        lines.append(f"<b>Validação link:</b> {status_labels.get(status, status)}")
        lines.append(f"Confiança: {confidence:.0%}")
        if origin == "search":
            lines.append("Origem: corrigido por pesquisa")
        if reason:
            lines.append(f"Motivo: <i>{_esc(reason[:150])}</i>")

    lines.extend([
        "",
        f"<b>Resumo:</b> <i>{_esc(first_words)}...</i>",
        "",
        "Aprovar envio para processamento?",
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
        f"[{profile_label}]",
        f"<b>{_esc(article.get('title', ''))}</b>",
        f"<b>{taxonomy}</b>",
        "",
    ]

    if PORTAL_PUBLIC_BASE_URL and preview_token:
        card_url = f"{PORTAL_PUBLIC_BASE_URL}/preview/card/{preview_token}/"
        article_url = f"{PORTAL_PUBLIC_BASE_URL}/preview/{preview_token}/"
        lines.append(f'Card: <a href="{_esc(card_url)}">{_esc(card_url)}</a>')
        lines.append(f'Artigo: <a href="{_esc(article_url)}">{_esc(article_url)}</a>')
    elif preview_token:
        lines.append(f"Preview token: {preview_token}")

    if source_url and "://" in source_url:
        lines.append(f'Fonte: <a href="{_esc(source_url)}">{_esc(source_url)}</a>')

    lines.extend(["", "Aprovar publicação no portal?"])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Inline keyboards
# ---------------------------------------------------------------------------

def _triage_buttons(article_id: int, validation_status: str = "", has_source_url: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("Enviar ao LLM", callback_data=f"process_{article_id}")],
        [InlineKeyboardButton("Marcar como Recurso", callback_data=f"resource_{article_id}")],
    ]
    if not has_source_url or validation_status in ("uncertain", "invalid"):
        buttons.append(
            [InlineKeyboardButton("Pedir texto/link completo", callback_data=f"needtext_{article_id}")]
        )
    buttons.append([InlineKeyboardButton("Rejeitar", callback_data=f"reject_{article_id}")])
    return InlineKeyboardMarkup(buttons)


def _approval_buttons(article_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Publicar", callback_data=f"publish_{article_id}")],
        [InlineKeyboardButton("Atualizar preview", callback_data=f"regen_{article_id}")],
        [
            InlineKeyboardButton("Editar titulo", callback_data=f"edittitle_{article_id}"),
            InlineKeyboardButton("Editar resumo", callback_data=f"editsummary_{article_id}"),
        ],
        [InlineKeyboardButton("Rejeitar", callback_data=f"reject_{article_id}")],
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


# ---------------------------------------------------------------------------
# Callback handlers
# ---------------------------------------------------------------------------

async def _handle_publish(query, article_id: int):
    """Approve and publish an article."""
    cfg = _get_cfg()
    try:
        _api_post(cfg, "articles/editorial-decision/", {
            "article_id": article_id,
            "decision": "approve",
        })
        try:
            _api_post(cfg, "articles/link-validation/", {
                "article_id": article_id,
                "telegram_triage_status": "approved",
            })
        except Exception:
            pass
        with _TRIAGE_SENT_LOCK:
            _TRIAGE_SENT_IDS.discard(article_id)
        await query.edit_message_text(text="Artigo aprovado e publicado.")
    except Exception as e:
        await query.edit_message_text(
            text=f"Erro na publicação: {_esc(str(e))}",
            parse_mode="HTML",
            reply_markup=_approval_buttons(article_id),
        )


async def _handle_reject(query, article_id: int):
    """Reject an article."""
    cfg = _get_cfg()
    try:
        _api_post(cfg, "articles/editorial-decision/", {
            "article_id": article_id,
            "decision": "reject",
        })
        try:
            _api_post(cfg, "articles/link-validation/", {
                "article_id": article_id,
                "telegram_triage_status": "rejected",
            })
        except Exception:
            pass
        with _TRIAGE_SENT_LOCK:
            _TRIAGE_SENT_IDS.discard(article_id)
        await query.edit_message_text(text="Artigo rejeitado.")
    except Exception as e:
        await query.edit_message_text(
            text=f"Erro ao rejeitar: {_esc(str(e))}",
            parse_mode="HTML",
            reply_markup=_approval_buttons(article_id),
        )


async def _handle_process(query, article_id: int):
    """Send article through LLM processing (already done by server pipeline)."""
    cfg = _get_cfg()
    try:
        # The article should already be processed by the server pipeline.
        # Show the structured preview from the existing data.
        result = _api_get(cfg, "articles/editorial-data/", {"article_id": str(article_id)})
        article = result.get("article", {})
        if not article:
            await query.edit_message_text(text="Artigo não encontrado.")
            return

        content_profile = article.get("content_profile", "news")
        preview = _format_structured_preview(article, content_profile)
        try:
            await query.edit_message_text(
                text=preview,
                parse_mode="HTML",
                reply_markup=_approval_buttons(article_id),
            )
        except Exception:
            plain = re.sub(r"</?[^>]+>", "", preview)
            await query.edit_message_text(text=plain, reply_markup=_approval_buttons(article_id))
    except Exception as e:
        await query.edit_message_text(
            text=f"Erro ao carregar artigo: {_esc(str(e))}",
            parse_mode="HTML",
            reply_markup=_approval_buttons(article_id),
        )


async def _handle_resource(query, article_id: int):
    """Mark article as resource then show approval preview."""
    cfg = _get_cfg()
    try:
        _api_post(cfg, "articles/editorial-decision/", {
            "article_id": article_id,
            "decision": "approve",
            "section": "Recursos",
        })
        try:
            _api_post(cfg, "articles/link-validation/", {
                "article_id": article_id,
                "telegram_triage_status": "approved",
                "content_profile": "resource",
            })
        except Exception:
            pass
        with _TRIAGE_SENT_LOCK:
            _TRIAGE_SENT_IDS.discard(article_id)
        await query.edit_message_text(text="Marcado como recurso e aprovado.")
    except Exception as e:
        await query.edit_message_text(
            text=f"Erro ao marcar como recurso: {_esc(str(e))}",
            parse_mode="HTML",
            reply_markup=_triage_buttons(article_id),
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


async def _handle_needtext(query, article_id: int):
    """Ask user for full text or link."""
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _set_pending_input(chat_id, article_id, "needtext")
    msg = (
        "<b>Texto/link completo necessário</b>\n\n"
        "Responda com:\n"
        "- um URL para o artigo completo, ou\n"
        "- o texto integral do artigo."
    )
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Rejeitar em vez disso", callback_data=f"reject_{article_id}")]
    ])
    await query.edit_message_text(text=msg, parse_mode="HTML", reply_markup=reply_markup)


async def _handle_edit_title(query, article_id: int):
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


async def _handle_edit_summary(query, article_id: int):
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


async def _handle_cancel(query, article_id: int):
    chat_id = query.message.chat_id if query.message else None
    if chat_id:
        _pop_pending_input(chat_id)
    await _handle_process(query, article_id)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    chat_id = query.message.chat_id if query.message else None
    if not _is_authorized(chat_id):
        await query.answer("Chat não autorizado.", show_alert=True)
        return

    await query.answer()
    data = query.data or ""

    # Any button click cancels any pending text input (edit/needtext)
    if chat_id:
        _pop_pending_input(chat_id)

    handlers = {
        "process_": _handle_process,
        "resource_": _handle_resource,
        "publish_": _handle_publish,
        "reject_": _handle_reject,
        "needtext_": _handle_needtext,
        "regen_": _handle_process,
        "edittitle_": _handle_edit_title,
        "editsummary_": _handle_edit_summary,
        "cancel_": _handle_cancel,
    }

    for prefix, handler in handlers.items():
        if data.startswith(prefix):
            try:
                article_id = int(data[len(prefix):])
                await handler(query, article_id)
            except ValueError:
                LOG.warning("Invalid article_id in callback: %s", data)
            return

    LOG.warning("Unknown callback: %s", data)


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages for edit flows and user link/text input."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not _is_authorized(chat_id):
        return

    if not update.message or not update.message.text:
        return

    user_text = update.message.text.strip()
    if not user_text:
        return

    # Check if there's a pending input action for this chat
    pending = _pop_pending_input(chat_id) if chat_id else None
    if not pending:
        return

    article_id = pending["article_id"]
    action = pending["action"]
    cfg = _get_cfg()

    try:
        if action == "edittitle":
            _api_post(cfg, "articles/editorial-decision/", {
                "article_id": article_id,
                "decision": "revise",
                "title": user_text[:500],
            })
            # Re-fetch article and show preview with buttons
            try:
                result = _api_get(cfg, "articles/editorial-data/", {"article_id": str(article_id)})
                article = result.get("article", {})
                if article:
                    cp = article.get("content_profile", "news")
                    preview = _format_structured_preview(article, cp)
                    await update.message.reply_text(
                        text=f"Titulo atualizado.\n\n{preview}",
                        parse_mode="HTML",
                        reply_markup=_approval_buttons(article_id),
                    )
                else:
                    await update.message.reply_text(
                        f"Titulo atualizado para: <b>{_esc(user_text[:500])}</b>",
                        parse_mode="HTML",
                        reply_markup=_approval_buttons(article_id),
                    )
            except Exception:
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
            try:
                result = _api_get(cfg, "articles/editorial-data/", {"article_id": str(article_id)})
                article = result.get("article", {})
                if article:
                    cp = article.get("content_profile", "news")
                    preview = _format_structured_preview(article, cp)
                    await update.message.reply_text(
                        text=f"Resumo atualizado.\n\n{preview}",
                        parse_mode="HTML",
                        reply_markup=_approval_buttons(article_id),
                    )
                else:
                    await update.message.reply_text(
                        "Resumo atualizado.",
                        reply_markup=_approval_buttons(article_id),
                    )
            except Exception:
                await update.message.reply_text(
                    "Resumo atualizado.",
                    reply_markup=_approval_buttons(article_id),
                )

        elif action == "needtext":
            if user_text.startswith("http://") or user_text.startswith("https://"):
                _api_post(cfg, "articles/link-validation/", {
                    "article_id": article_id,
                    "original_url": user_text,
                    "source_link_origin": "user",
                    "link_validation_status": "valid",
                    "link_validation_confidence": 1.0,
                    "link_validation_reason": "Link fornecido manualmente pelo utilizador.",
                })
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Enviar ao LLM", callback_data=f"process_{article_id}")],
                    [InlineKeyboardButton("Marcar como Recurso", callback_data=f"resource_{article_id}")],
                    [InlineKeyboardButton("Rejeitar", callback_data=f"reject_{article_id}")],
                ])
                await update.message.reply_text(
                    f"Link recebido: {_esc(user_text)}\n\nAprovar envio para processamento?",
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                )
            else:
                _api_post(cfg, "articles/editorial-decision/", {
                    "article_id": article_id,
                    "decision": "revise",
                    "article_body": user_text,
                })
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Enviar ao LLM", callback_data=f"process_{article_id}")],
                    [InlineKeyboardButton("Marcar como Recurso", callback_data=f"resource_{article_id}")],
                    [InlineKeyboardButton("Rejeitar", callback_data=f"reject_{article_id}")],
                ])
                await update.message.reply_text(
                    f"Texto recebido ({len(user_text)} chars).\n\nAprovar envio para processamento?",
                    reply_markup=reply_markup,
                )
    except Exception as e:
        await update.message.reply_text(f"Erro: {_esc(str(e))}", parse_mode="HTML")


# ---------------------------------------------------------------------------
# Triage job — sends next pending article for Telegram review
# ---------------------------------------------------------------------------

# Set of article IDs already sent for triage in this session (prevents infinite resend)
_TRIAGE_SENT_IDS: set[int] = set()
_TRIAGE_SENT_LOCK = threading.Lock()


async def _send_next_for_triage(bot, cfg: Config):
    """Poll Django for the next article needing Telegram review."""
    article_id = None
    try:
        # Get next article in editorial pending state (exclude already-triaged)
        result = _api_get(cfg, "articles/editorial-pending/", {
            "mode": "oldest",
            "exclude_tg_triaged": "true",
        })
        if result.get("status") != "ok":
            return

        article = result.get("article")
        if not article:
            return

        article_id = article.get("id")
        if not article_id:
            return

        # --- Guard: skip if already sent in this session ---
        with _TRIAGE_SENT_LOCK:
            if article_id in _TRIAGE_SENT_IDS:
                return
            # Add immediately to prevent concurrent job from picking same article
            _TRIAGE_SENT_IDS.add(article_id)

        title = article.get("title", "")
        summary = article.get("summary", "")
        source_url = article.get("original_url", "")
        newsletter_id = article.get("newsletter_id")

        # Get newsletter metadata
        newsletter_meta = None
        if newsletter_id:
            try:
                nl_data = _api_get(cfg, f"newsletter/{newsletter_id}/raw/")
                newsletter_meta = {
                    "subject": nl_data.get("subject", ""),
                    "original_sender_name": nl_data.get("original_sender_name", ""),
                    "original_sender_email": nl_data.get("original_sender_email", ""),
                }
            except Exception:
                pass

        # Run link validation (Fase 2)
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

        # Propose title via LLM (Fase 3)
        proposed_title = title  # Default to existing title
        try:
            loop = asyncio.get_running_loop()
            llm_title = await loop.run_in_executor(
                None, propose_title, cfg, summary or title, "news",
            )
            if llm_title:
                proposed_title = llm_title
        except Exception as e:
            LOG.warning("Title proposal failed: %s", e)

        # Build and send triage message
        val_status = validation_result.get("status", "") if validation_result else ""
        preview = _format_triage_preview(
            article,
            proposed_title=proposed_title,
            validation_result=validation_result,
            newsletter_meta=newsletter_meta,
        )
        reply_markup = _triage_buttons(
            article_id,
            validation_status=val_status,
            has_source_url=bool(source_url and "://" in source_url),
        )

        sent = False
        try:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=preview,
                parse_mode="HTML",
                reply_markup=reply_markup,
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
                )
                sent = True
            except Exception as e2:
                LOG.error("Total send failure: %s", e2)

        if sent:
            # Persist triage status + link validation in one API call
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
                # If link was corrected by search, update the URL too
                final_link = validation_result.get("final_link", "")
                if final_link and final_link != source_url and final_link != "NEEDS_USER_LINK":
                    persist_payload["original_url"] = final_link

            try:
                _api_post(cfg, "articles/link-validation/", persist_payload)
            except Exception as e:
                LOG.warning("Failed to persist triage/validation state: %s", e)

            LOG.info("Triage sent for article_id=%s", article_id)
        else:
            # Send failed — allow retry on next cycle
            with _TRIAGE_SENT_LOCK:
                _TRIAGE_SENT_IDS.discard(article_id)

    except requests.exceptions.HTTPError as e:
        # Cleanup on error so article can be retried
        if article_id:
            with _TRIAGE_SENT_LOCK:
                _TRIAGE_SENT_IDS.discard(article_id)
        if e.response and e.response.status_code == 404:
            return  # No pending articles
        LOG.error("Triage poll error: %s", e)
    except Exception as e:
        if article_id:
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
