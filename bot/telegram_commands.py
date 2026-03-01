"""Telegram command handlers.

AI overlay added to: /dailybrief /news /trends /funding /github /newprojects
NOT AI-powered: /rawsignals /sources /run /help

AI routing: HF → Gemini → existing non-AI output (fallback).
AI layer never crashes the bot; all failures are logged and silently fall back.
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from engine.pipeline import build_daily_payload
from engine.pipeline import run_pipeline
from storage.sqlite_store import SQLiteStore
from .formatter import (
    format_dailybrief,
    format_dailybrief_html,
    format_section_html,
)
from intelligence.llm_router import route_llm
from intelligence.prompts import (
    dailybrief_prompt,
    trends_prompt,
    news_prompt,
    funding_prompt,
    github_prompt,
    newprojects_prompt,
)

log = logging.getLogger(__name__)

# Telegram hard limit is 4096 characters for a message.
SAFE_MSG_LIMIT = 3800


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)


def _chunk_text(text: str, limit: int = SAFE_MSG_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]

    seps = ["\n\n", "\n", " "]
    chunks: list[str] = []
    remaining = text

    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        cut = -1
        for sep in seps:
            cut = remaining.rfind(sep, 0, limit)
            if cut > 0:
                cut = cut + len(sep)
                break
        if cut <= 0:
            cut = limit
        chunk = remaining[:cut].rstrip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[cut:].lstrip()

    return chunks


async def _send_chunks(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    parse_mode: Optional[str],
    disable_web_page_preview: bool,
) -> None:
    # Step 3: Per-chunk error handling with best-effort delivery.
    # Inter-chunk delay raised from 50ms → 500ms to respect Telegram's
    # ~1 message/second per-chat rate limit.
    chunks = _chunk_text(text, SAFE_MSG_LIMIT)
    log.info(
        "Telegram chunking: len=%s chunks=%s parse_mode=%s",
        len(text), len(chunks), parse_mode,
    )
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is None:
        return

    failed_chunks: list[int] = []

    for i, chunk in enumerate(chunks):
        try:
            if i == 0 and update.message is not None:
                await update.message.reply_text(
                    chunk,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                )
        except Exception as chunk_exc:
            log.warning(
                "Chunk %s/%s failed to send: %s — continuing best-effort delivery",
                i + 1, len(chunks), chunk_exc,
            )
            failed_chunks.append(i + 1)

        if len(chunks) > 1:
            await asyncio.sleep(0.5)  # 500ms between chunks

    # If any chunks failed, notify the user so they know output is partial.
    if failed_chunks and chat_id is not None:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ Part of the output failed to send "
                    f"(chunk(s) {failed_chunks} of {len(chunks)}) — "
                    "please use /rawsignals for full data."
                ),
                parse_mode=None,
                disable_web_page_preview=True,
            )
        except Exception:
            pass


async def _safe_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
) -> None:
    """Send with guaranteed delivery and never-raise guarantee.

    Step 2: Proactively chunks when len(text) > SAFE_MSG_LIMIT, eliminating
    reliance on Telegram's BadRequest exception to trigger chunking.
    """
    if update.message is None:
        return

    # Step 2: Proactive pre-check — chunk before attempting any Telegram send.
    if len(text) > SAFE_MSG_LIMIT:
        log.info(
            "Proactive chunking triggered: len=%s > SAFE_MSG_LIMIT=%s",
            len(text), SAFE_MSG_LIMIT,
        )
        try:
            await _send_chunks(update, context, text,
                               parse_mode=parse_mode,
                               disable_web_page_preview=disable_web_page_preview)
            return
        except Exception as e_chunk:
            log.warning("Proactive chunked send failed; attempting plain text: %s", e_chunk)
            try:
                plain = _strip_html(text) if (parse_mode == ParseMode.HTML) else text
                await _send_chunks(update, context, plain,
                                   parse_mode=None,
                                   disable_web_page_preview=disable_web_page_preview)
                return
            except Exception as e_plain:
                log.exception("Plain text chunked send also failed: %s", e_plain)
                try:
                    await update.message.reply_text(
                        "Output was too large to deliver completely. Please use /rawsignals.",
                        parse_mode=None,
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
                return

    # Text fits in a single message — attempt direct send.
    try:
        await update.message.reply_text(
            text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
        return
    except BadRequest as e:
        err = str(e)
        log.warning("Telegram send failed; recovering. err=%s", err)

        if "Message is too long" in err:
            try:
                await _send_chunks(update, context, text,
                                   parse_mode=parse_mode,
                                   disable_web_page_preview=disable_web_page_preview)
                return
            except BadRequest as e2:
                log.warning("Chunked send failed; plain text fallback. err=%s", e2)

        try:
            plain = _strip_html(text) if (parse_mode == ParseMode.HTML) else text
            await _send_chunks(update, context, plain,
                               parse_mode=None,
                               disable_web_page_preview=disable_web_page_preview)
        except Exception as e3:
            log.exception("Telegram recovery failed: %s", e3)
            if update.message is not None:
                await update.message.reply_text(
                    "Output was too large to deliver completely. Please use /rawsignals.",
                    parse_mode=None,
                    disable_web_page_preview=True,
                )


# ──────────────────────────────────────────────────────────────────────────────
# AI helper
# ──────────────────────────────────────────────────────────────────────────────

async def _ai_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    prompt: str,
    cfg: dict,
    fallback_text: str,
    fallback_parse_mode: Optional[str],
    *,
    cmd_name: str = "",          # command name for cache lookup
) -> None:
    """Serve cached AI response if available; otherwise call LLM router; otherwise fallback.

    Priority:
      1. Cached AI response from ai_responses table (fastest, no quota burn).
      2. On-demand LLM call via route_llm() (only when no cache exists).
      3. Existing formatter output (fallback if both above fail).

    Never raises.
    """
    store: SQLiteStore = context.application.bot_data.get("store")

    # 1. Try cache first
    if cmd_name and store is not None:
        try:
            cached = store.get_ai_response(cmd_name)
            if cached:
                # Step 5: Log cmd name + length for the cache-hit path.
                log.info(
                    "ai_cache: serving cached response for cmd=%s (len=%s)",
                    cmd_name, len(cached),
                )
                log.info(
                    "AI response for cmd=%s len=%s (source=cache)",
                    cmd_name, len(cached),
                )
                await _safe_reply(
                    update, context,
                    cached,
                    parse_mode=None,
                    disable_web_page_preview=True,
                )
                return
        except Exception as exc:
            log.warning(
                "ai_cache: cache lookup failed for cmd=%s: %s; falling through to LLM",
                cmd_name, exc,
            )

    # 2. On-demand LLM call (cache miss or no cmd_name)
    ai_text: Optional[str] = None
    try:
        ai_text = await route_llm(prompt, cfg)
    except Exception as exc:
        log.warning("AI layer raised unexpectedly (will use fallback): %s", exc)

    if ai_text:
        # Step 5: Log cmd name + length for the on-demand (cache-miss) path.
        log.info(
            "AI response received on-demand (len=%s), sending to Telegram", len(ai_text)
        )
        log.info(
            "AI response for cmd=%s len=%s (source=on-demand)", cmd_name, len(ai_text)
        )
        # Store for future requests
        if cmd_name and store is not None:
            try:
                from datetime import datetime, timezone
                window_id = datetime.now(timezone.utc).isoformat()
                store.save_ai_response(cmd_name, ai_text, window_id=window_id, provider="on-demand")
                log.info("ai_cache: stored on-demand response for cmd=%s", cmd_name)
            except Exception as exc:
                log.warning(
                    "ai_cache: failed to store on-demand response for cmd=%s: %s",
                    cmd_name, exc,
                )
        await _safe_reply(
            update, context,
            ai_text,
            parse_mode=None,
            disable_web_page_preview=True,
        )
    else:
        # 3. Fallback to existing formatter output
        log.info("AI unavailable; using existing formatter output for cmd=%s", cmd_name)
        await _safe_reply(
            update, context,
            fallback_text,
            parse_mode=fallback_parse_mode,
            disable_web_page_preview=True,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Window / limit helpers
# ──────────────────────────────────────────────────────────────────────────────

def _window_since(cfg) -> "datetime":
    from datetime import datetime, timedelta, timezone
    hours = int(cfg.get("storage", {}).get("rolling_window_hours", 24))
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)


def _section_limit(cfg) -> int:
    return int(cfg.get("analysis", {}).get("top_signals_to_analyze", 10))


# ──────────────────────────────────────────────────────────────────────────────
# AI-powered command handlers
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    payload = build_daily_payload(cfg, store)
    fallback = format_dailybrief_html(payload)
    prompt = dailybrief_prompt(payload)

    await _ai_reply(
        update, context,
        prompt=prompt,
        cfg=cfg,
        fallback_text=fallback,
        fallback_parse_mode=ParseMode.HTML,
        cmd_name="dailybrief",
    )


async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    signals = store.get_signals_since(since, "news", limit=limit)

    fallback = format_section_html("News", signals)
    prompt = news_prompt(signals)

    await _ai_reply(
        update, context,
        prompt=prompt,
        cfg=cfg,
        fallback_text=fallback,
        fallback_parse_mode=ParseMode.HTML,
        cmd_name="news",
    )


async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    payload = build_daily_payload(cfg, store, include_sections=False)
    trends_data = (payload.get("inputs", {}) or {}).get("trends", {})
    rows = (trends_data or {}).get("trends") or []

    # Build fallback (existing trend display)
    lines: list[str] = [f"<b>Trends — {html.escape(payload.get('date',''))}</b>"]
    if not rows:
        lines.append("<i>No trends in the last 24h.</i>")
    else:
        lines.append("<i>Top chain × sector clusters</i>")
        for r in rows:
            try:
                chain = html.escape(str(r.get("chain", "unknown")))
                sector = html.escape(str(r.get("sector", "unknown")))
                count = html.escape(str(r.get("count", 0)))
                score_sum = html.escape(str(round(float(r.get("score_sum", 0.0)), 2)))
            except Exception:
                continue
            lines.append(f"• <b>{chain}</b> · {sector} — count {count} — scoreΣ {score_sum}")
    fallback = "\n".join(lines).strip()

    # For trends prompt we pass all signals (not just top-N)
    since = _window_since(cfg)
    all_signals = store.get_signals_since(since, source=None, limit=50)
    prompt = trends_prompt(all_signals, trends_data)

    await _ai_reply(
        update, context,
        prompt=prompt,
        cfg=cfg,
        fallback_text=fallback,
        fallback_parse_mode=ParseMode.HTML,
        cmd_name="trends",
    )


async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    funding = store.get_signals_since(since, "funding", limit=limit)
    ecosystem = store.get_signals_since(since, "ecosystem", limit=limit)
    combined = (funding + ecosystem)[:limit]

    fallback = format_section_html("Funding & Ecosystem", combined)
    prompt = funding_prompt(combined)

    await _ai_reply(
        update, context,
        prompt=prompt,
        cfg=cfg,
        fallback_text=fallback,
        fallback_parse_mode=ParseMode.HTML,
        cmd_name="funding",
    )


async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    signals = store.get_signals_since(since, "github", limit=limit)

    fallback = format_section_html("GitHub", signals)
    prompt = github_prompt(signals)

    await _ai_reply(
        update, context,
        prompt=prompt,
        cfg=cfg,
        fallback_text=fallback,
        fallback_parse_mode=ParseMode.HTML,
        cmd_name="github",
    )


async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    twitter = store.get_signals_since(since, "twitter", limit=limit)
    github = store.get_signals_since(since, "github", limit=limit)
    combined = (twitter + github)[:limit]

    fallback = format_section_html("New Projects", combined)
    prompt = newprojects_prompt(combined)

    await _ai_reply(
        update, context,
        prompt=prompt,
        cfg=cfg,
        fallback_text=fallback,
        fallback_parse_mode=ParseMode.HTML,
        cmd_name="newprojects",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Non-AI command handlers (unchanged)
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    signals = store.get_signals_since(since, source=None, limit=50)
    await _safe_reply(
        update, context,
        format_section_html("Raw Signals", signals),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = [
        "<b>Web3 Intelligence Bot</b>",
        "",
        "Commands:",
        "• /dailybrief — AI-powered daily brief (last 24h)",
        "• /news — AI news analysis",
        "• /funding — AI funding & ecosystem analysis",
        "• /github — AI GitHub activity analysis",
        "• /newprojects — AI new projects analysis",
        "• /trends — AI market narrative & trend analysis",
        "• /rawsignals — ungrouped signal dump (raw)",
        "• /run — trigger ingestion now (rate-limited)",
        "• /sources — show configured ingestion sources",
    ]
    await _safe_reply(
        update, context,
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        log.exception("Unhandled telegram error", exc_info=context.error)
    except Exception:
        pass


def _manual_run_meta_key_utc() -> str:
    from datetime import datetime, timezone
    return f"manual_run_count:{datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d')}"


def _get_manual_run_count(store: SQLiteStore) -> int:
    try:
        v = store.get_meta(_manual_run_meta_key_utc())
        return int(v) if v is not None else 0
    except Exception:
        return 0


def _set_manual_run_count(store: SQLiteStore, count: int) -> None:
    store.set_meta(_manual_run_meta_key_utc(), str(int(count)))


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manual pipeline trigger (rate-limited, persisted). Non-AI."""
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    lock: asyncio.Lock | None = context.application.bot_data.get("pipeline_lock")
    if lock is None:
        lock = asyncio.Lock()
        context.application.bot_data["pipeline_lock"] = lock

    if lock.locked():
        await _safe_reply(update, context,
                          "Pipeline already running, try again shortly.",
                          parse_mode=None, disable_web_page_preview=True)
        return

    MAX_PER_DAY = 5
    count = _get_manual_run_count(store)
    remaining = max(0, MAX_PER_DAY - count)
    if remaining <= 0:
        await _safe_reply(update, context,
                          "Manual runs limit reached (5/day). Try again tomorrow.",
                          parse_mode=None, disable_web_page_preview=True)
        return

    from datetime import datetime, timedelta, timezone
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)
    log.info("Manual /run: since=%s remaining=%s", since.isoformat(), remaining - 1)

    try:
        _set_manual_run_count(store, count + 1)
    except Exception:
        log.exception("Failed to persist manual run count")

    try:
        async with lock:
            result = await run_pipeline(cfg, store, manual=True, since_override=since)
        inserted = result.get("inserted", 0)
        total_seen = result.get("count", 0)
        await _safe_reply(
            update, context,
            f"✅ Pipeline run complete (last 24h). inserted={inserted} total_seen={total_seen}. Remaining today: {max(0, MAX_PER_DAY - (count + 1))}",
            parse_mode=None, disable_web_page_preview=True,
        )
        # Trigger AI pre-computation after successful manual run.
        from intelligence.ai_cache import run_post_ingest_ai_generation
        from datetime import datetime, timezone as _tz
        window_id = datetime.now(_tz.utc).isoformat()
        await run_post_ingest_ai_generation(cfg, store, window_id)
    except Exception as e:
        log.exception("Manual /run failed")
        await _safe_reply(update, context,
                          f"⚠️ Pipeline run failed: {e}",
                          parse_mode=None, disable_web_page_preview=True)


async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show currently configured ingestion sources. Non-AI."""
    config = context.bot_data.get("config", {})
    ing = config.get("ingestion", {})

    sections = [
        ("News RSS", "news_sources", "NEWS_SOURCES / NEWS_RSS_EXTRA_SOURCES"),
        ("News Web", "news_web_sources", "NEWS_WEB_SOURCES / NEWS_WEB_EXTRA_SOURCES"),
        ("News API", "news_api_sources", "NEWS_API_SOURCES / NEWS_API_EXTRA_SOURCES"),
        ("Funding RSS", "funding_rss_sources", "FUNDING_RSS_SOURCES / FUNDING_RSS_EXTRA_SOURCES"),
        ("Funding Web", "funding_web_sources", "FUNDING_WEB_SOURCES / FUNDING_WEB_EXTRA_SOURCES"),
        ("Funding API", "funding_api_sources", "FUNDING_API_SOURCES / FUNDING_API_EXTRA_SOURCES"),
        ("Ecosystem RSS", "ecosystem_rss_sources", "ECOSYSTEM_RSS_SOURCES / ECOSYSTEM_RSS_EXTRA_SOURCES"),
        ("Ecosystem Web", "ecosystem_web_sources", "ECOSYSTEM_WEB_SOURCES / ECOSYSTEM_WEB_EXTRA_SOURCES"),
        ("Ecosystem API", "ecosystem_api_sources", "ECOSYSTEM_API_SOURCES / ECOSYSTEM_API_EXTRA_SOURCES"),
        ("GitHub Queries", "github_queries", "GITHUB_QUERIES / GITHUB_EXTRA_QUERIES"),
        ("Twitter Mode", "twitter_mode", "TWITTER_MODE"),
        ("Twitter RSS", "twitter_rss_sources", "TWITTER_RSS_SOURCES"),
    ]

    github_queries = config.get("github", {}).get("queries", [])

    lines: list[str] = ["<b>Current ingestion sources (runtime config)</b>", ""]
    for label, key, env_hint in sections:
        if key == "github_queries":
            sources = github_queries
        else:
            val = ing.get(key)
            if val is None:
                continue
            sources = [val] if isinstance(val, str) else [str(s) for s in val] if isinstance(val, list) else [str(val)]

        lines.append(f"<b>{html.escape(label)}</b> <i>(env: {html.escape(env_hint)})</i>")
        if sources:
            for s in sources[:20]:
                lines.append(f"• {html.escape(str(s))}")
            if len(sources) > 20:
                lines.append(f"… (+{len(sources) - 20} more)")
        else:
            lines.append("(none configured)")
        lines.append("")

    await _safe_reply(
        update, context,
        "\n".join(lines).strip(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
