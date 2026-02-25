from __future__ import annotations

import asyncio
import html
import logging
import re
from importlib import import_module
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

log = logging.getLogger(__name__)

# Telegram hard limit is 4096 characters for a message.
# Use a conservative limit to avoid edge cases with entities.
SAFE_MSG_LIMIT = 3800


def _strip_html(text: str) -> str:
    # Very small helper: Telegram HTML supports only a subset; stripping for plain fallback.
    return re.sub(r"<[^>]+>", "", text)


def _chunk_text(text: str, limit: int = SAFE_MSG_LIMIT) -> list[str]:
    """Split text into chunks <= limit.

    Strategy:
    - Prefer splitting on double newlines (section boundaries)
    - Then single newlines
    - Then spaces
    - Finally hard cut

    This works well for our HTML formatter because tags are closed inside each block.
    """
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
    chunks = _chunk_text(text, SAFE_MSG_LIMIT)
    log.info(
        "Telegram chunking: len=%s chunks=%s parse_mode=%s",
        len(text),
        len(chunks),
        parse_mode,
    )

    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is None:
        # Should never happen in real bot usage.
        return

    # First chunk as a reply (keeps context). Subsequent chunks as normal messages.
    for i, chunk in enumerate(chunks):
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
        # Tiny delay to be polite and avoid burst issues.
        if len(chunks) > 1:
            await asyncio.sleep(0.05)


async def _safe_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
) -> None:
    """Send a message with guaranteed delivery.

    Order:
    1) Try send as a single message.
    2) If "Message is too long" => chunk and send.
    3) If entity parse fails (HTML/Markdown) => retry once as plain text (chunked if needed).

    This function must never raise and must not crash handlers.
    """
    try:
        if update.message is None:
            return
        await update.message.reply_text(
            text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
        return
    except BadRequest as e:
        err = str(e)
        preview = text[:200].replace("\n", " ")
        log.warning(
            "Telegram send failed; attempting recovery. err=%s preview=%r", err, preview
        )

        # Chunking for message-too-long.
        if "Message is too long" in err:
            try:
                await _send_chunks(
                    update,
                    context,
                    text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                )
                return
            except BadRequest as e2:
                # If chunking still fails due to parse mode, fall through to plain text.
                log.warning("Chunked send failed; falling back to plain text. err=%s", e2)

        # Parse errors or chunking failures: retry plain text, still chunked.
        try:
            plain = _strip_html(text) if (parse_mode == ParseMode.HTML) else text
            await _send_chunks(
                update,
                context,
                plain,
                parse_mode=None,
                disable_web_page_preview=disable_web_page_preview,
            )
        except Exception as e3:
            # Absolute last resort: send a minimal error note.
            log.exception("Telegram recovery failed: %s", e3)
            if update.message is not None:
                await update.message.reply_text(
                    "Output was too large to deliver completely. Please use /rawsignals with a smaller window.",
                    parse_mode=None,
                    disable_web_page_preview=True,
                )


async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    payload = build_daily_payload(cfg, store)

    # Prefer HTML for robustness; fallback is handled in _safe_reply.
    await _safe_reply(
        update,
        context,
        format_dailybrief_html(payload),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# Backwards-compatible: keep /rawsignals simple.
async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    payload = build_daily_payload(cfg, store)
    # Raw view uses MarkdownV2 for brevity; safe reply will fallback.
    await _safe_reply(
        update,
        context,
        format_dailybrief(payload),
        parse_mode=ParseMode.MARKDOWN_V2,
        disable_web_page_preview=True,
    )


def _window_since(cfg) -> "datetime":
    from datetime import datetime, timedelta

    hours = int(cfg.get("storage", {}).get("rolling_window_hours", 24))
    return datetime.utcnow() - timedelta(hours=hours)


def _section_limit(cfg) -> int:
    return int(cfg.get("analysis", {}).get("top_signals_to_analyze", 10))


# Section-specific commands.
async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    signals = store.get_signals_since(since, "news", limit=limit)

    await _safe_reply(
        update,
        context,
        format_section_html("News", signals),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    payload = build_daily_payload(cfg, store, include_sections=False)
    trends = (payload.get("inputs", {}) or {}).get("trends", {})
    rows = (trends or {}).get("trends") or []

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

    await _safe_reply(
        update,
        context,
        "\n".join(lines).strip(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    funding = store.get_signals_since(since, "funding", limit=limit)
    ecosystem = store.get_signals_since(since, "ecosystem", limit=limit)
    combined = (funding + ecosystem)[:limit]

    await _safe_reply(
        update,
        context,
        format_section_html("Funding & Ecosystem", combined),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    signals = store.get_signals_since(since, "github", limit=limit)

    await _safe_reply(
        update,
        context,
        format_section_html("GitHub", signals),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    since = _window_since(cfg)
    limit = _section_limit(cfg)
    twitter = store.get_signals_since(since, "twitter", limit=limit)
    github = store.get_signals_since(since, "github", limit=limit)
    combined = (twitter + github)[:limit]

    await _safe_reply(
        update,
        context,
        format_section_html("New Projects", combined),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


def _manual_run_meta_key_utc() -> str:
    """Key used to persist manual /run counts in SQLite meta.

    Uses UTC date to be consistent across deployments.
    """

    from datetime import datetime

    return f"manual_run_count:{datetime.utcnow().strftime('%Y-%m-%d')}"


def _get_manual_run_count(store: SQLiteStore) -> int:
    try:
        v = store.get_meta(_manual_run_meta_key_utc())
        return int(v) if v is not None else 0
    except Exception:
        return 0


def _set_manual_run_count(store: SQLiteStore, count: int) -> None:
    store.set_meta(_manual_run_meta_key_utc(), str(int(count)))


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manual pipeline trigger (rate-limited, persisted).

    - Max 5 per UTC day
    - Persists across restarts using SQLite meta
    - Uses an in-process lock to prevent concurrent runs
    """

    store: SQLiteStore = context.application.bot_data.get("store")
    cfg = context.application.bot_data.get("config")

    # Concurrency guard
    lock: asyncio.Lock | None = context.application.bot_data.get("pipeline_lock")
    if lock is None:
        lock = asyncio.Lock()
        context.application.bot_data["pipeline_lock"] = lock

    if lock.locked():
        await _safe_reply(
            update,
            context,
            "Pipeline already running, try again shortly.",
            parse_mode=None,
            disable_web_page_preview=True,
        )
        return

    # Daily limit
    MAX_PER_DAY = 5
    count = _get_manual_run_count(store)
    remaining = max(0, MAX_PER_DAY - count)
    if remaining <= 0:
        log.info("Manual /run blocked: limit reached")
        await _safe_reply(
            update,
            context,
            "Manual runs limit reached (5/day). Try again tomorrow.",
            parse_mode=None,
            disable_web_page_preview=True,
        )
        return

    from datetime import datetime, timedelta

    since = datetime.utcnow() - timedelta(hours=24)
    log.info("Manual /run triggered: since=%s remaining_runs=%s", since.isoformat(), remaining - 1)

    # Reserve a slot before running (prevents double-spend if the process crashes mid-run).
    try:
        _set_manual_run_count(store, count + 1)
    except Exception:
        # If persistence fails, we still run, but log it.
        log.exception("Failed to persist manual run count")

    try:
        async with lock:
            result = await run_pipeline(cfg, store, manual=True, since_override=since)
        inserted = result.get("inserted", 0)
        total_seen = result.get("count", 0)
        await _safe_reply(
            update,
            context,
            f"✅ Pipeline run complete (last 24h). inserted={inserted} total_seen={total_seen}. Remaining today: {max(0, MAX_PER_DAY - (count + 1))}",
            parse_mode=None,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.exception("Manual /run failed")
        await _safe_reply(
            update,
            context,
            f"⚠️ Pipeline run failed: {e}",
            parse_mode=None,
            disable_web_page_preview=True,
        )



async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Compatibility help command used by main.py startup/handlers."""

    text = (
        "<b>Web3 Intelligence Bot</b>\n"
        "Available commands:\n"
        "• /dailybrief — daily brief summary\n"
        "• /news — latest news signals\n"
        "• /rawsignals — raw signal dump\n"
        "• /trends — trend clusters\n"
        "• /funding — funding & ecosystem signals\n"
        "• /github — GitHub activity\n"
        "• /newprojects — new project signals\n"
        "• /sources — show configured sources\n"
        "• /run — manual pipeline run (if enabled)"
    )

    await _safe_reply(
        update,
        context,
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

def _read_sources_from_module(mod_path: str, candidates: list[str]) -> tuple[str, list[str]]:
    """Best-effort extraction of sources from ingestion modules.

    Some deployments import/register cmd_sources from this module.
    This helper is intentionally defensive and never raises.
    """

    try:
        mod = import_module(mod_path)
    except Exception:
        return mod_path, []

    for name in candidates:
        try:
            val = getattr(mod, name)
        except Exception:
            continue

        if isinstance(val, (list, tuple)):
            return name, [str(x) for x in val]
        if isinstance(val, dict):
            out: list[str] = []
            for k, v in val.items():
                if isinstance(v, (list, tuple)):
                    out.append(f"{k}: {', '.join(map(str, v))}")
                else:
                    out.append(f"{k}: {v}")
            return name, out

    return mod_path, []


async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show currently configured ingestion sources.

    Minimal compatibility handler to satisfy imports/registrations.
    """

    mappings = [
        ("ingestion.twitter_ingest", ["ACCOUNTS", "TWITTER_ACCOUNTS", "TWITTER_USERS", "SOURCES", "FEEDS"]),
        ("ingestion.news_ingest", ["NEWS_FEEDS", "SOURCES", "FEEDS"]),
        ("ingestion.github_ingest", ["GITHUB_REPOS", "REPOS", "TOPICS", "ORGS", "SOURCES"]),
        ("ingestion.funding_ingest", ["FUNDING_FEEDS", "SOURCES", "FEEDS"]),
        ("ingestion.ecosystem_ingest", ["ECOSYSTEM_FEEDS", "SOURCES", "FEEDS"]),
    ]

    lines: list[str] = ["<b>Current ingestion sources</b>", ""]
    for mod_path, candidates in mappings:
        label, sources = _read_sources_from_module(mod_path, candidates)
        header = mod_path.split(".")[-1].replace("_ingest", "").replace("_", " ").title()
        lines.append(f"<b>{html.escape(header)}</b>")
        if sources:
            lines.append(f"<i>{html.escape(label)}</i>")
            for s in sources[:50]:
                lines.append(f"• {html.escape(s)}")
            if len(sources) > 50:
                lines.append(f"… (+{len(sources) - 50} more)")
        else:
            lines.append("(no sources found)")
        lines.append("")

    await _safe_reply(
        update,
        context,
        "\n".join(lines).strip(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled Telegram update error", exc_info=context.error)
