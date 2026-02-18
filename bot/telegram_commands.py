import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from bot.formatter import (
    escape_html,
    format_dailybrief_html,
    format_section_html,
)
from engine.pipeline import run_pipeline
from storage.sqlite_store import SQLiteStore

# Conservative fallback: some repos drifted and this symbol was renamed.
try:
    from engine.pipeline import build_daily_payload
except ImportError:  # pragma: no cover
    from engine.pipeline import build_daily_brief_payload as build_daily_payload

# Optional: source discovery command
try:
    from discovery.source_discovery import discover_sources
except Exception:  # pragma: no cover
    discover_sources = None

logger = logging.getLogger(__name__)


def _since(config: Dict[str, Any]) -> datetime:
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    return datetime.utcnow() - timedelta(hours=hours)


async def _safe_reply(update: Update, text: str, *, parse_mode: str | None) -> None:
    """Send message with a hard fallback to plain text (never crash handler)."""
    if not update.message:
        return
    preview = text.replace("\n", " ")[:200]
    try:
        await update.message.reply_text(
            text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except BadRequest as e:
        logger.warning(
            "Telegram send failed; retrying as plain text. err=%s preview=%r",
            str(e),
            preview,
        )
        await update.message.reply_text(
            escape_html(text) if parse_mode == "HTML" else text,
            parse_mode=None,
            disable_web_page_preview=True,
        )


async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]

    await run_pipeline(cfg, store, manual=True)
    payload = build_daily_payload(cfg, store)

    # Use HTML for robust, clean rendering.
    await _safe_reply(update, format_dailybrief_html(payload), parse_mode="HTML")


async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(
        _since(cfg),
        source="news",
        limit=cfg["analysis"]["top_signals_to_analyze"],
    )
    await _safe_reply(update, format_section_html("News", signals), parse_mode="HTML")


async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]

    tw = store.get_signals_since(_since(cfg), source="twitter", limit=10)
    gh = store.get_signals_since(_since(cfg), source="github", limit=10)
    combined = (tw + gh)[: cfg["analysis"]["top_signals_to_analyze"]]

    await _safe_reply(update, format_section_html("New Projects", combined), parse_mode="HTML")


async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]

    payload = build_daily_payload(cfg, store, include_sections=False)
    analysis = payload.get("analysis", {})

    mt = analysis.get("market_tone", {})
    tone = escape_html(str(mt.get("market_tone", "neutral")))
    conf = escape_html(str(mt.get("confidence", 0)))

    lines = [
        "<b>Trends & Market Tone</b>",
        f"<i>Tone:</i> <b>{tone}</b> <i>(conf {conf})</i>",
        "",
    ]

    narr = analysis.get("narratives") or []
    if narr:
        lines.append("<b>Narrative clusters</b>")
        for n in narr[:6]:
            chain = escape_html(str(n.get("chain", "unknown")))
            sector = escape_html(str(n.get("sector", "unknown")))
            count = escape_html(str(n.get("count", 0)))
            lines.append(f"• <code>{chain}/{sector}</code>: {count}")
    else:
        lines.append("<i>No narratives computed.</i>")

    await _safe_reply(update, "\n".join(lines), parse_mode="HTML")


async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(
        _since(cfg),
        source="funding",
        limit=cfg["analysis"]["top_signals_to_analyze"],
    )
    await _safe_reply(update, format_section_html("Funding", signals), parse_mode="HTML")


async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(
        _since(cfg),
        source="github",
        limit=cfg["analysis"]["top_signals_to_analyze"],
    )
    await _safe_reply(update, format_section_html("GitHub", signals), parse_mode="HTML")


async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(
        _since(cfg),
        source=None,
        limit=cfg["analysis"]["top_signals_to_analyze"],
    )
    await _safe_reply(update, format_section_html("Raw Signals", signals), parse_mode="HTML")


async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Suggest up to 5 new sources when discovery finds enough candidates."""
    if discover_sources is None:
        await _safe_reply(update, "Source discovery is not available in this build.", parse_mode=None)
        return

    cfg = context.bot_data["config"]
    suggestions = discover_sources(cfg)

    if len(suggestions) < 5:
        await _safe_reply(
            update,
            f"Found {len(suggestions)} candidate sources. I will post once we have 5+.",
            parse_mode=None,
        )
        return

    lines = ["<b>New Source Candidates (review)</b>"]
    for s in suggestions[:5]:
        lines.append(
            f"• <b>{escape_html(s['name'])}</b> — <code>{escape_html(s['score'])}</code>\n"
            f"  <a href=\"{escape_html(s['url'])}\">{escape_html(s['url'])}</a>\n"
            f"  <i>{escape_html(s['reason'])}</i>"
        )
    await _safe_reply(update, "\n".join(lines), parse_mode="HTML")


__all__ = [
    "cmd_dailybrief",
    "cmd_news",
    "cmd_newprojects",
    "cmd_trends",
    "cmd_funding",
    "cmd_github",
    "cmd_rawsignals",
    "cmd_sources",
]
