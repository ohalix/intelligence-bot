import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from bot.formatter import format_section, format_dailybrief, escape_md
from storage.sqlite_store import SQLiteStore
from engine.pipeline import run_pipeline

# Some repos drifted and this symbol was renamed, which caused VM startup
# ImportErrors. Keep a conservative fallback without changing behaviour.
try:
    from engine.pipeline import build_daily_payload
except ImportError:  # pragma: no cover
    from engine.pipeline import build_daily_brief_payload as build_daily_payload

logger = logging.getLogger(__name__)


async def _safe_reply(update: Update, text: str, *, markdown: bool = True) -> None:
    """Send a message without crashing the handler.

    We default to MarkdownV2 for formatting, but Telegram will hard-fail the
    request if any reserved character escapes were missed. In that case we
    retry once as plain text to guarantee command stability.
    """
    if not update.message:
        return
    try:
        await update.message.reply_text(
            text,
            parse_mode="MarkdownV2" if markdown else None,
            disable_web_page_preview=True,
        )
    except BadRequest as e:
        logger.exception(
            "MarkdownV2 send failed; retrying as plain text. err=%s preview=%r",
            e,
            text[:200],
        )
        await update.message.reply_text(text, parse_mode=None, disable_web_page_preview=True)

def _since(config: Dict[str, Any]) -> datetime:
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    return datetime.utcnow() - timedelta(hours=hours)

async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    # run a manual pipeline (override)
    await run_pipeline(cfg, store, manual=True)
    payload = build_daily_payload(cfg, store)
    await _safe_reply(update, format_dailybrief(payload), markdown=True)

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="news", limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("News", signals), markdown=True)

async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    # New projects proxy = twitter + github
    tw = store.get_signals_since(_since(cfg), source="twitter", limit=10)
    gh = store.get_signals_since(_since(cfg), source="github", limit=10)
    combined = (tw + gh)[:cfg["analysis"]["top_signals_to_analyze"]]
    await _safe_reply(update, format_section("New Projects", combined), markdown=True)

async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    payload = build_daily_payload(cfg, store, include_sections=False)
    analysis = payload.get("analysis", {})
    lines = ["*Trends & Market Tone*"]
    mt = analysis.get("market_tone", {})
    tone = escape_md(str(mt.get("market_tone", "neutral")))
    conf = escape_md(str(mt.get("confidence", 0)))
    lines.append(f"_Tone:_ *{tone}* \\(conf {conf}\\)")
    lines.append("")
    narr = analysis.get("narratives") or []
    if narr:
        lines.append("*Narrative clusters*")
        for n in narr[:6]:
            chain = escape_md(str(n.get("chain", "unknown")))
            sector = escape_md(str(n.get("sector", "unknown")))
            count = escape_md(str(n.get("count", 0)))
            # '-' must be escaped in MarkdownV2
            lines.append(f"\\- {chain}/{sector}: {count}")
    else:
        lines.append("_No narratives computed._")
    await _safe_reply(update, "\n".join(lines), markdown=True)

async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="funding", limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("Funding", signals), markdown=True)

async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="github", limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("GitHub", signals), markdown=True)

async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source=None, limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("Raw Signals", signals), markdown=True)


# Explicit exports to make `from bot.telegram_commands import cmd_*` robust.
__all__ = [
    "cmd_dailybrief",
    "cmd_news",
    "cmd_newprojects",
    "cmd_trends",
    "cmd_funding",
    "cmd_github",
    "cmd_rawsignals",
]
