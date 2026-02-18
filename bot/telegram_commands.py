import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from bot.formatter import format_section, format_dailybrief, escape_html
from storage.sqlite_store import SQLiteStore
from engine.pipeline import run_pipeline

# Some repos drifted and this symbol was renamed, which caused VM startup
# ImportErrors. Keep a conservative fallback without changing behaviour.
try:
    from engine.pipeline import build_daily_payload
except ImportError:  # pragma: no cover
    from engine.pipeline import build_daily_brief_payload as build_daily_payload

logger = logging.getLogger(__name__)


async def _safe_reply(update: Update, text_html: str) -> None:
    """Send a message without crashing the handler.

    Primary: Telegram HTML parse mode.
    Fallback: plain text (no parse_mode) to guarantee command stability.
    """
    if not update.message:
        return
    try:
        await update.message.reply_text(
            text_html,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except BadRequest as e:
        logger.exception(
            "HTML send failed; retrying as plain text. err=%s preview=%r",
            e,
            (text_html or "")[:200],
        )
        plain = re.sub(r"<[^>]+>", "", text_html or "")
        await update.message.reply_text(plain, parse_mode=None, disable_web_page_preview=True)

def _since(config: Dict[str, Any]) -> datetime:
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    return datetime.utcnow() - timedelta(hours=hours)

async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    # run a manual pipeline (override)
    await run_pipeline(cfg, store, manual=True)
    payload = build_daily_payload(cfg, store)
    await _safe_reply(update, format_dailybrief(payload))

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="news", limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("News", signals))

async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    # New projects proxy = twitter + github
    tw = store.get_signals_since(_since(cfg), source="twitter", limit=10)
    gh = store.get_signals_since(_since(cfg), source="github", limit=10)
    combined = (tw + gh)[:cfg["analysis"]["top_signals_to_analyze"]]
    await _safe_reply(update, format_section("New Projects", combined))

async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    payload = build_daily_payload(cfg, store, include_sections=False)
    analysis = payload.get("analysis", {})
    lines = ["<b>Trends &amp; Market Tone</b>"]
    mt = analysis.get("market_tone", {})
    tone = escape_html(str(mt.get("market_tone", "neutral")))
    conf = escape_html(str(mt.get("confidence", 0)))
    lines.append(f"Tone: <b>{tone}</b> <i>(conf {conf})</i>")
    lines.append("")
    narr = analysis.get("narratives") or []
    if narr:
        lines.append("<b>Narrative clusters</b>")
        for n in narr[:6]:
            chain = escape_html(str(n.get("chain", "unknown")))
            sector = escape_html(str(n.get("sector", "unknown")))
            count = escape_html(str(n.get("count", 0)))
            lines.append(f"• {chain}/{sector}: <b>{count}</b>")
    else:
        lines.append("<i>No narratives computed.</i>")
    await _safe_reply(update, "\n".join(lines))

async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="funding", limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("Funding", signals))

async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="github", limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("GitHub", signals))

async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source=None, limit=cfg["analysis"]["top_signals_to_analyze"])
    await _safe_reply(update, format_section("Raw Signals", signals))


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
