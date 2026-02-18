import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from telegram import Update
from telegram.ext import ContextTypes

from bot.formatter import format_section, format_dailybrief
from storage.sqlite_store import SQLiteStore
from engine.pipeline import run_pipeline, build_daily_payload

logger = logging.getLogger(__name__)

def _since(config: Dict[str, Any]) -> datetime:
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    return datetime.utcnow() - timedelta(hours=hours)

async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]
    store: SQLiteStore = context.bot_data["store"]
    # run a manual pipeline (override)
    await run_pipeline(cfg, store, manual=True)
    payload = build_daily_payload(cfg, store)
    await update.message.reply_text(format_dailybrief(payload), parse_mode="MarkdownV2", disable_web_page_preview=True)

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="news", limit=cfg["analysis"]["top_signals_to_analyze"])
    await update.message.reply_text(format_section("News", signals), parse_mode="MarkdownV2", disable_web_page_preview=True)

async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    # New projects proxy = twitter + github
    tw = store.get_signals_since(_since(cfg), source="twitter", limit=10)
    gh = store.get_signals_since(_since(cfg), source="github", limit=10)
    combined = (tw + gh)[:cfg["analysis"]["top_signals_to_analyze"]]
    await update.message.reply_text(format_section("New Projects", combined), parse_mode="MarkdownV2", disable_web_page_preview=True)

async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    payload = build_daily_payload(cfg, store, include_sections=False)
    analysis = payload.get("analysis", {})
    lines = ["*Trends & Market Tone*"]
    mt = analysis.get("market_tone", {})
    lines.append(f"_Tone:_ *{mt.get('market_tone','neutral')}* (conf {mt.get('confidence',0)})")
    lines.append("")
    narr = analysis.get("narratives") or []
    if narr:
        lines.append("*Narrative clusters*")
        for n in narr[:6]:
            lines.append(f"- {n.get('chain','unknown')}/{n.get('sector','unknown')}: {n.get('count',0)}")
    else:
        lines.append("_No narratives computed._")
    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2", disable_web_page_preview=True)

async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="funding", limit=cfg["analysis"]["top_signals_to_analyze"])
    await update.message.reply_text(format_section("Funding", signals), parse_mode="MarkdownV2", disable_web_page_preview=True)

async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source="github", limit=cfg["analysis"]["top_signals_to_analyze"])
    await update.message.reply_text(format_section("GitHub", signals), parse_mode="MarkdownV2", disable_web_page_preview=True)

async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.bot_data["config"]; store: SQLiteStore = context.bot_data["store"]
    signals = store.get_signals_since(_since(cfg), source=None, limit=cfg["analysis"]["top_signals_to_analyze"])
    await update.message.reply_text(format_section("Raw Signals", signals), parse_mode="MarkdownV2", disable_web_page_preview=True)
