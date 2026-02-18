"""bot.telegram_commands

Registers Telegram commands:
 /dailybrief /news /newprojects /trends /funding /github /rawsignals

Commands read from SQLite rolling 24h store.
The scheduler populates the store on an interval; commands are available anytime.
"""

from __future__ import annotations

import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from bot.formatter import format_brief, format_raw_signals
from storage.store import Store

logger = logging.getLogger(__name__)

def _get_store(context: ContextTypes.DEFAULT_TYPE) -> Store:
    store = context.application.bot_data.get("store")
    if not store:
        raise RuntimeError("Store not attached to application.bot_data['store']")
    return store

async def cmd_dailybrief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    text = format_brief(brief) if brief else "No brief available yet. Try again after the next run."
    await update.message.reply_text(text, disable_web_page_preview=True)

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    if not brief:
        await update.message.reply_text("No news available yet.", disable_web_page_preview=True)
        return
    news = [s for s in brief.get("signals", []) if s.get("category") == "news"]
    await update.message.reply_text(format_raw_signals(news), disable_web_page_preview=True)

async def cmd_newprojects(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    if not brief:
        await update.message.reply_text("No new projects available yet.", disable_web_page_preview=True)
        return
    projs = [s for s in brief.get("signals", []) if s.get("category") in {"newprojects", "ecosystem"}]
    await update.message.reply_text(format_raw_signals(projs), disable_web_page_preview=True)

async def cmd_trends(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    if not brief:
        await update.message.reply_text("No trends available yet.", disable_web_page_preview=True)
        return
    trends = brief.get("analysis", {})
    if not trends:
        await update.message.reply_text("No analysis available yet.", disable_web_page_preview=True)
        return
    # formatter already escapes safely
    await update.message.reply_text(format_brief({"analysis": trends, "signals": []}), disable_web_page_preview=True)

async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    if not brief:
        await update.message.reply_text("No funding signals available yet.", disable_web_page_preview=True)
        return
    funding = [s for s in brief.get("signals", []) if s.get("category") in {"funding", "ecosystem"}]
    await update.message.reply_text(format_raw_signals(funding), disable_web_page_preview=True)

async def cmd_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    if not brief:
        await update.message.reply_text("No GitHub signals available yet.", disable_web_page_preview=True)
        return
    gh = [s for s in brief.get("signals", []) if s.get("category") == "github"]
    await update.message.reply_text(format_raw_signals(gh), disable_web_page_preview=True)

async def cmd_rawsignals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    brief = store.get_latest_brief()
    if not brief:
        await update.message.reply_text("No signals available yet.", disable_web_page_preview=True)
        return
    await update.message.reply_text(format_raw_signals(brief.get("signals", [])), disable_web_page_preview=True)

def register_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("dailybrief", cmd_dailybrief))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("newprojects", cmd_newprojects))
    app.add_handler(CommandHandler("trends", cmd_trends))
    app.add_handler(CommandHandler("funding", cmd_funding))
    app.add_handler(CommandHandler("github", cmd_github))
    app.add_handler(CommandHandler("rawsignals", cmd_rawsignals))
