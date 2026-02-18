"""bot.telegram_commands

Registers Telegram commands:
 /dailybrief /news /newprojects /trends /funding /github /rawsignals

Commands read from SQLite rolling 24h store.
The scheduler populates the store on an interval; commands are available anytime.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from bot.formatter import TelegramFormatter
from engine.pipeline import Pipeline
from intelligence.web3_analysis_agent import Web3AnalysisAgent
from storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class TelegramCommands:
    def __init__(self, config: Dict[str, Any], store: SQLiteStore):
        self.config = config
        self.store = store
        self.formatter = TelegramFormatter(config)
        self.pipeline = Pipeline(config)
        self.pipeline.store = store
        self.agent = Web3AnalysisAgent(config)

    def register(self, app: Application) -> None:
        app.add_handler(CommandHandler("dailybrief", self.dailybrief))
        app.add_handler(CommandHandler("news", self.news))
        app.add_handler(CommandHandler("newprojects", self.newprojects))
        app.add_handler(CommandHandler("funding", self.funding))
        app.add_handler(CommandHandler("github", self.github))
        app.add_handler(CommandHandler("trends", self.trends))
        app.add_handler(CommandHandler("rawsignals", self.rawsignals))

    async def _send(self, update: Update, text: str) -> None:
        if not update.message:
            return
        await update.message.reply_text(text, parse_mode=None, disable_web_page_preview=False)

    def _limit(self) -> int:
        return int(self.config.get("bot", {}).get("max_signals", 10))

    async def dailybrief(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        signals = self.store.get_signals(limit=limit)
        # optional: /dailybrief refresh  (manual override)
        if context.args and context.args[0].lower() in {"refresh", "run"}:
            try:
                summary = await self.pipeline.run_once()
                signals = summary.get("top_signals", signals)
            except Exception as e:
                logger.exception(f"Manual refresh failed: {e}")
        text = self.formatter.format_dailybrief(signals, analysis=None)
        await self._send(update, text)

    async def news(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        signals = self.store.get_signals(limit=limit, source="news")
        await self._send(update, self.formatter.format_signals("📰 News", signals))

    async def github(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        signals = self.store.get_signals(limit=limit, source="github")
        await self._send(update, self.formatter.format_signals("💻 GitHub", signals))

    async def funding(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        # include both funding + ecosystem announcements
        signals = self.store.get_signals(limit=limit * 2)
        filtered = [s for s in signals if s.get("source") in {"funding", "ecosystem"}]
        await self._send(update, self.formatter.format_signals("💰 Funding & Ecosystem", filtered[:limit]))

    async def newprojects(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        signals = self.store.get_signals(limit=limit * 3)
        # heuristic: twitter signals with low followers OR mentions of launch/new/mainnet
        out: List[Dict[str, Any]] = []
        for s in signals:
            if s.get("source") != "twitter":
                continue
            followers = int(s.get("followers", 0) or 0)
            text = (str(s.get("title") or "") + " " + str(s.get("text") or "")).lower()
            if followers and followers <= 5000:
                out.append(s)
            elif any(k in text for k in ("launch", "mainnet", "testnet", "beta", "new protocol", "now live")):
                out.append(s)
        await self._send(update, self.formatter.format_signals("🆕 New Projects", out[:limit]))

    async def trends(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        signals = self.store.get_signals(limit=limit)
        analysis = await self.agent.analyze(signals)
        text = self.formatter.format_trends(analysis, signals)
        await self._send(update, text)

    async def rawsignals(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        limit = self._limit()
        signals = self.store.get_signals(limit=limit)
        await self._send(update, self.formatter.format_rawsignals(signals))

