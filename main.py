import logging
import os
import sys
from pathlib import Path
import asyncio

# Ensure repo root is on path for local runs
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from telegram.ext import Application, CommandHandler

from utils.config import load_config
from utils.logging import setup_logging
from storage.sqlite_store import SQLiteStore

from bot.telegram_commands import (
    cmd_dailybrief, cmd_news, cmd_newprojects, cmd_trends, cmd_funding, cmd_github, cmd_rawsignals
)
from bot.scheduler import start_scheduler

logger = logging.getLogger(__name__)

def main():
    config = load_config()
    setup_logging(config)

    token = config.get("bot", {}).get("telegram_token")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    store = SQLiteStore(config.get("storage", {}).get("database_path", "./data/web3_intelligence.db"))

    # Start APScheduler inside PTB's lifecycle so both share the same event loop.
    async def _post_init(application: Application):
        loop = asyncio.get_running_loop()
        scheduler = start_scheduler(config, store, application, loop=loop)
        application.bot_data["apscheduler"] = scheduler

    async def _post_shutdown(application: Application):
        scheduler = application.bot_data.get("apscheduler")
        try:
            if scheduler:
                scheduler.shutdown(wait=False)
        except Exception:
            logger.exception("Error shutting down scheduler")

    app = (
        Application.builder()
        .token(token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    app.bot_data["config"] = config
    app.bot_data["store"] = store

    app.add_handler(CommandHandler("dailybrief", cmd_dailybrief))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("newprojects", cmd_newprojects))
    app.add_handler(CommandHandler("trends", cmd_trends))
    app.add_handler(CommandHandler("funding", cmd_funding))
    app.add_handler(CommandHandler("github", cmd_github))
    app.add_handler(CommandHandler("rawsignals", cmd_rawsignals))

    logger.info("Bot started (long polling).")
    # IMPORTANT: run_polling is a blocking call that manages the asyncio loop internally.
    # Do NOT wrap it in asyncio.run() and do NOT await it.
    app.run_polling(allowed_updates=["message"])

if __name__ == "__main__":
    main()
