import asyncio
import logging
import sys
from pathlib import Path

# Ensure repo root is on path for local runs
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from telegram.ext import Application

from utils.config import load_config
from utils.logging import setup_logging
from storage.sqlite_store import SQLiteStore
from bot.telegram_commands import TelegramCommands
from bot.scheduler import start_scheduler

logger = logging.getLogger(__name__)


async def main():
    config = load_config()
    setup_logging(config)

    token = config.get("bot", {}).get("telegram_token")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    store = SQLiteStore(config)

    app = Application.builder().token(token).build()
    app.bot_data["config"] = config
    app.bot_data["store"] = store

    TelegramCommands(config, store).register(app)

    start_scheduler(config, store, app)
    logger.info("Bot started (long polling).")
    await app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    asyncio.run(main())
