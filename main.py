import asyncio
import logging
import sys
from pathlib import Path

from telegram.ext import Application, CommandHandler

from utils.config import load_config
from utils.logging import setup_logging
from storage.sqlite_store import SQLiteStore

from bot.telegram_commands import (
    cmd_dailybrief,
    cmd_funding,
    cmd_github,
    cmd_news,
    cmd_newprojects,
    cmd_rawsignals,
    cmd_sources,
    cmd_trends,
)
from bot.scheduler import start_scheduler

# Ensure repo root is on path for local runs
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)


async def on_error(update, context):
    """Global Telegram error handler.

    Without this, PTB logs: "No error handlers are registered" and users get no feedback.
    """
    try:
        chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
        update_id = getattr(update, "update_id", None)

        # Log full traceback server-side
        logger.exception(
            "Unhandled exception in handler (chat_id=%s, update_id=%s)",
            chat_id,
            update_id,
            exc_info=context.error,
        )

        # Best-effort user-facing message
        if chat_id is not None:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="⚠️ Something went wrong while processing that command. Please check logs.",
                )
            except Exception:
                logger.exception("Failed to send error message to chat_id=%s", chat_id)
    except Exception:
        # Never raise from an error handler
        logger.exception("Error inside global Telegram error handler")


def main():
    config = load_config()
    setup_logging(config)

    token = config.get("bot", {}).get("telegram_token")
    if not token:
        logger.error("Missing TELEGRAM_BOT_TOKEN. Set it in your environment or .env file.")
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    store = SQLiteStore(config.get("storage", {}).get("database_path", "./data/web3_intelligence.db"))

    # Start APScheduler inside PTB's lifecycle so both share the same event loop.
    async def _post_init(application: Application):
        loop = asyncio.get_running_loop()
        scheduler = start_scheduler(config, store, application, loop=loop)
        application.bot_data["apscheduler"] = scheduler

        # One-time startup notification (best-effort, never crashes bot).
        try:
            admin_chat_id = config.get("bot", {}).get("admin_chat_id")
            if admin_chat_id:
                from datetime import datetime

                ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
                await application.bot.send_message(
                    chat_id=admin_chat_id,
                    text=f"✅ Intelligence bot is running — {ts}",
                    disable_web_page_preview=True,
                )
        except Exception:
            logger.exception("Startup notification failed")

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

    # Global error handler
    app.add_error_handler(on_error)

    app.bot_data["config"] = config
    app.bot_data["store"] = store

    app.add_handler(CommandHandler("dailybrief", cmd_dailybrief))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("newprojects", cmd_newprojects))
    app.add_handler(CommandHandler("trends", cmd_trends))
    app.add_handler(CommandHandler("funding", cmd_funding))
    app.add_handler(CommandHandler("github", cmd_github))
    app.add_handler(CommandHandler("rawsignals", cmd_rawsignals))
    app.add_handler(CommandHandler("sources", cmd_sources))

    logger.info("Bot started (long polling).")
    # IMPORTANT: run_polling is a blocking call that manages the asyncio loop internally.
    # Do NOT wrap it in asyncio.run() and do NOT await it.
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
