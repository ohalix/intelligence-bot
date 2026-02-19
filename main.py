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
    cmd_run,
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

        # In-process lock to prevent concurrent pipeline runs (startup vs /run vs scheduler).
        # Stored in bot_data so command handlers can use it.
        application.bot_data.setdefault("pipeline_lock", asyncio.Lock())

        # Trigger a one-time startup ingestion for the last 24 hours.
        # Must never crash bot and must not block Telegram update handling.
        async def _startup_ingest():
            from datetime import datetime, timedelta

            lock: asyncio.Lock = application.bot_data["pipeline_lock"]
            if lock.locked():
                logger.info("Startup ingestion skipped: pipeline already running.")
                return

            since = datetime.utcnow() - timedelta(hours=24)
            logger.info("Startup ingestion run triggered: since=%s", since.isoformat())

            try:
                async with lock:
                    from engine.pipeline import run_pipeline

                    result = await run_pipeline(config, store, manual=False, since_override=since)
                    logger.info(
                        "Startup ingestion run completed: inserted=%s total_seen=%s",
                        result.get("inserted"),
                        result.get("count"),
                    )
            except Exception:
                logger.exception("Startup ingestion run failed")

        # Fire-and-forget.
        asyncio.create_task(_startup_ingest())

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
    app.add_handler(CommandHandler("run", cmd_run))

    logger.info("Bot started (long polling).")
    # IMPORTANT: run_polling is a blocking call that manages the asyncio loop internally.
    # Do NOT wrap it in asyncio.run() and do NOT await it.
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
