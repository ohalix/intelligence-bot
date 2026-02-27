import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from telegram.ext import Application, CommandHandler

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from bot.telegram_commands import (
    cmd_dailybrief,
    cmd_funding,
    cmd_github,
    cmd_help,
    cmd_newprojects,
    cmd_news,
    cmd_rawsignals,
    cmd_sources,
    cmd_trends,
    telegram_error_handler,
)

try:
    from bot.telegram_commands import cmd_run  # optional in some patch levels
except ImportError:
    cmd_run = None
from engine.pipeline import run_pipeline
from storage.sqlite_store import SQLiteStore
from utils.config import load_config

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def start_scheduler(app, config, store: SQLiteStore, scheduler: AsyncIOScheduler):
    async def job():
        from engine.pipeline import rolling_since

        since = rolling_since(config, store)
        logger.info("Scheduled pipeline run starting. since=%s", since.isoformat())
        try:
            result = await run_pipeline(config, store, since, manual=False)
            logger.info(
                "Scheduled pipeline run complete. inserted=%s total_seen=%s",
                result.get("inserted"),
                result.get("total_seen"),
            )
        except Exception:
            logger.exception("Scheduled pipeline run failed")

    hours = int(config.get("scheduler", {}).get("run_interval_hours", 24))
    scheduler.add_job(job, "interval", hours=hours)
    scheduler.start()
    return scheduler


async def _send_startup_notice(app, config):
    chat_id = str(
        os.getenv("ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
        or config.get("bot", {}).get("chat_id")
        or ""
    ).strip()
    if not chat_id:
        logger.info("Startup notification skipped: ADMIN_CHAT_ID/TELEGRAM_CHAT_ID not set")
        return
    try:
        msg = f"✅ Intelligence bot is running — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        await app.bot.send_message(chat_id=chat_id, text=msg)
        logger.info("Startup notification sent to admin chat")
    except Exception:
        logger.exception("Startup notification failed (non-fatal)")


async def _startup_ingest(config, store):
    if str(os.getenv("STARTUP_INGEST_ENABLED", "true")).strip().lower() not in {"1", "true", "yes", "on"}:
        logger.info("Startup ingestion run disabled by STARTUP_INGEST_ENABLED")
        return
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)
    logger.info("Startup ingestion run triggered: since=%s", since.isoformat())
    try:
        result = await run_pipeline(config, store, since, manual=False)
        logger.info(
            "Startup ingestion run completed: inserted=%s total_seen=%s",
            result.get("inserted"),
            result.get("total_seen"),
        )
    except Exception:
        logger.exception("Startup ingestion run failed (non-fatal)")


async def main():
    config = load_config()
    store = SQLiteStore(config["storage"]["db_path"])
    app = Application.builder().token(config["bot"]["token"]).build()

    app.bot_data["config"] = config
    app.bot_data["store"] = store

    app.add_handler(CommandHandler("start", cmd_help))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("sources", cmd_sources))
    app.add_handler(CommandHandler("news", cmd_news))
    app.add_handler(CommandHandler("funding", cmd_funding))
    app.add_handler(CommandHandler("github", cmd_github))
    app.add_handler(CommandHandler("newprojects", cmd_newprojects))
    app.add_handler(CommandHandler("rawsignals", cmd_rawsignals))
    app.add_handler(CommandHandler("trends", cmd_trends))
    app.add_handler(CommandHandler("dailybrief", cmd_dailybrief))
    if cmd_run is not None:
        app.add_handler(CommandHandler("run", cmd_run))

    # Global error handler for observability (must never crash).
    app.add_error_handler(telegram_error_handler)

    scheduler = AsyncIOScheduler()
    start_scheduler(app, config, store, scheduler)

    await app.initialize()
    await app.start()
    await _send_startup_notice(app, config)
    await _startup_ingest(config, store)
    await app.updater.start_polling()

    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    # CancelledError inside main() → asyncio.run() raises KeyboardInterrupt on
    # clean Ctrl+C in Python 3.13.  This is expected; suppress the traceback.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
