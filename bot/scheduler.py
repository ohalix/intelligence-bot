import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from engine.pipeline import run_pipeline, send_dailybrief
from storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

def start_scheduler(
    config: Dict[str, Any],
    store: SQLiteStore,
    app,
    loop: asyncio.AbstractEventLoop | None = None,
) -> AsyncIOScheduler:
    interval = int(config.get("scheduler", {}).get("run_interval_hours", 24))
    # Bind APScheduler to the SAME asyncio event loop managed by python-telegram-bot.
    # If we don't, PTB may create/own its own loop and APScheduler jobs can get cancelled
    # or raise "event loop is already running".
    sched = AsyncIOScheduler(
        timezone=config.get("bot", {}).get("timezone", "Africa/Lagos"),
        event_loop=loop,
    )

    async def job():
        try:
            await run_pipeline(config, store, manual=False)
            await send_dailybrief(config, store, app.bot)
            logger.info("Scheduled daily run completed.")
        except Exception as e:
            logger.exception(f"Scheduled run failed: {e}")

    # Use timezone-aware UTC to avoid datetime.utcnow deprecation patterns and
    # reduce misfire warnings. Allow a grace window for missed runs.
    sched.add_job(
        job,
        trigger=IntervalTrigger(hours=interval),
        next_run_time=datetime.now(timezone.utc),
        misfire_grace_time=7200,
        coalesce=True,
    )
    sched.start()
    return sched
