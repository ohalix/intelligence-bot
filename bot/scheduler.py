import logging
from datetime import datetime
from typing import Any, Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from engine.pipeline import run_pipeline, send_dailybrief
from storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

def start_scheduler(config: Dict[str, Any], store: SQLiteStore, app) -> AsyncIOScheduler:
    interval = int(config.get("scheduler", {}).get("run_interval_hours", 24))
    sched = AsyncIOScheduler(timezone=config.get("bot", {}).get("timezone", "Africa/Lagos"))

    async def job():
        try:
            await run_pipeline(config, store, manual=False)
            await send_dailybrief(config, store, app.bot)
            logger.info("Scheduled daily run completed.")
        except Exception as e:
            logger.exception(f"Scheduled run failed: {e}")

    sched.add_job(job, trigger=IntervalTrigger(hours=interval), next_run_time=datetime.now())
    sched.start()
    return sched
