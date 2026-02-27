"""Post-ingest AI response pre-computation and caching.

After every ingestion run (startup, scheduled, manual /run), this module:
1. Builds the prompt for each AI-backed command using the freshly ingested data.
2. Calls the LLM router (HF -> Gemini) sequentially, waiting 10 seconds between calls.
3. Stores each successful AI response in the ai_responses SQLite table.
4. On failure: logs the error, retains the previous cached response (no deletion).
5. On partial success: stores what succeeded, leaves failures as the old cache.

Concurrency guard: a module-level asyncio.Lock prevents overlapping auto-gen runs.
If a run is already in progress when a new one is triggered, the new one is skipped.

Public API:
    run_post_ingest_ai_generation(config, store, window_id) -> None
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# Module-level lock: only one auto-generation job runs at a time.
_AI_GEN_LOCK = asyncio.Lock()

# Seconds to wait AFTER receiving a response before sending the next prompt.
INTER_CALL_DELAY_SEC = 10

# Commands to pre-compute, in order. /sources and /rawsignals are excluded.
_AI_COMMANDS = ["dailybrief", "news", "funding", "github", "newprojects", "trends"]


async def run_post_ingest_ai_generation(
    config: Dict[str, Any],
    store: Any,          # SQLiteStore — not typed to avoid circular import
    window_id: str,      # ISO timestamp of the triggering ingestion run
) -> None:
    """Entry point: called after every ingestion run completes.

    Non-blocking to callers: all errors are caught and logged; the function
    never raises. The concurrency lock is non-blocking — if already running,
    the new trigger is logged and skipped.
    """
    if _AI_GEN_LOCK.locked():
        log.info(
            "ai_cache: auto-generation already in progress, skipping trigger for window_id=%s",
            window_id,
        )
        return

    # Fire and forget in the background so the caller (ingestion) is not blocked.
    asyncio.create_task(_run_generation(config, store, window_id))


async def _run_generation(
    config: Dict[str, Any],
    store: Any,
    window_id: str,
) -> None:
    """Internal: acquire lock and generate responses for all commands sequentially."""
    async with _AI_GEN_LOCK:
        log.info(
            "ai_cache: starting post-ingest AI generation for window_id=%s commands=%s",
            window_id, _AI_COMMANDS,
        )
        succeeded = 0
        failed = 0

        for i, cmd_name in enumerate(_AI_COMMANDS):
            try:
                response_text = await _generate_one(cmd_name, config, store)
                if response_text:
                    store.save_ai_response(
                        command_name=cmd_name,
                        response_text=response_text,
                        window_id=window_id,
                        provider="auto",
                    )
                    log.info(
                        "ai_cache: cached response for cmd=%s len=%s",
                        cmd_name, len(response_text),
                    )
                    succeeded += 1
                else:
                    log.warning(
                        "ai_cache: LLM returned None for cmd=%s; previous cache retained",
                        cmd_name,
                    )
                    failed += 1
            except Exception as exc:
                log.warning(
                    "ai_cache: generation failed for cmd=%s error=%s; previous cache retained",
                    cmd_name, exc,
                )
                failed += 1

            # Wait between calls (except after the last one).
            if i < len(_AI_COMMANDS) - 1:
                log.info(
                    "ai_cache: waiting %ss before next AI call (cmd=%s -> %s)",
                    INTER_CALL_DELAY_SEC, cmd_name, _AI_COMMANDS[i + 1],
                )
                await asyncio.sleep(INTER_CALL_DELAY_SEC)

        log.info(
            "ai_cache: post-ingest generation complete. succeeded=%s failed=%s window_id=%s",
            succeeded, failed, window_id,
        )


async def _generate_one(
    cmd_name: str,
    config: Dict[str, Any],
    store: Any,
) -> Optional[str]:
    """Build the prompt for a command and call the LLM router. Returns text or None."""
    from intelligence.llm_router import route_llm
    from intelligence.prompts import (
        dailybrief_prompt,
        news_prompt,
        funding_prompt,
        github_prompt,
        newprojects_prompt,
        trends_prompt,
    )
    from engine.pipeline import build_daily_payload

    # Helpers duplicated from telegram_commands to avoid circular import
    from datetime import timedelta
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)
    limit = int(config.get("analysis", {}).get("top_signals_to_analyze", 10))

    try:
        if cmd_name == "dailybrief":
            payload = build_daily_payload(config, store)
            prompt = dailybrief_prompt(payload)

        elif cmd_name == "news":
            signals = store.get_signals_since(since, "news", limit=limit)
            prompt = news_prompt(signals)

        elif cmd_name == "funding":
            funding = store.get_signals_since(since, "funding", limit=limit)
            ecosystem = store.get_signals_since(since, "ecosystem", limit=limit)
            combined = (funding + ecosystem)[:limit]
            prompt = funding_prompt(combined)

        elif cmd_name == "github":
            signals = store.get_signals_since(since, "github", limit=limit)
            prompt = github_prompt(signals)

        elif cmd_name == "newprojects":
            twitter = store.get_signals_since(since, "twitter", limit=limit)
            github = store.get_signals_since(since, "github", limit=limit)
            combined = (twitter + github)[:limit]
            prompt = newprojects_prompt(combined)

        elif cmd_name == "trends":
            payload = build_daily_payload(config, store, include_sections=False)
            trends_data = (payload.get("inputs", {}) or {}).get("trends", {})
            all_signals = store.get_signals_since(since, source=None, limit=50)
            prompt = trends_prompt(all_signals, trends_data)

        else:
            log.warning("ai_cache: unknown command %r; skipping", cmd_name)
            return None

    except Exception as exc:
        log.warning("ai_cache: prompt build failed for cmd=%s: %s", cmd_name, exc)
        return None

    return await route_llm(prompt, config)
