"""engine.pipeline

The pipeline ties together:
ingestion -> processing -> intelligence -> storage -> telegram output

This module intentionally stays small and modular.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from ingestion.twitter_ingest import TwitterIngester
from ingestion.news_ingest import NewsIngester
from ingestion.github_ingest import GitHubIngester
from ingestion.funding_ingest import FundingIngester
from ingestion.ecosystem_ingest import EcosystemIngester

from processing.deduplicator import Deduplicator
from processing.spam_filter import SpamFilter
from processing.sentiment_analyzer import SentimentAnalyzer
from processing.signal_ranker import SignalRanker
from processing.feature_engine import FeatureEngine

from intelligence.web3_analysis_agent import Web3AnalysisAgent

from storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.utcnow()


class Pipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.store = SQLiteStore(config)
        self.dedup = Deduplicator(config)
        self.spam_filter = SpamFilter(config)
        self.sentiment = SentimentAnalyzer(config)
        self.ranker = SignalRanker(config)
        self.feature_engine = FeatureEngine(config)
        self.analysis_agent = Web3AnalysisAgent(config)

    def _get_last_run(self) -> datetime:
        # checkpoint key shared across runs
        ts = self.store.get_checkpoint("last_run_timestamp")
        if ts:
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                pass
        # Default: 24h back
        return _utcnow() - timedelta(hours=24)

    def _set_last_run(self, ts: datetime) -> None:
        self.store.set_checkpoint("last_run_timestamp", ts.isoformat())

    async def _ingest_all(self, since: datetime, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        ing_cfg = self.config.get("ingestion", {})
        twitter_mode = (ing_cfg.get("twitter_mode") or "none").lower()

        ingesters = []

        if twitter_mode != "none":
            ingesters.append(TwitterIngester(self.config, session))
        ingesters.extend([
            NewsIngester(self.config, session),
            GitHubIngester(self.config, session),
            FundingIngester(self.config, session),
            EcosystemIngester(self.config, session),
        ])

        results: List[Dict[str, Any]] = []
        for ing in ingesters:
            try:
                items = await ing.ingest(since)
                if items:
                    results.extend(items)
                logger.info(f"Ingested {len(items)} items from {ing.__class__.__name__}")
            except Exception as e:
                logger.exception(f"Ingestion failed for {ing.__class__.__name__}: {e}")
        return results

    def _process(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not signals:
            return []

        # in-run dedup (cheap)
        signals = self.dedup.deduplicate(signals)

        # sentiment
        signals = self.sentiment.analyze_batch(signals)

        # spam filter
        signals = self.spam_filter.filter_signals(signals)

        # rank
        signals = self.ranker.rank_batch(signals)

        # add features (optional, stored for future ML)
        try:
            for s in signals:
                s["features_v1"] = self.feature_engine.extract_features(s)
        except Exception as e:
            logger.warning(f"Feature extraction failed (continuing): {e}")

        return signals

    def _store_24h_unique(self, signals: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """Insert signals; keep only those that were new (dedup_key unique).

        Returns (new_signals, dup_count)
        """
        new_signals = []
        dup = 0
        for s in signals:
            # store_signal returns False if duplicate dedup_key
            try:
                ok = self.store.store_signal(s)
            except Exception as e:
                logger.exception(f"Failed storing signal: {e}")
                ok = False
            if ok:
                new_signals.append(s)
            else:
                dup += 1

        # purge >24h
        try:
            self.store.purge_old_signals(hours=24)
        except Exception as e:
            logger.warning(f"Failed to purge old signals: {e}")

        return new_signals, dup

    async def run_once(self, force_since: Optional[datetime] = None) -> Dict[str, Any]:
        """Run the full pipeline once.

        force_since overrides checkpoint (useful for manual re-run).
        """
        started = _utcnow()
        since = force_since or self._get_last_run()

        logger.info(f"Pipeline run starting. since={since.isoformat()} started={started.isoformat()}")

        async with aiohttp.ClientSession() as session:
            raw = await self._ingest_all(since, session)

        processed = self._process(raw)

        new_signals, dup_count = self._store_24h_unique(processed)

        # analysis (can run on all processed or only new; we use new)
        analysis = await self.analysis_agent.analyze(new_signals)

        # update checkpoint even if empty to enforce rolling window
        self._set_last_run(started)

        summary = {
            "started_at": started.isoformat(),
            "since": since.isoformat(),
            "raw_count": len(raw),
            "processed_count": len(processed),
            "new_count": len(new_signals),
            "duplicates_skipped": dup_count,
            "analysis": analysis,
            "top_signals": new_signals[: self.config.get("bot", {}).get("max_signals", 10)],
        }
        logger.info(
            f"Pipeline complete. raw={len(raw)} processed={len(processed)} new={len(new_signals)} dup={dup_count}"
        )
        return summary

    def get_latest_signals(self, limit: int = 10, source: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.store.get_signals(limit=limit, source=source)

    def get_daily_brief(self, limit: int = 10) -> Dict[str, Any]:
        signals = self.store.get_signals(limit=limit)
        # analysis over stored signals (dry mode acceptable)
        return {"signals": signals}



# --- Backward-compatible helpers used by bot/scheduler and older modules ---

async def run_pipeline(config: Dict[str, Any], store: SQLiteStore, manual: bool = False) -> Dict[str, Any]:
    """Run once and return summary. Uses store from caller for shared DB."""
    p = Pipeline(config)
    # Ensure pipeline uses caller store (same DB connection/file)
    p.store = store
    summary = await p.run_once(force_since=None if not manual else None)
    return summary


async def send_dailybrief(config: Dict[str, Any], store: SQLiteStore, bot) -> None:
    """Send a daily brief message to default chat id, if configured."""
    from bot.formatter import TelegramFormatter

    chat_id = config.get("bot", {}).get("telegram_chat_id") or config.get("bot", {}).get("default_chat_id")
    if not chat_id:
        logger.info("No TELEGRAM_CHAT_ID set; skipping auto-send dailybrief.")
        return

    limit = int(config.get("bot", {}).get("max_signals", 10))
    signals = store.get_signals(limit=limit)
    formatter = TelegramFormatter(config)
    text = formatter.format_dailybrief(signals, analysis=None)
    await bot.send_message(chat_id=chat_id, text=text, parse_mode=None, disable_web_page_preview=False)
