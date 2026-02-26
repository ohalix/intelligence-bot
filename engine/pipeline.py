"""Pipeline: orchestrates ingestion → dedup → enrich → sentiment → rank → store.

Fixes:
- item 9: config key top_signals_to_analyze
- item 17: concurrent ingesters via asyncio.gather
- item 23: structured observability metrics
- item 6: pass store to deduplicator for persistent near-dupe detection
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from ingestion.ecosystem_ingest import EcosystemIngester
from ingestion.funding_ingest import FundingIngester
from ingestion.github_ingest import GitHubIngester
from ingestion.news_ingest import NewsIngester
from ingestion.twitter_ingest import TwitterIngester
from intelligence.market_state_classifier import MarketStateClassifier
from intelligence.trend_detector import TrendDetector
from processing.deduplicator import Deduplicator
from processing.feature_engine import FeatureEngine
from processing.sentiment_analyzer import SentimentAnalyzer
from processing.signal_ranker import SignalRanker
from storage.sqlite_store import SQLiteStore
from utils.http import make_timeout

logger = logging.getLogger(__name__)


def _utcnow_naive() -> datetime:
    """UTC now as naive datetime for backward-compatible internal comparisons."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def rolling_since(config: Dict[str, Any], store: SQLiteStore) -> datetime:
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    last_run = store.get_last_run()
    if not last_run:
        return _utcnow_naive() - timedelta(hours=hours)
    return last_run


def _normalize_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(sig)
    out.setdefault("source", "unknown")
    out.setdefault("title", "(untitled)")
    out.setdefault("url", "")
    out.setdefault("description", "")
    out.setdefault("ecosystem", "")
    out.setdefault("tags", [])
    if not out.get("published_at"):
        ts = out.get("timestamp")
        if isinstance(ts, datetime):
            out["published_at"] = ts.replace(tzinfo=None).isoformat()
        else:
            out["published_at"] = _utcnow_naive().isoformat()
    if not isinstance(out.get("tags"), list):
        out["tags"] = [str(out["tags"])]
    return out


def _sentiment_type_breakdown(signals: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for s in signals:
        v = s.get("sentiment")
        if isinstance(v, (int, float)):
            k = "numeric"
        elif isinstance(v, str):
            k = "label_str"
        elif v is None:
            k = "none"
        else:
            k = type(v).__name__
        counts[k] = counts.get(k, 0) + 1
    return counts


async def _run_ingester_timed(ing, since) -> tuple:
    """Run ingester with wall-clock timing (item 23)."""
    t0 = time.monotonic()
    name = ing.__class__.__name__
    try:
        items = await ing.ingest(since)
        elapsed = time.monotonic() - t0
        logger.info("Ingested %s items from %s in %.2fs", len(items), name, elapsed)
        return name, items, elapsed, None
    except Exception as exc:
        elapsed = time.monotonic() - t0
        logger.exception("Ingester failed: %s (%.2fs)", name, elapsed)
        return name, [], elapsed, str(exc)


async def run_pipeline(
    config: Dict[str, Any],
    store: SQLiteStore,
    since: Optional[datetime] = None,
    manual: bool = False,
    since_override: Optional[datetime] = None,
) -> Dict[str, Any]:
    # Support both legacy positional since and newer since_override kwarg.
    effective_since = since_override or since
    if effective_since is None:
        effective_since = _utcnow_naive() - timedelta(hours=24)
    if effective_since.tzinfo is not None:
        effective_since = effective_since.astimezone(timezone.utc).replace(tzinfo=None)

    pipeline_start = time.monotonic()

    # item 23: log DB stats at startup
    try:
        db_stats = store.get_db_stats()
        logger.info(
            "Pipeline start. since=%s manual=%s db_rows=%s db_size_bytes=%s",
            effective_since.isoformat(), manual,
            db_stats.get("total_rows", 0), db_stats.get("size_bytes", 0),
        )
    except Exception:
        logger.info("Pipeline start. since=%s manual=%s", effective_since.isoformat(), manual)

    timeout = make_timeout(config)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        ingesters = [
            NewsIngester(config, session),
            GitHubIngester(config, session),
            FundingIngester(config, session),
            EcosystemIngester(config, session),
            TwitterIngester(config, session),
        ]
        # Pass store to ingesters supporting conditional RSS caching (item 15)
        for ing in ingesters:
            ing._store = store

        # FIX item 17: run ingesters concurrently
        ingester_results = await asyncio.gather(
            *[_run_ingester_timed(ing, effective_since) for ing in ingesters]
        )

        raw_signals: List[Dict[str, Any]] = []
        ingestion_counts: Dict[str, Any] = {}
        ingester_errors: Dict[str, str] = {}
        for name, items, elapsed, error in ingester_results:
            raw_signals.extend(items)
            ingestion_counts[name] = {"count": len(items), "elapsed_s": round(elapsed, 2)}
            if error:
                ingester_errors[name] = error

        total_seen = len(raw_signals)
        normalized = [_normalize_signal(s) for s in raw_signals]

        # FIX item 6: persistent near-dupe dedup via store
        deduper = Deduplicator(store=store)
        deduped = deduper.dedup(normalized)
        dedup_stats = deduper.stats()
        logger.info(
            "Dedup: kept=%s dropped_url=%s dropped_content=%s",
            len(deduped), dedup_stats["dropped_url"], dedup_stats["dropped_content"],
        )

        fe = FeatureEngine(config.get("ecosystems", {}) or {})
        enriched = [fe.enrich(s) for s in deduped]

        sa = SentimentAnalyzer(config)
        with_sent = sa.add_sentiment(enriched)
        logger.info("Sentiment types: %s", _sentiment_type_breakdown(with_sent))

        ranker = SignalRanker(config)
        ranked = ranker.rank(with_sent)

        for s in ranked:
            if "score" not in s and "signal_score" in s:
                s["score"] = s.get("signal_score")

        inserted = store.insert_signals(ranked)
        store.set_last_run(_utcnow_naive())

        pipeline_elapsed = time.monotonic() - pipeline_start

        # item 23: structured metrics summary (one JSON log line)
        metrics = {
            "event": "pipeline_run",
            "manual": manual,
            "since": effective_since.isoformat(),
            "total_seen": total_seen,
            "deduped_kept": len(deduped),
            "dedup_dropped_url": dedup_stats["dropped_url"],
            "dedup_dropped_content": dedup_stats["dropped_content"],
            "inserted": inserted,
            "ingestion_counts": ingestion_counts,
            "ingester_errors": ingester_errors,
            "pipeline_elapsed_s": round(pipeline_elapsed, 2),
        }
        logger.info("PIPELINE_METRICS %s", json.dumps(metrics))

        return {
            "ingestion_counts": {k: v["count"] if isinstance(v, dict) else v for k, v in ingestion_counts.items()},
            "total_seen": total_seen,
            "count": total_seen,
            "kept": len(deduped),
            "inserted": inserted,
        }


def build_daily_payload(
    config: Dict[str, Any],
    store: SQLiteStore,
    max_signals: int | None = None,
    include_sections: bool = True,
) -> Dict[str, Any]:
    if max_signals is None:
        # FIX item 9: config uses analysis.top_signals_to_analyze
        max_signals = int(
            config.get("analysis", {}).get("top_signals_to_analyze")
            or config.get("analysis", {}).get("max_signals")
            or 10
        )

    since = _utcnow_naive() - timedelta(hours=int(config.get("storage", {}).get("rolling_window_hours", 24)))
    signals = store.get_signals_since(since, source=None, limit=None)
    state = MarketStateClassifier().classify(signals)
    top = sorted(signals, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:max_signals]

    sections: Dict[str, List[Dict[str, Any]]] = {}
    if include_sections:
        sections["Top Signals"] = top
        for src, header in (
            ("news", "News"),
            ("funding", "Funding"),
            ("ecosystem", "Ecosystem"),
            ("github", "GitHub"),
            ("twitter", "Twitter"),
        ):
            sections[header] = [s for s in signals if (s.get("source") or "").lower() == src][:max_signals]

    trends = TrendDetector().detect(signals)

    return {
        "date": _utcnow_naive().strftime("%Y-%m-%d"),
        "since": since.isoformat(),
        "analysis": {"market_tone": state, "summary": None},
        "sections": sections,
        "inputs": {"trends": trends},
        "total_signals": len(signals),
    }
