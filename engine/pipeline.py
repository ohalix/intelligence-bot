import logging
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
    # Accept both legacy 'timestamp' (datetime) and 'published_at' (ISO str).
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


async def run_pipeline(
    config: Dict[str, Any],
    store: SQLiteStore,
    since: Optional[datetime] = None,
    manual: bool = False,
    since_override: Optional[datetime] = None,
) -> Dict[str, Any]:
    # Compatibility: support both legacy positional since and newer since_override kwarg.
    effective_since = since_override or since
    if effective_since is None:
        effective_since = _utcnow_naive() - timedelta(hours=24)
    if effective_since.tzinfo is not None:
        effective_since = effective_since.astimezone(timezone.utc).replace(tzinfo=None)

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

        raw_signals: List[Dict[str, Any]] = []
        ingestion_counts: Dict[str, int] = {}
        for ing in ingesters:
            try:
                # All ingesters implement .ingest(since). Keep the name stable.
                items = await ing.ingest(effective_since)
                raw_signals.extend(items)
                ingestion_counts[ing.__class__.__name__] = len(items)
                logger.info("Ingested %s from %s", len(items), ing.__class__.__name__)
            except Exception as e:
                logger.exception("Ingester failed: %s", e)
                ingestion_counts[ing.__class__.__name__] = 0

        total_seen = len(raw_signals)
        normalized = [_normalize_signal(s) for s in raw_signals]

        deduper = Deduplicator()
        deduped = deduper.dedup(normalized)
        logger.info("Dedup: kept=%s dropped=%s", len(deduped), max(0, len(normalized) - len(deduped)))

        fe = FeatureEngine(config.get("ecosystems", {}) or {})
        enriched = [fe.enrich(s) for s in deduped]

        sa = SentimentAnalyzer(config)
        with_sent = sa.add_sentiment(enriched)
        logger.info("Sentiment types: %s", _sentiment_type_breakdown(with_sent))

        # Ranker accepts either weights dict or a config dict (compat).
        ranker = SignalRanker(config)
        ranked = ranker.rank(with_sent)

        # Ensure store-compatible fields exist.
        for s in ranked:
            if "score" not in s and "signal_score" in s:
                s["score"] = s.get("signal_score")

        inserted = store.insert_signals(ranked)
        store.set_last_run(_utcnow_naive())
        logger.info("Stored inserted=%s total_seen=%s", inserted, total_seen)

        return {
            "ingestion_counts": ingestion_counts,
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
        max_signals = int(config.get("analysis", {}).get("max_signals", 10))

    since = _utcnow_naive() - timedelta(hours=int(config.get("storage", {}).get("rolling_window_hours", 24)))
    # Store now supports (since, source=None, limit=None).
    signals = store.get_signals_since(since, source=None, limit=None)
    state = MarketStateClassifier().classify(signals)
    top = sorted(signals, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:max_signals]

    sections: Dict[str, List[Dict[str, Any]]] = {}
    if include_sections:
        sections["Top Signals"] = top
        # Keep stable section names expected by formatter.
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
