import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from ingestion.ecosystem_ingest import EcosystemIngester
from ingestion.funding_ingest import FundingIngester
from ingestion.github_ingest import GitHubIngester
from ingestion.news_ingest import NewsIngester
from ingestion.twitter_ingest import TwitterIngester
from processing.deduplicator import Deduplicator
from processing.feature_engine import FeatureEngine
from processing.sentiment_analyzer import SentimentAnalyzer
from processing.signal_ranker import SignalRanker
from storage.sqlite_store import SQLiteStore
from intelligence.market_state_classifier import MarketStateClassifier

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
        out["published_at"] = datetime.utcnow().isoformat()
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
    since: datetime | None = None,
    manual: bool = False,
    since_override: datetime | None = None,
) -> Dict[str, Any]:
    if since_override is not None:
        since = since_override
    if since is None:
        since = rolling_since(config, store)
    logger.info("Pipeline start. since=%s manual=%s", since.isoformat(), manual)

    ingesters = [
        NewsIngester(config),
        GitHubIngester(config),
        FundingIngester(config),
        EcosystemIngester(config),
        TwitterIngester(config),
    ]

    raw_signals: List[Dict[str, Any]] = []
    ingestion_counts: Dict[str, int] = {}
    for ing in ingesters:
        try:
            items = await ing.fetch(since)
            raw_signals.extend(items)
            ingestion_counts[ing.__class__.__name__] = len(items)
            logger.info("Ingested %s from %s", len(items), ing.__class__.__name__)
        except Exception as e:
            logger.exception("Ingester failed: %s", e)
            ingestion_counts[ing.__class__.__name__] = 0

    total_seen = len(raw_signals)
    normalized = [_normalize_signal(s) for s in raw_signals]

    deduper = Deduplicator(config)
    deduped = deduper.dedup(normalized)
    logger.info("Dedup: kept=%s dropped=%s", len(deduped), max(0, len(normalized) - len(deduped)))

    fe = FeatureEngine(config)
    enriched = fe.enrich(deduped)

    sa = SentimentAnalyzer(config)
    with_sent = sa.add_sentiment(enriched)
    logger.info("Sentiment types: %s", _sentiment_type_breakdown(with_sent))

    ranker = SignalRanker(config)
    ranked = ranker.rank(with_sent)

    inserted = store.insert_signals(ranked)
    store.set_last_run(_utcnow_naive())
    logger.info("Stored inserted=%s total_seen=%s", inserted, total_seen)

    return {
        "ingestion_counts": ingestion_counts,
        "total_seen": total_seen,
        "kept": len(deduped),
        "inserted": inserted,
    }


def build_daily_payload(config: Dict[str, Any], store: SQLiteStore, max_signals: int | None = None) -> Dict[str, Any]:
    if max_signals is None:
        max_signals = int(config.get("analysis", {}).get("max_signals", 10))

    since = _utcnow_naive() - timedelta(hours=int(config.get("storage", {}).get("rolling_window_hours", 24)))
    signals = store.get_signals_since(since)
    classifier = MarketStateClassifier(config)
    state = classifier.classify(signals)

    top = sorted(signals, key=lambda x: float(x.get("score", 0) or 0), reverse=True)[:max_signals]

    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for s in signals:
        by_source.setdefault(str(s.get("source", "unknown")), []).append(s)

    return {
        "date": _utcnow_naive().strftime("%Y-%m-%d"),
        "since": since.isoformat(),
        "market_state": state,
        "top_signals": top,
        "by_source": by_source,
        "counts": {k: len(v) for k, v in by_source.items()},
        "total_signals": len(signals),
    }
