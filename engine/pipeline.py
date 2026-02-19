import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import aiohttp

from ingestion.twitter_ingest import TwitterIngester
from ingestion.news_ingest import NewsIngester
from ingestion.github_ingest import GitHubIngester
from ingestion.funding_ingest import FundingIngester
from ingestion.ecosystem_ingest import EcosystemIngester

from processing.deduplicator import Deduplicator
from processing.spam_filter import SpamFilter
from processing.feature_engine import FeatureEngine
from processing.sentiment_analyzer import SentimentAnalyzer
from processing.signal_ranker import SignalRanker

from intelligence.market_state_classifier import MarketStateClassifier
from intelligence.narrative_generator import NarrativeGenerator
from intelligence.trend_detector import TrendDetector
from intelligence.web3_analysis_agent import Web3AnalysisAgent

from storage.sqlite_store import SQLiteStore
from utils.http import make_timeout

logger = logging.getLogger(__name__)


def _sentiment_type_stats(signals: List[Dict[str, Any]]) -> Dict[str, int]:
    """Aggregated sentiment representation stats for debugging/data-integrity."""
    stats: Dict[str, int] = {}
    for s in signals:
        v = s.get("sentiment")
        if v is None:
            k = "missing"
        elif isinstance(v, (int, float)):
            k = "numeric"
        elif isinstance(v, str):
            sv = v.strip().lower()
            if not sv:
                k = "empty_str"
            else:
                try:
                    float(sv)
                    k = "numeric_str"
                except Exception:
                    k = "label_str"
        else:
            k = type(v).__name__
        stats[k] = stats.get(k, 0) + 1
    return stats

def rolling_since(config: Dict[str, Any], store: SQLiteStore) -> datetime:
    hours = int(config.get("storage", {}).get("rolling_window_hours", 24))
    last = store.get_last_run()
    if last:
        return last
    return datetime.utcnow() - timedelta(hours=hours)

async def run_pipeline(
    config: Dict[str, Any],
    store: SQLiteStore,
    manual: bool = False,
    since_override: datetime | None = None,
) -> Dict[str, Any]:
    """Run the full ingestion + processing pipeline.

    Compatibility-first: existing callers can keep using (config, store, manual).
    - since_override: if provided, forces the ingestion window start.
    """

    since = since_override if since_override is not None else rolling_since(config, store)
    logger.info(f"Pipeline start. since={since.isoformat()} manual={manual}")

    raw: List[Dict[str, Any]] = []
    if config.get("offline_test"):
        logger.info("OFFLINE_TEST enabled: skipping network ingestion.")
    else:
        timeout = make_timeout(config)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            ingesters = [
                TwitterIngester(config, session),
                NewsIngester(config, session),
                GitHubIngester(config, session),
                FundingIngester(config, session),
                EcosystemIngester(config, session),
            ]
            for ing in ingesters:
                try:
                    batch = await ing.ingest(since)
                    logger.info(f"Ingested {len(batch)} from {ing.__class__.__name__}")
                    raw.extend(batch)
                except Exception as e:
                    logger.exception(f"{ing.__class__.__name__} failed: {e}")

    dedup = Deduplicator()
    spam = SpamFilter(config.get("ecosystems", {}).get("spam_patterns", []))
    feat = FeatureEngine(config.get("ecosystems", {}))
    sent = SentimentAnalyzer()
    ranker = SignalRanker(config)

    processed = dedup.dedup(raw)
    if len(processed) != len(raw):
        logger.info(f"Dedup: kept={len(processed)} dropped={max(0, len(raw) - len(processed))}")

    before_spam = len(processed)
    processed = spam.filter(processed)
    if len(processed) != before_spam:
        logger.info(f"SpamFilter: kept={len(processed)} dropped={max(0, before_spam - len(processed))}")
    processed = [feat.enrich(s) for s in processed]
    processed = sent.add_sentiment(processed)
    logger.info(f"Sentiment types: {_sentiment_type_stats(processed)}")
    ranked = ranker.rank(processed)

    inserted = store.upsert_signals(ranked)
    store.purge_old(int(config.get("storage", {}).get("max_signals_retained_days", 30)))
    store.set_last_run(datetime.utcnow())
    logger.info(f"Stored inserted={inserted} total_seen={len(ranked)}")

    return {"since": since, "inserted": inserted, "count": len(ranked)}

def build_daily_payload(config: Dict[str, Any], store: SQLiteStore, include_sections: bool=True) -> Dict[str, Any]:
    since = datetime.utcnow() - timedelta(hours=int(config.get("storage", {}).get("rolling_window_hours", 24)))
    limit = int(config.get("analysis", {}).get("top_signals_to_analyze", 10))

    all_signals = store.get_signals_since(since, None, limit=limit)
    news = store.get_signals_since(since, "news", limit=limit)
    twitter = store.get_signals_since(since, "twitter", limit=limit)
    github = store.get_signals_since(since, "github", limit=limit)
    funding = store.get_signals_since(since, "funding", limit=limit)
    ecosystem = store.get_signals_since(since, "ecosystem", limit=limit)

    ms = MarketStateClassifier().classify(all_signals)
    narr = NarrativeGenerator().cluster(all_signals)
    trends = TrendDetector().detect(all_signals)

    payload = {
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "inputs": {"market_state": ms, "narratives": narr, "trends": trends},
        "sections": {},
    }
    if include_sections:
        payload["sections"] = {
            "Top Signals": all_signals,
            "News": news,
            "New Projects": (twitter + github)[:limit],
            "Funding & Ecosystem": (funding + ecosystem)[:limit],
            "GitHub": github,
        }
    return payload


# Backwards-compatibility alias.
# Some earlier revisions referenced a different name; keep an alias to avoid
# ImportErrors without changing behaviour.
build_daily_brief_payload = build_daily_payload

async def compute_analysis(config: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    agent = Web3AnalysisAgent(config)
    top = payload.get("sections", {}).get("Top Signals") or []
    ms = payload.get("inputs", {}).get("market_state") or {}
    narr = payload.get("inputs", {}).get("narratives") or []
    return await agent.analyze(top, ms, narr)

async def send_dailybrief(config: Dict[str, Any], store: SQLiteStore, bot) -> None:
    chat_id = config.get("bot", {}).get("chat_id")
    if not chat_id:
        logger.warning("TELEGRAM_CHAT_ID not set; skipping scheduled send.")
        return
    payload = build_daily_payload(config, store, include_sections=True)
    payload["analysis"] = await compute_analysis(config, payload)
    from bot.formatter import format_dailybrief
    msg = format_dailybrief(payload)
    await bot.send_message(chat_id=chat_id, text=msg, parse_mode="MarkdownV2", disable_web_page_preview=True)
