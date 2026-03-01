import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import asyncio
from datetime import datetime, timedelta

import logging
from utils.config import load_config
from storage.sqlite_store import SQLiteStore
from engine.pipeline import run_pipeline, build_daily_payload
from processing.deduplicator import Deduplicator
from processing.feature_engine import FeatureEngine
from processing.sentiment_analyzer import SentimentAnalyzer
from processing.signal_ranker import SignalRanker

def seed_demo_signals(cfg, store):
    # Only used when running offline and nothing was ingested; keeps core bot behavior unchanged.
    demo = [
        {"source":"news","type":"news_article","title":"New L2 announces testnet with novel data availability","description":"A new rollup stack claims lower fees using DA sampling.","url":"https://example.com/l2","timestamp":datetime.now(timezone.utc).replace(tzinfo=None)},
        {"source":"funding","type":"funding_announcement","title":"Infra startup raises seed to build on-chain indexer","description":"Team targets EVM + SVM with realtime analytics.","url":"https://example.com/funding","timestamp":datetime.now(timezone.utc).replace(tzinfo=None)},
        {"source":"github","type":"github_repo","title":"awesome-web3-agents","description":"Repo tracking agent frameworks and onchain tooling.","url":"https://github.com/example/awesome","timestamp":datetime.now(timezone.utc).replace(tzinfo=None),"stars":120,"forks":10,"language":"Python"},
        {"source":"ecosystem","type":"ecosystem_announcement","title":"Ecosystem grants open for DeFi builders this quarter","description":"New grants program focuses on tooling and lending markets.","url":"https://example.com/grants","timestamp":datetime.now(timezone.utc).replace(tzinfo=None)},
    ]
    d = Deduplicator()
    f = FeatureEngine(cfg.get("ecosystems", {}))
    s = SentimentAnalyzer()
    r = SignalRanker(cfg)
    demo = d.dedup(demo)
    demo = [f.enrich(x) for x in demo]
    demo = s.add_sentiment(demo)
    demo = r.rank(demo)
    store.upsert_signals(demo)

async def run():
    cfg = load_config()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store = SQLiteStore(cfg.get("storage", {}).get("db_path") or cfg.get("storage", {}).get("database_path", "./data/web3_intelligence.db"))
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)
    _ = await run_pipeline(cfg, store, since=since, manual=True)

    # If offline / no ingestion results, seed demo signals so you can validate formatting end-to-end.
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=int(cfg.get("storage", {}).get("rolling_window_hours", 24)))
    if len(store.get_signals_since(since, None, limit=1)) == 0:
        seed_demo_signals(cfg, store)

    payload = build_daily_payload(cfg, store, include_sections=True)
    from bot.formatter import format_dailybrief_html
    print(format_dailybrief_html(payload))

if __name__ == "__main__":
    asyncio.run(run())
