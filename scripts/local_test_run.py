import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import asyncio
from datetime import datetime, timedelta

from utils.config import load_config
from utils.logging import setup_logging
from storage.sqlite_store import SQLiteStore
from engine.pipeline import run_pipeline, build_daily_payload, compute_analysis
from processing.deduplicator import Deduplicator
from processing.feature_engine import FeatureEngine
from processing.sentiment_analyzer import SentimentAnalyzer
from processing.signal_ranker import SignalRanker

def seed_demo_signals(cfg, store):
    # Only used when running offline and nothing was ingested; keeps core bot behavior unchanged.
    demo = [
        {"source":"news","type":"news_article","title":"New L2 announces testnet with novel data availability","description":"A new rollup stack claims lower fees using DA sampling.","url":"https://example.com/l2","timestamp":datetime.utcnow()},
        {"source":"funding","type":"funding_announcement","title":"Infra startup raises seed to build on-chain indexer","description":"Team targets EVM + SVM with realtime analytics.","url":"https://example.com/funding","timestamp":datetime.utcnow()},
        {"source":"github","type":"github_repo","title":"awesome-web3-agents","description":"Repo tracking agent frameworks and onchain tooling.","url":"https://github.com/example/awesome","timestamp":datetime.utcnow(),"stars":120,"forks":10,"language":"Python"},
        {"source":"ecosystem","type":"ecosystem_announcement","title":"Ecosystem grants open for DeFi builders this quarter","description":"New grants program focuses on tooling and lending markets.","url":"https://example.com/grants","timestamp":datetime.utcnow()},
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
    setup_logging(cfg)
    store = SQLiteStore(cfg.get("storage", {}).get("database_path", "./data/web3_intelligence.db"))
    result = await run_pipeline(cfg, store, manual=True)

    # If offline / no ingestion results, seed demo signals so you can validate formatting end-to-end.
    since = datetime.utcnow() - timedelta(hours=int(cfg.get("storage", {}).get("rolling_window_hours", 24)))
    if len(store.get_signals_since(since, None, limit=1)) == 0:
        seed_demo_signals(cfg, store)

    payload = build_daily_payload(cfg, store, include_sections=True)
    payload["analysis"] = await compute_analysis(cfg, payload)
    from bot.formatter import format_dailybrief
    print(format_dailybrief(payload))

if __name__ == "__main__":
    asyncio.run(run())
