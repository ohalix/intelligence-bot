"""scripts.local_test_run

Runs the pipeline end-to-end in a deterministic offline mode:
- OFFLINE_TEST=true returns sample signals without network calls
- DRY_MODE=true uses rule-based AI analysis layer

Usage:
  DRY_MODE=true OFFLINE_TEST=true python scripts/local_test_run.py
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import os
from pprint import pprint

from utils.config import load_config
from utils.logging import setup_logging
from storage.sqlite_store import SQLiteStore
from engine.pipeline import Pipeline


async def main():
    config = load_config()
    setup_logging(config)

    store = SQLiteStore(config)
    pipe = Pipeline(config)
    pipe.store = store

    summary = await pipe.run_once()

    print("\n=== PIPELINE SUMMARY ===")
    pprint({k: v for k, v in summary.items() if k not in {"top_signals"}})

    print("\n=== TOP SIGNALS ===")
    for i, s in enumerate(summary.get("top_signals", []), 1):
        print(f"{i}. {s.get('signal_score')} | {s.get('source')} | {s.get('title')}")
        if s.get("url"):
            print(f"   {s.get('url')}")
    print("\n=== ANALYSIS ===")
    pprint(summary.get("analysis"))


if __name__ == "__main__":
    asyncio.run(main())
