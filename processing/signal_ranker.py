"""processing.signal_ranker

Assigns a signal_score and lightweight tags (ecosystem/sector) to each signal.
This is deterministic and intentionally conservative (no aggressive scraping).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class SignalRanker:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.max_score = 100

        # Prioritize user-requested ecosystems (default)
        self.ecosystem_keywords = {
            "ethereum_l2s": ["arbitrum", "optimism", "base", "zksync", "scroll", "starknet", "linea", "polygon", "l2", "rollup"],
            "solana": ["solana", "spl", "jupiter", "raydium", "sol"],
            "bitcoin_l2s": ["bitcoin l2", "bitvm", "stacks", "lightning", "babylon", "rootstock", "rgb"],
        }

        self.sector_keywords = {
            "defi": ["dex", "amm", "lending", "borrow", "perps", "derivatives", "yield", "liquidity", "tvl", "vault"],
            "infrastructure": ["rpc", "indexer", "node", "oracle", "bridge", "sequencer", "data availability", "zk", "rollup", "infra"],
            "ai_crypto": ["agent", "agents", "ai", "llm", "gpu", "inference", "model", "data", "compute"],
            "gaming": ["game", "gaming", "metaverse"],
            "nft": ["nft", "collection", "mint"],
        }

    def _detect_tags(self, signal: Dict[str, Any]) -> None:
        text = " ".join([str(signal.get(k, "")) for k in ("title", "text", "description")]).lower()

        # ecosystem
        detected_eco = "unknown"
        for eco, kws in self.ecosystem_keywords.items():
            if any(k in text for k in kws):
                detected_eco = eco
                break
        signal["detected_ecosystem"] = signal.get("detected_ecosystem") or detected_eco

        # sector
        detected_sec = "unknown"
        for sec, kws in self.sector_keywords.items():
            if any(k in text for k in kws):
                detected_sec = sec
                break
        signal["detected_sector"] = signal.get("detected_sector") or detected_sec

    def _recency_score(self, ts: Any) -> float:
        if not isinstance(ts, datetime):
            return 0.3
        hours = max((datetime.utcnow() - ts).total_seconds() / 3600, 0.0)
        # 0h => 1.0, 24h => ~0.37
        return math.exp(-hours / 24)

    def _source_weight(self, source: str) -> float:
        return {
            "news": 1.0,
            "github": 0.9,
            "funding": 1.0,
            "ecosystem": 0.85,
            "twitter": 0.7,
            "unknown": 0.6,
        }.get(source or "unknown", 0.6)

    def _sector_weight(self, sector: str) -> float:
        return {
            "defi": 1.0,
            "infrastructure": 0.95,
            "ai_crypto": 0.9,
            "gaming": 0.6,
            "nft": 0.5,
            "unknown": 0.7,
        }.get(sector or "unknown", 0.7)

    def _ecosystem_weight(self, eco: str) -> float:
        return {
            "ethereum_l2s": 1.0,
            "solana": 0.95,
            "bitcoin_l2s": 0.95,
            "unknown": 0.7,
        }.get(eco or "unknown", 0.7)

    def _engagement_score(self, signal: Dict[str, Any]) -> float:
        # Works across sources; missing fields contribute 0
        likes = int(signal.get("likes", 0) or 0)
        retweets = int(signal.get("retweets", 0) or 0)
        replies = int(signal.get("replies", 0) or 0)
        stars = int(signal.get("stars", 0) or 0)
        forks = int(signal.get("forks", 0) or 0)

        raw = likes + 2 * retweets + replies + 5 * stars + 3 * forks
        # log-scale so whales don't dominate
        return math.log10(raw + 1) / 3  # ~0-1

    def score(self, signal: Dict[str, Any]) -> float:
        self._detect_tags(signal)

        source = signal.get("source", "unknown")
        recency = self._recency_score(signal.get("timestamp"))
        engagement = self._engagement_score(signal)
        eco_w = self._ecosystem_weight(signal.get("detected_ecosystem"))
        sec_w = self._sector_weight(signal.get("detected_sector"))
        src_w = self._source_weight(source)

        # base: 0-1
        base = 0.45 * recency + 0.35 * engagement + 0.20 * src_w
        weighted = base * eco_w * sec_w

        return min(weighted * self.max_score, float(self.max_score))

    def rank_batch(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for s in signals:
            try:
                s["signal_score"] = round(self.score(s), 2)
            except Exception as e:
                logger.warning(f"Failed scoring signal: {e}")
                s["signal_score"] = 0.0

        signals.sort(key=lambda x: x.get("signal_score", 0), reverse=True)
        return signals
