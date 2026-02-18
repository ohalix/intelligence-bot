"""High-signal source discovery (candidate suggestions only).

This module does NOT auto-add sources.
It proposes up to N candidates for manual review via /sources.

Scoring is lightweight and conservative to avoid noise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set


@dataclass
class Candidate:
    name: str
    url: str
    ecosystems: List[str]
    kind: str  # rss|atom|web
    reason: str


# Curated, high-signal candidates across major ecosystems.
# Keep this list small and quality-focused.
CANDIDATES: List[Candidate] = [
    Candidate(
        name="Ethereum Foundation Blog",
        url="https://blog.ethereum.org/feed.xml",
        ecosystems=["ethereum"],
        kind="rss",
        reason="Core L1 research + protocol updates; strong signal source.",
    ),
    Candidate(
        name="Arbitrum Foundation Blog",
        url="https://arbitrum.io/blog",
        ecosystems=["arbitrum"],
        kind="web",
        reason="Official L2 announcements and ecosystem updates.",
    ),
    Candidate(
        name="Optimism Governance (Discourse) Latest",
        url="https://gov.optimism.io/latest.rss",
        ecosystems=["optimism"],
        kind="rss",
        reason="Official governance posts; frequently updated.",
    ),
    Candidate(
        name="Base Blog",
        url="https://www.base.org/blog",
        ecosystems=["base"],
        kind="web",
        reason="Official Base updates and releases.",
    ),
    Candidate(
        name="Avalanche Blog",
        url="https://www.avax.network/blog",
        ecosystems=["avalanche"],
        kind="web",
        reason="Official Avalanche ecosystem and technical updates.",
    ),
    Candidate(
        name="Polygon Blog",
        url="https://polygon.technology/blog",
        ecosystems=["polygon"],
        kind="web",
        reason="Official Polygon / AggLayer ecosystem updates.",
    ),
    Candidate(
        name="BNB Chain Blog",
        url="https://www.bnbchain.org/en/blog",
        ecosystems=["bnb"],
        kind="web",
        reason="Large TVL ecosystem; official announcements.",
    ),
    Candidate(
        name="zkSync Blog",
        url="https://zksync.mirror.xyz/feed/atom",
        ecosystems=["zksync"],
        kind="atom",
        reason="Official zkSync posts (Mirror Atom feed).",
    ),
    Candidate(
        name="Starknet Blog",
        url="https://www.starknet.io/en/posts",
        ecosystems=["starknet"],
        kind="web",
        reason="Official Starknet updates; relevant to L2 TVL + dev.",
    ),
    Candidate(
        name="Solana Blog RSS",
        url="https://solana.com/rss.xml",
        ecosystems=["solana"],
        kind="rss",
        reason="Solana ecosystem updates; complements EVM focus.",
    ),
    Candidate(
        name="Stacks Blog",
        url="https://www.stacks.co/blog",
        ecosystems=["bitcoin-l2"],
        kind="web",
        reason="Bitcoin L2 narrative; ecosystem news and research.",
    ),
]


def _existing_sources() -> Set[str]:
    existing: Set[str] = set()

    try:
        from ingestion.news_ingest import NEWS_FEEDS

        existing.update(NEWS_FEEDS)
    except Exception:
        pass

    try:
        from ingestion.funding_ingest import FUNDING_FEEDS

        existing.update(FUNDING_FEEDS)
    except Exception:
        pass

    try:
        from ingestion.ecosystem_ingest import ECOSYSTEM_FEEDS, ECOSYSTEM_WEB_PAGES

        existing.update(ECOSYSTEM_FEEDS)
        existing.update(ECOSYSTEM_WEB_PAGES)
    except Exception:
        pass

    # GitHub is query-based; ignore.
    return existing


def discover_sources(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return candidate sources not currently configured.

    Output is a list of dicts with: name,url,score,reason
    """

    existing = _existing_sources()

    # Priority ecosystems: user wants all major EVM ecosystems + Solana + BTC L2.
    # We treat anything in the candidates list as eligible.

    out: List[Dict[str, Any]] = []
    for c in CANDIDATES:
        if c.url in existing:
            continue
        score = 0
        # Prefer RSS/Atom over web (more stable)
        if c.kind in ("rss", "atom"):
            score += 3
        else:
            score += 1
        # Reward multi-ecosystem relevance
        score += min(2, max(0, len(c.ecosystems) - 1))

        out.append(
            {
                "name": c.name,
                "url": c.url,
                "score": score,
                "reason": c.reason,
            }
        )

    out.sort(key=lambda x: x["score"], reverse=True)
    return out
