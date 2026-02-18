import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

import feedparser

from utils.http import fetch_text
from utils.web_scraper import scrape_page_links

logger = logging.getLogger(__name__)

# RSS/Atom sources (should be directly parseable)
ECOSYSTEM_FEEDS = [
    # Optimism: Discourse forum RSS (official)
    "https://gov.optimism.io/latest.rss",
    # Arbitrum
    "https://arbitrumfoundation.medium.com/feed",
    # Base
    "https://base.mirror.xyz/feed/atom",
    # Avalanche
    "https://medium.com/feed/avalancheavax",
    # Starknet
    "https://medium.com/feed/starkware",
    # Solana (official blog is sometimes JS-heavy; keep a stable mirror feed)
    "https://solana.com/rss.xml",
]

# Web pages to scrape (fallback + augment)
ECOSYSTEM_WEB_PAGES = [
    # Optimism blog page (DNS for blog.optimism.io has been flaky)
    "https://www.optimism.io/blog",
    # Arbitrum blog
    "https://arbitrum.io/blog",
    # Base blog
    "https://www.base.org/blog",
    # Avalanche blog
    "https://www.avax.network/blog",
    # Sonic (example)
    "https://www.soniclabs.com/blog",
    # Hyperliquid / HyperEVM ecosystem landing (scrape links)
    "https://hyperliquid.xyz/",
    # Bitcoin L2 narrative hubs (scrape links, no aggressive crawling)
    "https://www.stacks.co/blog",
]


class EcosystemIngester:
    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        concurrency = int(self.config.get("ingestion", {}).get("ecosystem_concurrency", 5))
        sem = asyncio.Semaphore(concurrency)

        stats = {
            "rss_attempted": 0,
            "rss_success": 0,
            "rss_fail": 0,
            "web_attempted": 0,
            "web_success": 0,
            "web_fail": 0,
            "items": 0,
            "errors": {},
        }

        async def _rss(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["rss_attempted"] += 1
                try:
                    content = await fetch_text(self.session, url)
                    parsed = feedparser.parse(content)
                    out: List[Dict[str, Any]] = []
                    for entry in parsed.entries:
                        published = getattr(entry, "published_parsed", None)
                        if published:
                            dt = datetime(*published[:6])
                            if dt < since:
                                continue
                        out.append(
                            {
                                "source": "ecosystem",
                                "source_id": url,
                                "title": getattr(entry, "title", ""),
                                "url": getattr(entry, "link", ""),
                                "description": getattr(entry, "summary", ""),
                                "published_at": getattr(entry, "published", ""),
                            }
                        )
                    stats["rss_success"] += 1
                    stats["items"] += len(out)
                    return out
                except Exception as e:
                    stats["rss_fail"] += 1
                    key = type(e).__name__
                    stats["errors"][key] = stats["errors"].get(key, 0) + 1
                    logger.warning("EcosystemIngester RSS failed for %s: %s", url, e)
                    return []

        async def _web(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["web_attempted"] += 1
                try:
                    links = await scrape_page_links(self.session, url, max_items=10)
                    out: List[Dict[str, Any]] = []
                    for it in links:
                        out.append(
                            {
                                "source": "ecosystem",
                                "source_id": url,
                                "title": it.get("title", ""),
                                "url": it.get("url", ""),
                                "description": "",
                                "published_at": "",
                            }
                        )
                    stats["web_success"] += 1
                    stats["items"] += len(out)
                    return out
                except Exception as e:
                    stats["web_fail"] += 1
                    key = type(e).__name__
                    stats["errors"][key] = stats["errors"].get(key, 0) + 1
                    # Explicitly call out common blocking patterns
                    msg = str(e)
                    if "403" in msg or "Just a moment" in msg:
                        logger.warning("EcosystemIngester WEB blocked for %s: %s", url, e)
                    else:
                        logger.warning("EcosystemIngester WEB failed for %s: %s", url, e)
                    return []

        # Run RSS + WEB concurrently to maximize coverage.
        rss_tasks = [_rss(u) for u in ECOSYSTEM_FEEDS]
        web_tasks = [_web(u) for u in ECOSYSTEM_WEB_PAGES]
        results = await asyncio.gather(*(rss_tasks + web_tasks))
        flattened = [x for sub in results for x in sub]

        logger.info(
            "EcosystemIngester run: rss_attempted=%s rss_success=%s rss_fail=%s web_attempted=%s web_success=%s web_fail=%s items=%s top_errors=%s",
            stats["rss_attempted"],
            stats["rss_success"],
            stats["rss_fail"],
            stats["web_attempted"],
            stats["web_success"],
            stats["web_fail"],
            stats["items"],
            dict(sorted(stats["errors"].items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )

        return flattened
