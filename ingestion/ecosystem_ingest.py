import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

import feedparser

from utils.http import fetch_text
from utils.web_scraper import scrape_page_links

from ingestion.api_sources import governance_from_snapshot

logger = logging.getLogger(__name__)

# RSS/Atom sources (should be directly parseable)
DEFAULT_ECOSYSTEM_FEEDS = [
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
DEFAULT_ECOSYSTEM_WEB_PAGES = [
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
        ing = self.config.get("ingestion", {})
        rss_sources = ing.get("ecosystem_rss_sources", DEFAULT_ECOSYSTEM_FEEDS)
        web_sources = ing.get("ecosystem_web_sources", DEFAULT_ECOSYSTEM_WEB_PAGES)
        api_sources = ing.get("ecosystem_api_sources", [])
        snapshot_spaces = ing.get("snapshot_spaces", [])

        concurrency = int(self.config.get("ingestion", {}).get("ecosystem_concurrency", 5))
        sem = asyncio.Semaphore(concurrency)

        stats = {
            "rss_attempted": 0,
            "rss_success": 0,
            "rss_fail": 0,
            "web_attempted": 0,
            "web_success": 0,
            "web_fail": 0,
            "api_attempted": 0,
            "api_success": 0,
            "api_fail": 0,
            "items": 0,
            "errors": {},
        }

        async def _rss(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["rss_attempted"] += 1
                try:
                    content = await fetch_text(self.session, url)
                    parsed = feedparser.parse(content)
                    # FIX item 16: bozo detection
                    if getattr(parsed, "bozo", False):
                        exc = getattr(parsed, "bozo_exception", None)
                        logger.debug("EcosystemIngester RSS bozo=True for %s: %s", url, type(exc).__name__ if exc else "unknown")
                    out: List[Dict[str, Any]] = []
                    for entry in parsed.entries:
                        # FIX item 5: timezone-aware date parsing
                        from utils.http import parse_rss_entry_datetime as _parse_dt
                        dt_entry = _parse_dt(entry)
                        if dt_entry is not None and dt_entry < since:
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

        async def _api(name: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["api_attempted"] += 1
                try:
                    api = (name or "").strip().lower()
                    if api == "snapshot_proposals":
                        out = await governance_from_snapshot(self.session, since, list(snapshot_spaces))
                    elif api == "defillama_chain_tvl":
                        # FIX item 22: was a no-op returning []; now explicitly warns operator
                        logger.warning(
                            "defillama_chain_tvl is not implemented (no schema defined). "
                            "Remove it from ECOSYSTEM_API_SOURCES to suppress this warning. Skipping."
                        )
                        out = []
                    else:
                        logger.warning("Unknown ecosystem API source: %s", name)
                        out = []
                    stats["api_success"] += 1
                    stats["items"] += len(out)
                    return out
                except Exception as e:
                    stats["api_fail"] += 1
                    key = type(e).__name__
                    stats["errors"][key] = stats["errors"].get(key, 0) + 1
                    logger.warning("EcosystemIngester API failed for %s: %s", name, e)
                    return []

        # Run RSS + WEB + API concurrently to maximize coverage.
        rss_tasks = [_rss(u) for u in rss_sources]
        web_tasks = [_web(u) for u in web_sources]
        api_tasks = [_api(u) for u in api_sources]
        results = await asyncio.gather(*(rss_tasks + web_tasks + api_tasks))
        flattened = [x for sub in results for x in sub]

        logger.info(
            "EcosystemIngester run: rss_attempted=%s rss_success=%s rss_fail=%s web_attempted=%s web_success=%s web_fail=%s api_attempted=%s api_success=%s api_fail=%s items=%s top_errors=%s",
            stats["rss_attempted"],
            stats["rss_success"],
            stats["rss_fail"],
            stats["web_attempted"],
            stats["web_success"],
            stats["web_fail"],
            stats.get("api_attempted", 0),
            stats.get("api_success", 0),
            stats.get("api_fail", 0),
            stats["items"],
            dict(sorted(stats["errors"].items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )

        return flattened
