import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

import feedparser

from ingestion.api_sources import funding_from_defillama_raises
from utils.http import fetch_text
from utils.web_scraper import scrape_page_links

logger = logging.getLogger(__name__)


DEFAULT_FUNDING_FEEDS = [
    "https://blockworks.co/feed",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]

DEFAULT_FUNDING_WEB_PAGES = [
    "https://decrypt.co/tag/funding",
    "https://www.coindesk.com/tag/venture-capital/",
]


class FundingIngester:
    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        ing = self.config.get("ingestion", {})
        rss_sources = ing.get("funding_rss_sources", DEFAULT_FUNDING_FEEDS)
        web_sources = ing.get("funding_web_sources", DEFAULT_FUNDING_WEB_PAGES)
        api_sources = ing.get("funding_api_sources", [])

        concurrency = int(ing.get("funding_concurrency", 4))
        sem = asyncio.Semaphore(concurrency)

        stats = {
            "rss": {"attempted": 0, "success": 0, "fail": 0, "items": 0},
            "web": {"attempted": 0, "success": 0, "fail": 0, "items": 0},
            "api": {"attempted": 0, "success": 0, "fail": 0, "items": 0},
            "errors": {},
        }

        async def _rss_one(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["rss"]["attempted"] += 1
                try:
                    content = await fetch_text(self.session, url)
                    parsed = feedparser.parse(content)
                    out: List[Dict[str, Any]] = []
                    for entry in parsed.entries:
                        published = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
                        if published:
                            dt = datetime(*published[:6])
                            if dt < since:
                                continue
                        out.append(
                            {
                                "source": "funding",
                                "source_id": url,
                                "title": getattr(entry, "title", ""),
                                "url": getattr(entry, "link", ""),
                                "description": getattr(entry, "summary", ""),
                                "published_at": getattr(entry, "published", "") or getattr(entry, "updated", ""),
                            }
                        )
                    stats["rss"]["success"] += 1
                    stats["rss"]["items"] += len(out)
                    return out
                except Exception as e:
                    stats["rss"]["fail"] += 1
                    k = type(e).__name__
                    stats["errors"][k] = stats["errors"].get(k, 0) + 1
                    logger.warning("Funding RSS failed for %s: %s", url, e)
                    return []

        async def _web_one(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["web"]["attempted"] += 1
                try:
                    links = await scrape_page_links(self.session, url, max_items=10)
                    out: List[Dict[str, Any]] = []
                    for it in links:
                        out.append(
                            {
                                "source": "funding",
                                "source_id": url,
                                "title": it.get("title", ""),
                                "url": it.get("url", ""),
                                "description": "",
                                "published_at": "",
                            }
                        )
                    stats["web"]["success"] += 1
                    stats["web"]["items"] += len(out)
                    return out
                except Exception as e:
                    stats["web"]["fail"] += 1
                    k = type(e).__name__
                    stats["errors"][k] = stats["errors"].get(k, 0) + 1
                    msg = str(e)
                    if "403" in msg or "Just a moment" in msg:
                        logger.warning("Funding WEB blocked for %s: %s", url, e)
                    else:
                        logger.warning("Funding WEB failed for %s: %s", url, e)
                    return []

        async def _api_one(name: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["api"]["attempted"] += 1
                try:
                    api = (name or "").strip().lower()
                    if api == "defillama_raises":
                        out = await funding_from_defillama_raises(self.session, since)
                    elif api == "coinmarketcal_events":
                        key = (self.config.get("keys", {}) or {}).get("coinmarketcal")
                        if not key:
                            logger.info("CoinMarketCal API disabled (missing COINMARKETCAL_API_KEY)")
                            out = []
                        else:
                            # Endpoint details vary by plan; keep disabled until configured.
                            logger.info("CoinMarketCal API configured, but endpoint not enabled by default")
                            out = []
                    else:
                        logger.warning("Unknown funding API source: %s", name)
                        out = []
                    stats["api"]["success"] += 1
                    stats["api"]["items"] += len(out)
                    return out
                except Exception as e:
                    stats["api"]["fail"] += 1
                    k = type(e).__name__
                    stats["errors"][k] = stats["errors"].get(k, 0) + 1
                    logger.warning("Funding API failed for %s: %s", name, e)
                    return []

        results = []
        if rss_sources:
            results.extend(await asyncio.gather(*[_rss_one(u) for u in rss_sources]))
        if web_sources:
            results.extend(await asyncio.gather(*[_web_one(u) for u in web_sources]))
        if api_sources:
            results.extend(await asyncio.gather(*[_api_one(a) for a in api_sources]))

        flattened = [x for sub in results for x in sub]

        logger.info(
            "FundingIngester run: rss(a=%s s=%s f=%s i=%s) web(a=%s s=%s f=%s i=%s) api(a=%s s=%s f=%s i=%s) top_errors=%s",
            stats["rss"]["attempted"],
            stats["rss"]["success"],
            stats["rss"]["fail"],
            stats["rss"]["items"],
            stats["web"]["attempted"],
            stats["web"]["success"],
            stats["web"]["fail"],
            stats["web"]["items"],
            stats["api"]["attempted"],
            stats["api"]["success"],
            stats["api"]["fail"],
            stats["api"]["items"],
            dict(sorted(stats["errors"].items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )
        return flattened
