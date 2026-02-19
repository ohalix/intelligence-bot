import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

import feedparser

from ingestion.api_sources import news_from_coinmarketcap_posts_latest, news_from_cryptocurrency_cv
from utils.http import fetch_text
from utils.web_scraper import scrape_page_links

logger = logging.getLogger(__name__)


DEFAULT_NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://decrypt.co/feed",
]
DEFAULT_NEWS_WEB_PAGES = [
    "https://decrypt.co/news",
    "https://www.coindesk.com/",
]
# CoinMarketCap's content endpoints can be restricted depending on the free plan.
# Keep it available via config/env, but do not call by default.
DEFAULT_NEWS_API_SOURCES = ["cryptocurrency_cv"]


class NewsIngester:
    """Aggregates News signals from RSS + Web + Free APIs.

    Compatibility-first: output schema matches existing pipeline expectations.
    """

    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        ing = self.config.get("ingestion", {})
        rss_sources = ing.get("news_sources") or DEFAULT_NEWS_FEEDS
        web_sources = ing.get("news_web_sources") or DEFAULT_NEWS_WEB_PAGES
        api_sources = ing.get("news_api_sources") or DEFAULT_NEWS_API_SOURCES

        concurrency = int(ing.get("news_concurrency", 5))
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
                                "source": "news",
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
                    logger.warning("News RSS failed for %s: %s", url, e)
                    return []

        async def _web_one(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["web"]["attempted"] += 1
                try:
                    links = await scrape_page_links(self.session, url, max_items=30)
                    out: List[Dict[str, Any]] = []
                    now = datetime.utcnow().isoformat() + "Z"
                    for it in links[:30]:
                        href = it.get("url")
                        title = it.get("title")
                        if not href or not title:
                            continue
                        out.append(
                            {
                                "source": "news",
                                "source_id": url,
                                "title": title,
                                "url": href,
                                "description": "",
                                "published_at": now,
                            }
                        )
                    stats["web"]["success"] += 1
                    stats["web"]["items"] += len(out)
                    return out
                except Exception as e:
                    stats["web"]["fail"] += 1
                    k = type(e).__name__
                    stats["errors"][k] = stats["errors"].get(k, 0) + 1
                    logger.warning("News WEB failed for %s: %s", url, e)
                    return []

        async def _api_one(api_name: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["api"]["attempted"] += 1
                try:
                    api = (api_name or "").strip().lower()
                    if api == "cryptocurrency_cv":
                        out = await news_from_cryptocurrency_cv(self.session, since)
                    elif api == "coinmarketcap_posts_latest":
                        key = (self.config.get("keys", {}) or {}).get("coinmarketcap")
                        out = await news_from_coinmarketcap_posts_latest(self.session, since, key)
                    else:
                        logger.warning("Unknown news API source: %s", api_name)
                        out = []
                    stats["api"]["success"] += 1
                    stats["api"]["items"] += len(out)
                    return out
                except Exception as e:
                    stats["api"]["fail"] += 1
                    k = type(e).__name__
                    stats["errors"][k] = stats["errors"].get(k, 0) + 1
                    logger.warning("News API failed for %s: %s", api_name, e)
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
            "NewsIngester run: rss(a=%s s=%s f=%s i=%s) web(a=%s s=%s f=%s i=%s) api(a=%s s=%s f=%s i=%s) top_errors=%s",
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
