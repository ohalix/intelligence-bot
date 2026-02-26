"""News ingestion: RSS + Web + API.

Fixes applied:
- item 5: Use parse_rss_entry_datetime for timezone-aware UTC dates
- item 12: Use datetime.now(timezone.utc) instead of utcnow()
- item 14: Pass User-Agent header via fetch_rss_conditional
- item 15: ETag/Last-Modified conditional fetching
- item 16: feedparser bozo detection and logging
- item 18/19: Better 403/429 handling via utils.http
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import feedparser

from ingestion.api_sources import news_from_coinmarketcap_posts_latest, news_from_cryptocurrency_cv
from utils.http import fetch_text, fetch_rss_conditional, parse_rss_entry_datetime, NonRetryableHTTPError
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
DEFAULT_NEWS_API_SOURCES = ["cryptocurrency_cv"]

# Consecutive failure tracking per source (item 18/23)
_FAIL_COUNTS: Dict[str, int] = {}
_SUPPRESS_AFTER = 3  # suppress repeated log after 3 consecutive failures


class NewsIngester:
    """Aggregates News signals from RSS + Web + Free APIs."""

    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session
        self._store = None  # set by pipeline if available

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        ing = self.config.get("ingestion", {})
        rss_sources = ing.get("news_sources") or DEFAULT_NEWS_FEEDS
        web_sources = ing.get("news_web_sources") or DEFAULT_NEWS_WEB_PAGES
        api_sources = ing.get("news_api_sources") or DEFAULT_NEWS_API_SOURCES

        concurrency = int(ing.get("news_concurrency", 5))
        sem = asyncio.Semaphore(concurrency)

        # Pre-init stats (item 13)
        stats: Dict[str, Any] = {
            "rss": {"attempted": 0, "success": 0, "fail": 0, "items": 0, "skipped_304": 0},
            "web": {"attempted": 0, "success": 0, "fail": 0, "items": 0},
            "api": {"attempted": 0, "success": 0, "fail": 0, "items": 0},
            "errors": {},
        }

        async def _rss_one(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["rss"]["attempted"] += 1
                try:
                    # item 15: conditional fetch with ETag/Last-Modified
                    store = getattr(self, "_store", None)
                    content, not_modified = await fetch_rss_conditional(
                        self.session, url, store=store
                    )
                    if not_modified:
                        stats["rss"]["skipped_304"] += 1
                        logger.debug("News RSS 304 Not Modified: %s", url)
                        return []
                    # item 16: check bozo
                    parsed = feedparser.parse(content)
                    if getattr(parsed, "bozo", False):
                        exc = getattr(parsed, "bozo_exception", None)
                        exc_type = type(exc).__name__ if exc else "unknown"
                        if "html" in (content or "")[:200].lower():
                            logger.warning(
                                "News RSS bozo=True for %s (likely HTML error page): %s",
                                url, exc_type,
                            )
                        else:
                            logger.debug("News RSS bozo=True for %s: %s", url, exc_type)
                    out: List[Dict[str, Any]] = []
                    for entry in parsed.entries:
                        # item 5: timezone-aware date parsing
                        dt = parse_rss_entry_datetime(entry)
                        if dt is not None and dt < since:
                            continue
                        out.append({
                            "source": "news",
                            "source_id": url,
                            "title": getattr(entry, "title", ""),
                            "url": getattr(entry, "link", ""),
                            "description": getattr(entry, "summary", ""),
                            "published_at": getattr(entry, "published", "") or getattr(entry, "updated", ""),
                        })
                    stats["rss"]["success"] += 1
                    stats["rss"]["items"] += len(out)
                    _FAIL_COUNTS[url] = 0
                    return out
                except NonRetryableHTTPError as e:
                    stats["rss"]["fail"] += 1
                    _FAIL_COUNTS[url] = _FAIL_COUNTS.get(url, 0) + 1
                    _log_source_failure("News RSS", url, e, _FAIL_COUNTS[url])
                    stats["errors"][type(e).__name__] = stats["errors"].get(type(e).__name__, 0) + 1
                    return []
                except Exception as e:
                    stats["rss"]["fail"] += 1
                    _FAIL_COUNTS[url] = _FAIL_COUNTS.get(url, 0) + 1
                    _log_source_failure("News RSS", url, e, _FAIL_COUNTS[url])
                    k = type(e).__name__
                    stats["errors"][k] = stats["errors"].get(k, 0) + 1
                    return []

        async def _web_one(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["web"]["attempted"] += 1
                try:
                    links = await scrape_page_links(self.session, url, max_items=30)
                    out: List[Dict[str, Any]] = []
                    # item 12: timezone-aware now
                    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
                    for it in links[:30]:
                        href = it.get("url")
                        title = it.get("title")
                        if not href or not title:
                            continue
                        out.append({
                            "source": "news",
                            "source_id": url,
                            "title": title,
                            "url": href,
                            "description": "",
                            "published_at": now,
                        })
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
            "NewsIngester run: rss(a=%s s=%s f=%s i=%s 304=%s) web(a=%s s=%s f=%s i=%s) api(a=%s s=%s f=%s i=%s) errors=%s",
            stats["rss"]["attempted"], stats["rss"]["success"], stats["rss"]["fail"],
            stats["rss"]["items"], stats["rss"]["skipped_304"],
            stats["web"]["attempted"], stats["web"]["success"], stats["web"]["fail"],
            stats["web"]["items"],
            stats["api"]["attempted"], stats["api"]["success"], stats["api"]["fail"],
            stats["api"]["items"],
            dict(sorted(stats["errors"].items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )
        return flattened


def _log_source_failure(category: str, url: str, exc: Exception, count: int) -> None:
    """Suppress repeated failure logs after _SUPPRESS_AFTER consecutive failures (item 18/23)."""
    if count <= _SUPPRESS_AFTER:
        logger.warning("%s failed for %s (consecutive=%s): %s", category, url, count, exc)
    elif count == _SUPPRESS_AFTER + 1:
        logger.warning(
            "%s: suppressing further failure logs for %s after %s consecutive failures.",
            category, url, count,
        )
