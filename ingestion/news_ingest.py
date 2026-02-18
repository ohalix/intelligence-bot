import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

import feedparser

from utils.http import fetch_text

logger = logging.getLogger(__name__)


NEWS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
]


class NewsIngester:
    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        concurrency = int(self.config.get("ingestion", {}).get("news_concurrency", 5))
        sem = asyncio.Semaphore(concurrency)

        stats = {"attempted": 0, "success": 0, "fail": 0, "items": 0, "errors": {}}

        async def _one(url: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["attempted"] += 1
                try:
                    # IMPORTANT: fetch_text correctly awaits aiohttp response text.
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
                                "source": "news",
                                "source_id": url,
                                "title": getattr(entry, "title", ""),
                                "url": getattr(entry, "link", ""),
                                "description": getattr(entry, "summary", ""),
                                "published_at": getattr(entry, "published", ""),
                            }
                        )
                    stats["success"] += 1
                    stats["items"] += len(out)
                    return out
                except Exception as e:
                    stats["fail"] += 1
                    key = type(e).__name__
                    stats["errors"][key] = stats["errors"].get(key, 0) + 1
                    logger.warning("NewsIngester failed for %s: %s", url, e)
                    return []

        results = await asyncio.gather(*[_one(u) for u in NEWS_FEEDS])
        flattened = [x for sub in results for x in sub]

        logger.info(
            "NewsIngester run: attempted=%s success=%s fail=%s items=%s top_errors=%s",
            stats["attempted"],
            stats["success"],
            stats["fail"],
            stats["items"],
            dict(sorted(stats["errors"].items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )

        return flattened
