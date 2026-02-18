import logging
import datetime as dt
from typing import Any, Dict, List

import feedparser

from .base_ingest import BaseIngester
from utils.http import fetch_text

logger = logging.getLogger(__name__)

DEFAULT_FUNDING_FEEDS = [
    "https://blockworks.co/feed",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]

FUNDING_KEYWORDS = [
    "raised",
    "funding",
    "seed",
    "series",
    "investment",
    "grant",
    "backed",
    "round",
    "strategic",
    "token sale",
]


class FundingIngester(BaseIngester):
    async def ingest(self, since: dt.datetime) -> List[Dict[str, Any]]:
        feeds = self.config.get("ingestion", {}).get("funding_sources") or DEFAULT_FUNDING_FEEDS
        signals: List[Dict[str, Any]] = []

        for feed_url in feeds:
            try:
                xml = await fetch_text(self.session, feed_url)
                parsed = feedparser.parse(xml)

                for entry in parsed.entries:
                    published_dt: dt.datetime | None = None
                    if getattr(entry, "published_parsed", None):
                        published_dt = dt.datetime(*entry.published_parsed[:6])

                    if published_dt and published_dt <= since:
                        continue

                    title = getattr(entry, "title", "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    blob = f"{title} {summary}".lower()

                    if not any(k in blob for k in FUNDING_KEYWORDS):
                        continue

                    signals.append(
                        {
                            "source": "funding",
                            "type": "funding_announcement",
                            "title": title,
                            "description": summary,
                            "url": getattr(entry, "link", "") or "",
                            "timestamp": published_dt or dt.datetime.utcnow(),
                            "source_name": getattr(parsed.feed, "title", "funding"),
                        }
                    )
            except Exception as e:
                logger.warning(f"FundingIngester failed for {feed_url}: {e}")

        return signals
