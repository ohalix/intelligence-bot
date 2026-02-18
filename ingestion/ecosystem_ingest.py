import datetime as dt
import logging
from typing import Any, Dict, List

import feedparser

from .base_ingest import BaseIngester
from utils.http import fetch_text

logger = logging.getLogger(__name__)

DEFAULT_ECOSYSTEM_FEEDS = [
    "https://blog.arbitrum.io/rss/",
    "https://blog.optimism.io/feed",
    "https://solana.com/rss.xml",
]


class EcosystemIngester(BaseIngester):
    """Ingest ecosystem announcements from RSS/Atom feeds.

    Contract: __init__(config, session)
    """

    def __init__(self, config: Dict[str, Any], session):
        super().__init__(config, session)

    def _feeds(self) -> List[str]:
        ingestion_cfg = self.config.get("ingestion", {})
        if "ecosystem_sources" in ingestion_cfg:
            return list(ingestion_cfg.get("ecosystem_sources") or [])
        return list(ingestion_cfg.get("ecosystem_sources") or DEFAULT_ECOSYSTEM_FEEDS)

    async def ingest(self, since: dt.datetime) -> List[Dict[str, Any]]:
        feeds = self._feeds()
        if not feeds:
            return []

        signals: List[Dict[str, Any]] = []
        for feed_url in feeds:
            try:
                xml = await fetch_text(self.session, feed_url)
                parsed = feedparser.parse(xml)

                for entry in parsed.entries:
                    published_dt: dt.datetime | None = None
                    tm = getattr(entry, "published_parsed", None)
                    if tm:
                        try:
                            published_dt = dt.datetime(*tm[:6])
                        except Exception:
                            published_dt = None

                    if published_dt and published_dt <= since:
                        continue

                    title = getattr(entry, "title", "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    url = getattr(entry, "link", "") or ""

                    signals.append(
                        {
                            "source": "ecosystem",
                            "type": "ecosystem_announcement",
                            "title": title,
                            "description": summary,
                            "url": url,
                            "timestamp": (published_dt or dt.datetime.utcnow()).isoformat(),
                            "source_name": getattr(parsed.feed, "title", "ecosystem"),
                            "raw_json": "{}",
                        }
                    )

            except Exception as e:
                # Non-fatal: warn and continue other feeds.
                logger.warning("EcosystemIngester failed for %s: %s", feed_url, e)

        return signals
