import logging
from datetime import datetime
from typing import Any, Dict, List
import feedparser

from .base_ingest import BaseIngester
from utils.http import fetch_text

logger = logging.getLogger(__name__)

DEFAULT_FUNDING_FEEDS = [
    "https://blockworks.co/feed",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]

FUNDING_KEYWORDS = ["raised","funding","seed","series","investment","grant","backed","round","strategic","token sale"]

class FundingIngester(BaseIngester):
    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        # SAMPLE_OFFLINE: deterministic local testing without network
        if self.config.get('offline_test'):
            from datetime import datetime
            return [{
                'id': f'offline-funding-1',
                'source': 'funding',
                'type': 'funding_item',
                'title': 'AI x Crypto startup raises seed round',
                'text': 'AI x Crypto startup raises seed round',
                'url': 'https://example.com/funding/1',
                'timestamp': datetime.utcnow(),
            }]
        feeds = self.config.get("ingestion", {}).get("funding_sources") or DEFAULT_FUNDING_FEEDS
        signals: List[Dict[str, Any]] = []
        for feed_url in feeds:
            try:
                xml = await fetch_text(self.session, feed_url)
                parsed = feedparser.parse(xml)
                for entry in parsed.entries:
                    published = None
                    if getattr(entry, "published_parsed", None):
                        published = datetime(*entry.published_parsed[:6])
                    if published and published <= since:
                        continue
                    title = getattr(entry, "title", "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    blob = f"{title} {summary}".lower()
                    if not any(k in blob for k in FUNDING_KEYWORDS):
                        continue
                    signals.append({
                        "source": "funding",
                        "type": "funding_announcement",
                        "title": title,
                        "description": summary,
                        "url": getattr(entry, "link", "") or "",
                        "timestamp": published or datetime.utcnow(),
                        "source_name": getattr(parsed.feed, "title", "funding"),
                    })
            except Exception as e:
                logger.warning(f"FundingIngester failed for {feed_url}: {e}")
        return signals