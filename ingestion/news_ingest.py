import logging
from datetime import datetime
from typing import Any, Dict, List
import feedparser

from .base_ingest import BaseIngester
from utils.http import fetch_text

logger = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://blockworks.co/feed",
]

class NewsIngester(BaseIngester):
    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        # SAMPLE_OFFLINE: deterministic local testing without network
        if self.config.get('offline_test'):
            from datetime import datetime
            return [{
                'id': f'offline-news-1',
                'source': 'news',
                'type': 'news_item',
                'title': 'Solana infra upgrade improves throughput',
                'text': 'Solana infra upgrade improves throughput',
                'url': 'https://example.com/news/1',
                'timestamp': datetime.utcnow(),
            }]
        feeds = self.config.get("ingestion", {}).get("news_sources") or DEFAULT_FEEDS
        signals: List[Dict[str, Any]] = []
        excl = [k.lower() for k in self.config.get("filtering", {}).get("news", {}).get("exclude_keywords", [])]
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
                    link = getattr(entry, "link", "") or ""
                    summary = getattr(entry, "summary", "") or ""
                    blob = f"{title} {summary}".lower()
                    if any(k in blob for k in excl):
                        continue
                    signals.append({
                        "source": "news",
                        "type": "news_article",
                        "title": title,
                        "description": summary,
                        "url": link,
                        "timestamp": published or datetime.utcnow(),
                        "source_name": getattr(parsed.feed, "title", "news"),
                    })
            except Exception as e:
                logger.warning(f"NewsIngester failed for {feed_url}: {e}")
        return signals