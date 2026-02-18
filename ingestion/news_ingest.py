import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import feedparser

from utils.http import fetch_text

logger = logging.getLogger(__name__)

class NewsIngester:
    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session
        ing = (config or {}).get("ingestion", {})
        self.urls: List[str] = ing.get("news_sources") or config.get("news_rss", []) or []

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        since_ts = since.astimezone(timezone.utc).timestamp()
        attempted = len(self.urls)
        ok = 0
        fail = 0
        exc_types: Dict[str, int] = {}
        out: List[Dict[str, Any]] = []

        for url in self.urls:
            try:
                content = await fetch_text(self.session, url)
                feed = feedparser.parse(content)

                for e in feed.entries:
                    # published_parsed may be missing; fall back to now for ordering/filtering
                    published = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
                    ts = datetime.now(timezone.utc).timestamp()
                    if published:
                        ts = datetime(*published[:6], tzinfo=timezone.utc).timestamp()
                    if ts < since_ts:
                        continue

                    out.append(
                        {
                            "source": "news",
                            "title": getattr(e, "title", "") or "",
                            "url": getattr(e, "link", "") or "",
                            "summary": getattr(e, "summary", "") or "",
                            "created_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                            "raw": {"feed_url": url},
                        }
                    )

                ok += 1
            except Exception as ex:
                fail += 1
                exc_types[type(ex).__name__] = exc_types.get(type(ex).__name__, 0) + 1
                logger.warning("NewsIngester failed for %s: %s", url, ex)

        logger.info(
            "NewsIngester: sources=%d ok=%d fail=%d items=%d top_exceptions=%s",
            attempted, ok, fail, len(out),
            dict(sorted(exc_types.items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )
        return out
