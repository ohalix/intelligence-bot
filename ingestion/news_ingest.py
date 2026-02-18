import datetime as dt
import hashlib
import logging
from typing import Any, Dict, List

import feedparser

logger = logging.getLogger(__name__)


class NewsIngester:
    """Fetch news via RSS feeds (low-rate, API-free)."""

    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    def _sources(self) -> List[str]:
        # Important: if ingestion.news_sources is present (even empty), do NOT fall back.
        ingestion_cfg = self.config.get("ingestion", {})
        if "news_sources" in ingestion_cfg:
            return list(ingestion_cfg.get("news_sources") or [])
        return list(self.config.get("news_sources") or [])

    async def ingest(self, since: dt.datetime) -> List[Dict[str, Any]]:
        sources = self._sources()
        if not sources:
            return []

        out: List[Dict[str, Any]] = []
        for url in sources:
            try:
                resp = await self.session.get(url)
                resp.raise_for_status()
                feed = feedparser.parse(resp.text)

                for entry in feed.entries:
                    published_dt = _parse_entry_datetime(entry)
                    if published_dt is None:
                        # If we can't parse a date, keep it but treat as "now" so it doesn't get stuck.
                        published_dt = dt.datetime.utcnow()

                    if published_dt < since:
                        continue

                    title = (entry.get("title") or "").strip()
                    link = (entry.get("link") or "").strip()
                    summary = (entry.get("summary") or entry.get("description") or "").strip()
                    if not title or not link:
                        continue

                    dedup_key = hashlib.sha256(link.encode("utf-8")).hexdigest()

                    out.append(
                        {
                            "dedup_key": dedup_key,
                            "source": "news",
                            "type": "news",
                            "title": title,
                            "description": summary,
                            "url": link,
                            "timestamp": published_dt.isoformat(),
                            "raw_json": "{}",
                        }
                    )

            except Exception as e:
                logger.warning("NewsIngester failed for %s: %s", url, e)

        return out


def _parse_entry_datetime(entry: Any) -> dt.datetime | None:
    # feedparser may give various date fields
    for key in ("published_parsed", "updated_parsed"):
        if key in entry and entry.get(key):
            try:
                tm = entry.get(key)
                return dt.datetime(*tm[:6])
            except Exception:
                pass

    # Try string fields
    for key in ("published", "updated"):
        v = entry.get(key)
        if not v:
            continue
        try:
            # best-effort ISO parse
            return dt.datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue

    return None
