import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import feedparser

logger = logging.getLogger(__name__)

def _hash_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]

class EcosystemIngester:
    """RSS/blog feeds from ecosystems (L2s, Solana, Bitcoin L2s, etc.)."""

    def __init__(self, sources: List[str]):
        self.sources = [s.strip() for s in (sources or []) if s and s.strip()]

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        if not self.sources:
            return []
        since_ts = since.astimezone(timezone.utc)
        out: List[Dict[str, Any]] = []

        for url in self.sources:
            try:
                feed = feedparser.parse(url)
                for e in feed.entries:
                    published = None
                    if getattr(e, "published_parsed", None):
                        published = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                    elif getattr(e, "updated_parsed", None):
                        published = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)

                    if published and published < since_ts:
                        continue

                    title = (getattr(e, "title", "") or "").strip()
                    link = (getattr(e, "link", "") or "").strip()
                    summary = (getattr(e, "summary", "") or "").strip()

                    if not link or not title:
                        continue

                    out.append({
                        "id": _hash_id(link),
                        "created_at": (published.isoformat() if published else datetime.now(timezone.utc).isoformat()),
                        "source": "ecosystem",
                        "title": title,
                        "url": link,
                        "summary": summary[:5000],
                        "metadata": {"feed": url},
                    })
            except Exception as ex:
                logger.warning("EcosystemIngester failed for %s: %s", url, ex)

        return out
