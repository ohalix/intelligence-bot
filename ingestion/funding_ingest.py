import datetime as dt
import hashlib
import logging
from typing import Any, Dict, List

import feedparser

logger = logging.getLogger(__name__)


class FundingIngester:
    """Funding-related signals via RSS feeds."""

    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    def _sources(self) -> List[str]:
        ingestion_cfg = self.config.get("ingestion", {})
        if "funding_sources" in ingestion_cfg:
            return list(ingestion_cfg.get("funding_sources") or [])
        return list(self.config.get("funding_sources") or [])

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
                    published_dt = _parse_entry_datetime(entry) or dt.datetime.utcnow()
                    if published_dt < since:
                        continue

                    title = (entry.get("title") or "").strip()
                    link = (entry.get("link") or "").strip()
                    summary = (entry.get("summary") or entry.get("description") or "").strip()
                    if not title or not link:
                        continue

                    # crude filter to keep this feed about funding/ecosystem
                    text = f"{title} {summary}".lower()
                    if not any(k in text for k in ("raise", "raised", "funding", "seed", "series", "round", "grant")):
                        continue

                    dedup_key = hashlib.sha256(link.encode("utf-8")).hexdigest()

                    out.append(
                        {
                            "dedup_key": dedup_key,
                            "source": "funding",
                            "type": "funding",
                            "title": title,
                            "description": summary,
                            "url": link,
                            "timestamp": published_dt.isoformat(),
                            "raw_json": "{}",
                        }
                    )

            except Exception as e:
                logger.warning("FundingIngester failed for %s: %s", url, e)

        return out


def _parse_entry_datetime(entry: Any) -> dt.datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        if key in entry and entry.get(key):
            try:
                tm = entry.get(key)
                return dt.datetime(*tm[:6])
            except Exception:
                pass

    for key in ("published", "updated"):
        v = entry.get(key)
        if not v:
            continue
        try:
            return dt.datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue

    return None
