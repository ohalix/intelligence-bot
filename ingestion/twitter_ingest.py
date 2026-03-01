"""Twitter/X ingestion: RSS mode or API mode.

Step 7:
- RSS mode: replaced fetch_text() with fetch_rss_conditional() for User-Agent,
  ETag/304, and 429 handling.
- RSS mode: replaced datetime(*entry.published_parsed[:6]) with
  parse_rss_entry_datetime(entry) for consistent RFC-2822 parsing.
- RSS mode: added bozo detection.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_ingest import BaseIngester
from utils.http import fetch_json, fetch_rss_conditional, parse_rss_entry_datetime

logger = logging.getLogger(__name__)


class TwitterIngester(BaseIngester):
    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        mode = (self.config.get("ingestion", {}).get("twitter_mode") or "none").lower()
        if mode == "none":
            return []

        if mode == "rss":
            # Requires TWITTER_RSS_SOURCES to be explicitly supplied (third-party).
            import feedparser
            urls = self.config.get("ingestion", {}).get("twitter_rss_sources") or []
            if not urls:
                logger.warning(
                    "TwitterIngester: TWITTER_MODE=rss but TWITTER_RSS_SOURCES not set. Skipping."
                )
                return []
            signals: List[Dict[str, Any]] = []
            store = getattr(self, "_store", None)
            for url in urls:
                try:
                    # Step 7: Use fetch_rss_conditional (adds User-Agent, ETag/304, 429 handling)
                    xml, not_modified = await fetch_rss_conditional(
                        self.session, url, store=store
                    )
                    if not_modified:
                        logger.debug("TwitterIngester RSS 304 Not Modified: %s", url)
                        continue
                    parsed = feedparser.parse(xml)
                    # Step 7: bozo detection
                    if getattr(parsed, "bozo", False):
                        exc = getattr(parsed, "bozo_exception", None)
                        logger.warning(
                            "TwitterIngester RSS bozo=True for %s: %s",
                            url, type(exc).__name__ if exc else "unknown",
                        )
                    for entry in parsed.entries:
                        # Step 7: Use parse_rss_entry_datetime for consistent RFC-2822 parsing
                        published = parse_rss_entry_datetime(entry)
                        if published is not None and published <= since:
                            continue
                        title = getattr(entry, "title", "") or ""
                        link = getattr(entry, "link", "") or ""
                        signals.append({
                            "source": "twitter",
                            "type": "tweet",
                            "title": title,
                            "description": getattr(entry, "summary", "") or "",
                            "url": link,
                            "timestamp": published or datetime.now(timezone.utc).replace(tzinfo=None),
                            "source_name": getattr(parsed.feed, "title", "twitter"),
                        })
                except Exception as e:
                    logger.warning("Twitter RSS failed for %s: %s", url, e)
            return signals

        # mode == api
        bearer = self.config.get("keys", {}).get("twitter_bearer")
        if not bearer:
            logger.warning(
                "TwitterIngester: TWITTER_MODE=api but TWITTER_BEARER_TOKEN not set. Skipping."
            )
            return []
        query = (
            self.config.get("ingestion", {}).get("twitter_query")
            or "web3 (launch OR mainnet OR grant OR partnership) -is:retweet lang:en"
        )
        url = "https://api.x.com/2/tweets/search/recent"
        headers = {"Authorization": f"Bearer {bearer}"}
        params = {
            "query": query,
            "max_results": 20,
            "tweet.fields": "created_at,public_metrics,author_id",
        }
        try:
            data = await fetch_json(self.session, url, headers=headers, params=params)
        except Exception as e:
            logger.warning("TwitterIngester API failed: %s", e)
            return []

        out: List[Dict[str, Any]] = []
        for t in (data.get("data") or []):
            created_at = t.get("created_at", "")
            try:
                ts = datetime.fromisoformat(created_at.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
            if ts <= since:
                continue
            tid = t.get("id", "")
            metrics = t.get("public_metrics") or {}
            out.append({
                "source": "twitter",
                "type": "tweet",
                "title": (t.get("text", "")[:80] + "…") if len(t.get("text", "")) > 80 else t.get("text", ""),
                "description": t.get("text", ""),
                "url": f"https://x.com/i/web/status/{tid}" if tid else "",
                "timestamp": ts,
                "tweet_id": tid,
                "likes": metrics.get("like_count", 0),
                "retweets": metrics.get("retweet_count", 0),
                "replies": metrics.get("reply_count", 0),
                "source_name": "X API",
            })
        return out
