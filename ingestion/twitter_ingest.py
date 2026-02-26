import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_ingest import BaseIngester
from utils.http import fetch_json, fetch_text

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
                logger.warning("TwitterIngester: TWITTER_MODE=rss but TWITTER_RSS_SOURCES not set. Skipping.")
                return []
            signals: List[Dict[str, Any]] = []
            for url in urls:
                try:
                    xml = await fetch_text(self.session, url)
                    parsed = feedparser.parse(xml)
                    for entry in parsed.entries:
                        published = None
                        if getattr(entry, "published_parsed", None):
                            published = datetime(*entry.published_parsed[:6])
                        if published and published <= since:
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
                    logger.warning(f"Twitter RSS failed for {url}: {e}")
            return signals

        # mode == api
        bearer = self.config.get("keys", {}).get("twitter_bearer")
        if not bearer:
            logger.warning("TwitterIngester: TWITTER_MODE=api but TWITTER_BEARER_TOKEN not set. Skipping.")
            return []
        query = self.config.get("ingestion", {}).get("twitter_query") or "web3 (launch OR mainnet OR grant OR partnership) -is:retweet lang:en"
        url = "https://api.x.com/2/tweets/search/recent"
        headers = {"Authorization": f"Bearer {bearer}"}
        params = {"query": query, "max_results": 20, "tweet.fields": "created_at,public_metrics,author_id"}
        try:
            data = await fetch_json(self.session, url, headers=headers, params=params)
        except Exception as e:
            logger.warning(f"TwitterIngester API failed: {e}")
            return []

        out: List[Dict[str, Any]] = []
        for t in (data.get("data") or []):
            created_at = t.get("created_at","")
            try:
                ts = datetime.fromisoformat(created_at.replace("Z","+00:00")).replace(tzinfo=None)
            except Exception:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
            if ts <= since:
                continue
            tid = t.get("id","")
            metrics = t.get("public_metrics") or {}
            out.append({
                "source": "twitter",
                "type": "tweet",
                "title": (t.get("text","")[:80] + "…") if len(t.get("text","")) > 80 else t.get("text",""),
                "description": t.get("text",""),
                "url": f"https://x.com/i/web/status/{tid}" if tid else "",
                "timestamp": ts,
                "tweet_id": tid,
                "likes": metrics.get("like_count", 0),
                "retweets": metrics.get("retweet_count", 0),
                "replies": metrics.get("reply_count", 0),
                "source_name": "X API",
            })
        return out
