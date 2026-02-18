import logging
import random
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

from .base_ingest import BaseIngester
from utils.http import fetch_json, fetch_text

import aiohttp

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

            # Optional fallback Nitter instances. If provided, we will try swapping the base URL.
            # Example: NITTER_INSTANCES="https://nitter.net,https://nitter.poast.org"
            nitter_instances: List[str] = self.config.get("ingestion", {}).get("nitter_instances", [])
            nitter_instances = [s.rstrip("/") for s in nitter_instances if s.strip()]

            # Server-friendly headers. Some public instances drop connections without a UA.
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; Web3IntelBot/1.0)",
                "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
            }

            warned: set[str] = set()  # one warning per source per run

            def _candidates(original: str) -> List[str]:
                cands = [original]
                if not nitter_instances:
                    return cands
                try:
                    p = urlparse(original)
                    if "nitter" not in p.netloc:
                        return cands
                    for base in nitter_instances:
                        bp = urlparse(base)
                        swapped = p._replace(scheme=bp.scheme or p.scheme, netloc=bp.netloc)
                        u2 = urlunparse(swapped)
                        if u2 not in cands:
                            cands.append(u2)
                except Exception:
                    return cands
                return cands

            async def _fetch_rss(url: str) -> str:
                """Fetch RSS content with limited retries/backoff.

                Keep retries low to avoid hammering unstable Nitter instances.
                """
                timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_connect=10, sock_read=15)
                last: Optional[BaseException] = None
                for attempt in range(3):
                    try:
                        async with self.session.get(url, headers=headers, timeout=timeout, allow_redirects=True) as resp:
                            text = await resp.text(errors="ignore")
                            if resp.status >= 400:
                                raise RuntimeError(f"HTTP {resp.status}")
                            return text
                    except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError, aiohttp.ClientOSError, asyncio.TimeoutError) as e:
                        last = e
                        if attempt < 2:
                            delay = (2 ** attempt) + random.random() * 0.4
                            await asyncio.sleep(delay)
                            continue
                        raise
                    except Exception as e:
                        last = e
                        raise
                raise RuntimeError(f"RSS fetch failed: {last}")

            signals: List[Dict[str, Any]] = []
            failures = 0
            for src in urls:
                xml: Optional[str] = None
                last_err: Optional[BaseException] = None
                for cand in _candidates(src):
                    try:
                        xml = await _fetch_rss(cand)
                        break
                    except Exception as e:
                        last_err = e
                        continue

                if xml is None:
                    failures += 1
                    if src not in warned:
                        warned.add(src)
                        logger.warning("Twitter RSS failed for %s: %s", src, last_err)
                    continue

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
                        "timestamp": published or datetime.utcnow(),
                        "source_name": getattr(parsed.feed, "title", "twitter"),
                    })

            if failures == len(urls):
                # One summary line instead of a wall of repeated warnings.
                logger.warning("TwitterIngester: all RSS sources failed this run (Nitter may be down or blocking).")

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
                ts = datetime.utcnow()
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
