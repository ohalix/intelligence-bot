"""Polite web scraping utilities.

FIX item 10: Use BeautifulSoup for anchor extraction instead of regex-based
lowercased HTML scan. This correctly handles single/double/unquoted quotes and
preserves URL case (never lowercases href values).

Design principles:
- Async, safe timeouts
- Per-domain rate limiting
- 24h cache to avoid refetching
- BeautifulSoup for robust anchor extraction
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from utils.http import fetch_text

logger = logging.getLogger(__name__)

@dataclass
class ScrapeItem:
    title: str
    url: str


CACHE_DIR = os.path.join(".cache", "web")
DEFAULT_CACHE_TTL_SEC = 24 * 60 * 60

_DOMAIN_LOCKS: dict[str, asyncio.Lock] = {}
_DOMAIN_LAST_TS: dict[str, float] = {}

DEFAULT_BOT_UA = "Mozilla/5.0 (compatible; IntelBot/1.0; +https://github.com/intel-bot)"


def _cache_path(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.json")


def _read_cache(url: str, ttl_sec: int) -> Optional[str]:
    try:
        p = _cache_path(url)
        if not os.path.exists(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if time.time() - float(data.get("ts", 0)) > ttl_sec:
            return None
        return data.get("content", "")
    except Exception:
        return None


def _write_cache(url: str, content: str) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    p = _cache_path(url)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"ts": time.time(), "content": content}, f)
    os.replace(tmp, p)


async def fetch_cached_html(
    session,
    url: str,
    *,
    cache_ttl_sec: int = DEFAULT_CACHE_TTL_SEC,
    min_delay_sec: float = 1.0,
) -> str:
    """Fetch HTML with cache and per-domain rate limiting."""
    cached = _read_cache(url, cache_ttl_sec)
    if cached is not None:
        return cached

    domain = urlparse(url).netloc
    lock = _DOMAIN_LOCKS.setdefault(domain, asyncio.Lock())

    async with lock:
        last = _DOMAIN_LAST_TS.get(domain, 0.0)
        now = time.time()
        sleep_for = max(0.0, min_delay_sec - (now - last))
        if sleep_for:
            await asyncio.sleep(sleep_for)

        # FIX item 14: always include User-Agent
        headers = {
            "User-Agent": DEFAULT_BOT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        content = await fetch_text(session, url, headers=headers)
        _DOMAIN_LAST_TS[domain] = time.time()

    _write_cache(url, content)
    return content


def _extract_anchors(html_text: str, base_url: str) -> List[ScrapeItem]:
    """Extract anchor links using BeautifulSoup.

    FIX item 10: Never lowercase the HTML before extraction (was corrupting URLs).
    BeautifulSoup handles all quote styles (single, double, none) natively.
    """
    try:
        soup = BeautifulSoup(html_text, "html.parser")
    except Exception as exc:
        logger.debug("BeautifulSoup parse error for %s: %s", base_url, exc)
        return []

    items: List[ScrapeItem] = []
    seen: set[str] = set()

    for tag in soup.find_all("a", href=True):
        # href is preserved as-is (not lowercased)
        href = tag.get("href", "").strip()
        if not href or href.startswith("#"):
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue

        full_url = urljoin(base_url, href)

        # Get visible text
        text = tag.get_text(separator=" ", strip=True)
        text = " ".join(text.split())

        if len(text) < 8:
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        items.append(ScrapeItem(title=text[:200], url=full_url))

    return items


def _relevance_filter(items: List[ScrapeItem], base_url: str) -> List[ScrapeItem]:
    """Keep likely 'post' links on same domain."""
    base_netloc = urlparse(base_url).netloc
    out: List[ScrapeItem] = []
    for it in items:
        u = urlparse(it.url)
        if u.netloc and u.netloc != base_netloc:
            continue
        path = u.path.lower()
        if any(k in path for k in ["blog", "post", "posts", "updates", "news", "announc", "grants"]):
            out.append(it)
    return out or items


async def scrape_page_links(
    session,
    url: str,
    *,
    max_items: int = 10,
    cache_ttl_sec: int = DEFAULT_CACHE_TTL_SEC,
) -> List[Dict[str, Any]]:
    """Scrape a page for candidate post links."""
    html_text = await fetch_cached_html(session, url, cache_ttl_sec=cache_ttl_sec)
    anchors = _extract_anchors(html_text, url)
    anchors = _relevance_filter(anchors, url)
    out: List[Dict[str, Any]] = []
    for it in anchors[:max_items]:
        out.append({"title": it.title, "url": it.url})
    return out
