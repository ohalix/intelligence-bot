"""Minimal, polite web scraping utilities.

Design goals:
- Async, safe timeouts
- Rate limit per domain
- Small cache (24h) to avoid refetching
- Very lightweight parsing: title + a[href] anchors

This is a fallback/augment layer when RSS is missing/broken/blocked.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from utils.http import fetch_text


@dataclass
class ScrapeItem:
    title: str
    url: str


CACHE_DIR = os.path.join(".cache", "web")
DEFAULT_CACHE_TTL_SEC = 24 * 60 * 60

# Per-domain rate limiting state
_DOMAIN_LOCKS: dict[str, asyncio.Lock] = {}
_DOMAIN_LAST_TS: dict[str, float] = {}


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
    """Fetch HTML with a small cache and per-domain rate limiting."""

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

        content = await fetch_text(session, url)
        _DOMAIN_LAST_TS[domain] = time.time()

    _write_cache(url, content)
    return content


def _extract_anchors(html_text: str, base_url: str) -> List[ScrapeItem]:
    """Very small HTML anchor extractor.

    We intentionally avoid site-specific selectors and heavy dependencies.
    This is a heuristic fallback to capture *some* recent links.
    """

    items: List[ScrapeItem] = []

    # Basic scanning for <a ... href="...">Title</a>
    lower = html_text
    pos = 0
    while True:
        a = lower.find("<a", pos)
        if a == -1:
            break
        href_idx = lower.find("href=", a)
        if href_idx == -1:
            pos = a + 2
            continue
        q = lower.find('"', href_idx)
        if q == -1:
            pos = href_idx + 5
            continue
        q2 = lower.find('"', q + 1)
        if q2 == -1:
            pos = q + 1
            continue
        href = lower[q + 1 : q2].strip()

        # Extract anchor text (best effort)
        gt = lower.find(">", q2)
        if gt == -1:
            pos = q2 + 1
            continue
        end = lower.find("</a>", gt)
        if end == -1:
            pos = gt + 1
            continue
        text = lower[gt + 1 : end]

        # Strip tags and whitespace
        text = " ".join(text.replace("&nbsp;", " ").split())
        # Remove any remaining tags crudely
        while "<" in text and ">" in text:
            l = text.find("<")
            r = text.find(">", l)
            if r == -1:
                break
            text = (text[:l] + " " + text[r + 1 :]).strip()

        pos = end + 4

        if not href or href.startswith("#"):
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue

        full = urljoin(base_url, href)
        if len(text) < 8:
            continue

        items.append(ScrapeItem(title=text[:200], url=full))

    # Deduplicate by url
    seen = set()
    out: List[ScrapeItem] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _relevance_filter(items: List[ScrapeItem], base_url: str) -> List[ScrapeItem]:
    """Keep likely 'post' links and same-domain links."""
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

    # Return as raw signals (title/url)
    out: List[Dict[str, Any]] = []
    for it in anchors[:max_items]:
        out.append({"title": it.title, "url": it.url})
    return out
