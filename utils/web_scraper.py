"""Minimal, polite web scraping utilities with BeautifulSoup-backed parsing.

Design goals:
- Async, safe timeouts (reuse utils.http fetch client)
- Rate limit per domain
- Small cache (24h) to avoid refetching
- Compatibility-first return shape for ingestion callers
- Better HTML parsing/extraction quality via BeautifulSoup

This remains a fallback/augment layer when RSS is missing/broken/blocked.
"""

from __future__ import annotations

import asyncio
import hashlib
import html as html_lib
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from utils.http import fetch_text


@dataclass
class ScrapeItem:
    title: str
    url: str
    snippet: str = ""


CACHE_DIR = os.path.join(".cache", "web")
DEFAULT_CACHE_TTL_SEC = 24 * 60 * 60
MAX_SNIPPET_LEN = 240

# Per-domain rate limiting state
_DOMAIN_LOCKS: dict[str, asyncio.Lock] = {}
_DOMAIN_LAST_TS: dict[str, float] = {}

# Known tracking params safe to strip for dedup-friendly URLs.
_TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "source",
}


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

        headers = {
            # Keep UA simple and honest; no bypass intent.
            "User-Agent": "Mozilla/5.0 (compatible; IntelBot/1.0; +https://example.com/bot)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        content = await fetch_text(session, url, headers=headers)
        _DOMAIN_LAST_TS[domain] = time.time()

    _write_cache(url, content)
    return content


def _normalize_url(url: str, base_url: str) -> str:
    full = urljoin(base_url, url or "")
    parsed = urlparse(full)
    if not parsed.scheme or not parsed.netloc:
        return full
    q = [(k, v) for (k, v) in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in _TRACKING_PARAMS]
    normalized = parsed._replace(fragment="", query=urlencode(q, doseq=True))
    return urlunparse(normalized)


def _clean_text(value: str) -> str:
    if not value:
        return ""
    value = html_lib.unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _is_probably_blocked_page(html_text: str) -> bool:
    head = (html_text or "")[:5000].lower()
    return (
        "just a moment" in head
        or "cf-browser-verification" in head
        or "/cdn-cgi/" in head
        or "captcha" in head and "cloudflare" in head
    )


def _extract_page_metadata(html_text: str, base_url: str) -> Dict[str, str]:
    """Best-effort page metadata extraction.

    Compatibility helper for current and future ingesters. Not all callers use every field,
    but this centralizes high-quality extraction.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    meta: Dict[str, str] = {
        "title": "",
        "canonical_url": "",
        "published_at": "",
        "site_name": "",
        "description": "",
    }

    if soup.title and soup.title.string:
        meta["title"] = _clean_text(soup.title.string)

    # Meta tags (OpenGraph / standard)
    for m in soup.find_all("meta"):
        key = (m.get("property") or m.get("name") or "").strip().lower()
        content = _clean_text(m.get("content") or "")
        if not content:
            continue
        if key in {"og:title", "twitter:title", "title"} and not meta["title"]:
            meta["title"] = content
        elif key in {"description", "og:description", "twitter:description"} and not meta["description"]:
            meta["description"] = content
        elif key in {"og:site_name", "application-name"} and not meta["site_name"]:
            meta["site_name"] = content
        elif key in {
            "article:published_time",
            "publishdate",
            "pubdate",
            "date",
            "dc.date",
            "dc.date.issued",
        } and not meta["published_at"]:
            meta["published_at"] = content
        elif key in {"og:url"} and not meta["canonical_url"]:
            meta["canonical_url"] = _normalize_url(content, base_url)

    link_canonical = soup.find("link", rel=lambda v: v and "canonical" in str(v).lower())
    if link_canonical and link_canonical.get("href") and not meta["canonical_url"]:
        meta["canonical_url"] = _normalize_url(link_canonical.get("href"), base_url)

    time_tag = soup.find("time")
    if time_tag and not meta["published_at"]:
        meta["published_at"] = _clean_text(time_tag.get("datetime") or time_tag.get_text(" ", strip=True))

    # Lightweight excerpt from first non-trivial paragraph/article content.
    if not meta["description"]:
        for sel in ["article p", "main p", "p"]:
            p = soup.select_one(sel)
            if p:
                txt = _clean_text(p.get_text(" ", strip=True))
                if len(txt) >= 40:
                    meta["description"] = txt[:MAX_SNIPPET_LEN]
                    break

    return meta


def _anchor_text(a) -> str:
    return _clean_text(a.get_text(" ", strip=True))


def _extract_anchors_bs4(html_text: str, base_url: str) -> List[ScrapeItem]:
    soup = BeautifulSoup(html_text, "html.parser")

    # Remove obvious noise nodes before text extraction.
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()

    items: List[ScrapeItem] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        if href.startswith("mailto:") or href.startswith("javascript:") or href.startswith("tel:"):
            continue

        title = _anchor_text(a)
        if len(title) < 8:
            continue

        full = _normalize_url(href, base_url)
        if not full or full in seen:
            continue

        # Optional local snippet from nearby DOM context.
        snippet = ""
        parent = a.find_parent(["article", "li", "div", "section"])
        if parent is not None:
            for p in parent.find_all(["p", "span"], limit=3):
                txt = _clean_text(p.get_text(" ", strip=True))
                if len(txt) >= 30 and txt != title:
                    snippet = txt[:MAX_SNIPPET_LEN]
                    break

        seen.add(full)
        items.append(ScrapeItem(title=title[:200], url=full, snippet=snippet))

    return items


def _relevance_filter(items: List[ScrapeItem], base_url: str) -> List[ScrapeItem]:
    """Keep likely post links and same-domain links; degrade gracefully if nothing matches."""
    base_netloc = urlparse(base_url).netloc
    out: List[ScrapeItem] = []
    for it in items:
        u = urlparse(it.url)
        if u.netloc and u.netloc != base_netloc:
            continue
        path = u.path.lower()
        if any(k in path for k in ["blog", "post", "posts", "updates", "news", "announc", "grants", "article"]):
            out.append(it)
    return out or items


async def scrape_page_links(
    session,
    url: str,
    *,
    max_items: int = 10,
    cache_ttl_sec: int = DEFAULT_CACHE_TTL_SEC,
) -> List[Dict[str, Any]]:
    """Scrape a page for candidate post links.

    Compatibility return shape remains a list[dict] with at least title/url keys.
    Extra keys are included on best-effort basis but are optional for callers.
    """

    html_text = await fetch_cached_html(session, url, cache_ttl_sec=cache_ttl_sec)
    if not html_text or not isinstance(html_text, str):
        return []

    sample = html_text.lstrip()[:200].lower()
    if sample.startswith("<?xml") or "<rss" in sample or "<feed" in sample:
        return []

    if _is_probably_blocked_page(html_text):
        raise RuntimeError("Blocked/anti-bot page detected")

    page_meta = _extract_page_metadata(html_text, url)
    anchors = _extract_anchors_bs4(html_text, url)
    anchors = _relevance_filter(anchors, url)

    out: List[Dict[str, Any]] = []
    for it in anchors[:max_items]:
        out.append(
            {
                "title": it.title,
                "url": it.url,
                "description": it.snippet,
                "page_title": page_meta.get("title", ""),
                "page_site_name": page_meta.get("site_name", ""),
                "page_published_at": page_meta.get("published_at", ""),
            }
        )
    return out
