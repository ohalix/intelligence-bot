import asyncio
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import ClientConnectorDNSError
from tenacity import retry, stop_after_attempt, wait_exponential_jitter


class HTTPError(Exception):
    """Base HTTP error."""


class RetryableHTTPError(HTTPError):
    """Safe to retry (e.g., 429, 5xx)."""


class NonRetryableHTTPError(HTTPError):
    """Should not be retried (e.g., 400/403/404)."""

def make_timeout(config: Dict[str, Any]) -> aiohttp.ClientTimeout:
    seconds = int(config.get("rate_limits", {}).get("request_timeout_seconds", 15))
    return aiohttp.ClientTimeout(total=seconds)


def _should_retry(exc: BaseException) -> bool:
    """Retry policy tuned for ingestion reliability.

    - Do NOT retry: 400/403/404 (invalid endpoint / blocked), DNS failures
    - Retry: 429 and 5xx, timeouts, transient aiohttp client errors
    """
    if isinstance(exc, NonRetryableHTTPError):
        return False
    if isinstance(exc, ClientConnectorDNSError):
        return False
    if isinstance(exc, RetryableHTTPError):
        return True
    if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientError)):
        return True
    return False

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=_should_retry,
)
async def fetch_json(session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str,str]]=None, params: Optional[Dict[str,Any]]=None) -> Any:
    timeout = getattr(session, "timeout", None)
    async with session.get(url, headers=headers, params=params, timeout=timeout) as r:
        if r.status >= 500:
            raise RetryableHTTPError(f"Server error {r.status}")
        if r.status == 429:
            raise RetryableHTTPError("Rate limited (429)")
        if r.status >= 400:
            text = await r.text()
            raise NonRetryableHTTPError(f"HTTP {r.status}: {text[:200]}")
        return await r.json()

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=_should_retry,
)
async def fetch_text(session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str,str]]=None, params: Optional[Dict[str,Any]]=None) -> str:
    timeout = getattr(session, "timeout", None)
    async with session.get(url, headers=headers, params=params, timeout=timeout) as r:
        if r.status >= 500:
            raise RetryableHTTPError(f"Server error {r.status}")
        if r.status == 429:
            raise RetryableHTTPError("Rate limited (429)")
        if r.status >= 400:
            text = await r.text()
            raise NonRetryableHTTPError(f"HTTP {r.status}: {text[:200]}")
        return await r.text()


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=_should_retry,
)
async def fetch_json_post(
    session: aiohttp.ClientSession,
    url: str,
    json_payload: Any,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    timeout = getattr(session, "timeout", None)
    async with session.post(url, headers=headers, params=params, json=json_payload, timeout=timeout) as r:
        if r.status >= 500:
            raise RetryableHTTPError(f"Server error {r.status}")
        if r.status == 429:
            raise RetryableHTTPError("Rate limited (429)")
        if r.status >= 400:
            text = await r.text()
            raise NonRetryableHTTPError(f"HTTP {r.status}: {text[:200]}")
        return await r.json()


# -------------------------
# RSS date parsing helpers (items 5, 12)
# -------------------------

from datetime import timezone as _tz
from email.utils import parsedate_to_datetime as _parsedate_to_datetime


def parse_rss_entry_datetime(entry) -> "datetime | None":
    """Parse RSS entry published/updated time into a timezone-aware UTC datetime.

    FIX item 5: Use email.utils.parsedate_to_datetime which handles RFC 2822 dates
    correctly, including timezone offsets. Normalize to UTC-aware so comparisons
    against naive UTC 'since' datetimes work consistently.

    Returns UTC-naive datetime (tzinfo=None) for backward-compat with pipeline 'since'.
    """
    from datetime import datetime
    raw = (
        getattr(entry, "published_parsed", None)
        or getattr(entry, "updated_parsed", None)
    )
    if raw:
        # time.struct_time — treat as UTC (feedparser normalizes to UTC)
        try:
            return datetime(*raw[:6], tzinfo=None)
        except Exception:
            pass

    # Try string-based fallback with parsedate_to_datetime for TZ-aware parsing
    raw_str = (
        getattr(entry, "published", None)
        or getattr(entry, "updated", None)
        or ""
    )
    if raw_str:
        try:
            dt = _parsedate_to_datetime(raw_str)
            # Normalize to UTC-naive
            return dt.astimezone(_tz.utc).replace(tzinfo=None)
        except Exception:
            pass
    return None


# -------------------------
# Conditional RSS fetching (item 15) + UA (item 14) + Retry-After (item 19)
# -------------------------

DEFAULT_BOT_UA = "Mozilla/5.0 (compatible; IntelBot/1.0; +https://github.com/intel-bot)"


async def fetch_rss_conditional(
    session,
    url: str,
    store=None,
    *,
    extra_headers: Optional[Dict[str, str]] = None,
) -> tuple:
    """Fetch RSS with ETag/Last-Modified conditional support.

    FIX item 15: Store and reuse ETag/Last-Modified per feed.
    FIX item 14: Always send a User-Agent.
    FIX item 19: Parse Retry-After header on 429.

    Returns (content: str, not_modified: bool).
    """
    headers: Dict[str, str] = {
        "User-Agent": DEFAULT_BOT_UA,
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
    }
    if extra_headers:
        headers.update(extra_headers)

    etag, last_modified = None, None
    if store is not None:
        try:
            etag, last_modified = store.get_feed_cache(url)
        except Exception:
            pass

    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    async with session.get(url, headers=headers) as resp:
        if resp.status == 304:
            return "", True  # not modified

        if resp.status == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = 60
            if retry_after:
                try:
                    wait = max(int(retry_after), 10)
                except Exception:
                    pass
            raise RetryableHTTPError(f"Rate limited (429), Retry-After={retry_after or 'none'}, suggested_wait={wait}s")

        if resp.status == 403:
            text_preview = (await resp.text())[:200]
            # Categorize: plan restriction vs blocked (item 18)
            if "plan" in text_preview.lower() or "subscription" in text_preview.lower():
                raise NonRetryableHTTPError(f"HTTP 403 plan-restricted: {text_preview[:100]}")
            raise NonRetryableHTTPError(f"HTTP 403 blocked: {text_preview[:100]}")

        if resp.status >= 500:
            raise RetryableHTTPError(f"Server error {resp.status}")

        if resp.status >= 400:
            text = await resp.text()
            raise NonRetryableHTTPError(f"HTTP {resp.status}: {text[:200]}")

        content = await resp.text()

        # Save ETag/Last-Modified for next call
        if store is not None:
            new_etag = resp.headers.get("ETag")
            new_lm = resp.headers.get("Last-Modified")
            if new_etag or new_lm:
                try:
                    store.set_feed_cache(url, new_etag, new_lm)
                except Exception:
                    pass

        return content, False
