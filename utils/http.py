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
