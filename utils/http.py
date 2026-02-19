import asyncio
from typing import Any, Dict, Optional

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

class HTTPError(Exception):
    pass


class RetryableHTTPError(HTTPError):
    """HTTP error that is safe to retry (e.g., 5xx, 429)."""


class NonRetryableHTTPError(HTTPError):
    """HTTP error that should not be retried (e.g., 4xx like 403/404)."""

def make_timeout(config: Dict[str, Any]) -> aiohttp.ClientTimeout:
    seconds = int(config.get("rate_limits", {}).get("request_timeout_seconds", 15))
    return aiohttp.ClientTimeout(total=seconds)

@retry(
    reraise=True,
    # Keep retries conservative to avoid wasting requests on blocked/invalid endpoints.
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, RetryableHTTPError)),
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
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, RetryableHTTPError)),
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
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, RetryableHTTPError)),
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
