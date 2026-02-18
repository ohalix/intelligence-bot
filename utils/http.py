import asyncio
from typing import Any, Dict, Optional

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

class HTTPError(Exception):
    pass

def make_timeout(config: Dict[str, Any]) -> aiohttp.ClientTimeout:
    seconds = int(config.get("rate_limits", {}).get("request_timeout_seconds", 15))
    return aiohttp.ClientTimeout(total=seconds)

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, HTTPError)),
)
async def fetch_json(session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str,str]]=None, params: Optional[Dict[str,Any]]=None) -> Any:
    timeout = getattr(session, "timeout", None)
    async with session.get(url, headers=headers, params=params, timeout=timeout) as r:
        if r.status >= 500:
            raise HTTPError(f"Server error {r.status}")
        if r.status == 429:
            raise HTTPError("Rate limited (429)")
        if r.status >= 400:
            text = await r.text()
            raise HTTPError(f"HTTP {r.status}: {text[:200]}")
        return await r.json()

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, HTTPError)),
)
async def fetch_text(session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str,str]]=None, params: Optional[Dict[str,Any]]=None) -> str:
    timeout = getattr(session, "timeout", None)
    async with session.get(url, headers=headers, params=params, timeout=timeout) as r:
        if r.status >= 500:
            raise HTTPError(f"Server error {r.status}")
        if r.status == 429:
            raise HTTPError("Rate limited (429)")
        if r.status >= 400:
            text = await r.text()
            raise HTTPError(f"HTTP {r.status}: {text[:200]}")
        return await r.text()
