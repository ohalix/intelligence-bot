import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

from utils.http import fetch_json, fetch_json_post

logger = logging.getLogger(__name__)


def _iso_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    try:
        if isinstance(v, (int, float)):
            return datetime.utcfromtimestamp(float(v)).isoformat() + "Z"
    except Exception:
        return None
    return None


async def news_from_cryptocurrency_cv(session: aiohttp.ClientSession, since: datetime) -> List[Dict[str, Any]]:
    """Public no-key crypto news.

    API docs: https://cryptocurrency.cv/api
    """
    url = "https://cryptocurrency.cv/api/news"
    try:
        data = await fetch_json(session, url)
    except Exception as e:
        logger.warning("cryptocurrency.cv fetch failed: %s", e)
        return []

    # The API may return either a list of articles OR an object containing an
    # "articles" list (as seen in production logs). Guard against unexpected
    # schemas (including string payloads).
    if isinstance(data, str):
        logger.warning("cryptocurrency.cv returned unexpected payload type=str; skipping")
        return []
    if isinstance(data, dict):
        data = data.get("articles") or data.get("data") or data.get("results") or []
    if not isinstance(data, list):
        logger.warning(
            "cryptocurrency.cv returned unexpected payload type=%s; skipping",
            type(data).__name__,
        )
        return []

    items: List[Dict[str, Any]] = []
    for it in (data or []):
        if not isinstance(it, dict):
            continue
        # Observed fields: title, url, source, created_at/updated_at (varies)
        published_at = _iso_or_none(it.get("published_at")) or _iso_or_none(it.get("created_at")) or _iso_or_none(it.get("updated_at"))
        if not published_at:
            continue
        try:
            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue
        if dt < since:
            continue
        items.append(
            {
                "source": "news",
                "source_id": "cryptocurrency_cv",
                "title": (it.get("title") or "").strip(),
                "url": (it.get("url") or "").strip(),
                "description": (it.get("description") or it.get("body") or "").strip(),
                "published_at": published_at,
            }
        )
    return items


async def news_from_coinmarketcap_posts_latest(session: aiohttp.ClientSession, since: datetime, api_key: Optional[str]) -> List[Dict[str, Any]]:
    """CoinMarketCap latest posts API (requires free API key).

    Endpoint: https://pro-api.coinmarketcap.com/v1/content/posts/latest
    """
    if not api_key:
        logger.info("CoinMarketCap API disabled (missing COINMARKETCAP_API_KEY)")
        return []
    url = "https://pro-api.coinmarketcap.com/v1/content/posts/latest"
    headers = {"X-CMC_PRO_API_KEY": api_key}
    try:
        data = await fetch_json(session, url, headers=headers)
    except Exception as e:
        logger.warning("CoinMarketCap fetch failed: %s", e)
        return []

    posts = (data or {}).get("data") or []
    items: List[Dict[str, Any]] = []
    for p in posts:
        published_at = _iso_or_none(p.get("created_at")) or _iso_or_none(p.get("released_at"))
        if not published_at:
            continue
        try:
            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue
        if dt < since:
            continue

        urlp = p.get("url") or p.get("source_url") or ""
        items.append(
            {
                "source": "news",
                "source_id": "coinmarketcap_posts_latest",
                "title": (p.get("title") or "").strip(),
                "url": str(urlp).strip(),
                "description": (p.get("subtitle") or p.get("meta") or "").strip(),
                "published_at": published_at,
            }
        )
    return items


async def funding_from_defillama_raises(session: aiohttp.ClientSession, since: datetime) -> List[Dict[str, Any]]:
    """DefiLlama raises endpoint (no key required)."""
    # Use the public open API endpoint. The pro-api path in older configs can
    # return a 404 with a router message.
    url = "https://api.llama.fi/raises"
    try:
        data = await fetch_json(session, url)
    except Exception as e:
        logger.warning("DefiLlama raises fetch failed: %s", e)
        return []

    if isinstance(data, dict):
        raises = data.get("raises") or data.get("data") or data.get("results") or []
    else:
        raises = data or []
    items: List[Dict[str, Any]] = []
    for r in raises:
        # fields vary; commonly include date or announcedAt
        published_at = _iso_or_none(r.get("date")) or _iso_or_none(r.get("announcedAt"))
        if not published_at:
            continue
        # dates can be YYYY-MM-DD
        try:
            if len(published_at) == 10:
                dt = datetime.fromisoformat(published_at)
            else:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue
        if dt < since:
            continue

        name = (r.get("name") or r.get("project") or "").strip()
        round_ = (r.get("round") or "").strip()
        amount = r.get("amount") or r.get("amountUsd") or ""
        urlp = (r.get("link") or r.get("url") or "").strip()

        title = "Funding: " + name
        if round_:
            title += f" ({round_})"
        desc = f"Amount: {amount}" if amount else ""
        items.append(
            {
                "source": "funding",
                "source_id": "defillama_raises",
                "title": title,
                "url": urlp or "https://defillama.com/raises",
                "description": desc,
                "published_at": published_at if "T" in published_at else dt.isoformat() + "Z",
            }
        )
    return items


async def governance_from_snapshot(session: aiohttp.ClientSession, since: datetime, spaces: List[str]) -> List[Dict[str, Any]]:
    """Snapshot Hub GraphQL proposals for a set of spaces."""
    if not spaces:
        return []
    endpoint = "https://hub.snapshot.org/graphql"
    query = """
    query Proposals($spaces: [String!], $created_gte: Int!) {
      proposals(first: 50, where: { space_in: $spaces, created_gte: $created_gte }, orderBy: "created", orderDirection: desc) {
        id
        title
        body
        created
        link
        space { id }
      }
    }
    """
    created_gte = int(since.timestamp())
    payload = {"query": query, "variables": {"spaces": spaces, "created_gte": created_gte}}
    try:
        data = await fetch_json_post(session, endpoint, json_payload=payload)
    except Exception as e:
        logger.warning("Snapshot GraphQL fetch failed: %s", e)
        return []

    proposals = (((data or {}).get("data") or {}).get("proposals")) or []
    out: List[Dict[str, Any]] = []
    for p in proposals:
        created = p.get("created")
        published_at = _iso_or_none(created)
        if not published_at:
            continue
        # created is epoch
        try:
            dt = datetime.utcfromtimestamp(int(created))
        except Exception:
            continue
        if dt < since:
            continue
        space_id = ((p.get("space") or {}).get("id")) or "snapshot"
        out.append(
            {
                "source": "ecosystem",
                "source_id": "snapshot_proposals",
                "title": f"Governance ({space_id}): {(p.get('title') or '').strip()}",
                "url": (p.get("link") or "").strip(),
                "description": (p.get("body") or "").strip()[:8000],
                "published_at": dt.isoformat() + "Z",
            }
        )
    return out
