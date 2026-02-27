import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


DEFAULT_QUERIES = [
    "language:Solidity stars:>10",
    "topic:defi stars:>10",
]


class GitHubIngester:
    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session
        # FIX item 2: config stores github token under keys.github_token (set by load_config from GITHUB_TOKEN env)
        # Fallback to legacy config.github.token for compatibility.
        self.token = (
            config.get("keys", {}).get("github_token")
            or config.get("github", {}).get("token")
        )
        if not self.token:
            logger.warning(
                "GitHubIngester: GITHUB_TOKEN not set. Requests will be unauthenticated "
                "(rate limit: 60 req/h). Set GITHUB_TOKEN env var to get 5000 req/h."
            )

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        window_days = int(self.config.get("github", {}).get("window_days", 7))
        q_since = max(since, datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=window_days)).date().isoformat()

        queries = self.config.get("github", {}).get("queries") or DEFAULT_QUERIES
        concurrency = int(self.config.get("github", {}).get("concurrency", 3))
        sem = asyncio.Semaphore(concurrency)

        stats = {"attempted": 0, "success": 0, "fail": 0, "items": 0, "errors": {}}

        async def _one(q: str) -> List[Dict[str, Any]]:
            async with sem:
                stats["attempted"] += 1
                try:
                    headers = {"Accept": "application/vnd.github+json"}
                    if self.token:
                        headers["Authorization"] = f"Bearer {self.token}"

                    url = "https://api.github.com/search/repositories"
                    params = {"q": f"{q} pushed:>={q_since}", "sort": "updated", "order": "desc"}

                    async with self.session.get(url, params=params, headers=headers) as resp:
                        # FIX item 23: log GitHub rate-limit headers
                        rl_remaining = resp.headers.get("X-RateLimit-Remaining")
                        rl_reset = resp.headers.get("X-RateLimit-Reset")
                        if rl_remaining is not None:
                            logger.debug(
                                "GitHub rate-limit: remaining=%s reset=%s", rl_remaining, rl_reset
                            )
                            if int(rl_remaining) < 5:
                                logger.warning(
                                    "GitHub rate-limit critically low: remaining=%s reset=%s",
                                    rl_remaining, rl_reset,
                                )
                        resp.raise_for_status()
                        data = await resp.json()  # aiohttp: must await

                    items = data.get("items", []) if isinstance(data, dict) else []
                    out: List[Dict[str, Any]] = []
                    for repo in items:
                        out.append(
                            {
                                "source": "github",
                                "source_id": q,
                                "title": repo.get("full_name", ""),
                                "url": repo.get("html_url", ""),
                                "description": repo.get("description", "") or "",
                                "published_at": repo.get("pushed_at", ""),
                            }
                        )

                    stats["success"] += 1
                    stats["items"] += len(out)
                    return out
                except Exception as e:
                    stats["fail"] += 1
                    key = type(e).__name__
                    stats["errors"][key] = stats["errors"].get(key, 0) + 1
                    logger.warning("GitHubIngester failed for query=%r: %s", q, e)
                    return []

        results = await asyncio.gather(*[_one(q) for q in queries])
        flattened = [x for sub in results for x in sub]

        logger.info(
            "GitHubIngester run: attempted=%s success=%s fail=%s items=%s top_errors=%s",
            stats["attempted"],
            stats["success"],
            stats["fail"],
            stats["items"],
            dict(sorted(stats["errors"].items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )

        return flattened


# Backwards-compatible alias (older code used GithubIngester)
GithubIngester = GitHubIngester
