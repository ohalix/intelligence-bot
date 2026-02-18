import datetime as dt
import hashlib
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class GitHubIngester:
    """GitHub public search-based ingestion (no auth required).

    Uses the GitHub REST Search API which is rate-limited but usable for low-frequency runs.
    """

    BASE = "https://api.github.com/search/repositories"

    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session

    def _queries(self) -> List[str]:
        ingestion_cfg = self.config.get("ingestion", {})
        qs = ingestion_cfg.get("github_queries")
        if qs is None:
            qs = self.config.get("github_queries")
        if isinstance(qs, str):
            return [q.strip() for q in qs.split(",") if q.strip()]
        return list(qs or [])

    async def ingest(self, since: dt.datetime) -> List[Dict[str, Any]]:
        queries = self._queries()
        if not queries:
            return []

        window_days = int(self.config.get("ingestion", {}).get("github_window_days", 7))
        min_since = dt.datetime.utcnow() - dt.timedelta(days=window_days)
        since_dt = max(since, min_since)
        q_since = since_dt.date().isoformat()

        headers = {"Accept": "application/vnd.github+json", "User-Agent": "Mozilla/5.0"}

        out: List[Dict[str, Any]] = []
        for q in queries:
            try:
                params = {"q": f"{q} pushed:>={q_since}", "sort": "updated", "order": "desc", "per_page": 10}
                async with self.session.get(self.BASE, params=params, headers=headers) as resp:
                    if resp.status >= 400:
                        raise RuntimeError(f"GitHub API HTTP {resp.status}")
                    data = await resp.json()

                for item in data.get("items", [])[:10]:
                    url = item.get("html_url")
                    title = item.get("full_name")
                    desc = (item.get("description") or "").strip()
                    updated_at = item.get("pushed_at") or item.get("updated_at")
                    try:
                        ts = dt.datetime.fromisoformat(updated_at.replace("Z", "+00:00")).replace(tzinfo=None)
                    except Exception:
                        ts = dt.datetime.utcnow()
                    if ts < since:
                        continue

                    dedup_key = hashlib.sha256((url or title or "").encode("utf-8")).hexdigest()

                    out.append(
                        {
                            "dedup_key": dedup_key,
                            "source": "github",
                            "type": "github",
                            "title": title or "",
                            "description": desc,
                            "url": url or "",
                            "timestamp": ts.isoformat(),
                            "raw_json": "{}",
                        }
                    )

            except Exception as e:
                logger.error("GitHubIngester failed: %s", e, exc_info=True)

        return out
