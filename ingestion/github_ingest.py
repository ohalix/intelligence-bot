import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from utils.http import fetch_json

logger = logging.getLogger(__name__)

GITHUB_SEARCH_API = "https://api.github.com/search/repositories"

class GitHubIngester:
    def __init__(self, config: Dict[str, Any], session):
        self.config = config
        self.session = session
        ing = (config or {}).get("ingestion", {})
        self.topics: List[str] = ing.get("github_topics") or config.get("github_topics", []) or []
        self.token: Optional[str] = (config or {}).get("keys", {}).get("github_token")

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/vnd.github+json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        # GitHub search requires date granularity; keep a small rolling window
        window_days = int((self.config or {}).get("analysis", {}).get("github_window_days", 7))
        q_since = max(since.astimezone(timezone.utc), datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()

        attempted = len(self.topics) if self.topics else 1
        ok = 0
        fail = 0
        exc_types: Dict[str, int] = {}
        out: List[Dict[str, Any]] = []

        topics = self.topics or ["web3"]
        for topic in topics:
            q = f"topic:{topic} pushed:>={q_since}"
            params = {"q": q, "sort": "updated", "order": "desc", "per_page": 20}
            try:
                data = await fetch_json(self.session, GITHUB_SEARCH_API, headers=self._headers(), params=params)
                items = data.get("items", []) if isinstance(data, dict) else []
                for it in items:
                    updated_at = it.get("updated_at") or it.get("pushed_at") or it.get("created_at")
                    # Filter in Python for precision
                    ts = datetime.now(timezone.utc)
                    if updated_at:
                        try:
                            ts = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                        except Exception:
                            ts = datetime.now(timezone.utc)
                    if ts < since.astimezone(timezone.utc):
                        continue
                    out.append(
                        {
                            "source": "github",
                            "title": it.get("full_name", ""),
                            "url": it.get("html_url", ""),
                            "summary": it.get("description", "") or "",
                            "created_at": ts.astimezone(timezone.utc).isoformat(),
                            "raw": {"topic": topic, "stars": it.get("stargazers_count"), "language": it.get("language")},
                        }
                    )
                ok += 1
            except Exception as ex:
                fail += 1
                exc_types[type(ex).__name__] = exc_types.get(type(ex).__name__, 0) + 1
                logger.warning("GitHubIngester failed for topic=%s: %s", topic, ex)

        logger.info(
            "GitHubIngester: topics=%d ok=%d fail=%d items=%d top_exceptions=%s",
            attempted, ok, fail, len(out),
            dict(sorted(exc_types.items(), key=lambda kv: kv[1], reverse=True)[:3]),
        )
        return out
