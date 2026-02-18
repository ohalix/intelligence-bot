import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from .base_ingest import BaseIngester
from utils.http import fetch_json

logger = logging.getLogger(__name__)

class GitHubIngester(BaseIngester):
    async def ingest(self, since: datetime) -> List[Dict[str, Any]]:
        # SAMPLE_OFFLINE: deterministic local testing without network
        if self.config.get('offline_test'):
            from datetime import datetime
            return [{
                'id': f'offline-github-1',
                'source': 'github',
                'type': 'github_item',
                'title': 'New repo spike: Bitcoin L2 bridge tooling',
                'text': 'New repo spike: Bitcoin L2 bridge tooling',
                'url': 'https://example.com/github/1',
                'timestamp': datetime.utcnow(),
            }]
        token = self.config.get("keys", {}).get("github_token")
        headers = {"Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        window_days = int(self.config.get("filtering", {}).get("github", {}).get("activity_window_days", 30))
        q_since = max(since, datetime.utcnow() - timedelta(days=window_days)).date().isoformat()
        min_stars = int(self.config.get("filtering", {}).get("github", {}).get("min_stars", 5))
        query = f"pushed:>={q_since} stars:>={min_stars}"
        url = "https://api.github.com/search/repositories"
        params = {"q": query, "sort": "updated", "order": "desc", "per_page": 30}

        try:
            data = await fetch_json(self.session, url, headers=headers, params=params)
        except Exception as e:
            logger.warning(f"GitHubIngester fetch failed: {e}")
            return []

        signals: List[Dict[str, Any]] = []
        max_age = int(self.config.get("filtering", {}).get("github", {}).get("max_repo_age_days", 365))
        for item in (data.get("items") or [])[:50]:
            pushed_at = item.get("pushed_at")
            created_at = item.get("created_at")
            try:
                ts = datetime.fromisoformat(pushed_at.replace("Z","+00:00")).replace(tzinfo=None) if pushed_at else datetime.utcnow()
            except Exception:
                ts = datetime.utcnow()
            if ts <= since:
                continue
            try:
                created = datetime.fromisoformat(created_at.replace("Z","+00:00")).replace(tzinfo=None) if created_at else None
            except Exception:
                created = None
            if created and (datetime.utcnow() - created).days > max_age:
                continue

            signals.append({
                "source": "github",
                "type": "github_repo",
                "title": item.get("full_name") or item.get("name"),
                "description": item.get("description") or "",
                "url": item.get("html_url") or "",
                "timestamp": ts,
                "stars": item.get("stargazers_count", 0),
                "forks": item.get("forks_count", 0),
                "language": item.get("language") or "Unknown",
            })
        return signals