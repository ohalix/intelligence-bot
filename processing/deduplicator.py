import hashlib
from typing import Any, Dict, List, Set


class Deduplicator:
    """In-memory deduplication for a single run (SQLite store enforces 24h dedup across runs)."""

    def __init__(self, config: Any = None) -> None:
        self.seen: Set[str] = set()

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def key(self, signal: Dict[str, Any]) -> str:
        for k in ("tweet_id", "repo_id", "id", "url"):
            v = signal.get(k)
            if v:
                return f"{signal.get('source','unknown')}:{self._hash(str(v))}"
        # fallback: hash title/text
        base = (signal.get("title") or "") + "|" + (signal.get("text") or "")
        return f"{signal.get('source','unknown')}:{self._hash(base)}"

    def deduplicate(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for s in signals:
            k = self.key(s)
            s["dedup_key"] = k
            if k in self.seen:
                continue
            self.seen.add(k)
            out.append(s)
        return out
