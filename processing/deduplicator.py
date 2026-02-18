import hashlib
from typing import Any, Dict, Iterable, List, Set

class Deduplicator:
    def __init__(self) -> None:
        self.seen: Set[str] = set()

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def key(self, signal: Dict[str, Any]) -> str:
        # Prefer stable IDs; else URL/title hash
        for k in ("tweet_id", "id", "repo_id", "url"):
            v = signal.get(k)
            if v:
                return f"{signal.get('source','unknown')}:{self._hash(str(v))}"
        return f"{signal.get('source','unknown')}:{self._hash((signal.get('title','') + signal.get('description',''))[:400])}"

    def dedup(self, signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for s in signals:
            k = self.key(s)
            if k in self.seen:
                continue
            self.seen.add(k)
            s["dedup_key"] = k
            out.append(s)
        return out
