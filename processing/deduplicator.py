import hashlib
from typing import Any, Dict, List, Set


class Deduplicator:
    """Simple deterministic de-duplication for a rolling window.

    Public API expected by the pipeline:
      - dedup(signals)

    Historically some versions used `deduplicate()`. We keep both names as
    a compatibility shim (no behavior change).
    """

    def __init__(self) -> None:
        self._seen: Set[str] = set()

    def _key(self, signal: Dict[str, Any]) -> str:
        parts = [
            str(signal.get("source", "")),
            str(signal.get("type", "")),
            str(signal.get("url", "")),
            str(signal.get("title", "")),
            str(signal.get("tweet_id", "")),
            str(signal.get("repo_id", "")),
        ]
        raw = "|".join(parts).encode("utf-8", "ignore")
        return hashlib.sha256(raw).hexdigest()

    def dedup(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for s in signals or []:
            k = self._key(s)
            if k in self._seen:
                continue
            self._seen.add(k)
            out.append(s)
        return out

    # Backwards-compatible alias
    def deduplicate(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.dedup(signals)
