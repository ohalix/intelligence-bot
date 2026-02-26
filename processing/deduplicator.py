"""Deduplication with URL normalization and content hash near-dupe detection.

FIX item 6:
- URL normalization (strip tracking params like utm_*, fbclid, etc.)
- Content hash (title+snippet) stored in DB for persistent near-dupe detection
- Per-run in-memory dedup (existing behavior preserved)
"""
import hashlib
import re
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

# Tracking/noise query params to strip for URL normalization
_STRIP_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid",
    "ref", "referer", "source", "_ga", "igshid",
})


def normalize_url(url: str) -> str:
    """Strip tracking query params and normalize URL for dedup (item 6)."""
    if not url:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=False)
        clean_qs = {k: v for k, v in qs.items() if k.lower() not in _STRIP_PARAMS}
        # Reconstruct query string (sorted for stability)
        new_query = urlencode(sorted(clean_qs.items()), doseq=True)
        clean = parsed._replace(query=new_query, fragment="")
        return urlunparse(clean)
    except Exception:
        return url


def content_hash(signal: Dict[str, Any]) -> str:
    """SHA-256 of first 300 chars of (title + description) for near-dupe detection."""
    title = (signal.get("title") or "").strip().lower()
    desc = (signal.get("description") or "").strip().lower()
    combined = (title + " " + desc)[:300]
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


class Deduplicator:
    def __init__(self, store=None) -> None:
        """
        store: optional SQLiteStore to check persistent content hashes.
        """
        self.seen_keys: Set[str] = set()
        self.seen_urls: Set[str] = set()
        self.seen_hashes: Set[str] = set()
        self._store = store
        self._dropped_url = 0
        self._dropped_content = 0

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def key(self, signal: Dict[str, Any]) -> str:
        for k in ("tweet_id", "id", "repo_id"):
            v = signal.get(k)
            if v:
                return f"{signal.get('source', 'unknown')}:{self._hash(str(v))}"
        norm_url = normalize_url(signal.get("url", ""))
        if norm_url:
            return f"{signal.get('source', 'unknown')}:{self._hash(norm_url)}"
        return f"{signal.get('source', 'unknown')}:{self._hash((signal.get('title', '') + signal.get('description', ''))[:400])}"

    def dedup(self, signals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for s in signals:
            # Normalize URL on the signal
            orig_url = s.get("url", "")
            norm = normalize_url(orig_url)
            if norm != orig_url:
                s = dict(s)
                s["url"] = norm

            # Key-based dedup (exact)
            k = self.key(s)
            if k in self.seen_keys:
                self._dropped_url += 1
                continue
            self.seen_keys.add(k)

            # Content hash near-dupe (in-memory)
            ch = content_hash(s)
            if ch in self.seen_hashes:
                self._dropped_content += 1
                continue
            self.seen_hashes.add(ch)

            # Persistent near-dupe via DB (if store available)
            if self._store is not None:
                try:
                    if self._store.content_hash_exists(ch):
                        self._dropped_content += 1
                        continue
                except Exception:
                    pass

            s["dedup_key"] = k
            s["content_hash"] = ch
            out.append(s)

        return out

    def stats(self) -> Dict[str, int]:
        return {
            "dropped_url": self._dropped_url,
            "dropped_content": self._dropped_content,
        }
