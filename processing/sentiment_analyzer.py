"""Sentiment analysis utilities.

This project intentionally avoids paid/external sentiment APIs by default.
We provide a lightweight, deterministic, local heuristic sentiment scorer.

The pipeline expects SentimentAnalyzer.add_sentiment(signals) -> list.
Older repo iterations may have implemented different method names; we keep
compatibility by exposing add_sentiment as the public entrypoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


_POS_WORDS = {
    "bull", "bullish", "pump", "pumped", "pumping", "up", "uptrend", "breakout", "ath",
    "approve", "approved", "partnership", "launch", "released", "ship", "shipped",
    "upgrade", "upgraded", "growth", "record", "surge", "rally", "green",
}
_NEG_WORDS = {
    "bear", "bearish", "dump", "dumped", "down", "downtrend", "breakdown", "rekt",
    "exploit", "hacked", "hack", "rug", "rugpull", "attack", "incident",
    "lawsuit", "ban", "banned", "fraud", "scam", "warning", "red",
}


def _coalesce_text(signal: Dict[str, Any]) -> str:
    # Prefer richer fields, but stay compatible with multiple schemas.
    parts: List[str] = []
    for k in ("title", "summary", "snippet", "text", "content", "description"):
        v = signal.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return " \n".join(parts).strip()


def _heuristic_score(text: str) -> Tuple[str, float]:
    """Return (label, confidence). Confidence is 0..1."""
    if not text:
        return "neutral", 0.0

    t = text.lower()
    # Tokenize loosely
    tokens = [tok.strip(".,:;!?()[]{}\"'`).") for tok in t.split()]
    pos = sum(1 for tok in tokens if tok in _POS_WORDS)
    neg = sum(1 for tok in tokens if tok in _NEG_WORDS)

    if pos == 0 and neg == 0:
        return "neutral", 0.0

    # Simple normalized difference
    total = pos + neg
    diff = pos - neg
    # confidence increases with both magnitude and evidence count
    conf = min(1.0, abs(diff) / max(1, total))
    if diff > 0:
        return "positive", conf
    if diff < 0:
        return "negative", conf
    return "neutral", conf


class SentimentAnalyzer:
    """Attaches a lightweight sentiment estimate to signals.

    Public API expected by pipeline:
        add_sentiment(signals: list[dict]) -> list[dict]
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    # ---- Compatibility entrypoint expected by engine/pipeline.py ----
    def add_sentiment(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self._add_one(s) for s in (signals or [])]

    # ---- If other parts of the repo call different names, keep them too ----
    def analyze(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.add_sentiment(signals)

    def _add_one(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        # Defensive copy to avoid surprising callers that reuse dicts.
        s = dict(signal or {})
        text = _coalesce_text(s)
        label, confidence = _heuristic_score(text)

        # Standardized keys used by downstream formatting/analysis if present.
        # Do not remove/rename existing keys.
        s.setdefault("sentiment", label)
        s.setdefault("sentiment_confidence", float(confidence))
        return s
