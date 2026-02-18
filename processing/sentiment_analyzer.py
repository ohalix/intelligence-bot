"""processing.sentiment_analyzer

Lightweight lexicon sentiment for offline and free-tier operation.
Outputs:
  sentiment: {score: float (-1..1), label: positive|neutral|negative}
"""

from __future__ import annotations

from typing import Any, Dict, List


POS_WORDS = {
    "breakthrough", "upgrade", "launch", "mainnet", "partnership", "integration", "grant",
    "funding", "raised", "growth", "record", "win", "improves", "improved", "expands"
}
NEG_WORDS = {
    "hack", "exploit", "down", "outage", "lawsuit", "ban", "rug", "scam",
    "attack", "liquidation", "collapse", "insolvency", "drained"
}


class SentimentAnalyzer:
    def __init__(self, config: Any = None):
        self.config = config or {}

    def score(self, text: str) -> float:
        t = (text or "").lower()
        pos = sum(1 for w in POS_WORDS if w in t)
        neg = sum(1 for w in NEG_WORDS if w in t)
        if pos == 0 and neg == 0:
            return 0.0
        # normalized
        return (pos - neg) / max(pos + neg, 1)

    def label(self, score: float) -> str:
        if score >= 0.2:
            return "positive"
        if score <= -0.2:
            return "negative"
        return "neutral"

    def analyze(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        parts = []
        for k in ("title", "text", "description"):
            if signal.get(k):
                parts.append(str(signal[k]))
        text = " ".join(parts)
        sc = float(self.score(text))
        signal["sentiment"] = {"score": round(sc, 3), "label": self.label(sc)}
        return signal

    def analyze_batch(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.analyze(s) for s in signals]
