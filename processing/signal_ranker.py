from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class RankedSignal:
    signal: Dict[str, Any]
    score: float


class SignalRanker:
    """Scores and sorts signals.

    Compatibility note:
    The pipeline expects a public .rank(signals) method.
    Older revisions used different method names; we keep both.
    """

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            "source": 1.0,
            "recency": 1.0,
            "keyword": 1.0,
            "sentiment": 0.5,
        }

    def rank(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Public API expected by engine.pipeline.

        Returns the same list of dicts, sorted best→worst, with 'signal_score' set.
        """
        return self.rank_signals(signals)

    def rank_signals(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ranked: List[RankedSignal] = []
        for s in signals or []:
            try:
                score = float(s.get("signal_score") or 0.0)
            except Exception:
                score = 0.0

            # very light heuristic boost (no new features; just stable defaults)
            src = (s.get("source") or "").lower()
            if src in ("funding", "github"):
                score += 1.0
            elif src in ("news", "ecosystem", "twitter"):
                score += 0.5

            # sentiment hint if present
            sent = s.get("sentiment")
            if isinstance(sent, str):
                if sent.lower() in ("bullish", "positive"):
                    score += 0.2
                elif sent.lower() in ("bearish", "negative"):
                    score -= 0.2

            s["signal_score"] = round(score, 4)
            ranked.append(RankedSignal(signal=s, score=score))

        ranked.sort(key=lambda x: x.score, reverse=True)
        return [r.signal for r in ranked]

