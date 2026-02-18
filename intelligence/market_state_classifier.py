from __future__ import annotations

from typing import Any, Dict, List

class MarketStateClassifier:
    # Simple 24h market tone classification, based on sentiment + keywords
    RISK_ON_KWS = {"breakout","up","surge","record","bull","risk-on","altseason","rally"}
    RISK_OFF_KWS = {"hack","exploit","lawsuit","ban","down","bear","risk-off","capitulation","liquidation"}

    # Stored/processed sentiment can be numeric OR label strings (e.g. "neutral").
    _SENTIMENT_LABEL_MAP = {
        "neutral": 0.0,
        "mixed": 0.0,
        "unclear": 0.0,
        "positive": 1.0,
        "bullish": 1.0,
        "negative": -1.0,
        "bearish": -1.0,
    }

    def _sentiment_to_float(self, v: Any) -> float:
        """Best-effort conversion of sentiment to float.

        Accepts floats/ints, numeric strings ("0.2"), and label strings
        ("neutral"/"bullish"/"bearish"/"positive"/"negative").
        Defaults to 0.0 for missing/invalid values.
        """
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            try:
                return float(v)
            except Exception:
                return 0.0
        if isinstance(v, str):
            s = v.strip().lower()
            if not s:
                return 0.0
            try:
                return float(s)
            except Exception:
                return float(self._SENTIMENT_LABEL_MAP.get(s, 0.0))
        try:
            return float(v)  # type: ignore[arg-type]
        except Exception:
            return 0.0

    def classify(self, signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not signals:
            return {"market_tone":"neutral","confidence":0.2,"drivers":[]}
        score = 0.0
        drivers = []
        for s in signals[:20]:
            blob = f"{s.get('title','')} {s.get('description','')}".lower()
            if any(k in blob for k in self.RISK_ON_KWS):
                score += 1.0
                drivers.append(s.get("title",""))
            if any(k in blob for k in self.RISK_OFF_KWS):
                score -= 1.2
                drivers.append(s.get("title",""))
            score += self._sentiment_to_float(s.get("sentiment", 0.0)) * 0.6
        tone = "neutral"
        if score > 2:
            tone = "risk-on"
        elif score < -2:
            tone = "risk-off"
        conf = min(0.9, 0.3 + abs(score)/10)
        return {"market_tone":tone,"confidence":round(conf,2),"drivers":drivers[:5]}
