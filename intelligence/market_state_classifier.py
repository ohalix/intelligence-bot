from typing import Any, Dict, List

class MarketStateClassifier:
    # Simple 24h market tone classification, based on sentiment + keywords
    RISK_ON_KWS = {"breakout","up","surge","record","bull","risk-on","altseason","rally"}
    RISK_OFF_KWS = {"hack","exploit","lawsuit","ban","down","bear","risk-off","capitulation","liquidation"}

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
            score += float(s.get("sentiment",0.0)) * 0.6
        tone = "neutral"
        if score > 2:
            tone = "risk-on"
        elif score < -2:
            tone = "risk-off"
        conf = min(0.9, 0.3 + abs(score)/10)
        return {"market_tone":tone,"confidence":round(conf,2),"drivers":drivers[:5]}
