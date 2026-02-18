from typing import Any, Dict, List

class TrendDetector:
    def detect(self, signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Very small v1: surface top chains/sectors by count + score
        agg = {}
        for s in signals:
            k = f"{s.get('chain','unknown')}|{s.get('sector','unknown')}"
            if k not in agg:
                agg[k] = {"chain": s.get("chain","unknown"), "sector": s.get("sector","unknown"), "count": 0, "score_sum": 0.0}
            agg[k]["count"] += 1
            v = s.get("signal_score")
            try:
                agg[k]["score_sum"] += float(v) if v is not None else 0.0
            except Exception:
                agg[k]["score_sum"] += 0.0
        trends = sorted(agg.values(), key=lambda x: (x["count"], x["score_sum"]), reverse=True)[:8]
        return {"trends": trends}
