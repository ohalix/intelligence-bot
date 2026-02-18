from collections import defaultdict
from typing import Any, Dict, List, Tuple

class NarrativeGenerator:
    def cluster(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Cluster by (chain, sector, source) coarse buckets
        buckets = defaultdict(list)
        for s in signals:
            key = (s.get("chain","unknown"), s.get("sector","unknown"))
            buckets[key].append(s)
        out = []
        for (chain, sector), items in buckets.items():
            out.append({
                "chain": chain,
                "sector": sector,
                "count": len(items),
                "top_titles": [
                    i.get("title", "")
                    for i in sorted(
                        items,
                        key=lambda x: (x.get("signal_score") or 0.0),
                        reverse=True,
                    )[:3]
                ],
            })
        out.sort(key=lambda x: x["count"], reverse=True)
        return out[:8]
