from __future__ import annotations
from typing import Any, Dict, Tuple

def _match_keywords(text: str, keyword_sets: Dict[str, Dict[str, Any]]) -> Tuple[str, float]:
    t = (text or "").lower()
    # FIX item 7: init best score at 0.0 so multiplier==1.0 ecosystems can win
    best = ("unknown", 0.0)
    for name, meta in keyword_sets.items():
        kws = [k.lower() for k in meta.get("keywords", [])]
        hits = sum(1 for k in kws if k in t)
        if hits > 0:
            mult = float(meta.get("multiplier", 1.0))
            if mult >= best[1]:
                best = (name, mult)
    return best[0], best[1]

class FeatureEngine:
    def __init__(self, ecosystems: Dict[str, Any]) -> None:
        self.chains = ecosystems.get("chains", {})
        self.sectors = ecosystems.get("sectors", {})

    def enrich(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        text = f"{signal.get('title','')} {signal.get('description','')}"
        chain, chain_mult = _match_keywords(text, self.chains)
        sector, sector_mult = _match_keywords(text, self.sectors)
        signal["chain"] = chain
        signal["sector"] = sector
        signal["chain_multiplier"] = chain_mult
        signal["sector_multiplier"] = sector_mult
        return signal
