"""Web3 analysis agent — legacy compatibility shim.

The OpenAI/Anthropic pathway has been removed and replaced with the
HF → Gemini router in intelligence/llm_router.py.

This file is kept as a shim so any code that imports Web3AnalysisAgent
continues to work. It does nothing (dry mode only) and is not called
from the main command flow.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class Web3AnalysisAgent:
    """Legacy shim. No longer performs real LLM calls.

    The active AI pathway is intelligence.llm_router.route_llm()
    called from bot.telegram_commands.
    """

    def __init__(self, config: Dict[str, Any], session=None) -> None:
        self.config = config

    async def analyze(
        self,
        top_signals: List[Dict[str, Any]],
        market_state: Dict[str, Any],
        narratives: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return self._dry_analyze(top_signals, market_state, narratives)

    def _dry_analyze(
        self,
        top_signals: List[Dict[str, Any]],
        market_state: Dict[str, Any],
        narratives: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        actions = []
        for s in top_signals[:5]:
            sector = s.get("sector", "unknown")
            if sector == "defi":
                actions.append("Track liquidity/TVL shifts; look for new pools and incentive programs.")
            elif sector == "infrastructure":
                actions.append("Watch for integration partners and developer adoption metrics.")
            else:
                actions.append("Monitor narrative + user adoption signals over the next 24–72h.")
        actions = list(dict.fromkeys(actions))[:4]
        return {
            "mode": "dry",
            "summary": (
                f"24h intelligence snapshot: market tone appears "
                f"{market_state.get('market_tone', 'neutral')} "
                f"(conf {market_state.get('confidence', 0)}). "
                "Top narratives clustered by chain/sector."
            ),
            "market_tone": market_state,
            "narratives": narratives,
            "strategic_actions": actions,
        }
