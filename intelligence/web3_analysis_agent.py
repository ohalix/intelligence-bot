"""Web3 analysis agent.

FIX item 8: Reuse a shared aiohttp.ClientSession instead of creating a new one
per call. Accept optional session parameter; fall back to creating one only if needed.
"""
import json
import logging
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class Web3AnalysisAgent:
    def __init__(self, config: Dict[str, Any], session: Optional[aiohttp.ClientSession] = None) -> None:
        self.config = config
        # FIX item 8: accept shared session to avoid churn
        self._session = session

    async def analyze(self, top_signals: List[Dict[str, Any]], market_state: Dict[str, Any], narratives: List[Dict[str, Any]]) -> Dict[str, Any]:
        if self.config.get("dry_mode") or (
            not self.config.get("keys", {}).get("openai")
            and not self.config.get("keys", {}).get("anthropic")
        ):
            return self._dry_analyze(top_signals, market_state, narratives)
        if self.config.get("keys", {}).get("openai"):
            return await self._openai_analyze(top_signals, market_state, narratives)
        return self._dry_analyze(top_signals, market_state, narratives)

    def _dry_analyze(self, top_signals: List[Dict[str, Any]], market_state: Dict[str, Any], narratives: List[Dict[str, Any]]) -> Dict[str, Any]:
        actions = []
        for s in top_signals[:5]:
            sector = s.get("sector", "unknown")
            if sector == "defi":
                actions.append("Track liquidity/TVL shifts; look for new pools and incentive programs tied to these signals.")
            elif sector == "infrastructure":
                actions.append("Check if there is an indexer/oracle/RPC angle; watch for integration partners and developer adoption.")
            else:
                actions.append("Monitor narrative + user adoption signals (wallets, volume, repos) over the next 24–72h.")
        actions = list(dict.fromkeys(actions))[:4]
        return {
            "mode": "dry",
            "summary": f"24h intelligence snapshot: market tone appears {market_state.get('market_tone', 'neutral')} (conf {market_state.get('confidence', 0)}). Top narratives clustered by chain/sector.",
            "market_tone": market_state,
            "narratives": narratives,
            "strategic_actions": actions,
            "ml_angles": [
                "Build a classifier to predict which signals become sustained narratives (label with 7d follow-through).",
                "Ranker tuning: learn weights from Telegram clicks / user saves as implicit feedback.",
                "Anomaly detection on GitHub push/stars velocity to spot dev attention spikes early.",
            ],
        }

    async def _openai_analyze(
        self,
        top_signals: List[Dict[str, Any]],
        market_state: Dict[str, Any],
        narratives: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        key = self.config.get("keys", {}).get("openai")
        model = self.config.get("analysis", {}).get("llm_model", "gpt-4o-mini")
        prompt = {
            "market_state": market_state,
            "narratives": narratives,
            "top_signals": [
                {
                    "source": s.get("source"), "title": s.get("title"),
                    "url": s.get("url"), "score": s.get("signal_score"),
                    "sentiment": s.get("sentiment"),
                }
                for s in top_signals[:10]
            ],
            "instructions": [
                "Be Web3-native and non-generic.",
                "Interpret significance and possible second-order effects.",
                "Detect sentiment and market tone (risk-on/risk-off) and explain why.",
                "Suggest strategic actions + analytics/ML angles.",
                "Return JSON with keys: summary, market_tone_explainer, narratives, actions, ml_angles.",
            ],
        }
        sys_msg = "You are a Web3 intelligence analyst. Respond with STRICT JSON only."
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": json.dumps(prompt)},
            ],
            "temperature": 0.3,
        }
        timeout = aiohttp.ClientTimeout(total=30)

        # FIX item 8: reuse shared session if available; otherwise create a temporary one
        async def _do_call(sess: aiohttp.ClientSession) -> Dict[str, Any]:
            async with sess.post(url, headers=headers, json=payload, timeout=timeout) as r:
                if r.status != 200:
                    text = await r.text()
                    logger.warning("OpenAI API error status=%s: %s", r.status, text[:200])
                    return self._dry_analyze(top_signals, market_state, narratives)
                data = await r.json()
            content = data["choices"][0]["message"]["content"]
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = {"summary": content}
            return {"mode": "real", "raw": parsed}

        try:
            if self._session is not None and not self._session.closed:
                return await _do_call(self._session)
            async with aiohttp.ClientSession() as tmp_session:
                return await _do_call(tmp_session)
        except Exception as exc:
            logger.warning("OpenAI analysis failed, falling back to dry mode: %s", exc)
            return self._dry_analyze(top_signals, market_state, narratives)
