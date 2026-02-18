"""intelligence.web3_analysis_agent

Two modes:
- REAL mode: call an LLM provider (OpenAI or Anthropic) if keys are present and DRY_MODE is false.
- DRY mode: deterministic, rule-based summary so local tests can run without keys.

The agent receives curated (ranked) signals and returns Web3-native analysis:
- significance + market tone (risk-on / risk-off / neutral)
- narrative clusters
- suggested strategic actions + analytics/ML angles
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


def _safe(s: Any) -> str:
    try:
        return str(s)
    except Exception:
        return ""


class Web3AnalysisAgent:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dry_mode = str(config.get("ai", {}).get("dry_mode", "true")).lower() == "true"
        self.provider = (config.get("ai", {}).get("provider") or "auto").lower()
        self.openai_key = config.get("ai", {}).get("openai_api_key") or os.getenv("OPENAI_API_KEY")
        self.anthropic_key = config.get("ai", {}).get("anthropic_api_key") or os.getenv("ANTHROPIC_API_KEY")
        self.model_openai = config.get("ai", {}).get("openai_model", "gpt-4o-mini")
        self.model_anthropic = config.get("ai", {}).get("anthropic_model", "claude-3-5-sonnet-latest")
        self.max_signals = int(config.get("bot", {}).get("max_signals", 10))

    def _pick_provider(self) -> Optional[str]:
        if self.provider in ("openai", "anthropic"):
            return self.provider
        if self.openai_key:
            return "openai"
        if self.anthropic_key:
            return "anthropic"
        return None

    def _compact_signals(self, signals: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        out = []
        for s in signals[:limit]:
            out.append({
                "source": s.get("source"),
                "type": s.get("type"),
                "title": s.get("title") or (s.get("text") or "")[:120],
                "url": s.get("url"),
                "signal_score": s.get("signal_score"),
                "sentiment_label": s.get("sentiment", {}).get("label") if isinstance(s.get("sentiment"), dict) else s.get("sentiment_label"),
                "ecosystem": s.get("detected_ecosystem"),
                "sector": s.get("detected_sector"),
            })
        return out

    def _dry_market_tone(self, signals: List[Dict[str, Any]]) -> str:
        # Use sentiment + sectors to infer tone (simple heuristic)
        pos = 0
        neg = 0
        for s in signals:
            lab = None
            if isinstance(s.get("sentiment"), dict):
                lab = s["sentiment"].get("label")
            lab = lab or s.get("sentiment_label")
            if lab == "positive":
                pos += 1
            elif lab == "negative":
                neg += 1
        if pos - neg >= 3:
            return "risk-on"
        if neg - pos >= 3:
            return "risk-off"
        return "neutral"

    def _dry_narratives(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # cluster by (ecosystem, sector, keyword bucket)
        buckets = defaultdict(list)
        for s in signals:
            eco = s.get("detected_ecosystem") or "unknown"
            sec = s.get("detected_sector") or "unknown"
            text = (_safe(s.get("title")) + " " + _safe(s.get("text")) + " " + _safe(s.get("description"))).lower()
            key = "general"
            for k in ("airdrop", "bridge", "restaking", "rwa", "stablecoin", "l2", "rollup", "solana", "bitcoin", "zk", "ai", "gpu", "agent", "infra", "oracle", "dex", "lending"):
                if k in text:
                    key = k
                    break
            buckets[(eco, sec, key)].append(s)
        clusters = []
        for (eco, sec, key), items in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]:
            clusters.append({
                "ecosystem": eco,
                "sector": sec,
                "narrative": key,
                "count": len(items),
                "top_urls": [i.get("url") for i in items[:3] if i.get("url")],
            })
        return clusters

    def _dry_actions(self, signals: List[Dict[str, Any]]) -> List[str]:
        actions = []
        # prioritize by sector + ecosystem
        secs = Counter([s.get("detected_sector") or "unknown" for s in signals])
        ecos = Counter([s.get("detected_ecosystem") or "unknown" for s in signals])
        top_sec = secs.most_common(1)[0][0] if secs else "unknown"
        top_eco = ecos.most_common(1)[0][0] if ecos else "unknown"
        actions.append(f"Track follow-up commits + releases for top cluster ({top_eco}/{top_sec}).")
        actions.append("Add to watchlist and set alerts for funding + exchange listings around the same narrative.")
        actions.append("Analytics angle: build a 7d retention + cohort view for new protocols mentioned today (wallets, TVL, volume).")

        # ML angle
        actions.append("ML angle: label historical 'high-signal' items and train a lightweight ranker to predict next-day engagement/volume spikes.")
        return actions

    def _dry_summary(self, signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        tone = self._dry_market_tone(signals)
        narratives = self._dry_narratives(signals)
        actions = self._dry_actions(signals)
        return {
            "mode": "dry",
            "market_tone": tone,
            "narrative_clusters": narratives,
            "strategic_actions": actions,
            "generated_at": datetime.utcnow().isoformat(),
        }

    async def _openai_call(self, payload: Dict[str, Any]) -> str:
        headers = {
            "Authorization": f"Bearer {self.openai_key}",
            "Content-Type": "application/json",
        }
        url = "https://api.openai.com/v1/chat/completions"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as r:
                txt = await r.text()
                if r.status >= 300:
                    raise RuntimeError(f"OpenAI error {r.status}: {txt[:500]}")
                data = json.loads(txt)
                return data["choices"][0]["message"]["content"]

    async def _anthropic_call(self, payload: Dict[str, Any]) -> str:
        headers = {
            "x-api-key": self.anthropic_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        url = "https://api.anthropic.com/v1/messages"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as r:
                txt = await r.text()
                if r.status >= 300:
                    raise RuntimeError(f"Anthropic error {r.status}: {txt[:500]}")
                data = json.loads(txt)
                # content is list of blocks
                blocks = data.get("content", [])
                if blocks and isinstance(blocks, list) and "text" in blocks[0]:
                    return blocks[0]["text"]
                return txt

    async def analyze(self, signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not signals:
            return {"mode": "dry" if self.dry_mode else "none", "market_tone": "neutral", "narrative_clusters": [], "strategic_actions": []}

        provider = self._pick_provider()
        if self.dry_mode or not provider:
            return self._dry_summary(signals)

        compact = self._compact_signals(signals, limit=self.max_signals)

        system = (
            "You are a Web3 intelligence analyst. You are concise, non-generic, and operator-focused. "
            "You must infer significance, cluster narratives, and output market tone as risk-on/risk-off/neutral. "
            "Provide strategic actions and analytics/ML angles. Do not hallucinate facts; only use the provided signals."
        )

        user = {
            "task": "Analyze the curated signals and produce Web3-native intelligence.",
            "required_output_schema": {
                "market_tone": "risk-on|risk-off|neutral",
                "narrative_clusters": [
                    {"narrative": "string", "ecosystem": "string", "sector": "string", "why_it_matters": "string", "supporting_links": ["url"]}
                ],
                "key_takeaways": ["string"],
                "strategic_actions": ["string"],
                "analytics_ml_angles": ["string"],
            },
            "signals": compact,
        }

        try:
            if provider == "openai":
                payload = {
                    "model": self.model_openai,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": json.dumps(user)},
                    ],
                    "temperature": 0.3,
                }
                content = await self._openai_call(payload)
            else:
                payload = {
                    "model": self.model_anthropic,
                    "max_tokens": 900,
                    "temperature": 0.3,
                    "system": system,
                    "messages": [
                        {"role": "user", "content": json.dumps(user)},
                    ],
                }
                content = await self._anthropic_call(payload)

            # Try parse JSON, else wrap
            try:
                parsed = json.loads(content)
                parsed["mode"] = "real"
                return parsed
            except Exception:
                return {"mode": "real", "raw": content}
        except Exception as e:
            logger.exception(f"AI analysis failed, falling back to dry mode: {e}")
            return self._dry_summary(signals)

