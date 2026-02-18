"""bot.formatter

Telegram-safe formatting helpers.

We send messages with parse_mode=None to avoid Markdown parse errors.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional


def _one_line(s: str) -> str:
    return (s or "").replace("\n", " ").replace("\r", " ").strip()


class TelegramFormatter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def _fmt_signal_line(self, s: Dict[str, Any], idx: int) -> str:
        title = _one_line(s.get("title") or s.get("text") or "(no title)")
        url = s.get("url") or ""
        score = s.get("signal_score", 0.0)
        eco = s.get("detected_ecosystem") or "unknown"
        sec = s.get("detected_sector") or "unknown"
        interp = _one_line((s.get("description") or "")[:140])
        bits = [f"{idx}. {title}", f"   score: {score} | {eco} / {sec}"]
        if interp:
            bits.append(f"   {interp}")
        if url:
            bits.append(f"   {url}")
        return "\n".join(bits)

    def format_signals(self, header: str, signals: List[Dict[str, Any]]) -> str:
        lines = [header]
        if not signals:
            lines.append("No signals in the last 24h.")
            return "\n".join(lines)
        for i, s in enumerate(signals, 1):
            lines.append(self._fmt_signal_line(s, i))
        return "\n\n".join(lines)

    def format_rawsignals(self, signals: List[Dict[str, Any]]) -> str:
        header = "🧾 Raw Top Signals (last 24h)"
        lines = [header]
        if not signals:
            lines.append("No signals in the last 24h.")
            return "\n".join(lines)
        for i, s in enumerate(signals, 1):
            title = _one_line(s.get("title") or s.get("text") or "(no title)")
            url = s.get("url") or ""
            score = s.get("signal_score", 0.0)
            lines.append(f"{i}. score={score} | {title}")
            if url:
                lines.append(f"   {url}")
        return "\n".join(lines)

    def format_trends(self, analysis: Dict[str, Any], signals: List[Dict[str, Any]]) -> str:
        tone = analysis.get("market_tone", "neutral")
        lines = [f"📈 Trends / Market Tone: {tone}"]

        clusters = analysis.get("narrative_clusters") or []
        if clusters:
            lines.append("\nTop Narratives:")
            for c in clusters[:5]:
                narrative = c.get("narrative") or c.get("narrative_name") or "general"
                eco = c.get("ecosystem", "unknown")
                sec = c.get("sector", "unknown")
                cnt = c.get("count", "")
                why = _one_line(c.get("why_it_matters") or "")
                lines.append(f"- {narrative} ({eco}/{sec}) — {cnt}")
                if why:
                    lines.append(f"  {why}")
                for u in (c.get("supporting_links") or c.get("top_urls") or [])[:3]:
                    if u:
                        lines.append(f"  {u}")

        actions = analysis.get("strategic_actions") or []
        if actions:
            lines.append("\nSuggested Actions:")
            for a in actions[:6]:
                lines.append(f"- {_one_line(a)}")

        ml = analysis.get("analytics_ml_angles") or []
        if ml:
            lines.append("\nAnalytics / ML Angles:")
            for a in ml[:6]:
                lines.append(f"- {_one_line(a)}")

        return "\n".join(lines)

    def format_dailybrief(self, signals: List[Dict[str, Any]], analysis: Optional[Dict[str, Any]] = None) -> str:
        date = datetime.utcnow().strftime("%Y-%m-%d")
        lines = [f"🗞️ Daily Brief — {date}"]
        if analysis:
            lines.append(f"Market tone: {analysis.get('market_tone','neutral')}")
        lines.append("")
        lines.append(self.format_rawsignals(signals))
        return "\n".join(lines)
