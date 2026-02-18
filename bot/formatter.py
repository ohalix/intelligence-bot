"""Telegram output formatting.

We use Telegram HTML parse mode for reliability.

MarkdownV2 is extremely strict and frequently causes 400 BadRequest errors when
any reserved character is not escaped correctly. HTML escaping is simpler and
produces clean output with fewer visible artifacts.
"""

from __future__ import annotations

import html
from typing import Any, Dict, List


def escape_html(text: Any) -> str:
    """Escape user/content text for Telegram HTML parse mode."""
    if text is None:
        return ""
    return html.escape(str(text), quote=True)


def _safe_link(url: str, label: str = "open") -> str:
    url = (url or "").strip()
    if not url:
        return ""
    return f"<a href=\"{escape_html(url)}\">{escape_html(label)}</a>"


def _fmt_signal(sig: Dict[str, Any]) -> str:
    title = escape_html(sig.get("title", ""))
    src = escape_html(sig.get("source", ""))
    url = str(sig.get("url", "") or "").strip()
    score = sig.get("signal_score")
    score_txt = escape_html(f"{score:.2f}" if isinstance(score, (int, float)) else (score or ""))
    summary = escape_html(sig.get("summary", "") or sig.get("snippet", ""))

    parts: List[str] = []
    if title:
        parts.append(f"<b>{title}</b>")
    if src:
        parts.append(f"<i>{src}</i>")
    if score_txt:
        parts.append(f"Score: <b>{score_txt}</b>")
    if summary:
        parts.append(summary)
    if url:
        parts.append(_safe_link(url, "open"))
    return "\n".join(parts)


def _truncate(text: str, limit: int = 3800) -> str:
    # Telegram max message length is 4096. Leave room for safety.
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "\n…(truncated)"


def format_dailybrief(payload: Dict[str, Any]) -> str:
    lines: List[str] = ["<b>🧠 Web3 Daily Brief</b>"]

    market = escape_html(payload.get("market_tone", ""))
    narrative = escape_html(payload.get("narrative", ""))
    if market:
        lines.append(f"Market: <b>{market}</b>")
    if narrative:
        lines.append(f"Narratives: {narrative}")

    signals = payload.get("signals", []) or []
    if signals:
        lines.append("\n<b>Top Signals</b>")
        for s in signals:
            lines.append("\n" + _fmt_signal(s))
    else:
        lines.append("\n<i>No signals found in the last 24h window.</i>")

    return _truncate("\n".join(lines).strip())


def format_signals(title: str, signals: List[Dict[str, Any]]) -> str:
    lines: List[str] = [f"<b>{escape_html(title)}</b>"]
    if not signals:
        lines.append("<i>No items found.</i>")
        return "\n".join(lines)
    for s in signals:
        lines.append("\n" + _fmt_signal(s))
    return _truncate("\n".join(lines).strip())


# --- Backwards-compat aliases (older handlers imported these) ---


def format_section(title: str, signals: List[Dict[str, Any]]) -> str:
    return format_signals(title, signals)


def escape_md(text: Any) -> str:  # pragma: no cover
    # Legacy name kept to avoid import errors; HTML formatting no longer needs it.
    return escape_html(text)

