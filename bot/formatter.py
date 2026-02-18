from __future__ import annotations

from typing import Any, Dict, List
import html
import re

# --------------------------------------------------------------------------------------
# Telegram formatting helpers
#
# We support BOTH MarkdownV2 and HTML formatting.
# For stability and clean rendering, commands should prefer HTML.
# --------------------------------------------------------------------------------------

MDV2_RESERVED = r'([_\*\[\]\(\)~`>#+\-=\|\{\}\.\!])'


def escape_md(text: str) -> str:
    """Escape Telegram MarkdownV2 reserved characters in plain text."""
    if text is None:
        return ""
    return re.sub(MDV2_RESERVED, r"\\\1", str(text))


def escape_html(text: str) -> str:
    """Escape text for Telegram HTML parse_mode."""
    if text is None:
        return ""
    return html.escape(str(text), quote=True)


# ----------------------------------
# HTML output (preferred)
# ----------------------------------

def _safe_href(url: str) -> str:
    # Telegram HTML expects a valid URL; escaping quotes is still safe.
    return escape_html(url)


def format_signal_html(s: Dict[str, Any]) -> str:
    title = escape_html(s.get("title", "(no title)"))
    url = s.get("url", "")

    score = s.get("signal_score", 0.0)
    chain = escape_html(s.get("chain", "unknown"))
    sector = escape_html(s.get("sector", "unknown"))
    short = escape_html((s.get("description", "") or "")[:180].replace("\n", " "))

    # Keep it clean and readable in Telegram UI.
    head = f"<b>{title}</b>"
    meta = f"<i>{chain} · {sector}</i>  |  <b>score:</b> <code>{escape_html(score)}</code>"

    if url:
        link = f"<a href=\"{_safe_href(url)}\">open</a>"
        return f"{head}\n{meta}\n{short}\n{link}"
    return f"{head}\n{meta}\n{short}"


def format_section_html(header: str, signals: List[Dict[str, Any]]) -> str:
    out: List[str] = [f"<b>{escape_html(header)}</b>"]
    if not signals:
        out.append("<i>No signals in the last 24h.</i>")
        return "\n".join(out)
    for s in signals:
        out.append(format_signal_html(s))
        out.append("")
    return "\n".join(out).strip()


def format_dailybrief_html(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    parts.append(f"<b>Daily Brief — {escape_html(payload.get('date', ''))}</b>")

    mt = payload.get("analysis", {}).get("market_tone", {})
    tone = escape_html(mt.get("market_tone", "neutral"))
    conf = escape_html(mt.get("confidence", 0))
    parts.append(f"<i>Market tone:</i> <b>{tone}</b> <i>(conf {conf})</i>")
    parts.append("")

    sections = payload.get("sections", {})
    for header, signals in sections.items():
        parts.append(format_section_html(header, signals))
        parts.append("")

    summary = payload.get("analysis", {}).get("summary")
    if summary:
        parts.append("<b>AI Analysis</b>")
        parts.append(escape_html(summary))

    return "\n".join(parts).strip()


# ----------------------------------
# MarkdownV2 output (kept for compatibility)
# ----------------------------------

def format_signal(s: Dict[str, Any]) -> str:
    title = escape_md(s.get("title", "(no title)"))
    url = s.get("url", "")
    score = escape_md(str(s.get("signal_score", 0.0)))
    chain = escape_md(s.get("chain", "unknown"))
    sector = escape_md(s.get("sector", "unknown"))
    short = escape_md((s.get("description", "") or "")[:140].replace("\n", " "))
    link = f"[link]({escape_md(url)})" if url else ""
    return f"*{title}*\n_{chain} · {sector}_  |  score: *{score}*\n{short}\n{link}"


def format_section(header: str, signals: List[Dict[str, Any]]) -> str:
    out = [f"*{escape_md(header)}*"]
    if not signals:
        out.append("_No signals in the last 24h._")
        return "\n".join(out)
    for s in signals:
        out.append(format_signal(s))
        out.append("")
    return "\n".join(out).strip()


def format_dailybrief(payload: Dict[str, Any]) -> str:
    parts = []
    parts.append(f"*Daily Brief — {escape_md(payload.get('date',''))}*")
    mt = payload.get("analysis", {}).get("market_tone", {})
    conf = escape_md(str(mt.get("confidence", 0)))
    parts.append(
        f"_Market tone:_ *{escape_md(mt.get('market_tone','neutral'))}* \\(conf {conf}\\)"
    )
    parts.append("")
    for header, signals in payload.get("sections", {}).items():
        parts.append(format_section(header, signals))
        parts.append("")
    summary = payload.get("analysis", {}).get("summary")
    if summary:
        parts.append("*AI Analysis*")
        parts.append(escape_md(summary))
    return "\n".join(parts).strip()
