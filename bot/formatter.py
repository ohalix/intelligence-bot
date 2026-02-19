from __future__ import annotations

from typing import Any, Dict, List
import re
import html


def escape_md(text: Any) -> str:
    """Escape Telegram MarkdownV2 reserved characters.

    Telegram MarkdownV2 reserved chars:
    _ * [ ] ( ) ~ ` > # + - = | { } . !

    Note: We keep this for compatibility (some commands may still use MarkdownV2).
    """
    return re.sub(r"([_\*\[\]()~`>#+\-=|{}.!])", r"\\\1", str(text))


def escape_html_text(text: Any) -> str:
    """Escape text content for Telegram HTML parse mode."""
    if text is None:
        return ""
    return html.escape(str(text), quote=False)


def escape_html_attr(text: Any) -> str:
    """Escape attribute values (e.g., href) for Telegram HTML parse mode."""
    if text is None:
        return ""
    return html.escape(str(text), quote=True)


# -------------------------
# MarkdownV2 formatters
# -------------------------

def format_signal(s: Dict[str, Any]) -> str:
    title = escape_md(s.get("title", "(no title)"))
    url = (s.get("url") or "").strip()
    score = s.get("signal_score", 0.0)
    chain = escape_md(s.get("chain", "unknown"))
    sector = escape_md(s.get("sector", "unknown"))
    short = escape_md((s.get("description", "") or "")[:140].replace("\n", " "))
    line1 = f"*{title}*"
    line2 = f"_{chain} · {sector}_  |  *score:* {score}"
    line3 = short if short else ""
    if url:
        # In MarkdownV2, URLs inside () must also be escaped minimally; we keep as-is.
        line4 = f"[link]({url})"
    else:
        line4 = ""
    return "\n".join([x for x in [line1, line2, line3, line4] if x]).strip()


def format_dailybrief(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    parts.append(f"*Daily Brief — {escape_md(payload.get('date',''))}*")

    mt = payload.get("analysis", {}).get("market_tone", {})
    tone = escape_md(mt.get("market_tone", "neutral"))
    conf = mt.get("confidence", 0)
    parts.append(f"_Market tone:_ *{tone}* _(conf {conf})_")
    parts.append("")

    for header, signals in payload.get("sections", {}).items():
        parts.append(f"*{escape_md(header)}*")
        if not signals:
            # Use \\. to avoid Python's invalid escape sequence warning.
            parts.append("_No signals in the last 24h\\._")
            parts.append("")
            continue
        for s in signals:
            parts.append(format_signal(s))
            parts.append("")

    summary = payload.get("analysis", {}).get("summary")
    if summary:
        parts.append("*AI Analysis*")
        parts.append(escape_md(summary))

    return "\n".join(parts).strip()


# -------------------------
# HTML formatters (preferred for reliability)
# -------------------------

def format_signal_html(s: Dict[str, Any]) -> str:
    title = escape_html_text(s.get("title", "(no title)"))
    url = (s.get("url") or "").strip()
    score = escape_html_text(s.get("signal_score", 0.0))
    chain = escape_html_text(s.get("chain", "unknown"))
    sector = escape_html_text(s.get("sector", "unknown"))
    short = escape_html_text((s.get("description", "") or "")[:180].replace("\n", " "))

    block = f"<b>{title}</b>\n<i>{chain} · {sector}</i>  |  <b>score:</b> {score}"
    if short:
        block += f"\n{short}"
    if url:
        block += f"\n<a href=\"{escape_html_attr(url)}\">link</a>"
    return block


def format_section_html(header: str, signals: List[Dict[str, Any]]) -> str:
    out: List[str] = [f"<b>{escape_html_text(header)}</b>"]
    if not signals:
        out.append("<i>No signals in the last 24h.</i>")
        return "\n".join(out)
    for s in signals:
        out.append(format_signal_html(s))
        out.append("")
    return "\n".join(out).strip()


def format_dailybrief_html(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    parts.append(f"<b>Daily Brief — {escape_html_text(payload.get('date',''))}</b>")

    mt = payload.get("analysis", {}).get("market_tone", {})
    tone = escape_html_text(mt.get("market_tone", "neutral"))
    conf = escape_html_text(mt.get("confidence", 0))
    parts.append(f"<i>Market tone:</i> <b>{tone}</b> <i>(conf {conf})</i>")
    parts.append("")

    for header, signals in payload.get("sections", {}).items():
        parts.append(format_section_html(header, signals))
        parts.append("")

    summary = payload.get("analysis", {}).get("summary")
    if summary:
        parts.append("<b>AI Analysis</b>")
        parts.append(escape_html_text(summary))

    return "\n".join(parts).strip()
