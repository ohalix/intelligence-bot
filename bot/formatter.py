from __future__ import annotations

from html.parser import HTMLParser
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
    # Telegram HTML only needs &, <, > escaped for safe rendering.
    # Escaping quotes produces visible entities (&#x27;, &quot;) if we ever fall back
    # to plain text, and it doesn't help our use-case.
    return html.escape(str(text), quote=False)


class _TextExtractor(HTMLParser):
    """Stdlib-only HTML -> text extractor for RSS/HTML fragments."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self._parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self._parts.append(f"&#{name};")

    def get_text(self) -> str:
        return "".join(self._parts)


_WS_RE = re.compile(r"\s+")


def clean_source_text(value: Any, *, max_len: int | None = None) -> str:
    """Decode entities, strip HTML tags, normalize whitespace.

    Returns plain text (no Telegram markup). Use escape_html() before inserting
    into Telegram HTML messages.
    """

    if value is None:
        return ""
    s = str(value)
    if not s:
        return ""

    # Decode entities like &amp; and &#x27;.
    s = html.unescape(s)

    # Strip tags if it looks like HTML.
    if "<" in s and ">" in s:
        try:
            p = _TextExtractor()
            p.feed(s)
            p.close()
            s = html.unescape(p.get_text())
        except Exception:
            s = re.sub(r"<[^>]+>", " ", s)

    s = _WS_RE.sub(" ", s).strip()
    if max_len is not None and max_len > 0 and len(s) > max_len:
        s = s[: max_len - 1].rstrip() + "…"
    return s


# ----------------------------------
# HTML output (preferred)
# ----------------------------------

def _safe_href(url: str) -> str:
    # Telegram HTML expects a valid URL; escaping quotes is still safe.
    return escape_html(url)


def format_signal_html(s: Dict[str, Any]) -> str:
    # Decode entities + strip HTML from upstream sources before escaping for Telegram.
    title_txt = clean_source_text(s.get("title", "(no title)"), max_len=160) or "(no title)"
    url = str(s.get("url", "") or "").strip()

    score = s.get("signal_score", 0.0)
    if isinstance(score, (int, float)):
        score_txt = f"{score:.2f}"
    else:
        score_txt = clean_source_text(score, max_len=32)

    chain_txt = clean_source_text(s.get("chain", "unknown"), max_len=40) or "unknown"
    sector_txt = clean_source_text(s.get("sector", "unknown"), max_len=60) or "unknown"
    short_txt = clean_source_text(s.get("description", "") or "", max_len=220)

    # Keep it clean and readable in Telegram UI.
    head = f"<b>{escape_html(title_txt)}</b>"
    meta = (
        f"<i>{escape_html(chain_txt)} · {escape_html(sector_txt)}</i>"
        f"  |  <b>score:</b> <code>{escape_html(score_txt)}</code>"
    )

    short = escape_html(short_txt)
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
        # Top N per section for readability + message sizing.
        parts.append(format_section_html(header, list(signals)[:5]))
        parts.append("")

    summary = payload.get("analysis", {}).get("summary")
    if summary:
        parts.append("<b>AI Analysis</b>")
        parts.append(escape_html(clean_source_text(summary, max_len=1400)))

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
