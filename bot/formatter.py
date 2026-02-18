from typing import Any, Dict, List
import re

def escape_md(text: str) -> str:
    # Telegram MarkdownV2 escaping (minimal)
    if text is None:
        return ""
    return re.sub(r'([_\*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(text))

def format_signal(s: Dict[str, Any]) -> str:
    title = escape_md(s.get("title","(no title)"))
    url = s.get("url","")
    # Numbers like 0.82 contain '.' which is reserved in MarkdownV2.
    score = escape_md(str(s.get("signal_score", 0.0)))
    chain = escape_md(s.get("chain","unknown"))
    sector = escape_md(s.get("sector","unknown"))
    short = escape_md((s.get("description","") or "")[:140].replace("\n"," "))
    link = f"[link]({escape_md(url)})" if url else ""
    return f"*{title}*\n_{chain} · {sector}_  |  score: *{score}*\n{short}\n{link}"

def format_section(header: str, signals: List[Dict[str, Any]]) -> str:
    out = [f"*{escape_md(header)}*"]
    if not signals:
        out.append("_No signals in the last 24h._")
        return "\n".join(out)
    for s in signals:
        out.append(format_signal(s))
        out.append("")  # spacer
    return "\n".join(out).strip()

def format_dailybrief(payload: Dict[str, Any]) -> str:
    parts = []
    parts.append(f"*Daily Brief — {escape_md(payload.get('date',''))}*")
    mt = payload.get("analysis", {}).get("market_tone", {})
    # Parentheses and '.' must be escaped in MarkdownV2.
    conf = escape_md(str(mt.get("confidence", 0)))
    parts.append(
        f"_Market tone:_ *{escape_md(mt.get('market_tone','neutral'))}* \\(conf {conf}\\)"
    )
    parts.append("")
    for header, signals in payload.get("sections", {}).items():
        parts.append(format_section(header, signals))
        parts.append("")
    # analysis summary
    summary = payload.get("analysis", {}).get("summary")
    if summary:
        parts.append("*AI Analysis*")
        parts.append(escape_md(summary))
    return "\n".join(parts).strip()
