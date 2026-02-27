"""Per-command prompt builders for the AI layer.

Each builder accepts a list of signal dicts (from SQLite) and returns
a single prompt string. Prompts are:
- Web3-native and non-generic
- Grounded only in the signals provided (no hallucination instructions)
- Structured for readable Telegram output (plain text sections)
- Capped in size to avoid token waste

Signal schema passed in:
  title, url, source, description, published_at, score, sentiment,
  ecosystem, tags, chain, sector (some may be absent/empty)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

MAX_SIGNALS_IN_PROMPT = 15   # cap to keep prompt size manageable
MAX_DESC_CHARS = 200         # truncate description per signal


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _format_signal(s: Dict[str, Any], idx: int) -> str:
    title = (s.get("title") or "(untitled)").strip()[:160]
    url = (s.get("url") or "").strip()
    desc = (s.get("description") or "").strip()[:MAX_DESC_CHARS]
    source = s.get("source", "")
    chain = s.get("chain", "")
    sector = s.get("sector", "")
    score = s.get("score", 0)
    sentiment = s.get("sentiment", 0)

    line = f"{idx}. [{source}] {title}"
    if chain and chain != "unknown":
        line += f" | chain:{chain}"
    if sector and sector != "unknown":
        line += f" | sector:{sector}"
    try:
        line += f" | score:{float(score):.1f} sent:{float(sentiment):.2f}"
    except Exception:
        pass
    if url:
        line += f"\n   url: {url}"
    if desc:
        line += f"\n   {desc}"
    return line


def _signals_block(signals: List[Dict[str, Any]]) -> str:
    capped = signals[:MAX_SIGNALS_IN_PROMPT]
    return "\n\n".join(_format_signal(s, i + 1) for i, s in enumerate(capped))


_SYSTEM_PRELUDE = (
    "You are a senior Web3 intelligence analyst. "
    "Analyse ONLY the data provided below — do not invent facts, prices, or events not present. "
    "If price data is not present, say so explicitly. "
    "Be concise, structured, and Web3-native (not generic finance). "
    "Output plain text with clear section headers using ALL-CAPS (e.g. SUMMARY, KEY SIGNALS, WHAT TO WATCH). "
    "Keep total response under 900 words."
)


# ──────────────────────────────────────────────────────────────────────────────
# /dailybrief
# ──────────────────────────────────────────────────────────────────────────────

def dailybrief_prompt(payload: Dict[str, Any]) -> str:
    """Build prompt from the full daily payload dict (from build_daily_payload)."""
    date = payload.get("date", _utcnow())
    sections = payload.get("sections", {})
    market_tone = payload.get("analysis", {}).get("market_tone", {})

    # Collect signals per section
    section_blocks: List[str] = []
    for header, sigs in sections.items():
        if not sigs:
            continue
        block = f"=== {header.upper()} ===\n{_signals_block(list(sigs)[:8])}"
        section_blocks.append(block)

    signals_text = "\n\n".join(section_blocks) if section_blocks else "(no signals)"
    tone_str = market_tone.get("market_tone", "unknown") if isinstance(market_tone, dict) else str(market_tone)

    return f"""{_SYSTEM_PRELUDE}

DATE: {date}
CURRENT MARKET TONE (from classifier): {tone_str}

SIGNALS BY CATEGORY:
{signals_text}

---
YOUR TASK — Daily Brief Analysis:

MARKET TONE INDICATOR
- State the overall market tone (risk-on / risk-off / neutral / mixed).
- Explain what is driving it based ONLY on the signals above.
- NOTE: Price data not provided; sentiment inferred from news/flow only unless price signals appear above.

TOP SIGNALS (pick 3–5 most important across all categories)
- Title + why it matters + second-order implications
- Include the URL for each

NEWS HIGHLIGHTS
FUNDING HIGHLIGHTS
ECOSYSTEM HIGHLIGHTS
GITHUB HIGHLIGHTS
TWITTER/SOCIAL HIGHLIGHTS (if present; skip section if empty)

WHAT TO WATCH NEXT (2–3 actionable items for the next 24–48h)
"""


# ──────────────────────────────────────────────────────────────────────────────
# /trends
# ──────────────────────────────────────────────────────────────────────────────

def trends_prompt(signals: List[Dict[str, Any]], trends_data: Optional[Dict] = None) -> str:
    trends_text = ""
    if trends_data:
        rows = trends_data.get("trends") or []
        if rows:
            trend_lines = []
            for r in rows[:10]:
                chain = r.get("chain", "?")
                sector = r.get("sector", "?")
                count = r.get("count", 0)
                score_sum = round(float(r.get("score_sum", 0)), 2)
                trend_lines.append(f"  - {chain} × {sector}: {count} signals, scoreΣ={score_sum}")
            trends_text = "CLUSTER DATA (chain × sector):\n" + "\n".join(trend_lines)

    signals_text = _signals_block(signals) if signals else "(no signals)"

    return f"""{_SYSTEM_PRELUDE}

DATE: {_utcnow()}

{trends_text}

TOP SIGNALS (for context):
{signals_text}

---
YOUR TASK — Deep Market Narrative & Trend Analysis:

MARKET NARRATIVE
- What is the dominant theme right now in Web3? (not just positive/negative — explain drivers)
- Which chains / sectors are getting the most signal volume and why?

SENTIMENT DRIVERS
- What specific events or flows are moving sentiment?
- Bullish drivers vs. bearish/risk signals — list separately

TREND CLUSTERS
- Explain the top chain × sector clusters above
- What does the cross-section tell us about where capital/dev attention is moving?

RISKS & COUNTER-SIGNALS
- What could invalidate the current trend?
- Any divergences or red flags in the data?

WHAT TO WATCH NEXT (2–3 items, practical and specific)
"""


# ──────────────────────────────────────────────────────────────────────────────
# /news
# ──────────────────────────────────────────────────────────────────────────────

def news_prompt(signals: List[Dict[str, Any]]) -> str:
    signals_text = _signals_block(signals) if signals else "(no news signals)"

    return f"""{_SYSTEM_PRELUDE}

DATE: {_utcnow()}

NEWS SIGNALS:
{signals_text}

---
YOUR TASK — News Analysis:

KEY STORIES (pick top 3–5)
- For each: what happened, why it matters for Web3, include the URL

NARRATIVE THREAD
- Is there a common theme across today's news? (regulatory, tech, adoption, funding?)

IMPACT ASSESSMENT
- Short-term market/dev implications (next 24–72h)
- Medium-term implications (if applicable)

WHAT TO WATCH NEXT
- 2–3 specific follow-on events or metrics to monitor
"""


# ──────────────────────────────────────────────────────────────────────────────
# /funding
# ──────────────────────────────────────────────────────────────────────────────

def funding_prompt(signals: List[Dict[str, Any]]) -> str:
    signals_text = _signals_block(signals) if signals else "(no funding/ecosystem signals)"

    return f"""{_SYSTEM_PRELUDE}

DATE: {_utcnow()}

FUNDING & ECOSYSTEM SIGNALS:
{signals_text}

---
YOUR TASK — Funding & Ecosystem Analysis:

KEY RAISES & ECOSYSTEM MOVES (top 3–5)
- Project name, amount (if present), sector, investors (if present), include URL
- Why this raise/move matters: narrative fit, team credibility, timing

CAPITAL FLOW PATTERNS
- Which sectors are attracting funding? (infra, DeFi, gaming, L2, etc.)
- Any notable investor patterns or repeat backers?

ECOSYSTEM IMPLICATIONS
- What does this funding activity signal about the next 3–6 months?
- Developer / protocol activity implications

WHAT TO WATCH NEXT
- 2–3 follow-on signals: token launches, mainnet timelines, governance votes, etc.
"""


# ──────────────────────────────────────────────────────────────────────────────
# /github
# ──────────────────────────────────────────────────────────────────────────────

def github_prompt(signals: List[Dict[str, Any]]) -> str:
    signals_text = _signals_block(signals) if signals else "(no GitHub signals)"

    return f"""{_SYSTEM_PRELUDE}

DATE: {_utcnow()}

GITHUB ACTIVITY SIGNALS:
{signals_text}

---
YOUR TASK — GitHub / Developer Activity Analysis:

HOT REPOSITORIES (top 3–5 by significance)
- Repo name, what it does, why developer attention here matters, include URL
- Stars / activity trend if available in the data

DEVELOPER FOCUS AREAS
- Which chains / protocols are seeing the most dev activity?
- Any notable new tooling, protocol upgrades, or infrastructure pushes?

TECH IMPLICATIONS
- What does this dev activity predict for product/protocol timelines?
- Indexer / oracle / bridge / L2 implications?

WHAT TO WATCH NEXT
- Upcoming releases, audit completions, testnet → mainnet transitions visible in data
"""


# ──────────────────────────────────────────────────────────────────────────────
# /newprojects
# ──────────────────────────────────────────────────────────────────────────────

def newprojects_prompt(signals: List[Dict[str, Any]]) -> str:
    signals_text = _signals_block(signals) if signals else "(no new project signals)"

    return f"""{_SYSTEM_PRELUDE}

DATE: {_utcnow()}

NEW PROJECT SIGNALS (Twitter + GitHub):
{signals_text}

---
YOUR TASK — New Projects & Emerging Players Analysis:

STANDOUT NEW PROJECTS (top 3–5)
- Project name, what it claims to do, sector, include URL
- Credibility signals: team, backers, repo activity, social traction

TREND FIT
- Which current narratives do these projects align with? (AI+Web3, L2, DeFi, etc.)
- Are they genuinely novel or repackaging existing concepts?

RISK FLAGS
- Any projects that look like hype without substance?
- Missing signals (no GitHub, no team, no traction)?

WHAT TO WATCH NEXT
- Which projects are worth deeper due diligence in the next 48–72h?
"""
