import json
import os
from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
DEFAULT_ECOSYSTEMS_PATH = Path(__file__).resolve().parents[1] / "config" / "ecosystems.json"

# -----------------------------
# Ingestion defaults (stable, high-signal)
# -----------------------------

# News
DEFAULT_NEWS_RSS_SOURCES = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://decrypt.co/feed",
]
DEFAULT_NEWS_WEB_SOURCES = [
    "https://decrypt.co/news",
    "https://www.coindesk.com/",
]
DEFAULT_NEWS_API_SOURCES = [
    # Public, no-key: cryptocurrency.cv
    "cryptocurrency_cv",
    # Free-keyed: CoinMarketCap /v1/content/posts/latest can be unavailable on
    # free plans (403 "plan doesn't support"). Keep opt-in via NEWS_API_SOURCES.
]

# Ecosystem (official blogs + governance)
DEFAULT_ECOSYSTEM_RSS_SOURCES = [
    "https://blog.ethereum.org/feed.xml",
    "https://blog.arbitrum.io/rss/",
]
DEFAULT_ECOSYSTEM_WEB_SOURCES = [
    "https://blog.optimism.io/",
    "https://www.starknet.io/en/content/",
]
DEFAULT_ECOSYSTEM_API_SOURCES = [
    # Public, no-key: Snapshot Hub GraphQL (governance proposals)
    "snapshot_proposals",
    # NOTE: defillama_chain_tvl removed from defaults — it was a no-op returning []
    # and was misleading in /sources output. Can be re-enabled via ECOSYSTEM_API_SOURCES env var
    # once a proper schema is defined.
]

# Funding
DEFAULT_FUNDING_RSS_SOURCES = []
DEFAULT_FUNDING_WEB_SOURCES = [
    "https://www.coindesk.com/tag/venture-capital/",
    # Decrypt funding tag URL is currently 404 in production logs; keep opt-in
    # via FUNDING_WEB_SOURCES / FUNDING_WEB_EXTRA_SOURCES.
]
DEFAULT_FUNDING_API_SOURCES = [
    # Public, no-key: DefiLlama raises
    "defillama_raises",
    # Free-keyed: CoinMarketCal events (disabled if no key)
    "coinmarketcal_events",
]

# GitHub input defaults
DEFAULT_GITHUB_QUERIES = [
    # High-signal OSS activity queries. Users can override via env.
    "topic:ethereum language:solidity pushed:>2025-01-01",
    "topic:layer-2 language:solidity pushed:>2025-01-01",
    "topic:defi language:solidity pushed:>2025-01-01",
    "topic:zk language:rust pushed:>2025-01-01",
]

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1","true","yes","y","on"}


def _env_csv(name: str) -> list[str] | None:
    """Parse a comma-separated env var.

    Returns None if the env var is unset OR empty/whitespace (meaning: no override).
    """
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    return [s.strip() for s in v.split(",") if s.strip()]


def _merge_sources(defaults: list[str], override: list[str] | None, extra: list[str] | None) -> list[str]:
    """Compatibility-first merge for ingestion sources."""
    base = list(override) if override is not None else list(defaults)
    if extra:
        seen = set(base)
        for s in extra:
            if s not in seen:
                base.append(s)
                seen.add(s)
    return base

def load_config() -> Dict[str, Any]:
    with open(DEFAULT_SETTINGS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    with open(DEFAULT_ECOSYSTEMS_PATH, "r", encoding="utf-8") as f:
        config["ecosystems"] = json.load(f)

    config.setdefault("bot", {})
    config["bot"]["telegram_token"] = os.getenv("TELEGRAM_BOT_TOKEN", config["bot"].get("telegram_token"))
    config["bot"]["chat_id"] = os.getenv("TELEGRAM_CHAT_ID", config["bot"].get("chat_id"))
    # Optional admin chat id for one-time startup notifications.
    # If unset, startup notice is skipped (must never crash bot).
    config["bot"]["admin_chat_id"] = os.getenv("ADMIN_CHAT_ID", config["bot"].get("admin_chat_id"))
    config["bot"]["timezone"] = os.getenv("TIMEZONE", config["bot"].get("timezone", "Africa/Lagos"))

    # Backward/forward compatible aliases used across repo iterations.
    # Some runtime paths expect config['bot']['token'].
    config["bot"].setdefault("token", config["bot"].get("telegram_token"))
    if config["bot"].get("telegram_token") and not config["bot"].get("token"):
        config["bot"]["token"] = config["bot"]["telegram_token"]

    config.setdefault("scheduler", {})
    config["scheduler"]["run_interval_hours"] = int(os.getenv("RUN_INTERVAL_HOURS", config["scheduler"].get("run_interval_hours", 24)))
    config.setdefault("analysis", {})
    config["analysis"]["top_signals_to_analyze"] = int(os.getenv("MAX_SIGNALS", config["analysis"].get("top_signals_to_analyze", 10)))

    config.setdefault("ingestion", {})
    config["ingestion"]["twitter_mode"] = os.getenv("TWITTER_MODE", config["ingestion"].get("twitter_mode", "none")).lower()

    # -----------------------------
    # Ingestion source configuration
    # -----------------------------
    # Back-compat: NEWS_SOURCES is treated as an RSS override for news.
    config["ingestion"]["news_sources"] = _merge_sources(
        DEFAULT_NEWS_RSS_SOURCES,
        _env_csv("NEWS_SOURCES"),
        _env_csv("NEWS_RSS_EXTRA_SOURCES"),
    )
    config["ingestion"]["news_web_sources"] = _merge_sources(
        DEFAULT_NEWS_WEB_SOURCES,
        _env_csv("NEWS_WEB_SOURCES"),
        _env_csv("NEWS_WEB_EXTRA_SOURCES"),
    )
    config["ingestion"]["news_api_sources"] = _merge_sources(
        DEFAULT_NEWS_API_SOURCES,
        _env_csv("NEWS_API_SOURCES"),
        _env_csv("NEWS_API_EXTRA_SOURCES"),
    )

    config["ingestion"]["ecosystem_rss_sources"] = _merge_sources(
        DEFAULT_ECOSYSTEM_RSS_SOURCES,
        _env_csv("ECOSYSTEM_RSS_SOURCES"),
        _env_csv("ECOSYSTEM_RSS_EXTRA_SOURCES"),
    )
    config["ingestion"]["ecosystem_web_sources"] = _merge_sources(
        DEFAULT_ECOSYSTEM_WEB_SOURCES,
        _env_csv("ECOSYSTEM_WEB_SOURCES"),
        _env_csv("ECOSYSTEM_WEB_EXTRA_SOURCES"),
    )
    config["ingestion"]["ecosystem_api_sources"] = _merge_sources(
        DEFAULT_ECOSYSTEM_API_SOURCES,
        _env_csv("ECOSYSTEM_API_SOURCES"),
        _env_csv("ECOSYSTEM_API_EXTRA_SOURCES"),
    )

    # Snapshot governance spaces (used by ecosystem API ingestion)
    default_spaces = [
        "arbitrum",
        "opcollective.eth",
        "aave.eth",
        "uniswap",
        "starknet",
        "polygon",
        "zksync",
        "scroll",
        "base",
    ]
    config["ingestion"]["snapshot_spaces"] = _merge_sources(
        default_spaces,
        _env_csv("ECOSYSTEM_SNAPSHOT_SPACES"),
        _env_csv("ECOSYSTEM_SNAPSHOT_EXTRA_SPACES"),
    )

    config["ingestion"]["funding_rss_sources"] = _merge_sources(
        DEFAULT_FUNDING_RSS_SOURCES,
        _env_csv("FUNDING_RSS_SOURCES"),
        _env_csv("FUNDING_RSS_EXTRA_SOURCES"),
    )
    config["ingestion"]["funding_web_sources"] = _merge_sources(
        DEFAULT_FUNDING_WEB_SOURCES,
        _env_csv("FUNDING_WEB_SOURCES"),
        _env_csv("FUNDING_WEB_EXTRA_SOURCES"),
    )
    config["ingestion"]["funding_api_sources"] = _merge_sources(
        DEFAULT_FUNDING_API_SOURCES,
        _env_csv("FUNDING_API_SOURCES"),
        _env_csv("FUNDING_API_EXTRA_SOURCES"),
    )

    # Twitter RSS sources
    config["ingestion"]["twitter_rss_sources"] = _merge_sources(
        config["ingestion"].get("twitter_rss_sources", []),
        _env_csv("TWITTER_RSS_SOURCES"),
        _env_csv("TWITTER_RSS_EXTRA_SOURCES"),
    )

    # GitHub inputs (still a platform integration, but configurable)
    config.setdefault("github", {})
    config["github"].setdefault("queries", DEFAULT_GITHUB_QUERIES)
    config["github"]["queries"] = _merge_sources(
        config["github"]["queries"],
        _env_csv("GITHUB_QUERIES"),
        _env_csv("GITHUB_EXTRA_QUERIES"),
    )

    config["keys"] = {
        # AI providers — new HF→Gemini pathway
        "hf_token": os.getenv("HF_TOKEN"),
        "gemini_api_key": os.getenv("GEMINI_API_KEY"),
        # Legacy keys — kept in config dict for backward-compat but no longer used
        # in the main AI pathway. Set them if you still need the old agent shim.
        "openai": os.getenv("OPENAI_API_KEY"),
        "anthropic": os.getenv("ANTHROPIC_API_KEY"),
        # Ingestion keys
        "twitter_bearer": os.getenv("TWITTER_BEARER_TOKEN"),
        "github_token": os.getenv("GITHUB_TOKEN"),
        # Optional free-keyed APIs (ingestion skips gracefully if missing)
        "coinmarketcap": os.getenv("COINMARKETCAP_API_KEY"),
        "coinmarketcal": os.getenv("COINMARKETCAL_API_KEY"),
    }

    config["dry_mode"] = _env_bool("DRY_MODE", False)
    config["offline_test"] = _env_bool("OFFLINE_TEST", False)

    # Storage compatibility: settings.yaml uses storage.database_path, while
    # some callers (e.g. main.py) expect storage.db_path.
    config.setdefault("storage", {})
    if "db_path" not in config["storage"]:
        dbp = config["storage"].get("database_path") or "./data/web3_intelligence.db"
        config["storage"]["db_path"] = dbp
    return config
