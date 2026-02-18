import json
import os
from pathlib import Path
from typing import Any, Dict, List

import yaml

DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
DEFAULT_ECOSYSTEMS_PATH = Path(__file__).resolve().parents[1] / "config" / "ecosystems.json"

# Reasonable defaults so the bot works out of the box (can be overridden via env)
DEFAULT_NEWS_SOURCES: List[str] = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
]

DEFAULT_FUNDING_SOURCES: List[str] = [
    "https://blockworks.co/feed",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]

# Optimism's old blog domain can be flaky; the governance forum provides RSS and is official.
DEFAULT_ECOSYSTEM_SOURCES: List[str] = [
    "https://blog.arbitrum.io/rss/",
    "https://gov.optimism.io/latest.rss",
    "https://solana.com/rss.xml",
]

DEFAULT_GITHUB_TOPICS: List[str] = [
    "rollup",
    "l2",
    "bridge",
    "defi",
]

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

def _env_csv(name: str) -> List[str]:
    v = os.getenv(name, "")
    return [s.strip() for s in v.split(",") if s.strip()]

def load_config() -> Dict[str, Any]:
    with open(DEFAULT_SETTINGS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    with open(DEFAULT_ECOSYSTEMS_PATH, "r", encoding="utf-8") as f:
        config["ecosystems"] = json.load(f)

    config.setdefault("bot", {})
    config["bot"]["telegram_token"] = os.getenv("TELEGRAM_BOT_TOKEN", config["bot"].get("telegram_token"))
    config["bot"]["chat_id"] = os.getenv("TELEGRAM_CHAT_ID", config["bot"].get("chat_id"))
    config["bot"]["timezone"] = os.getenv("TIMEZONE", config["bot"].get("timezone", "Africa/Lagos"))

    config.setdefault("scheduler", {})
    config["scheduler"]["run_interval_hours"] = int(os.getenv("RUN_INTERVAL_HOURS", config["scheduler"].get("run_interval_hours", 24)))

    config.setdefault("analysis", {})
    config["analysis"]["top_signals_to_analyze"] = int(os.getenv("MAX_SIGNALS", config["analysis"].get("top_signals_to_analyze", 10)))

    config.setdefault("ingestion", {})
    config["ingestion"]["twitter_mode"] = os.getenv("TWITTER_MODE", config["ingestion"].get("twitter_mode", "none")).lower()

    # Sources (env overrides defaults)
    config["ingestion"]["news_sources"] = _env_csv("NEWS_SOURCES") or list(DEFAULT_NEWS_SOURCES)
    config["ingestion"]["funding_sources"] = _env_csv("FUNDING_SOURCES") or list(DEFAULT_FUNDING_SOURCES)
    config["ingestion"]["ecosystem_sources"] = _env_csv("ECOSYSTEM_SOURCES") or list(DEFAULT_ECOSYSTEM_SOURCES)
    config["ingestion"]["github_topics"] = _env_csv("GITHUB_TOPICS") or list(DEFAULT_GITHUB_TOPICS)

    config["ingestion"]["twitter_rss_sources"] = _env_csv("TWITTER_RSS_SOURCES")

    config["keys"] = {
        "openai": os.getenv("OPENAI_API_KEY"),
        "anthropic": os.getenv("ANTHROPIC_API_KEY"),
        "twitter_bearer": os.getenv("TWITTER_BEARER_TOKEN"),
        "github_token": os.getenv("GITHUB_TOKEN"),
    }

    config["dry_mode"] = _env_bool("DRY_MODE", False)
    config["offline_test"] = _env_bool("OFFLINE_TEST", False)
    return config
