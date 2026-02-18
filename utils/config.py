import json
import os
from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
DEFAULT_ECOSYSTEMS_PATH = Path(__file__).resolve().parents[1] / "config" / "ecosystems.json"

def _env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1","true","yes","y","on"}

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
    news_sources_env = os.getenv("NEWS_SOURCES", "")
    config["ingestion"]["news_sources"] = [s.strip() for s in news_sources_env.split(",") if s.strip()]

    twitter_rss_env = os.getenv("TWITTER_RSS_SOURCES", "")
    config["ingestion"]["twitter_rss_sources"] = [s.strip() for s in twitter_rss_env.split(",") if s.strip()]

    # Optional: comma-separated list of Nitter base URLs to try as fallbacks.
    # Example: "https://nitter.net,https://nitter.poast.org"
    nitter_env = os.getenv("NITTER_INSTANCES", "")
    config["ingestion"]["nitter_instances"] = [s.strip().rstrip("/") for s in nitter_env.split(",") if s.strip()]

    config["keys"] = {
        "openai": os.getenv("OPENAI_API_KEY"),
        "anthropic": os.getenv("ANTHROPIC_API_KEY"),
        "twitter_bearer": os.getenv("TWITTER_BEARER_TOKEN"),
        "github_token": os.getenv("GITHUB_TOKEN"),
    }

    config["dry_mode"] = _env_bool("DRY_MODE", False)
    config["offline_test"] = _env_bool("OFFLINE_TEST", False)
    return config
