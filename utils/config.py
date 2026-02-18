import json
import os
from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
DEFAULT_ECOSYSTEMS_PATH = Path(__file__).resolve().parents[1] / "config" / "ecosystems.json"


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> Dict[str, Any]:
    # Base config files
    with open(DEFAULT_SETTINGS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    with open(DEFAULT_ECOSYSTEMS_PATH, "r", encoding="utf-8") as f:
        config["ecosystems"] = json.load(f)

    # --- Bot / runtime ---
    config.setdefault("bot", {})
    bot = config["bot"]
    bot["telegram_token"] = os.getenv("TELEGRAM_BOT_TOKEN", bot.get("telegram_token"))
    # spec name:
    bot["telegram_chat_id"] = os.getenv("TELEGRAM_CHAT_ID", bot.get("telegram_chat_id") or bot.get("chat_id"))
    # backward compat:
    bot["chat_id"] = bot["telegram_chat_id"]
    bot["timezone"] = os.getenv("TIMEZONE", bot.get("timezone", "Africa/Lagos"))
    bot["max_signals"] = int(os.getenv("MAX_SIGNALS", bot.get("max_signals", 10)))

    # --- Scheduler ---
    config.setdefault("scheduler", {})
    sched = config["scheduler"]
    sched["run_interval_hours"] = int(os.getenv("RUN_INTERVAL_HOURS", sched.get("run_interval_hours", 24)))

    # --- Ingestion ---
    config.setdefault("ingestion", {})
    ing = config["ingestion"]
    ing["twitter_mode"] = os.getenv("TWITTER_MODE", ing.get("twitter_mode", "none")).lower()

    news_sources_env = os.getenv("NEWS_SOURCES", "")
    if news_sources_env.strip():
        ing["news_sources"] = [s.strip() for s in news_sources_env.split(",") if s.strip()]

    twitter_rss_env = os.getenv("TWITTER_RSS_SOURCES", "")
    if twitter_rss_env.strip():
        ing["twitter_rss_sources"] = [s.strip() for s in twitter_rss_env.split(",") if s.strip()]

    # --- AI / analysis layer ---
    config.setdefault("ai", {})
    ai = config["ai"]
    ai["openai_api_key"] = os.getenv("OPENAI_API_KEY", ai.get("openai_api_key"))
    ai["anthropic_api_key"] = os.getenv("ANTHROPIC_API_KEY", ai.get("anthropic_api_key"))
    ai["dry_mode"] = str(os.getenv("DRY_MODE", str(ai.get("dry_mode", "true")))).lower()

    # --- Optional tokens (free-tier / public usage; tokens optional) ---
    config.setdefault("keys", {})
    config["keys"].update(
        {
            "twitter_bearer": os.getenv("TWITTER_BEARER_TOKEN", config["keys"].get("twitter_bearer")),
            "github_token": os.getenv("GITHUB_TOKEN", config["keys"].get("github_token")),
        }
    )

    # --- Test switches ---
    config["offline_test"] = _env_bool("OFFLINE_TEST", False)

    return config
