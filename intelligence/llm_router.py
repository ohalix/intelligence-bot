"""LLM Router: Hugging Face → Gemini fallback chain.

Primary:  Hugging Face Router (OpenAI-compatible completions API) via aiohttp
Fallback: Google Gemini via google-genai SDK (sync, run in executor)

Design:
- Fully async (bot is async; HF via aiohttp, Gemini via executor)
- 2 attempts per provider before failing over
- Classifies failures: auth / rate-limit / server / timeout / invalid-response
- Never raises to caller — returns None on total failure (caller uses non-AI path)
- 30s timeout per HF request, 25s total for Gemini executor call

Env vars:
  HF_TOKEN        — Hugging Face API token
  GEMINI_API_KEY  — Google Gemini API key
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

HF_API_URL = "https://router.huggingface.co/v1/chat/completions"
HF_MODEL = "Qwen/Qwen3-8B:fastest"
HF_TIMEOUT_SEC = 90
HF_MAX_TRIES = 2

GEMINI_MODEL = "gemini-3-flash-preview"  # stable flash preview
GEMINI_TIMEOUT_SEC = 25
GEMINI_MAX_TRIES = 2

# Step 1: Raised from 1200 → 3000 to prevent mid-sentence/mid-URL truncation.
# At ~4 chars/token, 1200 yielded ~4800 chars which could cut before model finished.
# 8000 is a safe ceiling for all commands including the richest (dailybrief).
MAX_TOKENS = 8000


# ──────────────────────────────────────────────────────────────────────────────
# Failure classification
# ──────────────────────────────────────────────────────────────────────────────

def _classify_hf_failure(status: int, body: str) -> str:
    if status in (401, 403):
        return f"auth_error(HTTP {status})"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return f"server_error(HTTP {status})"
    return f"client_error(HTTP {status})"


def _extract_hf_text(data: Any) -> Optional[str]:
    """Pull content from OpenAI-compatible response. Returns None if malformed."""
    try:
        content = data["choices"][0]["message"]["content"]
        if isinstance(content, str) and content.strip():
            return content.strip()
    except (KeyError, IndexError, TypeError):
        pass
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Hugging Face provider (async via aiohttp)
# ──────────────────────────────────────────────────────────────────────────────

async def _call_hf(prompt: str, hf_token: str) -> Optional[str]:
    """Try HF Router up to HF_MAX_TRIES times. Returns text or None."""
    headers = {
        "Authorization": f"Bearer {hf_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": HF_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MAX_TOKENS,
        "temperature": 0.4,
        "TopP": 0.95,
        "TopK": 20,
        "MinP": 0,
        "enable_thinking": True,
    }
    timeout = aiohttp.ClientTimeout(total=HF_TIMEOUT_SEC)

    for attempt in range(1, HF_MAX_TRIES + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.post(HF_API_URL, headers=headers, json=payload) as resp:
                    body = await resp.text()
                    if resp.status != 200:
                        reason = _classify_hf_failure(resp.status, body)
                        logger.warning(
                            "HF attempt %s/%s failed: %s — %s",
                            attempt, HF_MAX_TRIES, reason, body[:120],
                        )
                        # Auth/rate-limit: no point retrying immediately
                        if resp.status in (401, 403, 429):
                            return None
                        continue
                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError:
                        logger.warning(
                            "HF attempt %s/%s: invalid JSON in response: %s",
                            attempt, HF_MAX_TRIES, body[:120],
                        )
                        continue
                    text = _extract_hf_text(data)
                    if text:
                        logger.info("HF success on attempt %s (len=%s)", attempt, len(text))
                        # Step 4: Extract and log finish_reason for truncation observability.
                        try:
                            finish_reason = data["choices"][0].get("finish_reason")
                            if finish_reason == "length":
                                logger.warning(
                                    "HF response truncated at token limit "
                                    "(finish_reason=length) — consider raising MAX_TOKENS. "
                                    "Current MAX_TOKENS=%s",
                                    MAX_TOKENS,
                                )
                            else:
                                logger.info(
                                    "HF response finish_reason=%s", finish_reason
                                )
                        except (KeyError, IndexError, TypeError):
                            pass
                        return text
                    logger.warning(
                        "HF attempt %s/%s: response shape unexpected: %s",
                        attempt, HF_MAX_TRIES, str(data)[:120],
                    )
        except asyncio.TimeoutError:
            logger.warning(
                "HF attempt %s/%s: timeout after %ss",
                attempt, HF_MAX_TRIES, HF_TIMEOUT_SEC,
            )
        except aiohttp.ClientError as exc:
            logger.warning("HF attempt %s/%s: client error: %s", attempt, HF_MAX_TRIES, exc)
        except Exception as exc:
            logger.warning("HF attempt %s/%s: unexpected error: %s", attempt, HF_MAX_TRIES, exc)

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Gemini provider (sync SDK → executor)
# ──────────────────────────────────────────────────────────────────────────────

def _call_gemini_sync(prompt: str, api_key: str) -> Optional[str]:
    """Sync Gemini call. Runs inside executor. Returns text or None."""
    try:
        import google.genai as genai  # type: ignore
        client = genai.Client(api_key=api_key)

        # Step 1: Add GenerationConfig with max_output_tokens=3000 for parity
        # with HF cap and to prevent unbounded Gemini output.
        gen_config = None
        try:
            from google.genai import types as genai_types  # type: ignore
            gen_config = genai_types.GenerateContentConfig(max_output_tokens=15000)
        except Exception:
            pass  # graceful fallback if SDK shape differs

        kwargs: dict = {
            "model": "gemini-3-flash-preview",
            "contents": prompt,
        }
        if gen_config is not None:
            kwargs["config"] = gen_config

        response = client.models.generate_content(**kwargs)
        text = getattr(response, "text", None)
        if text and text.strip():
            return text.strip()
        logger.warning("Gemini returned empty text")
        return None
    except Exception as exc:
        logger.warning("Gemini sync call failed: %s", exc)
        return None


async def _call_gemini(prompt: str, api_key: str) -> Optional[str]:
    """Async wrapper for Gemini. Up to GEMINI_MAX_TRIES attempts."""
    loop = asyncio.get_event_loop()
    for attempt in range(1, GEMINI_MAX_TRIES + 1):
        try:
            text = await asyncio.wait_for(
                loop.run_in_executor(None, _call_gemini_sync, prompt, api_key),
                timeout=GEMINI_TIMEOUT_SEC,
            )
            if text:
                logger.info("Gemini success on attempt %s (len=%s)", attempt, len(text))
                return text
        except asyncio.TimeoutError:
            logger.warning(
                "Gemini attempt %s/%s: timeout after %ss",
                attempt, GEMINI_MAX_TRIES, GEMINI_TIMEOUT_SEC,
            )
        except Exception as exc:
            logger.warning("Gemini attempt %s/%s: error: %s", attempt, GEMINI_MAX_TRIES, exc)

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public router
# ──────────────────────────────────────────────────────────────────────────────

async def route_llm(prompt: str, config: dict) -> Optional[str]:
    """Route prompt through HF → Gemini → None.

    Returns the LLM response text, or None if both providers fail.
    Never raises. Caller must handle None (use non-AI fallback).

    Keys read from config["keys"]:
      hf_token      — Hugging Face token
      gemini_api_key — Google Gemini API key
    """
    keys = config.get("keys", {}) or {}
    hf_token: Optional[str] = keys.get("hf_token") or os.getenv("HF_TOKEN")
    gemini_key: Optional[str] = keys.get("gemini_api_key") or os.getenv("GEMINI_API_KEY")

    if not hf_token and not gemini_key:
        logger.debug("route_llm: no keys configured, skipping AI")
        return None

    # 1. Try Hugging Face
    if hf_token:
        logger.info("route_llm: trying HF provider")
        try:
            result = await _call_hf(prompt, hf_token)
        except Exception as exc:
            logger.warning("route_llm: HF raised unexpectedly: %s", exc)
            result = None
        if result:
            return result
        logger.info("route_llm: HF failed, trying Gemini fallback")

    # 2. Fallback to Gemini
    if gemini_key:
        try:
            result = await _call_gemini(prompt, gemini_key)
        except Exception as exc:
            logger.warning("route_llm: Gemini raised unexpectedly: %s", exc)
            result = None
        if result:
            return result
        logger.warning("route_llm: both providers failed")

    return None
