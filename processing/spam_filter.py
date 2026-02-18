"""
Spam Filter Module
===================
Detects and filters spam signals using pattern matching and heuristics.
Identifies bot-like behavior, scam patterns, and low-quality content.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import regex as regex_re  # for Unicode properties like \p{Emoji}

logger = logging.getLogger(__name__)


class SpamFilter:
    """Multi-heuristic spam detection and filtering."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ecosystems_config = config.get('ecosystems', {})
        self.filtering_config = config.get('filtering', {})

        # Spam patterns from config (fallback defaults)
        self.spam_patterns = self.ecosystems_config.get('spam_patterns', [
            "🚀🚀🚀",
            "MOON",
            "100x",
            "1000x",
            "FREE AIRDROP",
            "CLAIM NOW",
            "DM for alpha",
            "guaranteed returns",
            "no risk",
            "finite offer"
        ])

        # Compile regex patterns (treat each pattern as literal unless it looks like a regex)
        self.compiled_patterns = []
        for p in self.spam_patterns:
            try:
                self.compiled_patterns.append(re.compile(p, re.IGNORECASE))
            except re.error:
                self.compiled_patterns.append(re.compile(re.escape(p), re.IGNORECASE))

        # Additional keyword heuristics
        self.spam_keywords = [
            'giveaway', 'airdrop farming', 'free tokens', 'pump',
            'lambo', 'financial advice', 'dm me', 'telegram group',
            'whatsapp', 'limited spots', 'act now', 'urgent'
        ]

        # Track posting frequency per account (rolling 1h)
        self.account_post_times: Dict[str, List[datetime]] = defaultdict(list)

        self.stats = {
            'checked': 0,
            'spam_detected': 0,
            'pattern_match': 0,
            'frequency_flag': 0,
            'keyword_flag': 0
        }

    def _check_spam_patterns(self, text: str) -> bool:
        for pattern in self.compiled_patterns:
            if pattern.search(text):
                return True
        return False

    def _check_spam_keywords(self, text: str) -> bool:
        text_lower = (text or "").lower()
        match_count = sum(1 for kw in self.spam_keywords if kw in text_lower)
        return match_count >= 2

    def _check_posting_frequency(self, account: str, timestamp: datetime) -> bool:
        if not account:
            return False

        cutoff = datetime.utcnow() - timedelta(hours=1)
        self.account_post_times[account] = [t for t in self.account_post_times[account] if t > cutoff]
        self.account_post_times[account].append(timestamp)

        max_posts_per_hour = self.filtering_config.get('twitter', {}).get('spam_post_frequency_per_hour', 10)
        return len(self.account_post_times[account]) > max_posts_per_hour

    def _check_url_quality(self, url: Optional[str]) -> bool:
        if not url:
            return False

        url_lower = url.lower()
        low_quality_domains = ['bit.ly', 'tinyurl', 't.co', 'medium.com/@']
        suspicious_patterns = [r'claim-.*\.com', r'airdrop-.*\.com', r'free-.*\.com', r'giveaway.*\.com']

        if any(d in url_lower for d in low_quality_domains):
            return True
        if any(re.search(p, url_lower) for p in suspicious_patterns):
            return True
        return False

    def _check_text_quality(self, text: str) -> bool:
        if not text:
            return True

        # excessive emojis
        try:
            emoji_count = len(regex_re.findall(r'\p{Emoji}', text))
            if emoji_count > 5:
                return True
        except Exception:
            pass

        # excessive capitalization
        if text.isupper() and len(text) > 20:
            return True

        # very short
        if len(text) < 10:
            return True

        # repetitive chars
        if re.search(r'(.)\1{4,}', text):
            return True

        return False

    def is_spam(self, signal: Dict[str, Any]) -> Tuple[bool, List[str]]:
        reasons: List[str] = []

        # Compose text
        parts = []
        for k in ('title', 'text', 'description'):
            if signal.get(k):
                parts.append(str(signal[k]))
        text = " ".join(parts).strip()

        if self._check_spam_patterns(text):
            reasons.append('spam_pattern_match')
            self.stats['pattern_match'] += 1

        if self._check_spam_keywords(text):
            reasons.append('spam_keywords')
            self.stats['keyword_flag'] += 1

        account = signal.get('account') or signal.get('source_name') or ''
        timestamp = signal.get('timestamp')
        if not isinstance(timestamp, datetime):
            timestamp = datetime.utcnow()

        if self._check_posting_frequency(account, timestamp):
            reasons.append('high_posting_frequency')
            self.stats['frequency_flag'] += 1

        if self._check_url_quality(signal.get('url')):
            reasons.append('low_quality_url')

        if self._check_text_quality(text):
            reasons.append('low_text_quality')

        is_spam = len(reasons) >= 2

        if is_spam:
            self.stats['spam_detected'] += 1
            logger.debug(f"SpamFilter: Detected spam from {account}: {reasons}")

        self.stats['checked'] += 1
        return is_spam, reasons

    def filter_signals(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        clean = []
        for s in signals:
            spam, reasons = self.is_spam(s)
            s['is_spam'] = spam
            s['spam_reasons'] = reasons
            if not spam:
                clean.append(s)
        logger.info(f"SpamFilter: {len(signals) - len(clean)}/{len(signals)} signals filtered as spam")
        return clean

    def get_stats(self) -> Dict[str, int]:
        return dict(self.stats)

    def reset_stats(self) -> None:
        self.stats = {'checked': 0, 'spam_detected': 0, 'pattern_match': 0, 'frequency_flag': 0, 'keyword_flag': 0}
