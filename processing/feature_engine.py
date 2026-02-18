"""
Feature Engineering Module
===========================
Extracts ML-ready features from signals for future model training.
Designed for Phase 2 supervised learning integration.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List

import regex as regex_re  # for Unicode properties like \p{Emoji}

logger = logging.getLogger(__name__)


class FeatureEngine:
    """Feature extraction for ML-ready signal representation."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_version = "1.0"

    def _extract_text_features(self, text: str) -> Dict[str, Any]:
        if not text:
            return {
                'text_length': 0,
                'word_count': 0,
                'avg_word_length': 0,
                'exclamation_count': 0,
                'question_count': 0,
                'url_count': 0,
                'hashtag_count': 0,
                'mention_count': 0,
                'emoji_count': 0,
                'uppercase_ratio': 0
            }
        words = text.split()
        emoji_count = 0
        try:
            emoji_count = len(regex_re.findall(r'\p{Emoji}', text))
        except Exception:
            emoji_count = 0

        return {
            'text_length': len(text),
            'word_count': len(words),
            'avg_word_length': (sum(len(w) for w in words) / len(words)) if words else 0,
            'exclamation_count': text.count('!'),
            'question_count': text.count('?'),
            'url_count': len(re.findall(r'http[s]?://\S+', text)),
            'hashtag_count': len(re.findall(r'#\w+', text)),
            'mention_count': len(re.findall(r'@\w+', text)),
            'emoji_count': emoji_count,
            'uppercase_ratio': (sum(1 for c in text if c.isupper()) / len(text)) if text else 0
        }

    def _extract_temporal_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        ts = signal.get('timestamp')
        now = datetime.utcnow()
        if isinstance(ts, datetime):
            age_hours = (now - ts).total_seconds() / 3600
            day_of_week = ts.weekday()
            hour_of_day = ts.hour
        else:
            age_hours = -1
            day_of_week = -1
            hour_of_day = -1

        return {
            'age_hours': age_hours,
            'day_of_week': day_of_week,
            'hour_of_day': hour_of_day,
            'is_weekend': 1 if day_of_week >= 5 else 0,
            'is_business_hours': 1 if 9 <= hour_of_day <= 17 else 0
        }

    def _extract_engagement_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        followers = int(signal.get('followers', 0) or 0)
        likes = int(signal.get('likes', 0) or 0)
        return {
            'followers': followers,
            'following': int(signal.get('following', 0) or 0),
            'likes': likes,
            'retweets': int(signal.get('retweets', 0) or 0),
            'replies': int(signal.get('replies', 0) or 0),
            'stars': int(signal.get('stars', 0) or 0),
            'forks': int(signal.get('forks', 0) or 0),
            'watchers': int(signal.get('watchers', 0) or 0),
            'engagement_rate': likes / max(followers, 1)
        }

    def _extract_account_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        age_days = int(signal.get('account_age_days', 0) or 0)
        return {
            'account_age_days': age_days,
            'is_verified': 1 if signal.get('verified', False) else 0,
            'has_profile_image': 1 if signal.get('has_profile_image', True) else 0,
            'has_description': 1 if (signal.get('description') or signal.get('bio')) else 0,
            'is_new_account': 1 if age_days < 30 else 0,
            'is_established_account': 1 if age_days > 365 else 0
        }

    def _extract_content_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        source = signal.get('source', 'unknown')
        signal_type = signal.get('type', 'unknown')
        source_encoding = {
            'twitter': [1, 0, 0, 0],
            'news': [0, 1, 0, 0],
            'github': [0, 0, 1, 0],
            'funding': [0, 0, 0, 1]
        }
        type_encoding = {
            'tweet': [1, 0, 0, 0, 0],
            'news_article': [0, 1, 0, 0, 0],
            'github_repo': [0, 0, 1, 0, 0],
            'funding_announcement': [0, 0, 0, 1, 0],
            'other': [0, 0, 0, 0, 1]
        }
        return {
            'source_encoding': source_encoding.get(source, [0, 0, 0, 0]),
            'type_encoding': type_encoding.get(signal_type, [0, 0, 0, 0, 1]),
            'has_url': 1 if signal.get('url') else 0,
            'has_image': 1 if signal.get('image_url') else 0,
            'has_video': 1 if signal.get('video_url') else 0
        }

    def _extract_ecosystem_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        ecosystem = signal.get('detected_ecosystem', 'unknown')
        sector = signal.get('detected_sector', 'unknown')
        ecosystem_priorities = {
            'ethereum_l2s': 1.0,
            'solana': 0.9,
            'bitcoin_l2s': 0.9,
            'unknown': 0.5
        }
        sector_priorities = {
            'defi': 1.0,
            'infrastructure': 0.95,
            'ai_crypto': 0.8,
            'rwa': 0.8,
            'restaking': 0.8,
            'gaming': 0.6,
            'nft': 0.5,
            'unknown': 0.5
        }
        return {
            'ecosystem_priority': ecosystem_priorities.get(ecosystem, 0.5),
            'sector_priority': sector_priorities.get(sector, 0.5),
            'has_ecosystem_tag': 1 if ecosystem != 'unknown' else 0,
            'has_sector_tag': 1 if sector != 'unknown' else 0
        }

    def extract_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        parts = []
        for k in ('title','text','description'):
            if signal.get(k):
                parts.append(str(signal[k]))
        text = ' '.join(parts)

        features = {
            'text_features': self._extract_text_features(text),
            'temporal_features': self._extract_temporal_features(signal),
            'engagement_features': self._extract_engagement_features(signal),
            'account_features': self._extract_account_features(signal),
            'content_features': self._extract_content_features(signal),
            'ecosystem_features': self._extract_ecosystem_features(signal),
            'metadata': {
                'signal_id': signal.get('id', hashlib.md5(str(signal).encode()).hexdigest()[:12]),
                'feature_version': self.feature_version,
                'extracted_at': datetime.utcnow().isoformat()
            }
        }
        if 'signal_score' in signal:
            features['metadata']['existing_score'] = signal['signal_score']
        return features

    def extract_batch(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.extract_features(s) for s in signals]

    def to_feature_vector(self, features: Dict[str, Any]) -> List[float]:
        vector: List[float] = []
        tf = features.get('text_features', {})
        vector.extend([
            tf.get('text_length', 0),
            tf.get('word_count', 0),
            tf.get('avg_word_length', 0),
            tf.get('exclamation_count', 0),
            tf.get('question_count', 0),
            tf.get('url_count', 0),
            tf.get('hashtag_count', 0),
            tf.get('mention_count', 0),
            tf.get('emoji_count', 0),
            tf.get('uppercase_ratio', 0)
        ])
        temp = features.get('temporal_features', {})
        vector.extend([
            temp.get('age_hours', 0),
            temp.get('day_of_week', 0),
            temp.get('hour_of_day', 0),
            temp.get('is_weekend', 0),
            temp.get('is_business_hours', 0)
        ])
        eng = features.get('engagement_features', {})
        vector.extend([
            eng.get('followers', 0),
            eng.get('following', 0),
            eng.get('likes', 0),
            eng.get('retweets', 0),
            eng.get('replies', 0),
            eng.get('stars', 0),
            eng.get('forks', 0),
            eng.get('watchers', 0),
            eng.get('engagement_rate', 0)
        ])
        acc = features.get('account_features', {})
        vector.extend([
            acc.get('account_age_days', 0),
            acc.get('is_verified', 0),
            acc.get('has_profile_image', 0),
            acc.get('has_description', 0),
            acc.get('is_new_account', 0),
            acc.get('is_established_account', 0)
        ])
        content = features.get('content_features', {})
        vector.extend(content.get('source_encoding', [0,0,0,0]))
        vector.extend(content.get('type_encoding', [0,0,0,0,1]))
        vector.extend([
            content.get('has_url', 0),
            content.get('has_image', 0),
            content.get('has_video', 0)
        ])
        eco = features.get('ecosystem_features', {})
        vector.extend([
            eco.get('ecosystem_priority', 0.5),
            eco.get('sector_priority', 0.5),
            eco.get('has_ecosystem_tag', 0),
            eco.get('has_sector_tag', 0)
        ])
        return vector

    def get_feature_names(self) -> List[str]:
        names: List[str] = []
        names.extend([
            'text_length', 'word_count', 'avg_word_length',
            'exclamation_count', 'question_count', 'url_count',
            'hashtag_count', 'mention_count', 'emoji_count', 'uppercase_ratio'
        ])
        names.extend(['age_hours','day_of_week','hour_of_day','is_weekend','is_business_hours'])
        names.extend(['followers','following','likes','retweets','replies','stars','forks','watchers','engagement_rate'])
        names.extend(['account_age_days','is_verified','has_profile_image','has_description','is_new_account','is_established_account'])
        names.extend(['source_twitter','source_news','source_github','source_funding',
                      'type_tweet','type_news','type_github','type_funding','type_other',
                      'has_url','has_image','has_video'])
        names.extend(['ecosystem_priority','sector_priority','has_ecosystem_tag','has_sector_tag'])
        return names
