import re
from typing import Iterable, List, Optional, Union, Dict, Any

class SpamFilter:
    """Simple spam filter.

    Accepts either:
    - a list of regex patterns, OR
    - a config dict that may contain spam patterns under:
        config['ecosystems']['spam_patterns'] or config['spam_patterns']
    """

    def __init__(self, config: Union[List[str], Dict[str, Any], None] = None):
        patterns: List[str] = []

        if config is None:
            patterns = []
        elif isinstance(config, list):
            patterns = [str(p) for p in config]
        elif isinstance(config, dict):
            # Backward/forward compatible: support both shapes
            if isinstance(config.get("spam_patterns"), list):
                patterns = [str(p) for p in config.get("spam_patterns", [])]
            else:
                ecosystems = config.get("ecosystems") or {}
                if isinstance(ecosystems, dict) and isinstance(ecosystems.get("spam_patterns"), list):
                    patterns = [str(p) for p in ecosystems.get("spam_patterns", [])]
        else:
            patterns = []

        self.patterns = [re.compile(p, re.IGNORECASE) for p in patterns if p]

    def is_spam(self, title: str = "", summary: str = "", url: str = "") -> bool:
        text = f"{title} {summary} {url}".strip()
        if not text:
            return False
        return any(p.search(text) for p in self.patterns)

    def filter(self, signals):
        """Return only non-spam signals."""
        out = []
        for s in signals or []:
            title = (s.get("title") if isinstance(s, dict) else getattr(s, "title", "")) or ""
            summary = (s.get("summary") if isinstance(s, dict) else getattr(s, "summary", "")) or ""
            url = (s.get("url") if isinstance(s, dict) else getattr(s, "url", "")) or ""
            if not self.is_spam(title, summary, url):
                out.append(s)
        return out
