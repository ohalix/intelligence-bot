import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

"""Local regression harness (offline).

This harness simulates aiohttp responses to validate:
- No coroutine misuse (resp.text()/resp.json() must be awaited)
- Feed parsing produces >0 items with valid inputs
- Telegram HTML formatting contains no backslash escape artifacts

It is a developer tool only.
"""

import asyncio
from datetime import datetime, timedelta

try:
    from ingestion.news_ingest import NewsIngester
    from ingestion.funding_ingest import FundingIngester
    from ingestion.ecosystem_ingest import EcosystemIngester
    from ingestion.github_ingest import GitHubIngester
    from bot.formatter import format_dailybrief_html, format_section_html
except ModuleNotFoundError as e:
    # Developer tool: fail with a clear message instead of a traceback.
    missing = getattr(e, "name", str(e))
    print(f"Dependency missing for regression harness: {missing}.")
    print("Install requirements.txt into your venv, then rerun this script.")
    raise SystemExit(2)


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
<channel>
<title>Sample Feed</title>
<item>
<title>Test Post One</title>
<link>https://example.com/post1</link>
<description>Hello (world). Price moved 2.5%!</description>
<pubDate>{PUBDATE}</pubDate>
</item>
</channel>
</rss>
"""

SAMPLE_HTML = """<html><head><title>Blog</title></head>
<body>
<a href=\"/blog/post-a\">Big Update: EVM Launch</a>
<a href=\"/blog/post-b\">Grants Round (Season 2)</a>
</body></html>"""


class DummyResp:
    def __init__(self, url: str):
        self.url = url
        self.status = 200
        self.headers = {"Content-Type": "text/html"}

    def raise_for_status(self):
        return None

    async def text(self):
        # RSS for feeds, HTML for web pages
        if any(k in self.url for k in ["/blog", "hyperliquid", "stacks"]):
            return SAMPLE_HTML
        from email.utils import format_datetime
        from datetime import datetime, timezone

        return SAMPLE_RSS.format(PUBDATE=format_datetime(datetime.now(timezone.utc)))

    async def json(self):
        # Minimal JSON responses for API ingesters
        if "cryptocurrency.cv" in self.url:
            from datetime import datetime, timedelta
            return [
                {
                    "title": "API News Item",
                    "url": "https://example.com/api-news",
                    "description": "api desc",
                    # Keep relative so the harness doesn't become stale.
                    "published_at": (datetime.utcnow() - timedelta(hours=1)).isoformat() + "Z",
                }
            ]
        if "pro-api.coinmarketcap.com" in self.url:
            from datetime import datetime, timedelta
            return {
                "data": [
                    {
                        "title": "CMC Post",
                        "url": "https://example.com/cmc",
                        "subtitle": "sub",
                        "created_at": (datetime.utcnow() - timedelta(hours=1)).isoformat() + "Z",
                    }
                ]
            }
        if "api.llama.fi/raises" in self.url:
            from datetime import datetime, timedelta
            return {
                "raises": [
                    {
                        "name": "Example Protocol",
                        "round": "Seed",
                        "amount": 5000000,
                        "link": "https://example.com/raise",
                        "date": (datetime.utcnow() - timedelta(hours=2)).date().isoformat(),
                    }
                ]
            }
        # Minimal GitHub search response
        return {"items": [{"full_name": "acme/proto", "html_url": "https://github.com/acme/proto", "description": "test", "pushed_at": "2026-02-17T00:00:00Z"}]}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummySession:
    def get(self, url, *args, **kwargs):
        return DummyResp(str(url))


async def run_once() -> None:
    cfg = {
        "ingestion": {"news_concurrency": 3, "funding_concurrency": 3, "ecosystem_concurrency": 3},
        "github": {"queries": [], "concurrency": 1},
    }

    since = datetime.utcnow() - timedelta(days=3)
    sess = DummySession()

    news = NewsIngester(cfg, sess)
    funding = FundingIngester(cfg, sess)
    eco = EcosystemIngester(cfg, sess)
    gh = GitHubIngester(cfg, sess)

    items_news = await news.ingest(since)
    items_funding = await funding.ingest(since)
    items_eco = await eco.ingest(since)
    items_gh = await gh.ingest(since)

    assert len(items_news) > 0
    assert len(items_funding) > 0
    assert len(items_eco) > 0
    assert len(items_gh) > 0

    payload = {
        "date": "2026-02-18",
        "analysis": {"market_tone": {"market_tone": "neutral", "confidence": 0.5}, "summary": "Test summary."},
        "sections": {
            "News": items_news,
            "Funding": items_funding,
            "Ecosystem": items_eco,
            "GitHub": items_gh,
        },
    }

    msg = format_dailybrief_html(payload)
    assert "\\" not in msg, "HTML output should not contain markdown escapes"
    _ = format_section_html("News", items_news)


async def main():
    # Run 15 iterations to mimic regression pass
    for i in range(1, 16):
        await run_once()
        print(f"iteration {i}: PASS")


if __name__ == "__main__":
    asyncio.run(main())
