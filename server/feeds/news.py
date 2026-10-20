"""Forex news headlines from RSS feeds (headline + link + short summary, always linked back to the source)."""
import calendar as _cal
import hashlib
import html
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (FX platform news reader)"}
_TAGS = re.compile(r"<[^>]+>")


def _clean(text: str, limit: int) -> str:
    text = html.unescape(_TAGS.sub(" ", text or ""))
    return re.sub(r"\s+", " ", text).strip()[:limit]


def fetch_rss(url: str, max_items: int = 30) -> list[dict]:
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    feed = feedparser.parse(r.content)
    source = (feed.feed.get("title") or urlparse(url).netloc).strip()[:80]
    items = []
    for e in feed.entries[:max_items]:
        link = e.get("link") or ""
        if not link:
            continue
        parsed = e.get("published_parsed") or e.get("updated_parsed")
        published = (datetime.fromtimestamp(_cal.timegm(parsed), timezone.utc).replace(tzinfo=None)
                     if parsed else datetime.now(timezone.utc).replace(tzinfo=None))
        items.append({
            "id": hashlib.sha1(link.encode()).hexdigest(),
            "source": source,
            "title": _clean(e.get("title", ""), 500),
            "link": link[:1000],
            "summary": _clean(e.get("summary", ""), 600),
            "published_at": published,
        })
    return items
