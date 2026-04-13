"""Source-searching tools for the news scout agent."""

import json
import os
import urllib.parse
import xml.etree.ElementTree as ET

from models import Article
from utils.http import http_get_json, http_get_text

HN_ENDPOINT = "http://hn.algolia.com/api/v1/search"
GOOGLE_NEWS_ENDPOINT = "https://news.google.com/rss/search"
GROK_MODEL = "grok-4-latest"
GROK_BASE_URL = "https://api.x.ai/v1"


def search_news(query: str) -> list[Article]:
    """Search Hacker News and Google News for a query.

    Returns a merged, deduped list of articles with fingerprints.
    """
    articles: list[Article] = []
    seen_urls: set[str] = set()

    # --- Hacker News ---
    try:
        hn_url = f"{HN_ENDPOINT}?query={urllib.parse.quote(query)}&hitsPerPage=15"
        payload = http_get_json(hn_url)
        for hit in payload.get("hits", []):
            title = hit.get("title") or hit.get("story_title") or ""
            url = hit.get("url") or (
                f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
                if hit.get("objectID")
                else ""
            )
            if not title or not url or url in seen_urls:
                continue
            seen_urls.add(url)
            article = Article(title=title, url=url, source="hn")
            article.fingerprint = article.compute_fingerprint()
            articles.append(article)
    except Exception:
        pass

    # --- Google News RSS ---
    try:
        gn_query = urllib.parse.urlencode(
            {"q": f"{query} when:1d", "hl": "en-US", "gl": "US", "ceid": "US:en"}
        )
        gn_url = f"{GOOGLE_NEWS_ENDPOINT}?{gn_query}"
        raw = http_get_text(gn_url)
        root = ET.fromstring(raw)
        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            if title_el is None or link_el is None:
                continue
            title = (title_el.text or "").strip()
            link = (link_el.text or "").strip()
            if not title or not link or link in seen_urls:
                continue
            seen_urls.add(link)
            article = Article(
                title=title, url=link,
                summary=f"via Google News for '{query}'",
                source="gnews",
            )
            article.fingerprint = article.compute_fingerprint()
            articles.append(article)
    except Exception:
        pass

    return articles


def search_twitter(query: str) -> list[Article]:
    """Ask Grok what X/Twitter is saying about a query.

    Returns articles. Returns [] if XAI_API_KEY is missing.
    """
    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        return []

    try:
        from openai import OpenAI
    except ImportError:
        return []

    client = OpenAI(api_key=api_key, base_url=GROK_BASE_URL)
    user_prompt = (
        f"What are the most notable X/Twitter discussions about '{query}' "
        f"in the last 60 minutes? Return strict JSON: "
        '{"items": [{"title": str, "summary": str, "url": str}]}'
    )

    try:
        response = client.chat.completions.create(
            model=GROK_MODEL,
            messages=[
                {"role": "system", "content": "Return factual JSON only."},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
    except Exception:
        return []

    content = response.choices[0].message.content or "{}"
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return []

    articles: list[Article] = []
    for raw_item in payload.get("items", [])[:8]:
        title = (raw_item.get("title") or "").strip()
        url = (raw_item.get("url") or "").strip()
        summary = (raw_item.get("summary") or "").strip()
        if not title or not url:
            continue
        article = Article(
            title=title, url=url, summary=summary, source="grok:x"
        )
        article.fingerprint = article.compute_fingerprint()
        articles.append(article)
    return articles
