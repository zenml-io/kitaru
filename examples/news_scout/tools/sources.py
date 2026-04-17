"""Source-searching tools for the news scout agent."""

import json
import os
import urllib.parse
import xml.etree.ElementTree as ET

from models import Article, Source
from utils.http import http_get_json, http_get_text

try:
    from openai import OpenAI

    _HAS_OPENAI = True
except ImportError:
    OpenAI = None  # type: ignore[assignment]
    _HAS_OPENAI = False

HN_ENDPOINT = "http://hn.algolia.com/api/v1/search"
GOOGLE_NEWS_ENDPOINT = "https://news.google.com/rss/search"
GROK_MODEL = os.environ.get("KITARU_GROK_MODEL", "grok-4-latest")
GROK_BASE_URL = "https://api.x.ai/v1"


def _append_article(
    articles: list[Article],
    seen_urls: set[str],
    *,
    title: str,
    url: str,
    source: Source,
    summary: str = "",
) -> None:
    """Validate and append an Article, skipping blanks + intra-run URL duplicates."""
    if not title or not url or url in seen_urls:
        return
    seen_urls.add(url)
    articles.append(Article(title=title, url=url, summary=summary, source=source))


def search_news(query: str) -> list[Article]:
    """Search Hacker News and Google News for a query.

    Returns a merged, deduped list of articles. If either source fails, logs
    a warning and returns whatever succeeded.
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
            _append_article(articles, seen_urls, title=title, url=url, source=Source.HN)
    except Exception as exc:
        print(f"[search_news] HN source failed: {type(exc).__name__}: {exc}")

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
            _append_article(
                articles,
                seen_urls,
                title=(title_el.text or "").strip(),
                url=(link_el.text or "").strip(),
                source=Source.GNEWS,
                summary=f"via Google News for '{query}'",
            )
    except Exception as exc:
        print(f"[search_news] Google News source failed: {type(exc).__name__}: {exc}")

    return articles


def _disabled_grok_notice(reason: str) -> list[Article]:
    """Return a single placeholder Article so the agent can see why Grok is off.

    Returning an empty list silently would cause the agent to keep calling this
    tool. A placeholder with an explanatory ``summary`` reads as tool output in
    the agent's context and is enough for the model to stop calling it.
    """
    return [
        Article(
            title="search_twitter is currently unavailable",
            url="https://x.ai",
            summary=(
                f"{reason}. Do not call search_twitter again — it will keep "
                "returning this same notice until the environment is fixed."
            ),
            source=Source.GROK_DISABLED,
        )
    ]


def search_twitter(query: str) -> list[Article]:
    """Ask Grok what X/Twitter is saying about a query.

    Returns articles. If ``XAI_API_KEY`` is missing or the openai SDK isn't
    installed, returns a single placeholder Article explaining the situation so
    the agent stops calling this tool.
    """
    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        return _disabled_grok_notice("XAI_API_KEY is not set in the environment")
    if not _HAS_OPENAI:
        return _disabled_grok_notice("the `openai` SDK is not installed")

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
    except Exception as exc:
        print(f"[search_twitter] Grok API call failed: {type(exc).__name__}: {exc}")
        return _disabled_grok_notice(f"Grok API call failed: {exc}")

    content = response.choices[0].message.content or "{}"
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        print(f"[search_twitter] Grok returned non-JSON: {exc}")
        return _disabled_grok_notice("Grok returned non-JSON output")

    articles: list[Article] = []
    seen_urls: set[str] = set()
    for raw_item in payload.get("items", [])[:8]:
        _append_article(
            articles,
            seen_urls,
            title=(raw_item.get("title") or "").strip(),
            url=(raw_item.get("url") or "").strip(),
            source=Source.GROK_X,
            summary=(raw_item.get("summary") or "").strip(),
        )
    return articles
