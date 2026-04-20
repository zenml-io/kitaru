"""Source-searching tools for the news scout agent."""

import json
import logging
import os
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from models import Article, Source
from utils.http import http_get_json, http_get_text

try:
    from openai import OpenAI

    _HAS_OPENAI = True
except ImportError:
    OpenAI = None  # type: ignore[assignment]
    _HAS_OPENAI = False

logger = logging.getLogger(__name__)

HN_ENDPOINT = "http://hn.algolia.com/api/v1/search"
GOOGLE_NEWS_ENDPOINT = "https://news.google.com/rss/search"
GROK_MODEL = os.environ.get("KITARU_GROK_MODEL", "grok-4-latest")
GROK_BASE_URL = "https://api.x.ai/v1"

_HN_HITS_PER_PAGE = 15
_GROK_MAX_ITEMS = 8


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


def _fetch_hn(query: str) -> list[tuple[str, str, Source, str]]:
    """Return ``(title, url, source, summary)`` tuples from Hacker News."""
    hn_url = (
        f"{HN_ENDPOINT}?query={urllib.parse.quote(query)}"
        f"&hitsPerPage={_HN_HITS_PER_PAGE}"
    )
    payload = http_get_json(hn_url)
    out: list[tuple[str, str, Source, str]] = []
    for hit in payload.get("hits", []):
        title = hit.get("title") or hit.get("story_title") or ""
        url = hit.get("url") or (
            f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
            if hit.get("objectID")
            else ""
        )
        out.append((title, url, Source.HN, ""))
    return out


def _fetch_google_news(query: str) -> list[tuple[str, str, Source, str]]:
    """Return ``(title, url, source, summary)`` tuples from Google News RSS."""
    gn_query = urllib.parse.urlencode(
        {"q": f"{query} when:1d", "hl": "en-US", "gl": "US", "ceid": "US:en"}
    )
    gn_url = f"{GOOGLE_NEWS_ENDPOINT}?{gn_query}"
    raw = http_get_text(gn_url)
    root = ET.fromstring(raw)
    summary = f"via Google News for '{query}'"
    out: list[tuple[str, str, Source, str]] = []
    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        if title_el is None or link_el is None:
            continue
        out.append(
            (
                (title_el.text or "").strip(),
                (link_el.text or "").strip(),
                Source.GNEWS,
                summary,
            )
        )
    return out


_SOURCE_FETCHERS = (
    ("HN", _fetch_hn),
    ("Google News", _fetch_google_news),
)


def search_news(query: str) -> list[Article]:
    """Search Hacker News and Google News for a query in parallel.

    Returns a merged, deduped list of articles. If either source fails, logs
    a warning and returns whatever succeeded.
    """
    articles: list[Article] = []
    seen_urls: set[str] = set()

    with ThreadPoolExecutor(max_workers=len(_SOURCE_FETCHERS)) as pool:
        futures = {pool.submit(fn, query): label for label, fn in _SOURCE_FETCHERS}
        for future, label in futures.items():
            try:
                hits = future.result()
            except Exception as exc:
                logger.warning(
                    "search_news %s source failed: %s: %s",
                    label,
                    type(exc).__name__,
                    exc,
                )
                continue
            for title, url, source, summary in hits:
                _append_article(
                    articles,
                    seen_urls,
                    title=title,
                    url=url,
                    source=source,
                    summary=summary,
                )
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


@lru_cache(maxsize=1)
def _grok_client() -> "OpenAI":
    """Lazily build (and reuse) the Grok OpenAI-compatible client."""
    return OpenAI(api_key=os.environ["XAI_API_KEY"], base_url=GROK_BASE_URL)


def search_twitter(query: str) -> list[Article]:
    """Ask Grok what X/Twitter is saying about a query.

    Returns articles. If ``XAI_API_KEY`` is missing or the openai SDK isn't
    installed, returns a single placeholder Article explaining the situation so
    the agent stops calling this tool.
    """
    if not os.environ.get("XAI_API_KEY"):
        return _disabled_grok_notice("XAI_API_KEY is not set in the environment")
    if not _HAS_OPENAI:
        return _disabled_grok_notice("the `openai` SDK is not installed")

    user_prompt = (
        f"What are the most notable X/Twitter discussions about '{query}' "
        f"in the last 60 minutes? Return strict JSON: "
        '{"items": [{"title": str, "summary": str, "url": str}]}'
    )

    try:
        response = _grok_client().chat.completions.create(
            model=GROK_MODEL,
            messages=[
                {"role": "system", "content": "Return factual JSON only."},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
    except Exception as exc:
        logger.warning(
            "search_twitter Grok API call failed: %s: %s", type(exc).__name__, exc
        )
        return _disabled_grok_notice(f"Grok API call failed: {exc}")

    content = response.choices[0].message.content or "{}"
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.warning("search_twitter Grok returned non-JSON: %s", exc)
        return _disabled_grok_notice("Grok returned non-JSON output")

    articles: list[Article] = []
    seen_urls: set[str] = set()
    for raw_item in payload.get("items", [])[:_GROK_MAX_ITEMS]:
        _append_article(
            articles,
            seen_urls,
            title=(raw_item.get("title") or "").strip(),
            url=(raw_item.get("url") or "").strip(),
            source=Source.GROK_X,
            summary=(raw_item.get("summary") or "").strip(),
        )
    return articles
