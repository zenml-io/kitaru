"""Web-fetching tools for the news scout agent."""

import re

from utils.http import http_get_text


def investigate(url: str) -> str:
    """Fetch a URL and return a plain-text summary of the content.

    Strips HTML tags and returns the first ~2000 characters.
    """
    try:
        raw = http_get_text(url, timeout=10.0)
    except Exception as exc:
        return f"Failed to fetch {url}: {exc}"

    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:2000]


def fetch_url(url: str) -> str:
    """Raw HTTP GET. Returns response body as text (capped at 5000 chars)."""
    try:
        raw = http_get_text(url, timeout=10.0)
    except Exception as exc:
        return f"Failed to fetch {url}: {exc}"
    return raw[:5000]
