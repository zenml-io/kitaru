"""Web-fetching tools for the news scout agent."""

import re

from utils.http import http_get_text

_FETCH_TIMEOUT = 10.0
_INVESTIGATE_MAX_CHARS = 2000
_FETCH_URL_MAX_CHARS = 5000
# Read ~4 bytes per returned char to leave headroom for HTML tags/whitespace
# that get stripped out before truncation.
_BYTES_PER_CHAR = 4


def _safe_get_text(url: str, max_chars: int) -> tuple[str, str | None]:
    """Fetch ``url`` and return ``(body, error)``. Only one of the pair is set.

    Reads at most ``max_chars * _BYTES_PER_CHAR`` bytes so huge pages don't
    fully buffer just to be truncated downstream.
    """
    try:
        raw = http_get_text(
            url, timeout=_FETCH_TIMEOUT, max_bytes=max_chars * _BYTES_PER_CHAR
        )
    except Exception as exc:
        return "", f"Failed to fetch {url}: {exc}"
    return raw, None


def investigate(url: str) -> str:
    """Fetch a URL and return a plain-text summary of the content.

    Strips HTML tags and returns the first ~2000 characters.
    """
    raw, error = _safe_get_text(url, _INVESTIGATE_MAX_CHARS)
    if error is not None:
        return error
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:_INVESTIGATE_MAX_CHARS]


def fetch_url(url: str) -> str:
    """Raw HTTP GET. Returns response body as text (capped at 5000 chars)."""
    raw, error = _safe_get_text(url, _FETCH_URL_MAX_CHARS)
    if error is not None:
        return error
    return raw[:_FETCH_URL_MAX_CHARS]
