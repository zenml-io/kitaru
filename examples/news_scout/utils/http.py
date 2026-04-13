"""HTTP helpers using stdlib urllib."""

import json
import urllib.request


def http_get_json(url: str, timeout: float = 15.0) -> dict:
    """GET a URL and parse the response as JSON."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "kitaru-news-scout/0.2"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get_text(url: str, timeout: float = 15.0) -> str:
    """GET a URL and return the response body as text."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "kitaru-news-scout/0.2"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")
