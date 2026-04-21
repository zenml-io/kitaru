"""HTTP helpers using stdlib urllib."""

import json
import urllib.request
from typing import Any

USER_AGENT = "kitaru-news-scout/0.2"


def http_get_json(url: str, timeout: float = 15.0) -> dict[str, Any]:
    """GET a URL and parse the response as JSON."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get_text(url: str, timeout: float = 15.0, max_bytes: int | None = None) -> str:
    """GET a URL and return the response body as text.

    ``max_bytes`` stops reading once the cap is hit, so huge pages don't
    fully buffer in memory when the caller only needs a prefix.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read() if max_bytes is None else resp.read(max_bytes)
        return data.decode("utf-8", errors="replace")
