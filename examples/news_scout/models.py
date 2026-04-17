"""Data models shared across the news scout agent."""

import hashlib
from enum import StrEnum

from pydantic import BaseModel, computed_field


class Source(StrEnum):
    """Stable identifiers for the sources the agent's tools can return."""

    HN = "hn"
    GNEWS = "gnews"
    GROK_X = "grok:x"
    GROK_DISABLED = "grok:disabled"


class Article(BaseModel):
    """One candidate news item from any source.

    ``fingerprint`` is derived from ``url + title`` so dedup is idempotent
    and doesn't require callers to remember to recompute after construction.
    """

    title: str
    url: str
    summary: str = ""
    source: str

    @computed_field
    @property
    def fingerprint(self) -> str:
        raw = f"{self.url.strip().lower()}|{self.title.strip().lower()}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
