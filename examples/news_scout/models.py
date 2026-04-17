"""Data models shared across the news scout agent."""

import hashlib

from pydantic import BaseModel, Field


class Article(BaseModel):
    """One candidate news item from any source."""

    title: str
    url: str
    summary: str = ""
    source: str
    fingerprint: str = ""

    def compute_fingerprint(self) -> str:
        raw = f"{self.url.strip().lower()}|{self.title.strip().lower()}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


class JudgedItem(BaseModel):
    """An article the agent has scored."""

    article: Article
    score: float = Field(ge=0.0, le=10.0)
    verdict: str
    reason: str
