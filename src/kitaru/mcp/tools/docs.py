#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Search the documentation index bundled with the MCP server."""

import json
import re
from collections import Counter, defaultdict
from functools import lru_cache
from importlib.resources import files

from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import MCPModel
from kitaru.mcp.models.docs import DocsSearchData, DocsSearchMatch, DocsSearchRequest

_TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:_[a-z0-9]+)*")
_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "can",
        "do",
        "does",
        "for",
        "from",
        "how",
        "i",
        "in",
        "is",
        "it",
        "my",
        "of",
        "on",
        "or",
        "the",
        "to",
        "use",
        "what",
        "when",
        "where",
        "with",
    }
)


class _IndexEntry(MCPModel):
    title: str
    heading: str
    excerpt: str
    url: str
    source: str


class _DocsIndex(MCPModel):
    source_revision: str
    entries: list[_IndexEntry]


@lru_cache(maxsize=1)
def _load_index() -> _DocsIndex:
    resource = files("kitaru.mcp.data").joinpath("docs_index.json")
    return _DocsIndex.model_validate(json.loads(resource.read_text(encoding="utf-8")))


def _stem(token: str) -> str:
    if len(token) > 5 and token.endswith("ies"):
        return f"{token[:-3]}y"
    if (
        len(token) > 4
        and token.endswith("s")
        and not token.endswith(("ss", "us", "is"))
    ):
        return token[:-1]
    return token


@lru_cache(maxsize=2048)
def _terms(value: str) -> frozenset[str]:
    return frozenset(
        _stem(token)
        for token in _TOKEN_PATTERN.findall(value.lower())
        if token not in _STOP_WORDS
    )


def _score(entry: _IndexEntry, query_terms: frozenset[str]) -> int:
    title = _terms(entry.title)
    heading = _terms(entry.heading)
    excerpt = _terms(entry.excerpt)
    matched = query_terms & (title | heading | excerpt)
    if not matched:
        return 0
    return (
        6 * len(query_terms & heading)
        + 4 * len(query_terms & title)
        + len(query_terms & excerpt)
        + 3 * len(matched)
    )


async def handle_docs_search(
    _state: MCPServerState, request: DocsSearchRequest
) -> DocsSearchData:
    """Find source-attributed sections in the bundled Kitaru documentation."""
    query_terms = _terms(request.query)
    if not query_terms:
        raise MCPToolError(
            "invalid_arguments",
            "The query needs at least one searchable Kitaru term.",
        )
    index = _load_index()
    technical_terms = {term for term in query_terms if "_" in term}
    page_terms: dict[str, Counter[str]] = defaultdict(Counter)
    if technical_terms:
        for entry in index.entries:
            page_terms[entry.source].update(technical_terms & _terms(entry.excerpt))
    ranked = [
        (
            score
            + 3
            * sum(min(8, page_terms[entry.source][term]) for term in technical_terms),
            entry,
        )
        for entry in index.entries
        if (score := _score(entry, query_terms)) > 0
    ]
    ranked.sort(key=lambda item: (-item[0], item[1].source, item[1].heading))
    matches: list[DocsSearchMatch] = []
    seen_urls: set[str] = set()
    for _, entry in ranked:
        if entry.url in seen_urls:
            continue
        seen_urls.add(entry.url)
        matches.append(
            DocsSearchMatch(
                title=entry.title,
                heading=entry.heading,
                excerpt=entry.excerpt,
                url=entry.url,
            )
        )
        if len(matches) == request.limit:
            break
    return DocsSearchData(
        matches=matches,
        source_revision=index.source_revision,
    )
