#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded local documentation search contracts."""

from pydantic import Field

from kitaru.mcp.models.common import MCPModel, ToolResult


class DocsSearchRequest(MCPModel):
    """Search the hand-written Kitaru documentation bundled with this release."""

    query: str = Field(min_length=2, max_length=240)
    limit: int = Field(default=5, ge=1, le=8)


class DocsSearchMatch(MCPModel):
    """One source-attributed documentation section."""

    title: str
    heading: str
    excerpt: str
    url: str


class DocsSearchData(MCPModel):
    """Ranked search results and the bundled source revision."""

    matches: list[DocsSearchMatch]
    source_revision: str


class DocsSearchResult(ToolResult):
    """Typed documentation search result."""

    data: DocsSearchData | None = None
