#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Feedback issue draft contracts."""

from pydantic import Field

from kitaru.feedback import MAX_TITLE_LENGTH
from kitaru.mcp.models.common import MCPModel, ToolResult


class FeedbackDraftRequest(MCPModel):
    """Describe a problem with Kitaru or its MCP tools for a GitHub issue draft."""

    title: str = Field(min_length=8, max_length=MAX_TITLE_LENGTH)
    problem: str = Field(
        min_length=20,
        max_length=6000,
        description="What went wrong: the tool, the arguments, and what it returned.",
    )
    expected: str | None = Field(default=None, max_length=3000)
    proposed_fix: str | None = Field(
        default=None,
        max_length=6000,
        description="The change to Kitaru that would fix it, if you have one.",
    )


class FeedbackDraftData(MCPModel):
    """A prefilled GitHub issue link and the text it carries."""

    issue_url: str
    body: str
    truncated: bool


class FeedbackDraftResult(ToolResult):
    """Typed feedback draft result."""

    data: FeedbackDraftData | None = None
