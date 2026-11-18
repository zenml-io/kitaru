#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Draft prefilled GitHub issues from assistant feedback."""

from importlib.metadata import version

from kitaru.feedback import build_issue_draft
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.feedback import FeedbackDraftData, FeedbackDraftRequest


async def handle_feedback_draft(
    state: MCPServerState, request: FeedbackDraftRequest
) -> FeedbackDraftData:
    """Build a prefilled GitHub issue link that the user reviews and submits."""
    draft = build_issue_draft(
        f"MCP feedback: {request.title}",
        [
            ("What happened", request.problem),
            ("What I expected", request.expected),
            ("Proposed fix", request.proposed_fix),
        ],
        f"Kitaru {version('kitaru')}, MCP mode `{state.settings.mode.value}`",
    )
    return FeedbackDraftData(
        issue_url=draft.url, body=draft.body, truncated=draft.truncated
    )
