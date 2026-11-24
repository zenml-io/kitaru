#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Draft prefilled GitHub issues from assistant feedback."""

from kitaru.feedback import draft_feedback
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.feedback import FeedbackDraftData, FeedbackDraftRequest


async def handle_feedback_draft(
    state: MCPServerState, request: FeedbackDraftRequest
) -> FeedbackDraftData:
    """Build a prefilled GitHub issue link that the user reviews and submits."""
    draft = draft_feedback(
        "MCP",
        request.title,
        request.problem,
        expected=request.expected,
        proposed_fix=request.proposed_fix,
        context=f"mode `{state.settings.mode.value}`",
    )
    return FeedbackDraftData(
        issue_url=draft.url, body=draft.body, truncated=draft.truncated
    )
