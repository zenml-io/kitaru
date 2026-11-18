#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Prefilled GitHub issue drafts through the public MCP protocol."""

import json
from urllib.parse import parse_qs, urlsplit

from mcp.types import CallToolResult, TextContent
from mcp_fakes import NullClient, build_server_context

from kitaru.feedback import MAX_URL_LENGTH
from kitaru.mcp.settings import CapabilityMode


async def test_draft_returns_a_prefilled_issue_link() -> None:
    server, context = build_server_context(NullClient(), mode=CapabilityMode.READ_ONLY)
    result = await server.call_tool(
        "kitaru_feedback_draft",
        {
            "request": {
                "title": "session_read drops tool errors",
                "problem": "kitaru_session_read with full=true hid a tool error.",
                "proposed_fix": "x" * 6000,
            }
        },
        context,
    )

    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert result.structured_content is not None
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == result.structured_content
    data = result.structured_content["data"]
    assert data["truncated"] is False
    assert len(data["issue_url"]) <= MAX_URL_LENGTH
    query = parse_qs(urlsplit(data["issue_url"]).query)
    assert query["title"] == ["MCP feedback: session_read drops tool errors"]
    assert query["body"] == [data["body"]]
    assert data["body"].endswith(", mode `read-only`")
