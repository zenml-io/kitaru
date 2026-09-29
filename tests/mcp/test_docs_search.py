#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bundled documentation search through the public MCP protocol."""

import json

from mcp.types import CallToolResult, TextContent
from mcp_fakes import NullClient, build_server_context

from kitaru.mcp.settings import CapabilityMode


async def test_replay_question_returns_a_citable_guide() -> None:
    server, context = build_server_context(NullClient())
    result = await server.call_tool(
        "kitaru_docs_search",
        {"request": {"query": "replay on_miss", "limit": 3}},
        context,
    )

    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert result.structured_content is not None
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == result.structured_content
    data = result.structured_content["data"]
    assert len(data["source_revision"]) == 64
    assert 1 <= len(data["matches"]) <= 3
    assert any(
        "on_miss" in f"{match['heading']} {match['excerpt']}".lower()
        for match in data["matches"]
    )
    assert "https://docs.zenml.io/kitaru/guides/tool-policies" in {
        match["url"] for match in data["matches"]
    }
    assert len({match["url"] for match in data["matches"]}) == len(data["matches"])
    assert all(
        match["url"].startswith("https://docs.zenml.io/kitaru/")
        for match in data["matches"]
    )


async def test_search_is_deterministic_and_bounded_across_modes() -> None:
    for mode in CapabilityMode:
        server, context = build_server_context(NullClient(), mode=mode)
        arguments = {"request": {"query": "import sessions", "limit": 2}}
        first = await server.call_tool("kitaru_docs_search", arguments, context)
        second = await server.call_tool("kitaru_docs_search", arguments, context)
        assert isinstance(first, CallToolResult)
        assert isinstance(second, CallToolResult)
        assert first.is_error is False
        assert first.structured_content == second.structured_content
        assert first.structured_content is not None
        assert len(first.structured_content["data"]["matches"]) <= 2


async def test_unmatched_query_does_not_invent_a_source() -> None:
    server, context = build_server_context(NullClient())
    result = await server.call_tool(
        "kitaru_docs_search",
        {"request": {"query": "zyxwvut987654321"}},
        context,
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert result.structured_content is not None
    assert result.structured_content["data"]["matches"] == []


async def test_whitespace_only_query_is_rejected() -> None:
    server, context = build_server_context(NullClient())
    result = await server.call_tool(
        "kitaru_docs_search",
        {"request": {"query": "   "}},
        context,
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is True
    assert result.structured_content is not None
    assert result.structured_content["error"]["code"] == "invalid_arguments"
