#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Reply text selectors on recorded Claude Agent SDK model calls."""

import uuid
from typing import Any, cast

import pytest
from claude_agent_sdk import AssistantMessage, TextBlock, ThinkingBlock, ToolUseBlock

from kitaru.api_models.v1.session_node import NodeType
from kitaru.client import KitaruAPIClient
from kitaru.json_pointer import resolve_json_pointer
from kitaru_claude_agent_sdk.recording import InvocationRecorder

from .conftest import FakeClient, nodes


@pytest.mark.parametrize(
    ("blocks", "selector", "reply"),
    [
        (
            [
                ThinkingBlock("planning", "sig"),
                TextBlock("Checking that order now."),
                ToolUseBlock("tool-1", "lookup", {"q": 1}),
            ],
            "/text/0",
            "Checking that order now.",
        ),
        (
            [ThinkingBlock("planning", "sig"), ToolUseBlock("tool-1", "lookup", {})],
            None,
            None,
        ),
        ([TextBlock("First."), TextBlock("Second.")], None, None),
    ],
    ids=["one-text-block", "tool-call-only", "several-text-blocks"],
)
async def test_model_call_points_at_its_only_text_block(
    fake_client: FakeClient, blocks: list[Any], selector: str | None, reply: str | None
) -> None:
    recorder = await InvocationRecorder.start(
        client=cast(KitaruAPIClient, fake_client),
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        safe_options={"model": "claude-test"},
    )
    await recorder.record_message(
        AssistantMessage(content=blocks, model="claude-test", message_id="message-1")
    )

    model = next(
        node for node in nodes(fake_client) if node.node_type is NodeType.LLM_CALL
    )
    assert model.output_text_selector == selector
    if selector is not None:
        assert resolve_json_pointer(model.outputs, selector) == (True, reply)
