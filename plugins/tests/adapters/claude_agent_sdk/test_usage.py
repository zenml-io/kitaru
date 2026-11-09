#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Token usage recording tests for the Claude Agent SDK adapter."""

import uuid
from typing import cast

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

from kitaru.api_models.v1.session import TokenUsage
from kitaru.client import KitaruAPIClient
from kitaru_claude_agent_sdk.recording import (
    ROOT_EXTERNAL_ID,
    InvocationRecorder,
    finalize_terminal,
)

from .conftest import FakeClient, nodes


async def _recorder(client: FakeClient) -> InvocationRecorder:
    return await InvocationRecorder.start(
        client=cast(KitaruAPIClient, client),
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        safe_options={"model": "claude-test", "permission_mode": "default"},
    )


async def _record_turn(recorder: InvocationRecorder, usage: dict[str, int]) -> None:
    await recorder.record_message(
        AssistantMessage(
            content=[TextBlock("done")],
            model="claude-test",
            message_id="message-1",
            usage=usage,
        )
    )


@pytest.mark.parametrize(
    "usage",
    [
        {"input_tokens": 7, "cache_creation_input_tokens": 5},
        {"inputTokens": 7, "cacheCreationInputTokens": 5},
    ],
)
async def test_cache_creation_tokens_count_toward_input_usage(
    fake_client: FakeClient, usage: dict[str, int]
) -> None:
    recorder = await _recorder(fake_client)
    await _record_turn(recorder, usage)

    model = nodes(fake_client)[-1]
    assert model.tokens is not None
    assert model.tokens.input_tokens == 12
    assert model.tokens.cache_write_input_tokens == 5


async def test_cache_reads_and_writes_are_recorded_separately(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await _record_turn(
        recorder,
        {
            "input_tokens": 1200,
            "cache_read_input_tokens": 800,
            "cache_creation_input_tokens": 300,
        },
    )
    terminal = ResultMessage(
        subtype="success",
        duration_ms=20,
        duration_api_ms=10,
        is_error=False,
        num_turns=1,
        session_id="session-1",
        total_cost_usd=0.25,
        usage={
            "input_tokens": 1200,
            "cache_read_input_tokens": 800,
            "cache_creation_input_tokens": 500,
        },
        result="done",
        uuid="result-1",
    )

    await finalize_terminal(recorder, terminal)

    latest = {node.external_id: node for node in nodes(fake_client)}
    model = next(node for node in latest.values() if node.model == "claude-test")
    assert model.tokens == TokenUsage(
        input_tokens=1500, cached_input_tokens=800, cache_write_input_tokens=300
    )
    root = latest[ROOT_EXTERNAL_ID]
    assert root.tokens == TokenUsage(
        input_tokens=200,
        cached_input_tokens=0,
        cache_write_input_tokens=200,
    )
