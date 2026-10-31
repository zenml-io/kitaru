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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Token completeness across PostgreSQL-backed ingestion requests."""

from collections.abc import AsyncGenerator, Mapping

import httpx
import pytest

from conftest import db_settings, lifespan_client

TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cached_input_tokens",
    "reasoning_tokens",
)
ZERO_TOKENS = dict.fromkeys(TOKEN_FIELDS, 0)


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with lifespan_client(db_settings()) as client:
        yield client


@pytest.fixture
async def session_url(client: httpx.AsyncClient) -> str:
    agent = await client.post("/api/v1/agents", json={"name": "assistant"})
    assert agent.status_code == 201
    session = await client.post(
        "/api/v1/sessions",
        json={
            "agent_id": agent.json()["id"],
            "origin": "recorded",
            "inputs": None,
            "outputs": None,
        },
    )
    assert session.status_code == 201
    return f"/api/v1/sessions/{session.json()['id']}"


def _node(
    external_id: str,
    tokens: Mapping[str, int | None] | None,
    node_type: str = "llm_call",
) -> dict[str, object]:
    return {
        "external_id": external_id,
        "node_type": node_type,
        "name": "call",
        "status": "completed",
        "tokens": tokens,
        "inputs": None,
        "outputs": None,
        "attributes": None,
    }


@pytest.mark.parametrize("node_type", ["llm_call", "tool_call", "span"])
async def test_absent_token_measurements_remain_unknown(
    client: httpx.AsyncClient, session_url: str, node_type: str
) -> None:
    """An unmeasured node must not manufacture a measured zero."""
    response = await client.post(
        f"{session_url}/nodes", json={"nodes": [_node("unknown", None, node_type)]}
    )
    assert response.status_code == 200
    session = await client.get(session_url)
    assert session.status_code == 200
    assert session.json()["tokens"] is None


@pytest.mark.parametrize("field", TOKEN_FIELDS)
async def test_token_completeness_recovers_and_clears_across_replacements(
    client: httpx.AsyncClient, session_url: str, field: str
) -> None:
    """Keep per-field completeness through batches, retries, and node type changes."""
    unknown = {**ZERO_TOKENS, field: None}
    known = {**ZERO_TOKENS, field: 10}
    steps = [
        ([_node("first", unknown)], {**ZERO_TOKENS, field: None}, 1, 0),
        ([_node("second", known)], {**ZERO_TOKENS, field: None}, 2, 0),
        ([_node("first", {**ZERO_TOKENS, field: 3})], {**ZERO_TOKENS, field: 13}, 2, 0),
        ([_node("second", unknown)], {**ZERO_TOKENS, field: None}, 2, 0),
        ([_node("second", ZERO_TOKENS)], {**ZERO_TOKENS, field: 3}, 2, 0),
        ([_node("first", None, "tool_call")], ZERO_TOKENS, 1, 1),
        (
            [_node("tool", None, "tool_call"), _node("span", None, "span")],
            ZERO_TOKENS,
            1,
            2,
        ),
        (
            [_node("span", {field: 2}, "span")],
            {**ZERO_TOKENS, field: 2},
            1,
            2,
        ),
    ]
    for nodes, expected, llm_calls, tool_calls in steps:
        for _ in range(2):
            response = await client.post(f"{session_url}/nodes", json={"nodes": nodes})
            assert response.status_code == 200
            session = await client.get(session_url)
            assert session.status_code == 200
            assert session.json()["tokens"] == expected
            assert session.json()["llm_call_count"] == llm_calls
            assert session.json()["tool_call_count"] == tool_calls


async def test_mixed_usage_in_one_batch_does_not_expose_a_subtotal(
    client: httpx.AsyncClient, session_url: str
) -> None:
    """A known call and an unmeasured call are still an unknown total."""
    response = await client.post(
        f"{session_url}/nodes",
        json={"nodes": [_node("known", {"input_tokens": 100}), _node("unknown", None)]},
    )
    assert response.status_code == 200
    session = await client.get(session_url)
    assert session.json()["tokens"] is None
