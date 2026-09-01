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
"""Round-trip tests for the synchronous user-facing Kitaru client."""

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from kitaru.api_models.v1.agent import AgentListParams, AgentResponse
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.client.sync_client import KitaruSyncClient


def _agent_response(**overrides: Any) -> AgentResponse:
    """Build an agent response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "owner_id": uuid.uuid4(),
        "created": now,
        "updated": now,
        "name": "assistant",
        "description": None,
        "latest_version": 1,
    }
    values.update(overrides)
    return AgentResponse(**values)


def _replay_response(**overrides: Any) -> ReplayResponse:
    """Build a replay response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "job_id": uuid.uuid4(),
        "experiment_run_id": None,
        "baseline_session_id": uuid.uuid4(),
        "result_session_id": None,
        "override": None,
        "tool_policy": {"default": {"type": "passthrough"}, "tools": {}},
        "evaluators": [{"evaluator": "accuracy", "version": 1, "params": {}}],
        "evaluate_baselines": False,
        "baseline_evaluation_mode": BaselineEvaluationMode.NONE,
        "status": ReplayStatus.PENDING,
        "error": None,
        "created": now,
        "updated": now,
    }
    values.update(overrides)
    return ReplayResponse(**values)


@pytest.fixture
def mock_client() -> Iterator[tuple[KitaruAPIClient, KitaruSyncClient]]:
    """Provide a KitaruSyncClient over a bare, unrouted API client for mocking."""
    api_client = KitaruAPIClient(base_url="http://test", api_key="k")
    client = KitaruSyncClient(api_client=api_client)
    yield api_client, client
    client.close()


def test_context_manager_returns_self_and_closes_the_loop() -> None:
    """Enter the context manager and stop the event loop thread on exit."""
    api_client = KitaruAPIClient(base_url="http://test", api_key="k")
    with KitaruSyncClient(api_client=api_client) as client:
        assert client.api is api_client
    assert client._loop.is_closed()


def test_get_agent_by_name(
    mock_client: tuple[KitaruAPIClient, KitaruSyncClient],
) -> None:
    """Get an agent by its exact name."""
    api_client, client = mock_client
    agent = _agent_response(name="assistant")
    api_client.agents.list = AsyncMock(
        return_value=Page(items=[agent], next_cursor=None)
    )

    loaded = client.get_agent("assistant")

    assert loaded == agent


def test_get_agent_by_name_not_found(
    mock_client: tuple[KitaruAPIClient, KitaruSyncClient],
) -> None:
    """Raise NotFoundError for an unknown agent name."""
    api_client, client = mock_client
    api_client.agents.list = AsyncMock(return_value=Page(items=[], next_cursor=None))

    with pytest.raises(NotFoundError):
        client.get_agent("missing")


def test_list_agents_yields_items_lazily(
    mock_client: tuple[KitaruAPIClient, KitaruSyncClient],
) -> None:
    """Drain agents one at a time instead of collecting the whole page upfront."""
    api_client, client = mock_client
    first = _agent_response(name="assistant")
    second = _agent_response(name="reviewer")
    advanced: list[str] = []

    async def fake_iter(
        params: AgentListParams | None = None,
    ) -> AsyncIterator[AgentResponse]:
        advanced.append("first")
        yield first
        advanced.append("second")
        yield second

    api_client.agents.iter = cast(Any, fake_iter)
    iterator = client.list_agents()

    assert advanced == []
    assert next(iterator) == first
    assert advanced == ["first"]
    assert next(iterator) == second
    assert advanced == ["first", "second"]
    with pytest.raises(StopIteration):
        next(iterator)


def test_replay_wait_true_returns_terminal_replay(
    mock_client: tuple[KitaruAPIClient, KitaruSyncClient],
) -> None:
    """Wait for a replay to reach a terminal status and return it."""
    api_client, client = mock_client
    session_id = uuid.uuid4()
    pending = _replay_response(status=ReplayStatus.PENDING)
    completed = _replay_response(id=pending.id, status=ReplayStatus.COMPLETED)
    api_client.replays.create = AsyncMock(return_value=pending)
    api_client.replays.get = AsyncMock(return_value=completed)

    result = client.replay(
        session_id, evaluators=[EvaluatorConfig(evaluator="accuracy")]
    )

    assert result.status == ReplayStatus.COMPLETED
    api_client.replays.get.assert_awaited_once_with(pending.id)
