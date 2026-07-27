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
"""Round-trip tests for the replays SDK resource."""

from collections.abc import AsyncGenerator

import pytest
from test_experiments import SCORING_POLICY
from test_jobs import create_session

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.jobs import (
    HistoryPolicy,
    HistoryScope,
    PassthroughPolicy,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.replays import ReplayCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def test_create_with_default_constructed_policy(
    api_client: KitaruAPIClient,
) -> None:
    """Round-trip a tool policy whose members carry only defaulted fields."""
    session_id, _ = await create_session(api_client)
    tool_policy = ToolPolicyConfig(
        default=PassthroughPolicy(), tools={"get_weather": HistoryPolicy()}
    )
    created = await api_client.replays.create(
        ReplayCreateRequest(
            input_session_id=session_id,
            scoring_policy=SCORING_POLICY,
            tool_policy=tool_policy,
        )
    )
    assert created.tool_policy == tool_policy


async def test_create_rejects_cohort_scope(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 for a history policy scoped to a cohort."""
    session_id, _ = await create_session(api_client)
    with pytest.raises(APIError) as exc_info:
        await api_client.replays.create(
            ReplayCreateRequest(
                input_session_id=session_id,
                scoring_policy=SCORING_POLICY,
                tool_policy=ToolPolicyConfig(
                    default=HistoryPolicy(scope=HistoryScope.COHORT)
                ),
            )
        )
    assert exc_info.value.status_code == 422


async def test_get_and_list_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a replay through create, get, and list."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    assert created.experiment_run_id is None
    assert created.input_session_id == session_id
    assert created.result_session_id is None
    assert created.passed is None
    assert created.tool_policy == ToolPolicyConfig(default=HistoryPolicy())
    assert created.scoring_policy == SCORING_POLICY

    loaded = await api_client.replays.get(created.id)
    assert loaded == created

    page = await api_client.replays.list(input_session_id=session_id)
    assert page.total == 1
    assert page.items[0].id == created.id

    page = await api_client.replays.list(passed=True)
    assert page.total == 0


async def test_get_diff_requires_a_result_session(
    api_client: KitaruAPIClient,
) -> None:
    """Surface HTTP 409 while the replay has no result session."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.replays.get_diff(created.id)
    assert exc_info.value.status_code == 409
