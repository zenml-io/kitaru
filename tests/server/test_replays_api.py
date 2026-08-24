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
"""Tests for the replay routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_plugin,
    create_session,
    override_idempotency,
)
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_replay_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.session_node import SessionNode

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed replay, experiment, and run services."""
    return build_replay_services()


@pytest.fixture
async def client(services: ReplayServices) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed replay services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_replay_service] = lambda: services.replay_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def baseline_session_id(services: ReplayServices) -> uuid.UUID:
    """Provide a recorded session with a runnable agent version to replay."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh"),
    )
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="accuracy"
    )
    blob = await create_blob(services.blobs, ACCOUNT.id, content=b"score")
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )
    session = await create_session(
        services.sessions,
        ACCOUNT.id,
        agent_id=agent.id,
        agent_version_id=version.id,
        origin=SessionOrigin.RECORDED,
        inputs={"q": "hi"},
    )
    return session.id


async def test_create_replay(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """Create a replay and observe HTTP 201."""
    response = await client.post(
        "/api/v1/replays",
        json={
            "baseline_session_id": str(baseline_session_id),
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["baseline_session_id"] == str(baseline_session_id)
    assert body["status"] == "pending"
    assert body["result_session_id"] is None
    assert body["evaluate_baselines"] is False
    assert body["evaluators"][0]["evaluator"] == "accuracy"


async def test_create_replay_unknown_baseline_session(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 for an unknown baseline session."""
    response = await client.post(
        "/api/v1/replays",
        json={
            "baseline_session_id": str(uuid.uuid4()),
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )
    assert response.status_code == 404


async def test_get_replay(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """Get a replay by id."""
    created = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": str(baseline_session_id),
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    response = await client.get(f"/api/v1/replays/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_replay_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing replay."""
    response = await client.get(f"/api/v1/replays/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_replays_filters_by_baseline_session(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """List replays filters by baseline session id."""
    await client.post(
        "/api/v1/replays",
        json={
            "baseline_session_id": str(baseline_session_id),
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )
    matching_filter = {
        "field": "baseline_session_id",
        "op": "eq",
        "value": str(baseline_session_id),
    }
    response = await client.get(
        "/api/v1/replays", params={"filter": json.dumps(matching_filter)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["baseline_session_id"] == str(baseline_session_id)

    other_filter = {
        "field": "baseline_session_id",
        "op": "eq",
        "value": str(uuid.uuid4()),
    }
    response = await client.get(
        "/api/v1/replays", params={"filter": json.dumps(other_filter)}
    )
    assert response.json()["items"] == []


async def test_tool_lookup_not_configured_for_history(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """Observe HTTP 422 when a tool has no history config."""
    created = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": str(baseline_session_id),
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    response = await client.post(
        f"/api/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "search", "cache_key": "a" * 64},
    )
    assert response.status_code == 422


async def test_tool_lookup_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when looking up a tool on a missing replay."""
    response = await client.post(
        f"/api/v1/replays/{uuid.uuid4()}/tool-lookup",
        json={"tool_name": "search", "cache_key": "a" * 64},
    )
    assert response.status_code == 404


async def test_tool_lookup_invalid_cache_key_length(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """Observe HTTP 422 for a cache key that is not 64 characters."""
    created = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": str(baseline_session_id),
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    response = await client.post(
        f"/api/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "search", "cache_key": "short"},
    )
    assert response.status_code == 422


async def test_tool_lookup_negative_occurrence(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """Observe HTTP 422 for a negative occurrence."""
    created = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": str(baseline_session_id),
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    response = await client.post(
        f"/api/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "search", "cache_key": "a" * 64, "occurrence": -1},
    )
    assert response.status_code == 422


async def _replay_with_history_tool(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> dict[str, object]:
    return (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": str(baseline_session_id),
                "evaluators": [{"evaluator": "accuracy"}],
                "tool_policy": {
                    "default": {"type": "passthrough"},
                    "tools": {
                        "search": {
                            "type": "history",
                            "scope": "baseline",
                            "on_miss": "fail",
                        }
                    },
                },
            },
        )
    ).json()


async def test_tool_lookup_match_carries_status_and_error(
    client: httpx.AsyncClient,
    services: ReplayServices,
    baseline_session_id: uuid.UUID,
) -> None:
    """A hit for a failed node nests its status and error under match."""
    created = await _replay_with_history_tool(client, baseline_session_id)
    cache_key = "a" * 64
    await services.session_nodes.upsert_batch(
        baseline_session_id,
        [
            SessionNode(
                session_id=baseline_session_id,
                index=0,
                node_type=NodeType.TOOL_CALL,
                name="search",
                status=NodeStatus.FAILED,
                error="tool raised an exception",
                tool_name="search",
                cache_key=cache_key,
            )
        ],
    )

    response = await client.post(
        f"/api/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "search", "cache_key": cache_key, "occurrence": 0},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["match"]["status"] == "failed"
    assert body["match"]["error"] == "tool raised an exception"


async def test_tool_lookup_miss_returns_a_null_match(
    client: httpx.AsyncClient, baseline_session_id: uuid.UUID
) -> None:
    """A miss returns a null match."""
    created = await _replay_with_history_tool(client, baseline_session_id)

    response = await client.post(
        f"/api/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "search", "cache_key": "a" * 64},
    )

    assert response.status_code == 200
    assert response.json() == {"match": None}
