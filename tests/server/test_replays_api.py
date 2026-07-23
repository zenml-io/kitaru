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

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api import (
    SCORING_POLICY,
    SCORING_POLICY_RESPONSE,
    create_agent,
    create_completed_session,
    create_runnable_version,
)

from conftest import experiment_app
from kitaru.hashing import tool_call_cache_key


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_replay(
    client: httpx.AsyncClient, session_id: str, **overrides: object
) -> dict:
    """Store a standalone replay through the API.

    Args:
        client: HTTP client for the app.
        session_id: Id of the session to replay.
        **overrides: Create request body overrides.

    Returns:
        Created replay body.
    """
    body: dict[str, object] = {
        "original_session_id": session_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    response = await client.post("/v1/replays", json=body)
    assert response.status_code == 201
    return response.json()


async def test_create_replay_defaults(client: httpx.AsyncClient) -> None:
    """Create a standalone replay with the history default tool policy."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={"original_session_id": session_id, "scoring_policy": SCORING_POLICY},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["experiment_run_id"] is None
    assert body["original_session_id"] == session_id
    assert body["result_session_id"] is None
    assert body["agent_version_id"] == version_id
    assert body["status"] == "pending"
    assert body["attempt"] == 1
    assert body["worker_id"] is None
    assert body["passed"] is None
    assert body["override"] is None
    assert body["tool_policy"] == {
        "default": {
            "type": "history",
            "scope": "original_session",
            "on_miss": "fail",
        },
        "tools": {},
    }
    assert body["scoring_policy"] == SCORING_POLICY_RESPONSE
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_replay_repeats_freely(client: httpx.AsyncClient) -> None:
    """Replay the same session standalone any number of times."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    first = await create_replay(client, session_id)
    second = await create_replay(client, session_id)
    assert first["id"] != second["id"]


async def test_create_replay_rejects_cohort_scope(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a history policy scoped to a cohort."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={
            "original_session_id": session_id,
            "scoring_policy": SCORING_POLICY,
            "tool_policy": {
                "default": {"type": "history", "scope": "cohort"},
                "tools": {},
            },
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Standalone replays cannot use history scope 'cohort'"
    }


async def test_create_replay_in_progress_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an in-progress original session."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.post(
        "/v1/replays",
        json={"original_session_id": session_id, "scoring_policy": SCORING_POLICY},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": f"Session {session_id} is in progress"}


async def test_create_replay_unknown_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown session id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/replays",
        json={
            "original_session_id": str(missing_id),
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Session {missing_id} was not found"}


async def test_create_replay_no_runnable_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when no runnable version resolves."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={"original_session_id": session_id, "scoring_policy": SCORING_POLICY},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent {agent_id} has no runnable version"}


async def test_create_replay_cross_agent_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a version of another agent."""
    agent_id = await create_agent(client)
    other_id = await create_agent(client, name="triage-bot")
    other_version_id = await create_runnable_version(client, other_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={
            "original_session_id": session_id,
            "agent_version_id": other_version_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": f"Agent version {other_version_id} does not belong to "
        f"agent {agent_id}"
    }


async def test_create_replay_missing_scoring_policy(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 without a scoring policy."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays", json={"original_session_id": session_id}
    )
    assert response.status_code == 422


async def test_get_replay(client: httpx.AsyncClient) -> None:
    """Get a replay by id."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.get(f"/v1/replays/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_replay_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown replay id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/replays/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Replay {missing_id} was not found"}


async def test_list_replays_filters(client: httpx.AsyncClient) -> None:
    """List replays filtered by session, status, and standalone."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    first_session = await create_completed_session(client, agent_id)
    second_session = await create_completed_session(client, agent_id)
    first = await create_replay(client, first_session)
    await create_replay(client, second_session)

    response = await client.get("/v1/replays")
    assert response.status_code == 200
    assert response.json()["total"] == 2

    response = await client.get(
        "/v1/replays", params={"original_session_id": first_session}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == first["id"]

    response = await client.get("/v1/replays", params={"status": "pending"})
    assert response.status_code == 200
    assert response.json()["total"] == 2
    response = await client.get("/v1/replays", params={"status": "running"})
    assert response.status_code == 200
    assert response.json()["total"] == 0

    response = await client.get("/v1/replays", params={"standalone": "true"})
    assert response.status_code == 200
    assert response.json()["total"] == 2
    response = await client.get("/v1/replays", params={"standalone": "false"})
    assert response.status_code == 200
    assert response.json()["total"] == 0

    response = await client.get("/v1/replays", params={"page": 2, "page_size": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert len(body["items"]) == 1


async def test_list_replays_invalid_status(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an unknown status filter value."""
    response = await client.get("/v1/replays", params={"status": "bogus"})
    assert response.status_code == 422


async def test_delete_session_referenced_by_replay(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when deleting a session a replay references."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    await create_replay(client, session_id)

    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Session {session_id} is referenced by replays"
    }


async def start_replay(client: httpx.AsyncClient, replay_id: str) -> None:
    """Move a replay to running through the API.

    Args:
        client: HTTP client for the app.
        replay_id: Id of the replay.
    """
    response = await client.patch(
        f"/v1/replays/{replay_id}", json={"status": "running"}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "running"


async def link_result_session(client: httpx.AsyncClient, replay_id: str) -> str:
    """Open the replay's result session through the API.

    Args:
        client: HTTP client for the app.
        replay_id: Id of the replay.

    Returns:
        Id of the result session.
    """
    response = await client.get(f"/v1/replays/{replay_id}")
    assert response.status_code == 200
    replay = response.json()
    original = await client.get(f"/v1/sessions/{replay['original_session_id']}")
    assert original.status_code == 200
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": original.json()["agent_id"],
            "origin": "recorded",
            "replay_id": replay_id,
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


async def test_get_spec(client: httpx.AsyncClient) -> None:
    """Resolve a replay spec with the run command and inputs."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "recorded", "inputs": {"prompt": "hi"}},
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    created = await create_replay(client, session_id)

    response = await client.get(f"/v1/replays/{created['id']}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec == {
        "replay_id": created["id"],
        "inputs": {"prompt": "hi"},
        "override": None,
        "tool_policy": created["tool_policy"],
        "scoring_policy": SCORING_POLICY_RESPONSE,
        "score_baselines": True,
        "run": {
            "command": "python agent.py",
            "working_dir": None,
            "env": {},
            "timeout_seconds": 600,
        },
        "secret_env": {},
        "original_session_id": session_id,
    }


async def test_get_spec_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown replay id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/replays/{missing_id}/spec")
    assert response.status_code == 404


async def test_runner_flow_completes_replay(client: httpx.AsyncClient) -> None:
    """Walk a standalone replay through the runner endpoints."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    replay_id = created["id"]

    await start_replay(client, replay_id)
    result_session_id = await link_result_session(client, replay_id)

    response = await client.get(f"/v1/replays/{replay_id}")
    assert response.json()["result_session_id"] == result_session_id

    response = await client.post(f"/v1/replays/{replay_id}/heartbeat")
    assert response.status_code == 200
    assert response.json() == {"canceled": False}

    response = await client.patch(
        f"/v1/sessions/{result_session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/replays/{replay_id}",
        json={
            "status": "completed",
            "passed": True,
            "score": 0.8,
            "scores": {"conciseness": 0.8},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["passed"] is True
    assert body["score"] == 0.8
    assert body["scores"] == {"conciseness": 0.8}
    assert body["ended_at"] is not None
    assert body["diff"]["status_changed"] is False
    assert body["diff"]["tool_calls"] == {
        "matched": 0,
        "mocked": 0,
        "added": 0,
        "removed": 0,
    }

    response = await client.get(f"/v1/replays/{replay_id}/diff")
    assert response.status_code == 200
    diff = response.json()
    assert diff["replay_id"] == replay_id
    assert diff["original_session_id"] == session_id
    assert diff["result_session_id"] == result_session_id
    assert diff["node_pairs"] == []
    assert diff["added_nodes"] == []
    assert diff["removed_nodes"] == []


async def test_patch_replay_illegal_transition(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for an illegal runner transition."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.patch(
        f"/v1/replays/{created['id']}",
        json={"status": "completed", "passed": True, "score": 1.0, "scores": {}},
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} cannot transition from 'pending' "
        f"to 'completed'"
    }


async def test_patch_replay_completed_without_result_session(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 when completing an unlinked replay."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    await start_replay(client, created["id"])
    response = await client.patch(
        f"/v1/replays/{created['id']}",
        json={"status": "completed", "passed": True, "score": 1.0, "scores": {}},
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} has no result session"
    }


async def test_patch_replay_failed_requires_error(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when failing without an error."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    await start_replay(client, created["id"])
    response = await client.patch(
        f"/v1/replays/{created['id']}", json={"status": "failed"}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Failing a replay requires an error"}

    response = await client.patch(
        f"/v1/replays/{created['id']}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"] == "agent exited with code 1"


async def test_heartbeat_canceled_replay(client: httpx.AsyncClient) -> None:
    """Report cancellation through the heartbeat."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)

    response = await client.post(f"/v1/replays/{created['id']}/heartbeat")
    assert response.status_code == 409

    await start_replay(client, created["id"])
    response = await client.patch(
        f"/v1/replays/{created['id']}", json={"status": "canceled"}
    )
    assert response.status_code == 200
    response = await client.post(f"/v1/replays/{created['id']}/heartbeat")
    assert response.status_code == 200
    assert response.json() == {"canceled": True}


async def test_tool_lookup(client: httpx.AsyncClient) -> None:
    """Resolve a history lookup against the original session's nodes."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    inputs = {"city": "Berlin"}
    response = await client.post(
        f"/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                {
                    "id": str(uuid.uuid4()),
                    "sequence": 0,
                    "node_type": "tool_call",
                    "name": "get_weather",
                    "status": "completed",
                    "tool_name": "get_weather",
                    "inputs": inputs,
                    "outputs": {"temp": 21},
                }
            ]
        },
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    created = await create_replay(client, session_id)

    cache_key = tool_call_cache_key("get_weather", inputs)
    response = await client.post(
        f"/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "get_weather", "inputs": inputs, "cache_key": cache_key},
    )
    assert response.status_code == 200
    assert response.json() == {"found": True, "result": {"temp": 21}}

    paris = {"city": "Paris"}
    response = await client.post(
        f"/v1/replays/{created['id']}/tool-lookup",
        json={
            "tool_name": "get_weather",
            "inputs": paris,
            "cache_key": tool_call_cache_key("get_weather", paris),
        },
    )
    assert response.status_code == 200
    assert response.json() == {"found": False, "result": None}

    response = await client.post(
        f"/v1/replays/{created['id']}/tool-lookup",
        json={"tool_name": "get_weather", "inputs": paris, "cache_key": "a" * 64},
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Cache key does not match the tool name and inputs"
    }


async def test_diff_requires_result_session(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a diff without a result session."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.get(f"/v1/replays/{created['id']}/diff")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} has no result session"
    }


async def test_session_link_conflicts(client: httpx.AsyncClient) -> None:
    """Observe link errors for inactive, linked, and unknown replays."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)

    body = {"agent_id": agent_id, "origin": "recorded", "replay_id": created["id"]}
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} is not claimed or running"
    }

    await start_replay(client, created["id"])
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} already has a result session"
    }

    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "recorded", "replay_id": str(missing_id)},
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Replay {missing_id} was not found"}
