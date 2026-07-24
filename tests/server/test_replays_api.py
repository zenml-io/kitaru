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
    create_cohort,
    create_completed_session,
    create_experiment,
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


async def test_list_replays_by_run_and_worker(client: httpx.AsyncClient) -> None:
    """List replays filtered by experiment run and claiming worker."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id, session_count=2)
    experiment = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{experiment['id']}/runs", json={})
    assert response.status_code == 201
    run_id = response.json()["id"]
    standalone_session = await create_completed_session(client, agent_id)
    await create_replay(client, standalone_session)
    response = await client.post(
        f"/v1/experiment-runs/{run_id}/claim",
        json={"worker_id": "worker-1", "max_replays": 1},
    )
    assert response.status_code == 200
    claimed_id = response.json()["replays"][0]["id"]

    response = await client.get("/v1/replays", params={"experiment_run_id": run_id})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert all(item["experiment_run_id"] == run_id for item in body["items"])

    response = await client.get("/v1/replays", params={"worker_id": "worker-1"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == claimed_id

    response = await client.get(
        "/v1/replays",
        params={"experiment_run_id": run_id, "worker_id": "worker-2"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


async def test_list_replays_stale_claim_matches_pending() -> None:
    """Match a stale claim as pending in the status filter and the body."""
    transport = httpx.ASGITransport(app=experiment_app(heartbeat_timeout_seconds=-60))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent_id = await create_agent(client)
        await create_runnable_version(client, agent_id)
        cohort_id = await create_cohort(client, agent_id)
        experiment = await create_experiment(client, cohort_id)
        response = await client.post(
            f"/v1/experiments/{experiment['id']}/runs", json={}
        )
        assert response.status_code == 201
        run_id = response.json()["id"]
        response = await client.post(
            f"/v1/experiment-runs/{run_id}/claim",
            json={"worker_id": "worker-1", "max_replays": 1},
        )
        assert response.status_code == 200
        replay_id = response.json()["replays"][0]["id"]

        response = await client.get("/v1/replays", params={"status": "pending"})
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["id"] == replay_id
        assert body["items"][0]["status"] == "pending"

        response = await client.get("/v1/replays", params={"status": "claimed"})
        assert response.status_code == 200
        assert response.json()["total"] == 0


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
    assert response.json() == {"status": "running", "canceled": False}

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

    response = await client.post(f"/v1/replays/{replay_id}/heartbeat")
    assert response.status_code == 200
    assert response.json() == {"status": "completed", "canceled": True}


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
    assert response.json() == {"status": "canceled", "canceled": True}


async def test_heartbeat_stops_on_terminal_replays(client: httpx.AsyncClient) -> None:
    """Report the stop flag for failed and timed out replays."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    for status, body in (
        ("failed", {"status": "failed", "error": "agent exited with code 1"}),
        ("timed_out", {"status": "timed_out", "error": "wall clock limit exceeded"}),
    ):
        session_id = await create_completed_session(client, agent_id)
        created = await create_replay(client, session_id)
        await start_replay(client, created["id"])
        response = await client.patch(f"/v1/replays/{created['id']}", json=body)
        assert response.status_code == 200
        response = await client.post(f"/v1/replays/{created['id']}/heartbeat")
        assert response.status_code == 200
        assert response.json() == {"status": status, "canceled": True}


async def create_run_replay(client: httpx.AsyncClient) -> str:
    """Store an experiment run with one pending replay through the API.

    Args:
        client: HTTP client for the app.

    Returns:
        Id of the run's replay.
    """
    agent_id = await create_agent(client, name="run-bot")
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id, name="run-cohort")
    experiment = await create_experiment(client, cohort_id, name="run-experiment")
    response = await client.post(f"/v1/experiments/{experiment['id']}/runs", json={})
    assert response.status_code == 201
    run_id = response.json()["id"]
    response = await client.get(f"/v1/experiment-runs/{run_id}/replays")
    assert response.status_code == 200
    return response.json()["items"][0]["id"]


async def test_claim_replay(client: httpx.AsyncClient) -> None:
    """Claim a standalone replay for a worker."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    replay_id = created["id"]

    response = await client.post(
        f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-1"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "claimed"
    assert body["worker_id"] == "worker-1"
    assert body["claimed_at"] is not None
    assert body["heartbeat_at"] is not None

    response = await client.post(
        f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-2"}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {replay_id} cannot transition from 'claimed' to 'claimed'"
    }


async def test_claim_replay_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown replay id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/replays/{missing_id}/claim", json={"worker_id": "worker-1"}
    )
    assert response.status_code == 404


async def test_claim_run_replay(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when claiming a run replay directly."""
    replay_id = await create_run_replay(client)
    response = await client.post(
        f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-1"}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {replay_id} belongs to an experiment run"
    }


async def test_claim_resolves_stale_started_replay() -> None:
    """Claim a started standalone replay whose worker lost its heartbeat."""
    transport = httpx.ASGITransport(app=experiment_app(heartbeat_timeout_seconds=-60))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent_id = await create_agent(client)
        await create_runnable_version(client, agent_id)
        session_id = await create_completed_session(client, agent_id)
        created = await create_replay(client, session_id)
        replay_id = created["id"]
        await start_replay(client, replay_id)

        response = await client.get(f"/v1/replays/{replay_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "pending"

        response = await client.post(
            f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-2"}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "claimed"
        assert body["worker_id"] == "worker-2"
        assert body["attempt"] == 2

        response = await client.patch(
            f"/v1/replays/{replay_id}", json={"status": "running"}
        )
        assert response.status_code == 200


async def test_claim_times_out_exhausted_stale_replay() -> None:
    """Observe HTTP 409 claiming a stale replay out of attempts."""
    transport = httpx.ASGITransport(
        app=experiment_app(heartbeat_timeout_seconds=-60, max_attempts=1)
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent_id = await create_agent(client)
        await create_runnable_version(client, agent_id)
        session_id = await create_completed_session(client, agent_id)
        created = await create_replay(client, session_id)
        replay_id = created["id"]
        await start_replay(client, replay_id)

        response = await client.post(
            f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-2"}
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": f"Replay {replay_id} cannot transition from 'timed_out' "
            f"to 'claimed'"
        }
        response = await client.get(f"/v1/replays/{replay_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "timed_out"


async def test_release_replay(client: httpx.AsyncClient) -> None:
    """Requeue a claimed or running replay through release."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    replay_id = created["id"]

    response = await client.post(
        f"/v1/replays/{replay_id}/claim", json={"worker_id": "worker-1"}
    )
    assert response.status_code == 200
    response = await client.post(f"/v1/replays/{replay_id}/release")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt"] == 2
    assert body["worker_id"] is None

    await start_replay(client, replay_id)
    response = await client.post(f"/v1/replays/{replay_id}/release")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt"] == 3

    response = await client.post(f"/v1/replays/{replay_id}/release")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {replay_id} cannot transition from 'pending' to 'pending'"
    }


async def test_release_replay_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown replay id."""
    missing_id = uuid.uuid4()
    response = await client.post(f"/v1/replays/{missing_id}/release")
    assert response.status_code == 404


async def test_retry_replay(client: httpx.AsyncClient) -> None:
    """Requeue a failed standalone replay through retry."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    replay_id = created["id"]
    await start_replay(client, replay_id)
    await link_result_session(client, replay_id)
    response = await client.patch(
        f"/v1/replays/{replay_id}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200

    response = await client.post(f"/v1/replays/{replay_id}/retry")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt"] == 2
    assert body["error"] is None
    assert body["result_session_id"] is None
    assert body["started_at"] is None
    assert body["ended_at"] is None


async def test_retry_replay_conflicts(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 retrying a pending or run replay."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.post(f"/v1/replays/{created['id']}/retry")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} cannot transition from 'pending' "
        f"to 'pending'"
    }

    run_replay_id = await create_run_replay(client)
    response = await client.post(f"/v1/replays/{run_replay_id}/retry")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {run_replay_id} belongs to an experiment run"
    }


async def test_delete_replay(client: httpx.AsyncClient) -> None:
    """Delete a standalone replay."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)

    response = await client.delete(f"/v1/replays/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/replays/{created['id']}")
    assert response.status_code == 404


async def test_delete_replay_conflicts(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 deleting a running or run replay."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    await start_replay(client, created["id"])
    response = await client.delete(f"/v1/replays/{created['id']}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {created['id']} is claimed or running"
    }

    run_replay_id = await create_run_replay(client)
    response = await client.delete(f"/v1/replays/{run_replay_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Replay {run_replay_id} belongs to an experiment run"
    }


async def test_delete_replay_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown replay id."""
    missing_id = uuid.uuid4()
    response = await client.delete(f"/v1/replays/{missing_id}")
    assert response.status_code == 404


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
