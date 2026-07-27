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
"""Tests for the job routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api import (
    create_agent,
    create_cohort,
    create_completed_session,
    create_experiment,
    create_runnable_version,
)
from test_replays_api import create_replay

from conftest import experiment_app
from kitaru.hashing import tool_call_cache_key


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def register_worker(client: httpx.AsyncClient, name: str = "worker-1") -> str:
    """Register a worker through the API.

    Args:
        client: HTTP client for the app.
        name: Worker name.

    Returns:
        Id of the worker.
    """
    response = await client.post("/v1/workers", json={"name": name})
    assert response.status_code == 200
    return response.json()["id"]


async def test_get_job(client: httpx.AsyncClient) -> None:
    """Get a job by id."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.get(f"/v1/jobs/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_job_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown job id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/jobs/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Job {missing_id} was not found"}


async def test_list_jobs_filters(client: httpx.AsyncClient) -> None:
    """List jobs filtered by session, status, and standalone."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    first_session = await create_completed_session(client, agent_id)
    second_session = await create_completed_session(client, agent_id)
    first = await create_replay(client, first_session)
    await create_replay(client, second_session)

    response = await client.get("/v1/jobs")
    assert response.status_code == 200
    assert response.json()["total"] == 2

    response = await client.get("/v1/jobs", params={"input_session_id": first_session})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == first["id"]

    response = await client.get("/v1/jobs", params={"status": "pending"})
    assert response.status_code == 200
    assert response.json()["total"] == 2
    response = await client.get("/v1/jobs", params={"status": "running"})
    assert response.status_code == 200
    assert response.json()["total"] == 0

    response = await client.get("/v1/jobs", params={"standalone": "true"})
    assert response.status_code == 200
    assert response.json()["total"] == 2
    response = await client.get("/v1/jobs", params={"standalone": "false"})
    assert response.status_code == 200
    assert response.json()["total"] == 0

    response = await client.get("/v1/jobs", params={"page": 2, "page_size": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert len(body["items"]) == 1


async def test_list_jobs_by_run_and_worker(client: httpx.AsyncClient) -> None:
    """List jobs filtered by experiment run and claiming worker."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id, session_count=2)
    experiment = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{experiment['id']}/runs", json={})
    assert response.status_code == 201
    run_id = response.json()["id"]
    standalone_session = await create_completed_session(client, agent_id)
    await create_replay(client, standalone_session)
    worker_id = await register_worker(client)
    response = await client.post(
        "/v1/jobs/claim",
        json={"worker_id": worker_id, "max_jobs": 1, "experiment_run_id": run_id},
    )
    assert response.status_code == 200
    claimed_id = response.json()["jobs"][0]["job"]["id"]

    response = await client.get("/v1/jobs", params={"experiment_run_id": run_id})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert all(item["experiment_run_id"] == run_id for item in body["items"])

    response = await client.get("/v1/jobs", params={"worker_id": worker_id})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == claimed_id

    response = await client.get(
        "/v1/jobs",
        params={"experiment_run_id": run_id, "worker_id": str(uuid.uuid4())},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


async def test_list_jobs_stale_claim_matches_pending() -> None:
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
        worker_id = await register_worker(client)
        response = await client.post(
            "/v1/jobs/claim",
            json={"worker_id": worker_id, "max_jobs": 1, "experiment_run_id": run_id},
        )
        assert response.status_code == 200
        job_id = response.json()["jobs"][0]["job"]["id"]

        response = await client.get("/v1/jobs", params={"status": "pending"})
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["id"] == job_id
        assert body["items"][0]["status"] == "pending"

        response = await client.get("/v1/jobs", params={"status": "claimed"})
        assert response.status_code == 200
        assert response.json()["total"] == 0


async def test_list_jobs_invalid_status(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an unknown status filter value."""
    response = await client.get("/v1/jobs", params={"status": "bogus"})
    assert response.status_code == 422


async def test_delete_session_referenced_by_job(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when deleting a session a job references."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    await create_replay(client, session_id)

    response = await client.delete(f"/v1/sessions/{session_id}")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Session {session_id} is referenced by jobs"}


async def start_job(client: httpx.AsyncClient, job_id: str) -> None:
    """Move a job to running through the API.

    Args:
        client: HTTP client for the app.
        job_id: Id of the job.
    """
    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
    assert response.status_code == 200
    assert response.json()["status"] == "running"


async def link_result_session(client: httpx.AsyncClient, job_id: str) -> str:
    """Open the job's result session through the API.

    Args:
        client: HTTP client for the app.
        job_id: Id of the job.

    Returns:
        Id of the result session.
    """
    response = await client.get(f"/v1/jobs/{job_id}")
    assert response.status_code == 200
    job = response.json()
    original = await client.get(f"/v1/sessions/{job['input_session_id']}")
    assert original.status_code == 200
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": original.json()["agent_id"],
            "origin": "recorded",
            "job_id": job_id,
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


async def run_score_jobs(
    client: httpx.AsyncClient, job_id: str, scores: dict[str, float]
) -> None:
    """Run every score job of a replay to completion through the API.

    Args:
        client: HTTP client for the app.
        job_id: Id of the parent replay.
        scores: Score values by scorer name.
    """
    response = await client.get("/v1/jobs", params={"kind": "score"})
    assert response.status_code == 200
    children = [
        job for job in response.json()["items"] if job["parent_job_id"] == job_id
    ]
    assert children
    for child in children:
        response = await client.patch(
            f"/v1/jobs/{child['id']}", json={"status": "running"}
        )
        assert response.status_code == 200
        response = await client.patch(
            f"/v1/jobs/{child['id']}",
            json={"status": "completed", "score": scores[child["scorer"]["name"]]},
        )
        assert response.status_code == 200


async def test_get_spec(client: httpx.AsyncClient) -> None:
    """Resolve a job spec with the run command and inputs."""
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

    response = await client.get(f"/v1/jobs/{created['id']}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec == {
        "job_id": created["id"],
        "kind": "replay",
        "inputs": {"prompt": "hi"},
        "override": None,
        "tool_policy": created["tool_policy"],
        "scorer": None,
        "importer": None,
        "run": {
            "command": "python agent.py",
            "working_dir": None,
            "env": {},
            "timeout_seconds": 600,
        },
        "secret_env": {},
        "input_session_id": session_id,
        "name": None,
    }


async def test_get_spec_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown job id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/jobs/{missing_id}/spec")
    assert response.status_code == 404


async def test_runner_flow_completes_job(client: httpx.AsyncClient) -> None:
    """Walk a standalone job through the runner endpoints."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    job_id = created["id"]

    await start_job(client, job_id)
    result_session_id = await link_result_session(client, job_id)

    response = await client.get(f"/v1/jobs/{job_id}")
    assert response.json()["result_session_id"] == result_session_id

    response = await client.patch(
        f"/v1/sessions/{result_session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "scoring"})
    assert response.status_code == 200
    assert response.json()["status"] == "scoring"

    await run_score_jobs(client, job_id, {"conciseness": 0.8})
    response = await client.get(f"/v1/jobs/{job_id}")
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

    response = await client.get(f"/v1/jobs/{job_id}/diff")
    assert response.status_code == 200
    diff = response.json()
    assert diff["replay_id"] == job_id
    assert diff["original_session_id"] == session_id
    assert diff["result_session_id"] == result_session_id
    assert diff["node_pairs"] == []
    assert diff["added_nodes"] == []
    assert diff["removed_nodes"] == []


async def test_patch_job_illegal_transition(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for an illegal runner transition."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.patch(
        f"/v1/jobs/{created['id']}", json={"status": "scoring"}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {created['id']} cannot transition from 'pending' to 'scoring'"
    }


async def test_patch_job_scoring_without_result_session(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 when scoring an unlinked job."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    await start_job(client, created["id"])
    response = await client.patch(
        f"/v1/jobs/{created['id']}", json={"status": "scoring"}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Job {created['id']} has no result session"}


async def test_patch_job_failed_requires_error(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when failing without an error."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    await start_job(client, created["id"])
    response = await client.patch(
        f"/v1/jobs/{created['id']}", json={"status": "failed"}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Failing a job requires an error"}

    response = await client.patch(
        f"/v1/jobs/{created['id']}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"] == "agent exited with code 1"


async def test_worker_heartbeat_touches_owned_active_jobs(
    client: httpx.AsyncClient,
) -> None:
    """Record a heartbeat on a claimed job and abandon nothing."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    worker_id = await register_worker(client)
    response = await client.post(
        f"/v1/jobs/{created['id']}/claim", json={"worker_id": worker_id}
    )
    assert response.status_code == 200

    response = await client.post(
        f"/v1/workers/{worker_id}/heartbeat", json={"job_ids": [created["id"]]}
    )
    assert response.status_code == 200
    assert response.json() == {"abandon": []}

    response = await client.get(f"/v1/jobs/{created['id']}")
    assert response.json()["heartbeat_at"] is not None


async def test_worker_heartbeat_abandons_terminal_and_foreign_jobs(
    client: httpx.AsyncClient,
) -> None:
    """Abandon reported jobs that are terminal, foreign, or unknown."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    worker_id = await register_worker(client)
    other_worker_id = await register_worker(client, name="worker-2")
    canceled = await create_replay(
        client, await create_completed_session(client, agent_id)
    )
    await start_job(client, canceled["id"])
    response = await client.patch(
        f"/v1/jobs/{canceled['id']}", json={"status": "canceled"}
    )
    assert response.status_code == 200
    foreign = await create_replay(
        client, await create_completed_session(client, agent_id)
    )
    response = await client.post(
        f"/v1/jobs/{foreign['id']}/claim", json={"worker_id": other_worker_id}
    )
    assert response.status_code == 200
    missing_id = str(uuid.uuid4())

    response = await client.post(
        f"/v1/workers/{worker_id}/heartbeat",
        json={"job_ids": [canceled["id"], foreign["id"], missing_id]},
    )
    assert response.status_code == 200
    assert response.json() == {"abandon": [canceled["id"], foreign["id"], missing_id]}


async def test_worker_heartbeat_unknown_worker(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/workers/{missing_id}/heartbeat", json={"job_ids": []}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Worker {missing_id} was not found"}


async def create_run_job(client: httpx.AsyncClient) -> str:
    """Store an experiment run with one pending job through the API.

    Args:
        client: HTTP client for the app.

    Returns:
        Id of the run's job.
    """
    agent_id = await create_agent(client, name="run-bot")
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id, name="run-cohort")
    experiment = await create_experiment(client, cohort_id, name="run-experiment")
    response = await client.post(f"/v1/experiments/{experiment['id']}/runs", json={})
    assert response.status_code == 201
    run_id = response.json()["id"]
    response = await client.get(f"/v1/experiment-runs/{run_id}/jobs")
    assert response.status_code == 200
    return response.json()["items"][0]["id"]


async def test_claim_job(client: httpx.AsyncClient) -> None:
    """Claim a standalone job for a worker."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    job_id = created["id"]

    worker_id = await register_worker(client)
    other_worker_id = await register_worker(client, name="worker-2")
    response = await client.post(
        f"/v1/jobs/{job_id}/claim", json={"worker_id": worker_id}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "claimed"
    assert body["worker_id"] == worker_id
    assert body["claimed_at"] is not None
    assert body["heartbeat_at"] is not None

    response = await client.post(
        f"/v1/jobs/{job_id}/claim", json={"worker_id": other_worker_id}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {job_id} cannot transition from 'claimed' to 'claimed'"
    }


async def test_claim_job_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown job id."""
    missing_id = uuid.uuid4()
    worker_id = await register_worker(client)
    response = await client.post(
        f"/v1/jobs/{missing_id}/claim", json={"worker_id": worker_id}
    )
    assert response.status_code == 404


async def test_claim_run_job(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when claiming a run job directly."""
    job_id = await create_run_job(client)
    worker_id = await register_worker(client)
    response = await client.post(
        f"/v1/jobs/{job_id}/claim", json={"worker_id": worker_id}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Job {job_id} belongs to an experiment run"}


async def test_claim_resolves_stale_started_job() -> None:
    """Claim a started standalone job whose worker lost its heartbeat."""
    transport = httpx.ASGITransport(app=experiment_app(heartbeat_timeout_seconds=-60))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent_id = await create_agent(client)
        await create_runnable_version(client, agent_id)
        session_id = await create_completed_session(client, agent_id)
        created = await create_replay(client, session_id)
        job_id = created["id"]
        await start_job(client, job_id)

        response = await client.get(f"/v1/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "pending"

        worker_id = await register_worker(client, name="worker-2")
        response = await client.post(
            f"/v1/jobs/{job_id}/claim", json={"worker_id": worker_id}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "claimed"
        assert body["worker_id"] == worker_id
        assert body["attempt"] == 2

        response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
        assert response.status_code == 200


async def test_claim_times_out_exhausted_stale_job() -> None:
    """Observe HTTP 409 claiming a stale job out of attempts."""
    transport = httpx.ASGITransport(
        app=experiment_app(heartbeat_timeout_seconds=-60, max_attempts=1)
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent_id = await create_agent(client)
        await create_runnable_version(client, agent_id)
        session_id = await create_completed_session(client, agent_id)
        created = await create_replay(client, session_id)
        job_id = created["id"]
        await start_job(client, job_id)

        worker_id = await register_worker(client, name="worker-2")
        response = await client.post(
            f"/v1/jobs/{job_id}/claim", json={"worker_id": worker_id}
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": f"Job {job_id} cannot transition from 'timed_out' to 'claimed'"
        }
        response = await client.get(f"/v1/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "timed_out"


async def test_release_job(client: httpx.AsyncClient) -> None:
    """Requeue a claimed or running job through release."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    job_id = created["id"]

    worker_id = await register_worker(client)
    response = await client.post(
        f"/v1/jobs/{job_id}/claim", json={"worker_id": worker_id}
    )
    assert response.status_code == 200
    response = await client.post(f"/v1/jobs/{job_id}/release")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt"] == 2
    assert body["worker_id"] is None

    await start_job(client, job_id)
    response = await client.post(f"/v1/jobs/{job_id}/release")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt"] == 3

    response = await client.post(f"/v1/jobs/{job_id}/release")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {job_id} cannot transition from 'pending' to 'pending'"
    }


async def test_release_job_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown job id."""
    missing_id = uuid.uuid4()
    response = await client.post(f"/v1/jobs/{missing_id}/release")
    assert response.status_code == 404


async def test_retry_job(client: httpx.AsyncClient) -> None:
    """Requeue a failed standalone job through retry."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    job_id = created["id"]
    await start_job(client, job_id)
    await link_result_session(client, job_id)
    response = await client.patch(
        f"/v1/jobs/{job_id}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200

    response = await client.post(f"/v1/jobs/{job_id}/retry")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt"] == 2
    assert body["error"] is None
    assert body["result_session_id"] is None
    assert body["started_at"] is None
    assert body["ended_at"] is None


async def test_retry_job_conflicts(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 retrying a pending or run job."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    response = await client.post(f"/v1/jobs/{created['id']}/retry")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {created['id']} cannot transition from 'pending' to 'pending'"
    }

    run_job_id = await create_run_job(client)
    response = await client.post(f"/v1/jobs/{run_job_id}/retry")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {run_job_id} belongs to an experiment run"
    }


async def test_delete_job(client: httpx.AsyncClient) -> None:
    """Delete a standalone job."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)

    response = await client.delete(f"/v1/jobs/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/jobs/{created['id']}")
    assert response.status_code == 404


async def test_delete_job_conflicts(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 deleting a running or run job."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)
    await start_job(client, created["id"])
    response = await client.delete(f"/v1/jobs/{created['id']}")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Job {created['id']} is claimed or running"}

    run_job_id = await create_run_job(client)
    response = await client.delete(f"/v1/jobs/{run_job_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {run_job_id} belongs to an experiment run"
    }


async def test_delete_job_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown job id."""
    missing_id = uuid.uuid4()
    response = await client.delete(f"/v1/jobs/{missing_id}")
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
        f"/v1/jobs/{created['id']}/tool-lookup",
        json={"tool_name": "get_weather", "inputs": inputs, "cache_key": cache_key},
    )
    assert response.status_code == 200
    assert response.json() == {"found": True, "result": {"temp": 21}}

    paris = {"city": "Paris"}
    response = await client.post(
        f"/v1/jobs/{created['id']}/tool-lookup",
        json={
            "tool_name": "get_weather",
            "inputs": paris,
            "cache_key": tool_call_cache_key("get_weather", paris),
        },
    )
    assert response.status_code == 200
    assert response.json() == {"found": False, "result": None}

    response = await client.post(
        f"/v1/jobs/{created['id']}/tool-lookup",
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
    response = await client.get(f"/v1/jobs/{created['id']}/diff")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Job {created['id']} has no result session"}


async def test_session_link_conflicts(client: httpx.AsyncClient) -> None:
    """Observe link errors for inactive, linked, and unknown jobs."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(client, session_id)

    body = {"agent_id": agent_id, "origin": "recorded", "job_id": created["id"]}
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {created['id']} is not claimed or running"
    }

    await start_job(client, created["id"])
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job {created['id']} already has a result session"
    }

    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "recorded", "job_id": str(missing_id)},
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Job {missing_id} was not found"}


REGISTRY_SCORING_POLICY = {
    "scorers": [{"type": "scorer", "name": "relevance"}],
    "pass_threshold": 0.5,
}


async def register_scorer(client: httpx.AsyncClient, name: str = "relevance") -> str:
    """Register a scorer with one code version through the API.

    Args:
        client: HTTP client for the app.
        name: Scorer name.

    Returns:
        Id of the scorer.
    """
    response = await client.post(
        "/v1/blobs", files={"file": (f"{name}.py", b"def score(session): ...")}
    )
    assert response.status_code in (200, 201)
    blob_id = response.json()["id"]
    response = await client.post("/v1/scorers", json={"name": name})
    assert response.status_code == 201
    scorer_id = response.json()["id"]
    response = await client.post(
        f"/v1/scorers/{scorer_id}/versions",
        json={"blob_id": blob_id, "entrypoint": "score"},
    )
    assert response.status_code == 201
    return scorer_id


async def test_create_replay_rejects_unregistered_scorer(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 for a replay naming an unregistered scorer."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/replays",
        json={
            "input_session_id": session_id,
            "scoring_policy": REGISTRY_SCORING_POLICY,
        },
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": "Plugin 'relevance' of kind 'scorer' was not found"
    }


async def test_score_job_spec_and_lifecycle(client: httpx.AsyncClient) -> None:
    """Walk a registry score job from claim through its spec to completed."""
    await register_scorer(client)
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    session_id = await create_completed_session(client, agent_id)
    created = await create_replay(
        client, session_id, scoring_policy=REGISTRY_SCORING_POLICY
    )
    job_id = created["id"]
    await start_job(client, job_id)
    result_session_id = await link_result_session(client, job_id)
    response = await client.patch(
        f"/v1/sessions/{result_session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "scoring"})
    assert response.status_code == 200

    worker_id = await register_worker(client)
    response = await client.post(
        "/v1/jobs/claim",
        json={"worker_id": worker_id, "max_jobs": 5, "parent_job_id": job_id},
    )
    assert response.status_code == 200
    children = [claimed["job"] for claimed in response.json()["jobs"]]
    assert len(children) == 2
    child = next(
        entry for entry in children if entry["input_session_id"] == result_session_id
    )
    assert child["kind"] == "score"
    assert child["agent_version_id"] is None
    assert child["scorer"] == {
        "type": "scorer",
        "name": "relevance",
        "version": 1,
        "params": {},
        "weight": 1.0,
        "fail_below": None,
    }

    response = await client.get(f"/v1/jobs/{child['id']}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec["kind"] == "score"
    assert spec["run"] is None
    assert spec["input_session_id"] == result_session_id
    assert spec["scorer"]["input_session_id"] == result_session_id
    assert spec["scorer"]["plugin"]["format"] == "inline"
    assert spec["scorer"]["plugin"]["entrypoint"] == "score"
    assert len(spec["scorer"]["plugin"]["sha256"]) == 64

    for entry in children:
        response = await client.patch(
            f"/v1/jobs/{entry['id']}", json={"status": "running"}
        )
        assert response.status_code == 200
        response = await client.patch(f"/v1/jobs/{entry['id']}", json={"score": 0.9})
        assert response.status_code == 200
        assert response.json()["score"] == 0.9
        response = await client.patch(
            f"/v1/jobs/{entry['id']}", json={"status": "completed"}
        )
        assert response.status_code == 200

    response = await client.get(f"/v1/jobs/{job_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["passed"] is True
    assert body["score"] == 0.9
    assert body["scores"] == {"relevance": 0.9}

    response = await client.get(f"/v1/sessions/{session_id}")
    assert response.json()["scores"] == {"relevance": 0.9}
    response = await client.get(f"/v1/sessions/{result_session_id}")
    assert response.json()["scores"] == {"relevance": 0.9}
