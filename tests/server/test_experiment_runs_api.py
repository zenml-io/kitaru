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
"""Tests for the experiment run routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api import (
    SCORING_POLICY_RESPONSE,
    create_agent,
    create_cohort,
    create_experiment,
    create_runnable_version,
)

from conftest import experiment_app


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_run(client: httpx.AsyncClient, experiment_id: str) -> dict:
    """Store an experiment run through the API.

    Args:
        client: HTTP client for the app.
        experiment_id: Id of the experiment.

    Returns:
        Created experiment run body.
    """
    response = await client.post(f"/v1/experiments/{experiment_id}/runs", json={})
    assert response.status_code == 201
    return response.json()


async def seed_run(client: httpx.AsyncClient, session_count: int = 2) -> dict:
    """Store an experiment with one run through the API.

    Args:
        client: HTTP client for the app.
        session_count: Number of cohort sessions.

    Returns:
        Created experiment run body.
    """
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id, session_count=session_count)
    experiment = await create_experiment(client, cohort_id)
    return await create_run(client, experiment["id"])


async def test_get_experiment_run(client: httpx.AsyncClient) -> None:
    """Get an experiment run with its computed progress."""
    created = await seed_run(client)
    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body == created
    assert body["progress"]["pending"] == 2
    assert body["progress"]["total"] == 2


async def test_get_experiment_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/experiment-runs/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Experiment run {missing_id} was not found"}


async def test_list_experiment_runs(client: httpx.AsyncClient) -> None:
    """List experiment runs with pagination."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    experiment = await create_experiment(client, cohort_id)
    first = await create_run(client, experiment["id"])
    second = await create_run(client, experiment["id"])

    response = await client.get("/v1/experiment-runs")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [item["id"] for item in body["items"]] == [first["id"], second["id"]]

    response = await client.get(
        "/v1/experiment-runs", params={"page": 2, "page_size": 1}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [item["id"] for item in body["items"]] == [second["id"]]


async def test_list_experiment_runs_filters(client: httpx.AsyncClient) -> None:
    """List experiment runs filtered by experiment and status."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    experiment = await create_experiment(client, cohort_id)
    other_cohort_id = await create_cohort(client, agent_id, name="other-cohort")
    other_experiment = await create_experiment(client, other_cohort_id, name="other")
    first = await create_run(client, experiment["id"])
    other = await create_run(client, other_experiment["id"])
    response = await client.post(
        f"/v1/experiment-runs/{first['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 1},
    )
    assert response.status_code == 200

    response = await client.get(
        "/v1/experiment-runs", params={"experiment_id": experiment["id"]}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == first["id"]

    response = await client.get("/v1/experiment-runs", params={"status": "running"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == first["id"]

    response = await client.get(
        "/v1/experiment-runs",
        params={"experiment_id": other_experiment["id"], "status": "pending"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == other["id"]

    response = await client.get(
        "/v1/experiment-runs",
        params={"experiment_id": other_experiment["id"], "status": "running"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


async def test_list_experiment_runs_unknown_experiment(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 for an unknown experiment id filter."""
    missing_id = uuid.uuid4()
    response = await client.get(
        "/v1/experiment-runs", params={"experiment_id": str(missing_id)}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Experiment {missing_id} was not found"}


async def test_list_experiment_runs_invalid_status(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an unknown status filter value."""
    response = await client.get("/v1/experiment-runs", params={"status": "bogus"})
    assert response.status_code == 422


async def test_list_experiment_runs_by_tag(client: httpx.AsyncClient) -> None:
    """List experiment runs attached to a tag name."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    experiment = await create_experiment(client, cohort_id)
    tagged = await create_run(client, experiment["id"])
    await create_run(client, experiment["id"])
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "experiment_run", "resource_id": tagged["id"]},
    )
    assert response.status_code == 201

    response = await client.get("/v1/experiment-runs", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged["id"]


async def test_list_experiment_run_jobs(client: httpx.AsyncClient) -> None:
    """List the jobs of a run with their inlined config."""
    created = await seed_run(client)
    response = await client.get(f"/v1/experiment-runs/{created['id']}/jobs")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    for item in body["items"]:
        assert item["experiment_run_id"] == created["id"]
        assert item["status"] == "pending"
        assert item["attempt"] == 1
        assert item["result_session_id"] is None
        assert item["diff"] is None
        assert item["tool_policy"] == {
            "default": {"type": "passthrough"},
            "tools": {},
        }
        assert item["scoring_policy"] == SCORING_POLICY_RESPONSE

    response = await client.get(
        f"/v1/experiment-runs/{created['id']}/jobs",
        params={"page": 2, "page_size": 1},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert len(body["items"]) == 1


async def test_list_experiment_run_jobs_by_status(
    client: httpx.AsyncClient,
) -> None:
    """List the jobs of a run filtered by status."""
    created = await seed_run(client)
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 1},
    )
    assert response.status_code == 200
    claimed_id = response.json()["jobs"][0]["id"]

    response = await client.get(
        f"/v1/experiment-runs/{created['id']}/jobs", params={"status": "claimed"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == claimed_id

    response = await client.get(
        f"/v1/experiment-runs/{created['id']}/jobs", params={"status": "pending"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] != claimed_id

    response = await client.get(
        f"/v1/experiment-runs/{created['id']}/jobs",
        params={"status": "completed"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0

    response = await client.get(
        f"/v1/experiment-runs/{created['id']}/jobs", params={"status": "bogus"}
    )
    assert response.status_code == 422


async def test_list_experiment_run_jobs_stale_claim_matches_pending() -> None:
    """Match a stale claim as pending in the run jobs status filter."""
    transport = httpx.ASGITransport(app=experiment_app(heartbeat_timeout_seconds=-60))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        created = await seed_run(client, session_count=1)
        response = await client.post(
            f"/v1/experiment-runs/{created['id']}/claim",
            json={"worker_id": "worker-1", "max_jobs": 1},
        )
        assert response.status_code == 200
        job_id = response.json()["jobs"][0]["id"]

        response = await client.get(
            f"/v1/experiment-runs/{created['id']}/jobs",
            params={"status": "pending"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 1
        assert body["items"][0]["id"] == job_id
        assert body["items"][0]["status"] == "pending"

        response = await client.get(
            f"/v1/experiment-runs/{created['id']}/jobs",
            params={"status": "claimed"},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 0


async def test_list_experiment_run_jobs_not_found(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/experiment-runs/{missing_id}/jobs")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Experiment run {missing_id} was not found"}


async def test_claim_jobs(client: httpx.AsyncClient) -> None:
    """Claim pending jobs and move the run to running."""
    created = await seed_run(client)
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 1},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["jobs"]) == 1
    claimed = body["jobs"][0]
    assert claimed["status"] == "claimed"
    assert claimed["worker_id"] == "worker-1"
    assert claimed["claimed_at"] is not None
    assert claimed["heartbeat_at"] is not None
    assert claimed["scoring_policy"] == SCORING_POLICY_RESPONSE

    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    body = response.json()
    assert body["status"] == "running"
    assert body["started_at"] is not None
    assert body["progress"]["pending"] == 1
    assert body["progress"]["claimed"] == 1

    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-2", "max_jobs": 5},
    )
    assert response.status_code == 200
    assert len(response.json()["jobs"]) == 1
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-2", "max_jobs": 5},
    )
    assert response.status_code == 200
    assert response.json()["jobs"] == []


async def test_claim_unknown_run(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/experiment-runs/{missing_id}/claim",
        json={"worker_id": "worker-1", "max_jobs": 1},
    )
    assert response.status_code == 404


async def test_claim_invalid_body(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid claim request."""
    created = await seed_run(client)
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 0},
    )
    assert response.status_code == 422


async def test_cancel_run(client: httpx.AsyncClient) -> None:
    """Cancel a run and observe the immediate canceled state."""
    created = await seed_run(client)
    response = await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "canceled"
    assert body["ended_at"] is not None
    assert body["summary"]["replay_counts_by_status"] == {"canceled": 2}
    assert body["progress"]["canceled"] == 2

    response = await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Experiment run {created['id']} cannot transition from "
        f"'canceled' to 'canceling'"
    }

    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 1},
    )
    assert response.status_code == 200
    assert response.json()["jobs"] == []


async def test_cancel_run_drains_through_job_patch(
    client: httpx.AsyncClient,
) -> None:
    """Leave running jobs to the heartbeat path and drain on patch."""
    created = await seed_run(client, session_count=1)
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 1},
    )
    job_id = response.json()["jobs"][0]["id"]
    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
    assert response.status_code == 200

    response = await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "canceling"

    response = await client.post(f"/v1/jobs/{job_id}/heartbeat")
    assert response.status_code == 200
    assert response.json() == {"status": "running", "canceled": True}

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "canceled"})
    assert response.status_code == 200
    assert response.json()["status"] == "canceled"

    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    body = response.json()
    assert body["status"] == "canceled"
    assert body["ended_at"] is not None
    assert body["summary"]["replay_counts_by_status"] == {"canceled": 1}


async def test_run_finalizes_with_summary(client: httpx.AsyncClient) -> None:
    """Complete every job of a run and observe the stored summary."""
    created = await seed_run(client, session_count=2)
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 5},
    )
    jobs = response.json()["jobs"]
    assert len(jobs) == 2
    for index, job in enumerate(jobs):
        job_id = job["id"]
        response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
        assert response.status_code == 200
        original = await client.get(f"/v1/sessions/{job['original_session_id']}")
        response = await client.post(
            "/v1/sessions",
            json={
                "agent_id": original.json()["agent_id"],
                "origin": "recorded",
                "job_id": job_id,
            },
        )
        assert response.status_code == 201
        result_session_id = response.json()["id"]
        assert response.json()["origin"] == "replay"
        response = await client.patch(
            f"/v1/sessions/{result_session_id}", json={"status": "completed"}
        )
        assert response.status_code == 200
        response = await client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "completed",
                "passed": index == 0,
                "score": 0.8 - index * 0.6,
                "scores": {"conciseness": 0.8 - index * 0.6},
            },
        )
        assert response.status_code == 200

    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    body = response.json()
    assert body["status"] == "completed"
    assert body["ended_at"] is not None
    summary = body["summary"]
    assert summary["replay_counts_by_status"] == {"completed": 2}
    assert summary["pass_rate"] == 0.5
    assert summary["scores"]["conciseness"]["replay"]["mean"] == pytest.approx(0.5)
    assert body["progress"]["completed"] == 2


async def test_run_finalizes_failed_with_error(client: httpx.AsyncClient) -> None:
    """Land a run with failed and timed out jobs on failed."""
    created = await seed_run(client, session_count=2)
    response = await client.post(
        f"/v1/experiment-runs/{created['id']}/claim",
        json={"worker_id": "worker-1", "max_jobs": 5},
    )
    jobs = response.json()["jobs"]
    assert len(jobs) == 2
    response = await client.patch(
        f"/v1/jobs/{jobs[0]['id']}",
        json={"status": "failed", "error": "agent exited with code 1"},
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/jobs/{jobs[1]['id']}",
        json={"status": "timed_out", "error": "wall clock limit exceeded"},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"] == "1 of 2 jobs failed, 1 timed out"
    assert body["ended_at"] is not None
    assert body["summary"]["replay_counts_by_status"] == {
        "failed": 1,
        "timed_out": 1,
    }
    assert body["summary"]["pass_rate"] == 0.0


async def test_delete_run(client: httpx.AsyncClient) -> None:
    """Delete a terminal run with its jobs."""
    created = await seed_run(client, session_count=1)
    response = await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    assert response.status_code == 200

    response = await client.delete(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 404
    response = await client.get("/v1/jobs", params={"experiment_run_id": created["id"]})
    assert response.status_code == 200
    assert response.json()["total"] == 0


async def test_delete_run_rejects_non_terminal(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 deleting a run that is not terminal."""
    created = await seed_run(client, session_count=1)
    response = await client.delete(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Experiment run {created['id']} is not terminal"
    }


async def test_delete_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    response = await client.delete(f"/v1/experiment-runs/{missing_id}")
    assert response.status_code == 404
