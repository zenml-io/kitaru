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
"""End-to-end experiment run tests against PostgreSQL."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api_pg import SCORING_POLICY, seed_cohort

from conftest import db_settings, lifespan_client
from kitaru.hashing import tool_call_cache_key


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created runs.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_run_flow_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Start runs, count the numbers, and read progress and jobs."""
    cohort_id = await seed_cohort(client)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    experiment_id = response.json()["id"]

    response = await client.post(f"/v1/experiments/{experiment_id}/runs", json={})
    assert response.status_code == 201
    first = response.json()
    response = await client.post(
        f"/v1/experiments/{experiment_id}/runs", json={"score_baselines": True}
    )
    assert response.status_code == 201
    second = response.json()
    assert [first["number"], second["number"]] == [1, 2]
    assert second["score_baselines"] is True

    response = await client.get("/v1/experiment-runs")
    assert response.status_code == 200
    assert response.json()["total"] == 2

    response = await client.get(f"/v1/experiment-runs/{first['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["progress"]["pending"] == 2
    assert body["progress"]["total"] == 2

    response = await client.get(f"/v1/experiment-runs/{first['id']}/jobs")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert all(item["status"] == "pending" for item in body["items"])

    response = await client.get(f"/v1/experiments/{experiment_id}/runs")
    assert response.status_code == 200
    assert response.json()["total"] == 2


async def test_runner_loop_end_to_end(client: httpx.AsyncClient) -> None:
    """Drive a run through claim, spec, linking, lookup, and completion."""
    response = await client.post(
        "/v1/secrets",
        json={"name": "openai", "values": {"OPENAI_API_KEY": "sk-1"}},
    )
    assert response.status_code == 201
    secret_id = response.json()["id"]
    response = await client.post("/v1/agents", json={"name": "runner-bot"})
    assert response.status_code == 201
    agent_id = response.json()["id"]
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": {
                "command": "python agent.py",
                "env": {"MODE": "replay"},
                "secret_ids": [secret_id],
                "timeout_seconds": 600,
            },
        },
    )
    assert response.status_code == 201

    inputs = {"city": "Berlin"}
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "origin": "recorded",
            "inputs": {"prompt": "weather?"},
        },
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
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

    response = await client.post(
        "/v1/cohorts",
        json={"name": "runner", "agent_id": agent_id, "session_ids": [session_id]},
    )
    assert response.status_code == 201
    cohort_id = response.json()["id"]
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "runner-loop",
            "cohort_id": cohort_id,
            "tool_policy": {"default": {"type": "history"}, "tools": {}},
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    experiment_id = response.json()["id"]
    response = await client.post(
        f"/v1/experiments/{experiment_id}/runs", json={"score_baselines": True}
    )
    assert response.status_code == 201
    run_id = response.json()["id"]

    response = await client.post("/v1/workers", json={"name": "worker-1"})
    assert response.status_code == 200
    worker_id = response.json()["id"]
    response = await client.post(
        "/v1/jobs/claim",
        json={"worker_id": worker_id, "max_jobs": 5, "experiment_run_id": run_id},
    )
    assert response.status_code == 200
    jobs = response.json()["jobs"]
    assert len(jobs) == 1
    job_id = jobs[0]["job"]["id"]
    assert jobs[0]["spec"]["job_id"] == job_id
    response = await client.get(f"/v1/experiment-runs/{run_id}")
    assert response.json()["status"] == "running"

    response = await client.get(f"/v1/jobs/{job_id}/spec")
    assert response.status_code == 200
    spec = response.json()
    assert spec["inputs"] == {"prompt": "weather?"}
    assert spec["scorer"] is None
    assert spec["run"]["env"] == {"MODE": "replay"}
    assert spec["secret_env"] == {"OPENAI_API_KEY": "sk-1"}
    assert spec["tool_policy"]["default"]["type"] == "history"

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "running"})
    assert response.status_code == 200
    response = await client.post(
        f"/v1/workers/{worker_id}/heartbeat", json={"job_ids": [job_id]}
    )
    assert response.status_code == 200
    assert response.json() == {"abandon": []}

    response = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "origin": "recorded", "job_id": job_id},
    )
    assert response.status_code == 201
    result_session = response.json()
    assert result_session["origin"] == "replay"
    result_session_id = result_session["id"]

    cache_key = tool_call_cache_key("get_weather", inputs)
    response = await client.post(
        f"/v1/jobs/{job_id}/tool-lookup",
        json={"tool_name": "get_weather", "inputs": inputs, "cache_key": cache_key},
    )
    assert response.status_code == 200
    assert response.json() == {"found": True, "result": {"temp": 21}}

    response = await client.post(
        f"/v1/sessions/{result_session_id}/nodes",
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
                    "attributes": {"mocked": True, "policy": "history"},
                }
            ]
        },
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/sessions/{result_session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200

    response = await client.patch(f"/v1/jobs/{job_id}", json={"status": "scoring"})
    assert response.status_code == 200
    assert response.json()["status"] == "scoring"

    response = await client.post(
        "/v1/jobs/claim",
        json={"worker_id": worker_id, "max_jobs": 5, "experiment_run_id": run_id},
    )
    assert response.status_code == 200
    children = [claimed["job"] for claimed in response.json()["jobs"]]
    assert len(children) == 2
    assert all(child["kind"] == "score" for child in children)
    assert all(child["parent_job_id"] == job_id for child in children)
    for child in children:
        response = await client.patch(
            f"/v1/jobs/{child['id']}", json={"status": "running"}
        )
        assert response.status_code == 200
        response = await client.patch(
            f"/v1/jobs/{child['id']}", json={"status": "completed", "score": 0.8}
        )
        assert response.status_code == 200

    response = await client.get(f"/v1/jobs/{job_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["diff"]["tool_calls"] == {
        "matched": 1,
        "mocked": 1,
        "added": 0,
        "removed": 0,
    }

    response = await client.get(f"/v1/experiment-runs/{run_id}")
    assert response.status_code == 200
    run = response.json()
    assert run["status"] == "completed"
    assert run["ended_at"] is not None
    assert run["summary"]["replay_counts_by_status"] == {"completed": 1}
    assert run["summary"]["pass_rate"] == 1.0
    assert run["progress"]["completed"] == 1

    response = await client.get(f"/v1/jobs/{job_id}/diff")
    assert response.status_code == 200
    diff = response.json()
    assert diff["original_session_id"] == session_id
    assert diff["result_session_id"] == result_session_id
    assert len(diff["node_pairs"]) == 1
    assert diff["node_pairs"][0]["mocked"] is True
    assert diff["node_pairs"][0]["cache_key_changed"] is False


async def test_cancel_run_end_to_end(client: httpx.AsyncClient) -> None:
    """Cancel a run and observe the immediate canceled state."""
    cohort_id = await seed_cohort(client)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "cancel-me",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    response = await client.post(
        f"/v1/experiments/{response.json()['id']}/runs", json={}
    )
    assert response.status_code == 201
    run_id = response.json()["id"]

    response = await client.post(f"/v1/experiment-runs/{run_id}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "canceled"
    assert body["summary"]["replay_counts_by_status"] == {"canceled": 2}

    response = await client.post("/v1/workers", json={"name": "worker-1"})
    assert response.status_code == 200
    worker_id = response.json()["id"]
    response = await client.post(
        "/v1/jobs/claim",
        json={"worker_id": worker_id, "max_jobs": 5, "experiment_run_id": run_id},
    )
    assert response.status_code == 200
    assert response.json()["jobs"] == []

    response = await client.delete(f"/v1/experiment-runs/{run_id}")
    assert response.status_code == 204
    response = await client.get(f"/v1/experiment-runs/{run_id}")
    assert response.status_code == 404
    response = await client.get("/v1/jobs", params={"experiment_run_id": run_id})
    assert response.status_code == 200
    assert response.json()["total"] == 0


async def test_delete_run_end_to_end(client: httpx.AsyncClient) -> None:
    """Reject deleting a run until it is terminal."""
    cohort_id = await seed_cohort(client)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "delete-me",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    response = await client.post(
        f"/v1/experiments/{response.json()['id']}/runs", json={}
    )
    assert response.status_code == 201
    run_id = response.json()["id"]

    response = await client.delete(f"/v1/experiment-runs/{run_id}")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Experiment run {run_id} is not terminal"}

    response = await client.post(f"/v1/experiment-runs/{run_id}/cancel")
    assert response.status_code == 200
    response = await client.delete(f"/v1/experiment-runs/{run_id}")
    assert response.status_code == 204
