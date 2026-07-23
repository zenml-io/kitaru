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
"""End-to-end experiment tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

SCORING_POLICY = {
    "scorers": [{"name": "conciseness", "source": "my_pkg.scorers:conciseness"}],
    "pass_threshold": 0.5,
}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created experiments.
    async with lifespan_client(db_settings()) as client:
        yield client


async def seed_cohort(client: httpx.AsyncClient, session_count: int = 2) -> str:
    """Store an agent, a runnable version, and a cohort through the API.

    Args:
        client: HTTP client for the app.
        session_count: Number of member sessions.

    Returns:
        Id of the created cohort.
    """
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 201
    agent_id = response.json()["id"]
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": {"command": "python agent.py", "timeout_seconds": 600},
        },
    )
    assert response.status_code == 201
    session_ids = []
    for _ in range(session_count):
        response = await client.post(
            "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
        )
        assert response.status_code == 201
        session_id = response.json()["id"]
        response = await client.patch(
            f"/v1/sessions/{session_id}", json={"status": "completed"}
        )
        assert response.status_code == 200
        session_ids.append(session_id)
    response = await client.post(
        "/v1/cohorts",
        json={"name": "baseline", "agent_id": agent_id, "session_ids": session_ids},
    )
    assert response.status_code == 201
    return response.json()["id"]


async def test_experiment_flow_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Create, reconfigure, run, and freeze an experiment."""
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
    assert response.json()["tool_policy"] == {
        "default": {"type": "passthrough"},
        "tools": {},
    }

    # A pre-run config PATCH repoints the experiment at a new config row.
    response = await client.patch(
        f"/v1/experiments/{experiment_id}",
        json={"override": {"model": "claude-sonnet-5"}},
    )
    assert response.status_code == 200
    assert response.json()["override"]["model"] == "claude-sonnet-5"

    response = await client.post(f"/v1/experiments/{experiment_id}/runs", json={})
    assert response.status_code == 201
    run = response.json()
    assert run["number"] == 1
    assert run["progress"]["pending"] == 2

    # The run freezes the experiment's config.
    response = await client.patch(
        f"/v1/experiments/{experiment_id}",
        json={"override": {"model": "gpt-5"}},
    )
    assert response.status_code == 409

    response = await client.get(f"/v1/experiment-runs/{run['id']}/replays")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert all(item["override"]["model"] == "claude-sonnet-5" for item in body["items"])

    response = await client.delete(f"/v1/experiments/{experiment_id}")
    assert response.status_code == 409

    response = await client.get(f"/v1/experiments/{experiment_id}")
    assert response.status_code == 200


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    cohort_id = await seed_cohort(client)
    body = {
        "name": "swap-model",
        "cohort_id": cohort_id,
        "scoring_policy": SCORING_POLICY,
    }
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Experiment name 'swap-model' is already registered"
    }


async def test_cohort_delete_blocked_and_released(client: httpx.AsyncClient) -> None:
    """Block cohort deletion while an experiment references it."""
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

    response = await client.delete(f"/v1/cohorts/{cohort_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Cohort {cohort_id} is referenced by experiments"
    }

    response = await client.delete(f"/v1/experiments/{experiment_id}")
    assert response.status_code == 204
    response = await client.delete(f"/v1/cohorts/{cohort_id}")
    assert response.status_code == 204


async def test_tag_filter(client: httpx.AsyncClient) -> None:
    """Filter experiments through a tag link."""
    cohort_id = await seed_cohort(client)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "tagged",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    tagged_id = response.json()["id"]
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "other",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "experiment", "resource_id": tagged_id},
    )
    assert response.status_code == 201

    response = await client.get("/v1/experiments", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged_id
