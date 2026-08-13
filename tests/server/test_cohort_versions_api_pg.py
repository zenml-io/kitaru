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
"""End-to-end cohort version tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


@pytest.fixture
async def agent_id(client: httpx.AsyncClient) -> str:
    """Provide the id of an agent to attach cohorts and sessions to."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


@pytest.fixture
async def cohort_id(client: httpx.AsyncClient, agent_id: str) -> str:
    """Provide the id of a cohort to version."""
    created = (
        await client.post("/v1/cohorts", json={"name": "cohort", "agent_id": agent_id})
    ).json()
    return created["id"]


async def _make_session_id(client: httpx.AsyncClient, agent_id: str) -> str:
    """Store a session on the given agent and return its id."""
    response = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "origin": "recorded",
            "inputs": None,
            "outputs": None,
        },
    )
    return response.json()["id"]


async def test_create_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str, cohort_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    session_id = await _make_session_id(client, agent_id)
    response = await client.post(
        f"/v1/cohorts/{cohort_id}/versions",
        json={"add_session_ids": [session_id], "display_version": "v1"},
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/cohort-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Persist an update across requests."""
    created = (
        await client.post(
            f"/v1/cohorts/{cohort_id}/versions", json={"display_version": "v1"}
        )
    ).json()
    response = await client.patch(
        f"/v1/cohort-versions/{created['id']}", json={"display_version": "v1.1"}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/cohort-versions/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["display_version"] == "v1.1"
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Persist a deletion across requests."""
    created = (await client.post(f"/v1/cohorts/{cohort_id}/versions", json={})).json()
    response = await client.delete(f"/v1/cohort-versions/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/cohort-versions/{created['id']}")
    assert response.status_code == 404


async def test_delete_does_not_lower_latest_version(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Keep the cohort's latest_version high-water mark after a delete."""
    await client.post(f"/v1/cohorts/{cohort_id}/versions", json={})
    second = (await client.post(f"/v1/cohorts/{cohort_id}/versions", json={})).json()
    await client.delete(f"/v1/cohort-versions/{second['id']}")

    response = await client.get(f"/v1/cohorts/{cohort_id}")
    assert response.json()["latest_version"] == 2


async def test_delete_conflict_when_referenced_by_experiment_run(
    client: httpx.AsyncClient, agent_id: str, cohort_id: str
) -> None:
    """Translate the database restriction into HTTP 409."""
    agent_version = (
        await client.post(
            f"/v1/agents/{agent_id}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    blob = (
        await client.post(
            "/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post("/v1/evaluators", json={"name": "accuracy", "metadata": {}})
    ).json()
    await client.post(
        f"/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    session_id = await _make_session_id(client, agent_id)
    cohort_version = (
        await client.post(
            f"/v1/cohorts/{cohort_id}/versions",
            json={"add_session_ids": [session_id]},
        )
    ).json()
    experiment = (
        await client.post(
            "/v1/experiments",
            json={
                "name": "exp1",
                "agent_id": agent_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    await client.post(
        f"/v1/experiments/{experiment['id']}/runs",
        json={
            "cohort_version_id": cohort_version["id"],
            "agent_version_id": agent_version["id"],
        },
    )

    response = await client.delete(f"/v1/cohort-versions/{cohort_version['id']}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Cohort version {cohort_version['id']} is in use by an "
        "experiment run"
    }


async def test_create_cohort_version_missing_cohort(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the cohort does not exist."""
    response = await client.post(
        "/v1/cohorts/00000000-0000-0000-0000-000000000000/versions", json={}
    )
    assert response.status_code == 404
