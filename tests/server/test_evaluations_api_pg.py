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
"""End-to-end evaluation tests against PostgreSQL."""

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
async def session_id(client: httpx.AsyncClient) -> str:
    """Provide the id of a session to merge evaluations into."""
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    session = (
        await client.post(
            "/v1/sessions",
            json={
                "agent_id": agent["id"],
                "origin": "recorded",
                "inputs": {"prompt": "hi"},
                "outputs": None,
                "expected": None,
                "metadata": {},
            },
        )
    ).json()
    return session["id"]


async def test_merge_and_list_persist_across_requests(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        f"/v1/sessions/{session_id}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )
    assert response.status_code == 200
    created = response.json()[0]

    response = await client.get(f"/v1/evaluations/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/evaluations", params={"session_id": session_id})
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_merge_overwrite_persists_across_requests(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Persist an overwrite of a resent evaluation name across requests."""
    first = (
        await client.post(
            f"/v1/sessions/{session_id}/evaluations",
            json={"evaluations": [{"name": "accuracy", "score": 0.5}]},
        )
    ).json()[0]
    second = (
        await client.post(
            f"/v1/sessions/{session_id}/evaluations",
            json={"evaluations": [{"name": "accuracy", "value": "high"}]},
        )
    ).json()[0]
    assert second["id"] == first["id"]

    response = await client.get(f"/v1/evaluations/{first['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["value"] == "high"
    assert body["score"] is None
    assert body["data_type"] == "str"


async def test_merge_passed_persists_across_requests(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Persist the pass flag and clear it when a resent name omits it."""
    created = (
        await client.post(
            f"/v1/sessions/{session_id}/evaluations",
            json={"evaluations": [{"name": "accuracy", "score": 0.9, "passed": True}]},
        )
    ).json()[0]

    response = await client.get(f"/v1/evaluations/{created['id']}")
    assert response.status_code == 200
    assert response.json()["passed"] is True

    await client.post(
        f"/v1/sessions/{session_id}/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )
    response = await client.get(f"/v1/evaluations/{created['id']}")
    assert response.status_code == 200
    assert response.json()["passed"] is None


async def test_merge_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing session."""
    response = await client.post(
        "/v1/sessions/019632fa-0000-7000-8000-000000000000/evaluations",
        json={"evaluations": [{"name": "accuracy", "score": 0.9}]},
    )
    assert response.status_code == 404


async def test_merge_rejects_duplicate_name_in_batch(
    client: httpx.AsyncClient, session_id: str
) -> None:
    """Observe HTTP 422 when the request names the same evaluation twice."""
    response = await client.post(
        f"/v1/sessions/{session_id}/evaluations",
        json={
            "evaluations": [
                {"name": "accuracy", "score": 0.9},
                {"name": "accuracy", "score": 0.1},
            ]
        },
    )
    assert response.status_code == 422
