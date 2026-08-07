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
"""End-to-end agent version tests against PostgreSQL."""

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
    """Provide the id of an agent to version."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist an update across requests."""
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions", json={"display_version": "v1"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}", json={"display_version": "v1.1"}
    )
    assert response.status_code == 200

    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["display_version"] == "v1.1"
    assert body["updated"] > created["updated"]


async def test_update_replaces_run_spec_secrets_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a replaced run spec's secret ids across requests."""
    old_secret = (
        await client.post(
            "/api/v1/secrets", json={"name": "old-secret", "values": {"k": "v"}}
        )
    ).json()
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions",
            json={
                "run_spec": {
                    "type": "command",
                    "command": "old.sh",
                    "secret_ids": [old_secret["id"]],
                }
            },
        )
    ).json()

    new_secret = (
        await client.post(
            "/api/v1/secrets", json={"name": "new-secret", "values": {"k": "v"}}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}",
        json={
            "run_spec": {
                "type": "command",
                "command": "new.sh",
                "secret_ids": [new_secret["id"]],
            }
        },
    )
    assert response.status_code == 200

    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["run_spec"]["command"] == "new.sh"
    assert body["run_spec"]["secret_ids"] == [new_secret["id"]]


async def test_delete_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a deletion across requests."""
    created = (await client.post(f"/api/v1/agents/{agent_id}/versions", json={})).json()
    response = await client.delete(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 404


async def test_delete_version_allows_agent_delete(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Allow deleting the agent once its only version is gone."""
    created = (await client.post(f"/api/v1/agents/{agent_id}/versions", json={})).json()
    await client.delete(f"/api/v1/agent-versions/{created['id']}")

    response = await client.delete(f"/api/v1/agents/{agent_id}")
    assert response.status_code == 204
