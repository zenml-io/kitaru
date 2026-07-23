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
"""End-to-end tag tests against PostgreSQL."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created tags.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_tags_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    created = response.json()

    response = await client.get("/v1/tags")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Tag name 'prod' is already registered"}


async def test_links_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a tag link across requests."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    body = {"resource_type": "session", "resource_id": str(resource_id)}
    response = await client.post(f"/v1/tags/{created['id']}/links", json=body)
    assert response.status_code == 201

    response = await client.post(f"/v1/tags/{created['id']}/links", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": (f"Tag {created['id']} is already attached to session {resource_id}")
    }

    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 204

    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 404


async def test_delete_cascades_links(client: httpx.AsyncClient) -> None:
    """Remove the links of a tag through the database cascade."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    response = await client.post(
        f"/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": str(resource_id)},
    )
    assert response.status_code == 201

    response = await client.delete(f"/v1/tags/{created['id']}")
    assert response.status_code == 204

    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 404


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    response = await client.delete(f"/v1/tags/{created['id']}")
    assert response.status_code == 204

    response = await client.delete(f"/v1/tags/{created['id']}")
    assert response.status_code == 404
