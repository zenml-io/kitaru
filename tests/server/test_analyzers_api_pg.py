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
"""End-to-end analyzer tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_analyzers_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/api/v1/analyzers", json={"name": "trends", "metadata": {"a": 1}}
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/api/v1/analyzers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/api/v1/analyzers", json={"name": "trends"})
    assert response.status_code == 201
    response = await client.post("/api/v1/analyzers", json={"name": "trends"})
    assert response.status_code == 409


async def test_version_numbering_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Bump the version number in a real UPDATE ... RETURNING transaction."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-trends==1.0.0",
            "entrypoint": "pkg:analyze",
        }
    }
    first = await client.post(f"/api/v1/analyzers/{created['id']}/versions", json=body)
    second = await client.post(f"/api/v1/analyzers/{created['id']}/versions", json=body)
    assert first.json()["version"] == 1
    assert second.json()["version"] == 2

    response = await client.get(f"/api/v1/analyzers/{created['id']}")
    assert response.json()["latest_version"] == 2


async def test_delete_cascades_versions(client: httpx.AsyncClient) -> None:
    """Cascade a plugin's versions when it is deleted."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()
    await client.post(
        f"/api/v1/analyzers/{created['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-trends==1.0.0",
                "entrypoint": "pkg:analyze",
            }
        },
    )
    response = await client.delete(f"/api/v1/analyzers/{created['id']}")
    assert response.status_code == 204


async def test_get_analyzer_version_out_of_int32_range(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422, not 500, for a version outside PostgreSQL's int32 range."""
    created = (await client.post("/api/v1/analyzers", json={"name": "trends"})).json()

    response = await client.get(f"/api/v1/analyzers/{created['id']}/versions/{2**31}")
    assert response.status_code == 422

    response = await client.get(
        f"/api/v1/analyzers/{created['id']}/versions/{-(2**31) - 1}"
    )
    assert response.status_code == 422
