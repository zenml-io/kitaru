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
"""End-to-end importer tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_importers_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/importers", json={"name": "langfuse-import", "provider": "langfuse"}
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/importers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_local_server_provides_default_importers(
    client: httpx.AsyncClient,
) -> None:
    """Provide package-backed importers after local startup."""
    response = await client.get("/v1/importers")

    assert response.status_code == 200
    importers = {item["name"]: item for item in response.json()["items"]}
    defaults = {"braintrust", "langfuse", "otlp"}
    assert defaults <= importers.keys()
    assert all(importers[name]["latest_version"] == 1 for name in defaults)


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/importers", json={"name": "langfuse-import"})
    assert response.status_code == 201
    response = await client.post("/v1/importers", json={"name": "langfuse-import"})
    assert response.status_code == 409


async def test_evaluator_and_importer_share_a_name(client: httpx.AsyncClient) -> None:
    """Let an evaluator and an importer register the same name."""
    response = await client.post("/v1/importers", json={"name": "shared"})
    assert response.status_code == 201
    response = await client.post("/v1/evaluators", json={"name": "shared"})
    assert response.status_code == 201


async def test_version_numbering_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Bump the version number in a real UPDATE ... RETURNING transaction."""
    created = (
        await client.post("/v1/importers", json={"name": "langfuse-import"})
    ).json()
    body = {
        "source": {
            "type": "package",
            "requirement": "kitaru-importer==1.0.0",
            "entrypoint": "pkg:run",
        }
    }
    first = await client.post(f"/v1/importers/{created['id']}/versions", json=body)
    second = await client.post(f"/v1/importers/{created['id']}/versions", json=body)
    assert first.json()["version"] == 1
    assert second.json()["version"] == 2
