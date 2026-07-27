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

CONTENT = b"def parse(payload):\n    return []\n"


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created importers.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_importers_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/importers",
        json={
            "name": "langfuse",
            "provider": "langfuse",
            "metadata": {"region": "eu"},
        },
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/importers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/importers", params={"provider": "langfuse"})
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_same_name_across_kinds(client: httpx.AsyncClient) -> None:
    """Accept the same name for a scorer and an importer."""
    response = await client.post("/v1/scorers", json={"name": "shared"})
    assert response.status_code == 201
    response = await client.post("/v1/importers", json={"name": "shared"})
    assert response.status_code == 201


async def test_versions_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Allocate version numbers across separate requests."""
    blob_id = (
        await client.post(
            "/v1/blobs", files={"file": ("importer.py", CONTENT, "text/x-python")}
        )
    ).json()["id"]
    created = (await client.post("/v1/importers", json={"name": "langfuse"})).json()

    response = await client.post(
        f"/v1/importers/{created['id']}/versions",
        json={"blob_id": blob_id, "entrypoint": "parse"},
    )
    assert response.status_code == 201
    assert response.json()["version"] == 1

    response = await client.get(f"/v1/importers/{created['id']}/versions")
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["entrypoint"] == "parse"
