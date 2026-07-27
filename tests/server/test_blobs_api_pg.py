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
"""End-to-end blob tests against PostgreSQL."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

CONTENT = b"def score(session):\n    return 1.0\n"


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all uploaded blobs.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_blobs_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/blobs/{created['id']}/content")
    assert response.status_code == 200
    assert response.content == CONTENT


async def test_upload_deduplicates_across_requests(client: httpx.AsyncClient) -> None:
    """Return the stored blob for content uploaded in an earlier request."""
    created = (
        await client.post(
            "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
        )
    ).json()
    response = await client.post(
        "/v1/blobs", files={"file": ("scorer.py", CONTENT, "text/x-python")}
    )
    assert response.status_code == 200
    assert response.json() == created


async def test_download_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown blob id."""
    response = await client.get(f"/v1/blobs/{uuid.uuid4()}/content")
    assert response.status_code == 404
