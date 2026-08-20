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
"""End-to-end idempotency key tests against PostgreSQL."""

import asyncio
from collections.abc import AsyncGenerator

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from conftest import db_settings, lifespan_client
from kitaru.server.adapters.db.orm.idempotency_key import IdempotencyKeyORM
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService


@pytest.fixture
async def settings() -> APISettings:
    """Provide settings for a fresh test database."""
    return db_settings()


@pytest.fixture
async def client(settings: APISettings) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created resources.
    async with lifespan_client(settings) as client:
        yield client


@pytest.fixture
async def db_engine(settings: APISettings) -> AsyncGenerator[AsyncEngine, None]:
    """Provide an engine on the same database the client fixture runs against."""
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        yield engine
    finally:
        await engine.dispose()


async def _count_idempotency_keys(engine: AsyncEngine, key: str) -> int:
    """Count idempotency key rows stored under the given key.

    Args:
        engine: Engine bound to the application database.
        key: Idempotency key to count rows for.

    Returns:
        Number of matching rows.
    """
    statement = (
        select(func.count())
        .select_from(IdempotencyKeyORM)
        .where(IdempotencyKeyORM.key == key)
    )
    async with engine.connect() as connection:
        result = await connection.execute(statement)
        return result.scalar_one()


async def test_replay_of_a_create_yields_one_row_and_the_same_id(
    client: httpx.AsyncClient, db_engine: AsyncEngine
) -> None:
    """Replay a create through the real database, storing exactly one row."""
    headers = {"Idempotency-Key": "create-prod"}
    body = {"name": "prod"}

    first = await client.post("/api/v1/tags", json=body, headers=headers)
    assert first.status_code == 201

    second = await client.post("/api/v1/tags", json=body, headers=headers)
    assert second.status_code == 201
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()

    listing = await client.get("/api/v1/tags")
    assert len(listing.json()["items"]) == 1
    assert await _count_idempotency_keys(db_engine, "create-prod") == 1


async def test_concurrent_duplicates_create_exactly_one_resource(
    client: httpx.AsyncClient, db_engine: AsyncEngine
) -> None:
    """Fire concurrent identical requests and settle on exactly one resource."""
    headers = {"Idempotency-Key": "create-concurrent"}
    body = {"name": "concurrent"}
    concurrency = 5

    responses = await asyncio.gather(
        *(
            client.post("/api/v1/tags", json=body, headers=headers)
            for _ in range(concurrency)
        )
    )

    assert all(response.status_code == 201 for response in responses)
    bodies = [response.json() for response in responses]
    assert all(item == bodies[0] for item in bodies)

    replayed = [
        response.headers.get("Idempotent-Replayed") == "true" for response in responses
    ]
    assert replayed.count(True) == concurrency - 1
    assert replayed.count(False) == 1

    listing = await client.get("/api/v1/tags")
    assert len(listing.json()["items"]) == 1
    assert await _count_idempotency_keys(db_engine, "create-concurrent") == 1


async def test_failed_handler_leaves_no_row(
    client: httpx.AsyncClient, db_engine: AsyncEngine
) -> None:
    """Roll back the key row alongside a failed handler, so a retry re-executes."""
    await client.post("/api/v1/tags", json={"name": "prod"})

    failed = await client.post(
        "/api/v1/tags",
        json={"name": "prod"},
        headers={"Idempotency-Key": "retry-me"},
    )
    assert failed.status_code == 409
    assert await _count_idempotency_keys(db_engine, "retry-me") == 0

    retried = await client.post(
        "/api/v1/tags",
        json={"name": "staging"},
        headers={"Idempotency-Key": "retry-me"},
    )
    assert retried.status_code == 201
    assert "Idempotent-Replayed" not in retried.headers
    assert await _count_idempotency_keys(db_engine, "retry-me") == 1
