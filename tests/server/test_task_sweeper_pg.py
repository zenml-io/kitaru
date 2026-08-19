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
"""End-to-end tests for the background stale-task sweep loop against PostgreSQL."""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, lifespan_client
from kitaru.server.adapters.db.repositories.idempotency_key_repository import (
    SQLIdempotencyKeyRepository,
)
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.idempotency_key import IdempotencyKey

RUNTIME = {"platform": "bare"}
SCOPE = {"claims": [{"kind": "agent"}]}


async def _wait_until(
    poll: httpx.AsyncClient, url: str, field: str, value: str, timeout: float = 10.0
) -> dict[str, object]:
    """Poll a resource until a field reaches a value, or fail after a timeout.

    Args:
        poll: HTTP client to poll with.
        url: Resource URL.
        field: Response field to check.
        value: Expected value.
        timeout: Seconds to poll before failing.

    Returns:
        The resource body once the field matches.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    body: dict[str, object] = {}
    while asyncio.get_running_loop().time() < deadline:
        body = (await poll.get(url)).json()
        if body[field] == value:
            return body
        await asyncio.sleep(0.1)
    raise AssertionError(f"{url} never reached {field}={value!r}, last body: {body}")


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its lifespan with a fast sweeper."""
    settings = db_settings(
        TASK_SWEEP_INTERVAL_SECONDS=1,
        TASK_HEARTBEAT_TIMEOUT_SECONDS=1,
        TASK_RETRY_LIMIT=1,
    )
    async with lifespan_client(settings) as client:
        yield client


async def test_background_sweep_abandons_a_stale_task_and_settles_the_job(
    client: httpx.AsyncClient,
) -> None:
    """A worker that stops reporting has its task abandoned by the sweep loop alone.

    No claim or heartbeat request runs after the initial claim, so the only
    thing that can move the task and job forward is the background sweeper
    started from the app lifespan.
    """
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    job = (
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version["id"], "inputs": {"q": "hi"}},
        )
    ).json()
    registration = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    worker_headers = {"Authorization": f"Bearer {registration['token']}"}
    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    task = claimed["tasks"][0]["task"]
    assert task["status"] == "claimed"

    task_after = await _wait_until(
        client, f"/api/v1/tasks/{task['id']}", "status", "abandoned"
    )
    assert task_after["attempt"] == 1

    job_after = await _wait_until(
        client, f"/api/v1/jobs/{job['id']}", "status", "failed"
    )
    assert job_after["error"] is not None


async def test_background_sweep_reaches_replay_settlement_subscribers(
    client: httpx.AsyncClient,
) -> None:
    """A replay's job settling through the sweep still settles the replay.

    The sweep abandons the stale agent task exactly like the claim path
    would, and the resulting JobsSettled event must still reach the same
    replay-settlement subscriber a request wires, proving the background
    tick shares the request's event dispatcher composition rather than
    running the transition without it.
    """
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    baseline = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": agent["id"],
                "agent_version_id": version["id"],
                "origin": "recorded",
                "inputs": {"q": "hi"},
                "outputs": None,
            },
        )
    ).json()
    blob = (
        await client.post(
            "/api/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post(
            "/api/v1/evaluators", json={"name": "accuracy", "metadata": {}}
        )
    ).json()
    await client.post(
        f"/api/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    replay = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": baseline["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    assert replay["status"] == "pending"

    registration = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    worker_headers = {"Authorization": f"Bearer {registration['token']}"}
    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    assert claimed["tasks"][0]["task"]["kind"] == "agent"

    replay_after = await _wait_until(
        client, f"/api/v1/replays/{replay['id']}", "status", "failed"
    )
    assert replay_after["status"] == "failed"


async def test_background_sweep_deletes_expired_idempotency_keys() -> None:
    """The sweep loop deletes a key past retention and keeps a fresh one.

    No route creates idempotency keys yet, so the keys are stored directly
    through the repository against the app's own database.
    """
    settings = db_settings(
        TASK_SWEEP_INTERVAL_SECONDS=1, IDEMPOTENCY_KEY_RETENTION_SECONDS=1
    )
    async with lifespan_client(settings) as client:
        account_id = uuid.UUID((await client.get("/api/v1/accounts/me")).json()["id"])

        engine = create_async_engine(DatabaseService.generate_database_uri(settings))
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        try:
            async with session_factory() as session:
                await SQLIdempotencyKeyRepository(session).create(
                    IdempotencyKey(
                        account_id=account_id,
                        key="expired",
                        fingerprint="f" * 64,
                        method="POST",
                        path="/api/v1/agents",
                    )
                )
                await session.commit()

            # Age the first key past the one-second retention before storing
            # the second, so the sweep's cutoff only catches the first.
            await asyncio.sleep(1.5)

            async with session_factory() as session:
                await SQLIdempotencyKeyRepository(session).create(
                    IdempotencyKey(
                        account_id=account_id,
                        key="fresh",
                        fingerprint="f" * 64,
                        method="POST",
                        path="/api/v1/agents",
                    )
                )
                await session.commit()

            deadline = asyncio.get_running_loop().time() + 10.0
            while asyncio.get_running_loop().time() < deadline:
                async with session_factory() as session:
                    found = await SQLIdempotencyKeyRepository(session).get(
                        account_id, "expired"
                    )
                if found is None:
                    break
                await asyncio.sleep(0.2)
            else:
                raise AssertionError("expired idempotency key was never swept")

            async with session_factory() as session:
                assert (
                    await SQLIdempotencyKeyRepository(session).get(account_id, "fresh")
                    is not None
                )
        finally:
            await engine.dispose()
