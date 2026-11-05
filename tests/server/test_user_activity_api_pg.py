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
"""End-to-end user activity reporting tests against PostgreSQL."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, update

from conftest import drop_test_database, local_settings, postgres_available
from kitaru.server.adapters.db.orm.server_settings import ServerSettingsORM
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.services.user_activity import UserActivityRecorder
from kitaru.server.database.service import DatabaseService

RUNTIME = {"platform": "bare"}
SCOPE = {"claims": [{"kind": "agent"}]}


@pytest.fixture
async def settings() -> AsyncGenerator[APISettings, None]:
    """Provide settings for a fresh database that several servers can share."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    values = local_settings(use_db=True, DEFAULT_ACCOUNT_PASSWORD="secret")
    await DatabaseService.create_db(values)
    try:
        yield values
    finally:
        await drop_test_database(values)


@asynccontextmanager
async def _serve(
    settings: APISettings,
) -> AsyncGenerator[tuple[FastAPI, httpx.AsyncClient], None]:
    """Run one application instance on the shared database through its lifespan."""
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client,
    ):
        yield app, client


async def _login(client: httpx.AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/api/v1/login", data={"username": "default", "password": "secret"}
    )
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def _get_activity(client: httpx.AsyncClient) -> datetime | None:
    value = (await client.get("/api/v1/info")).json()["last_user_activity"]
    return None if value is None else datetime.fromisoformat(value)


async def _register_worker(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> tuple[str, dict[str, str]]:
    registration = (
        await client.post(
            "/api/v1/workers",
            json={"name": "w", "scope": SCOPE, "runtime": RUNTIME, "metadata": {}},
            headers=headers,
        )
    ).json()
    return registration["worker"]["id"], {
        "Authorization": f"Bearer {registration['token']}"
    }


async def test_user_requests_advance_activity_but_info_and_health_do_not(
    settings: APISettings,
) -> None:
    """Report activity from authenticated requests only."""
    now = datetime(2026, 1, 1, tzinfo=UTC)

    async def keep_in_memory(at: datetime) -> None:
        return None

    async with _serve(settings) as (app, client):
        # Swap in a recorder on a controlled clock so the later request is
        # later by construction.
        app.state.user_activity = UserActivityRecorder(
            keep_in_memory, clock=lambda: now
        )
        assert await _get_activity(client) is None
        headers = await _login(client)

        await client.get("/api/v1/agents", headers=headers)
        assert await _get_activity(client) == now

        for path in ("/api/v1/info", "/health", "/health/live"):
            await client.get(path)
        now += timedelta(minutes=1)
        assert await _get_activity(client) == now - timedelta(minutes=1)

        await client.get("/api/v1/agents", headers=headers)
        assert await _get_activity(client) == now


async def test_idle_worker_traffic_does_not_advance_activity(
    settings: APISettings,
) -> None:
    """Leave the time alone for worker registration, lookup, polling, and renewal."""
    async with _serve(settings) as (_, client):
        headers = await _login(client)
        await client.get("/api/v1/agents", headers=headers)
        before = await _get_activity(client)
        assert before is not None
        worker_id, worker_headers = await _register_worker(client, headers)

        claimed = await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
        assert claimed.json()["tasks"] == []
        heartbeat = await client.post(
            f"/api/v1/workers/{worker_id}/heartbeat",
            json={"task_ids": []},
            headers=worker_headers,
        )
        assert heartbeat.status_code == 200
        renewal = await client.post(
            f"/api/v1/workers/{worker_id}/token", headers=headers
        )
        assert renewal.status_code == 200
        lookup = await client.get(f"/api/v1/workers/{worker_id}", headers=headers)
        assert lookup.status_code == 200

        assert await _get_activity(client) == before


async def test_a_task_in_flight_reports_fresh_activity(settings: APISettings) -> None:
    """Report the current time while a task runs, whatever time is stored."""
    stale = datetime.now(UTC) - timedelta(hours=23)
    async with _serve(settings) as (app, client):
        headers = await _login(client)
        agent = (
            await client.post("/api/v1/agents", json={"name": "a"}, headers=headers)
        ).json()
        version = (
            await client.post(
                f"/api/v1/agents/{agent['id']}/versions",
                json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
                headers=headers,
            )
        ).json()
        await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version["id"], "inputs": None},
            headers=headers,
        )
        _, worker_headers = await _register_worker(client, headers)
        claimed = (
            await client.post(
                "/api/v1/tasks/claim", json={"max_tasks": 1}, headers=worker_headers
            )
        ).json()["tasks"][0]
        task_headers = {"Authorization": f"Bearer {claimed['token']}"}
        async for session in app.state.database.get_async_session():
            await session.execute(
                update(ServerSettingsORM).values(last_user_activity=stale)
            )
            await session.commit()

        # A server that never saw a user request, as after a restart.
        async with _serve(settings) as (_, other):
            running = await _get_activity(other)
            assert running is not None
            assert running - stale > timedelta(hours=22)

            finished = await other.patch(
                f"/api/v1/tasks/{claimed['task']['id']}",
                json={"status": "failed"},
                headers=task_headers,
            )
            assert finished.status_code == 200
            assert await _get_activity(other) == stale


async def test_activity_is_shared_between_servers_and_survives_a_restart(
    settings: APISettings,
) -> None:
    """Persist the time so another server and a restarted server report it."""
    async with _serve(settings) as (app, client):
        await client.get("/api/v1/agents", headers=await _login(client))
        await app.state.user_activity.drain()
        seen = await _get_activity(client)
        assert seen is not None

        async with _serve(settings) as (_, other):
            assert await _get_activity(other) == seen

    async with _serve(settings) as (_, restarted):
        assert await _get_activity(restarted) == seen


async def _connect(settings: APISettings) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PWD,
        database=DatabaseService.application_database_name(settings),
    )


async def test_a_failing_write_does_not_fail_the_request(
    settings: APISettings,
) -> None:
    """Answer the request and keep the time in memory when the write fails."""
    async with _serve(settings) as (app, client):
        headers = await _login(client)
        connection = await _connect(settings)
        try:
            await connection.execute(
                "CREATE FUNCTION reject_write() RETURNS trigger AS "
                "$$ BEGIN RAISE EXCEPTION 'write rejected'; END $$ "
                "LANGUAGE plpgsql"
            )
            await connection.execute(
                "CREATE TRIGGER reject_write BEFORE UPDATE ON server_settings "
                "FOR EACH ROW EXECUTE FUNCTION reject_write()"
            )
            response = await client.get("/api/v1/agents", headers=headers)
            await app.state.user_activity.drain()
            stored = await connection.fetchval(
                "SELECT last_user_activity FROM server_settings"
            )
        finally:
            await connection.execute("DROP TRIGGER reject_write ON server_settings")
            await connection.close()

        assert response.status_code == 200
        assert stored is None
        assert await _get_activity(client) is not None


async def test_a_slow_write_does_not_hold_up_the_request(settings: APISettings) -> None:
    """Answer while the write waits on a lock, then store the time once it clears."""
    async with _serve(settings) as (app, client):
        headers = await _login(client)
        connection = await _connect(settings)
        transaction = connection.transaction()
        await transaction.start()
        try:
            await connection.fetch("SELECT * FROM server_settings FOR UPDATE")
            response = await asyncio.wait_for(
                client.get("/api/v1/agents", headers=headers), timeout=10
            )
            reported = await _get_activity(client)
        finally:
            await transaction.rollback()
            await connection.close()

        assert response.status_code == 200
        assert reported is not None
        await app.state.user_activity.drain()
        assert await _get_activity(client) == reported
        async for session in app.state.database.get_async_session():
            stored = (
                await session.execute(select(ServerSettingsORM.last_user_activity))
            ).scalar_one()
        assert stored == reported


async def test_info_answers_null_when_the_read_hangs(settings: APISettings) -> None:
    """Answer the info route with no time instead of waiting on a locked table."""
    async with _serve(settings) as (_, client):
        await client.get("/api/v1/agents", headers=await _login(client))
        connection = await _connect(settings)
        transaction = connection.transaction()
        await transaction.start()
        try:
            await connection.execute(
                "LOCK TABLE server_settings IN ACCESS EXCLUSIVE MODE"
            )
            response = await asyncio.wait_for(client.get("/api/v1/info"), timeout=15)
        finally:
            await transaction.rollback()
            await connection.close()

        assert response.status_code == 200
        assert response.json()["last_user_activity"] is None


async def test_shutdown_does_not_wait_on_a_locked_write(settings: APISettings) -> None:
    """Finish the lifespan while the activity write waits on a row lock."""
    server = _serve(settings)
    _, client = await server.__aenter__()
    connection = await _connect(settings)
    transaction = connection.transaction()
    try:
        headers = await _login(client)
        await transaction.start()
        await connection.fetch("SELECT * FROM server_settings FOR UPDATE")
        response = await client.get("/api/v1/agents", headers=headers)
        assert response.status_code == 200
        await asyncio.wait_for(server.__aexit__(None, None, None), timeout=30)
    finally:
        await transaction.rollback()
        await connection.close()
