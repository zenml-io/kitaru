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
"""Lifecycle tests for the fuzzing harness's live server."""

import asyncio
from typing import Any

import fuzz_server
import pytest
from fuzz_server import FuzzServer
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from tests.conftest import postgres_available

from kitaru.server.database.service import DatabaseService


@pytest.fixture(autouse=True)
async def _require_postgres() -> None:
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")


def _database_exists(server: FuzzServer) -> bool:
    name = DatabaseService.application_database_name(server.settings)

    async def query() -> bool:
        engine = create_async_engine(
            DatabaseService.generate_database_uri(server.settings, use_default_db=True)
        )
        try:
            async with engine.connect() as connection:
                row = await connection.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :name"),
                    {"name": name},
                )
                return row.first() is not None
        finally:
            await engine.dispose()

    return asyncio.run(query())


def test_stop_drops_database() -> None:
    server = FuzzServer()
    server.start()
    try:
        assert _database_exists(server)
    finally:
        server.stop()
    assert not _database_exists(server)


def test_boot_failure_drops_database(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_boot() -> None:
        raise RuntimeError("simulated boot failure")

    server = FuzzServer()
    monkeypatch.setattr(server, "_boot", fail_to_boot)
    with pytest.raises(RuntimeError, match="boot failure"):
        server.start()
    assert not _database_exists(server)


def test_login_failure_drops_database_and_stops_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_to_post(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("simulated login failure")

    monkeypatch.setattr(fuzz_server.httpx, "post", fail_to_post)
    server = FuzzServer()
    with pytest.raises(RuntimeError, match="login failure"):
        server.start()
    assert not _database_exists(server)
    assert server._thread is not None
    assert not server._thread.is_alive()
