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
"""Tests for the server settings repository."""

import uuid

import pytest

from conftest import pg_session, postgres_available
from kitaru.server.adapters.db.repositories.server_settings_repository import (
    SQLServerSettingsRepository,
)
from kitaru.server.api.bootstrap import ensure_server_id


async def test_ensure_server_id_stores_the_first_id() -> None:
    """Store the offered id when the table is empty."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    server_id = uuid.uuid4()
    async with pg_session() as session:
        repository = SQLServerSettingsRepository(session)
        assert await repository.ensure_server_id(server_id) == server_id


async def test_ensure_server_id_keeps_the_stored_id() -> None:
    """Return the stored id instead of a later offered one."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    server_id = uuid.uuid4()
    async with pg_session() as session:
        repository = SQLServerSettingsRepository(session)
        await repository.ensure_server_id(server_id)
        await session.commit()
        assert await repository.ensure_server_id(uuid.uuid4()) == server_id


async def test_ensure_server_id_generates_without_a_configured_id() -> None:
    """Generate an id at bootstrap when none is configured and keep it after."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session() as session:
        repository = SQLServerSettingsRepository(session)
        server_id = await ensure_server_id(repository, None)
        await session.commit()
        assert await ensure_server_id(repository, None) == server_id
