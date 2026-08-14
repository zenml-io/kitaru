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
"""Tests for the database service engine configuration."""

import pytest

from conftest import db_settings, local_settings
from kitaru.server.database.service import DatabaseService


async def test_engine_enables_pool_pre_ping() -> None:
    """Liveness-check a pooled connection on checkout."""
    engine = DatabaseService.create_async_engine(db_settings())
    try:
        assert engine.pool._pre_ping is True
    finally:
        await engine.dispose()


async def test_generate_read_database_uri_returns_none_when_unconfigured() -> None:
    """No read URL and no read host leaves the read replica unconfigured."""
    settings = local_settings()

    assert DatabaseService.generate_read_database_uri(settings) is None


async def test_generate_read_database_uri_from_db_read_host() -> None:
    """DB_READ_HOST reuses the primary user, password, port, and database name."""
    settings = local_settings(
        DB_USER="alice",
        DB_PWD="secret",
        DB_PORT=6543,
        DB_NAME="app",
        DB_READ_HOST="replica-host",
    )

    uri = DatabaseService.generate_read_database_uri(settings)

    assert uri is not None
    assert uri.drivername == "postgresql+asyncpg"
    assert uri.host == "replica-host"
    assert uri.username == "alice"
    assert uri.password == "secret"
    assert uri.port == 6543
    assert uri.database == "app"


async def test_generate_read_database_uri_prefers_read_database_url() -> None:
    """READ_DATABASE_URL wins over DB_READ_HOST when both are set."""
    settings = local_settings(
        DB_READ_HOST="replica-host",
        READ_DATABASE_URL="postgresql+asyncpg://reader:pw@read.example.com:5555/replica_db",
    )

    uri = DatabaseService.generate_read_database_uri(settings)

    assert uri is not None
    assert uri.host == "read.example.com"
    assert uri.username == "reader"
    assert uri.password == "pw"
    assert uri.port == 5555
    assert uri.database == "replica_db"


async def test_generate_read_database_uri_rejects_ssl_query_params() -> None:
    """READ_DATABASE_URL cannot configure SSL through query parameters."""
    settings = local_settings(
        READ_DATABASE_URL="postgresql+asyncpg://reader:pw@read.example.com/db?sslmode=require"
    )

    with pytest.raises(ValueError, match="READ_DATABASE_URL"):
        DatabaseService.generate_read_database_uri(settings)


async def test_read_engine_falls_back_to_the_primary_engine_when_unconfigured() -> None:
    """The read engine is the primary engine when no read replica is configured."""
    database = DatabaseService(local_settings())
    try:
        assert database.read_engine is database.engine
    finally:
        await database.cleanup()


async def test_read_engine_is_distinct_when_a_read_replica_is_configured() -> None:
    """A configured read replica gets its own engine."""
    database = DatabaseService(local_settings(DB_READ_HOST="replica-host"))
    try:
        assert database.read_engine is not database.engine
    finally:
        await database.cleanup()


async def test_get_async_session_read_only_binds_to_the_read_engine() -> None:
    """A read-only session binds to the read engine, not the primary engine."""
    database = DatabaseService(local_settings(DB_READ_HOST="replica-host"))
    try:
        async for session in database.get_async_session(read_only=True):
            assert session.get_bind() is database.read_engine.sync_engine
    finally:
        await database.cleanup()


async def test_get_async_session_default_binds_to_the_primary_engine() -> None:
    """A default session binds to the primary engine, not a configured read replica."""
    database = DatabaseService(local_settings(DB_READ_HOST="replica-host"))
    try:
        async for session in database.get_async_session():
            assert session.get_bind() is database.engine.sync_engine
    finally:
        await database.cleanup()
