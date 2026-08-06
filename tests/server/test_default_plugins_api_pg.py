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
"""End-to-end default plugin registration tests against PostgreSQL."""

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import (
    db_settings,
    lifespan_client,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import SQLPluginRepository
from kitaru.server.api import bootstrap
from kitaru.server.api.bootstrap import (
    DefaultPluginDefinition,
    register_default_plugins,
)
from kitaru.server.application.models.plugin import PluginVersionFilter
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX
from kitaru.server.domain.plugin import PluginKind


async def test_default_plugins_are_registered_at_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """List a catalog-declared default plugin with a null owner after startup."""
    definition = DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}evaluator",
        description="Test evaluator.",
        provider=None,
        entrypoint="evaluate",
        content=b"def evaluate(): ...",
        version=1,
    )
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", (definition,))

    async with lifespan_client(db_settings()) as client:
        evaluators = (await client.get("/v1/evaluators")).json()["items"]

    matches = [item for item in evaluators if item["name"] == definition.name]
    assert len(matches) == 1
    assert matches[0]["owner_id"] is None


async def test_concurrent_registration_creates_one_declared_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize concurrent startup around the plugin's declared revision."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    definition = DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}concurrent-evaluator",
        description="Concurrent test evaluator.",
        provider=None,
        entrypoint="evaluate",
        content=b"def evaluate(): ...",
        version=1,
    )
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", (definition,))

    async with pg_session_with_engine() as (_, engine):
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )

        async def register() -> None:
            async with session_factory() as session:
                await register_default_plugins(
                    SQLPluginRepository(session), SQLBlobRepository(session)
                )
                await session.commit()

        await asyncio.gather(register(), register())

        async with session_factory() as session:
            repository = SQLPluginRepository(session)
            plugin = await repository.get_by_name(definition.kind, definition.name)
            assert plugin.latest_version == 1
            versions, _ = await repository.query_versions(
                PluginVersionFilter(plugin_id=plugin.id)
            )
            assert [version.version for version in versions] == [1]
