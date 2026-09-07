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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Insight analyzer provenance migration tests."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.api_models.v1.insight import TextInsightData
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginKind,
)

ANALYZER_PROVENANCE_REVISION = "017_insight_analyzer_provenance"
PREVIOUS_REVISION = "016_analyzer"


async def test_downgrade_nulls_deleted_analyzer_version_ids() -> None:
    """Restore the old foreign key after an analyzer version has disappeared."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        await alembic.upgrade(ANALYZER_PROVENANCE_REVISION)
        factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with factory() as session:
            owner = await SQLAccountRepository(session).create(Account(name="owner"))
            agent = await SQLAgentRepository(session).create(
                Agent(owner_id=owner.id, name="assistant")
            )
            plugins = SQLPluginRepository(session)
            plugin = await plugins.create(
                Plugin(
                    owner_id=owner.id,
                    kind=PluginKind.ANALYZER,
                    name="trends",
                )
            )
            version = await plugins.create_version(
                plugin.id,
                PackagePluginSource(
                    requirement="kitaru-analyzer==1.0.0",
                    entrypoint="pkg:analyze",
                ),
                display_version=None,
            )
            [insight] = await SQLInsightRepository(session).create_many(
                [
                    Insight(
                        owner_id=owner.id,
                        agent_id=agent.id,
                        analyzer_version_id=version.id,
                        name="summary",
                        title="Summary",
                        data=TextInsightData(content="Finding"),
                    )
                ]
            )
            await plugins.delete(plugin.id)
            await session.commit()

        await alembic.downgrade(PREVIOUS_REVISION)

        assert await alembic.current_revisions() == [PREVIOUS_REVISION]
        async with engine.connect() as connection:
            analyzer_version_id = await connection.scalar(
                text("SELECT analyzer_version_id FROM insight WHERE id = :insight_id"),
                {"insight_id": insight.id},
            )
        assert analyzer_version_id is None
    finally:
        await engine.dispose()
        await drop_test_database(settings)
