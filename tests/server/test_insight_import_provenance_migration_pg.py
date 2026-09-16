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
"""Insight import provenance migration tests."""

import json
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.api_models.v1.job import JobKind
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginKind,
)
from kitaru.server.domain.task import AnalysisTask

IMPORT_PROVENANCE_REVISION = "018_insight_import_provenance"
PREVIOUS_REVISION = "017_connection"


async def test_upgrade_backfills_imports_and_downgrade_preserves_insights() -> None:
    """Backfill valid import references without assigning manual or orphaned rows."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        await alembic.upgrade(PREVIOUS_REVISION)
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
            import_id = uuid.uuid4()
            # Seed the historical schema without newer ORM columns such as
            # max_sessions, which was added after this migration.
            await session.execute(
                text(
                    "INSERT INTO import (id, owner_id, agent_id, fetch_query, "
                    "params, evaluators, analyzers, created, updated) VALUES "
                    "(:id, :owner_id, :agent_id, '{}'::jsonb, '{}'::jsonb, "
                    "'[]'::jsonb, '[]'::jsonb, now(), now())"
                ),
                {"id": import_id, "owner_id": owner.id, "agent_id": agent.id},
            )
            job = await SQLJobRepository(session).create(
                Job(owner_id=owner.id, kind=JobKind.IMPORT)
            )
            tasks = SQLTaskRepository(session)
            task = await tasks.create(
                AnalysisTask(
                    job_id=job.id,
                    agent_id=agent.id,
                    plugin_version_id=version.id,
                    import_id=import_id,
                )
            )
            orphaned_task = await tasks.create(
                AnalysisTask(
                    job_id=job.id,
                    agent_id=agent.id,
                    plugin_version_id=version.id,
                    import_id=uuid.uuid4(),
                )
            )
            insight_ids = [uuid.uuid4() for _ in range(3)]
            # Insert the previous schema, which has no insight.import_id column.
            for insight_id, task_id in zip(
                insight_ids, [task.id, orphaned_task.id, None], strict=True
            ):
                await session.execute(
                    text(
                        "INSERT INTO insight (id, owner_id, agent_id, "
                        "analyzer_version_id, task_id, name, title, type, data, "
                        "metadata, created, updated) VALUES "
                        "(:id, :owner_id, :agent_id, :version_id, :task_id, "
                        "'summary', 'Summary', 'text', CAST(:data AS jsonb), "
                        "'{}'::jsonb, now(), now())"
                    ),
                    {
                        "id": insight_id,
                        "owner_id": owner.id,
                        "agent_id": agent.id,
                        "version_id": version.id if task_id else None,
                        "task_id": task_id,
                        "data": json.dumps({"type": "text", "content": "Finding"}),
                    },
                )
            await session.commit()

        await alembic.upgrade(IMPORT_PROVENANCE_REVISION)
        async with factory() as session:
            insights = SQLInsightRepository(session)
            assert (await insights.get(insight_ids[0])).import_id == import_id
            assert (await insights.get(insight_ids[1])).import_id is None
            assert (await insights.get(insight_ids[2])).import_id is None
            plugins = SQLPluginRepository(session)
            await plugins.delete(plugin.id)
            await session.commit()

        await alembic.downgrade(PREVIOUS_REVISION)

        assert await alembic.current_revisions() == [PREVIOUS_REVISION]
        async with engine.connect() as connection:
            analyzer_version_id = await connection.scalar(
                text("SELECT analyzer_version_id FROM insight WHERE id = :insight_id"),
                {"insight_id": insight_ids[0]},
            )
            count = await connection.scalar(text("SELECT count(*) FROM insight"))
        assert analyzer_version_id is None
        assert count == 3
    finally:
        await engine.dispose()
        await drop_test_database(settings)
