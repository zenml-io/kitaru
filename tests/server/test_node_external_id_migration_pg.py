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
"""Node external id migration tests."""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.session import Session

NODE_EXTERNAL_ID_REVISION = "020_node_external_id"
PREVIOUS_REVISION = "019_import_max_sessions"

SESSION_NODE_COLUMNS = text("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'session_node'
""")

INSERT_NODE = text("""
    INSERT INTO session_node (
        id, session_id, parent_id, secondary_parent_ids, "index", external_id,
        node_type, name, status, started_at, metadata, created, updated
    ) VALUES (
        :id, :session_id, :parent_id, cast(:secondary_parent_ids AS jsonb),
        :index, :external_id, 'llm_call', 'call', 'completed', :started_at,
        '{}'::jsonb, :created, :created
    )
""")


async def test_upgrade_backfills_identity_and_downgrade_rebuilds_indexes() -> None:
    """Backfill identity and references, restoring the id links on downgrade."""
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
        created = datetime(2026, 5, 1, tzinfo=UTC)
        started_at = datetime(2026, 5, 1, 12, tzinfo=UTC)
        # Fixed ids because the untimed rows sort by id alone.
        root_id = uuid.UUID("00000000-0000-7000-8000-000000000001")
        child_id = uuid.UUID("00000000-0000-7000-8000-000000000002")
        orphan_id = uuid.UUID("00000000-0000-7000-8000-000000000003")
        async with factory() as db_session:
            owner = await SQLAccountRepository(db_session).create(Account(name="owner"))
            agent = await SQLAgentRepository(db_session).create(
                Agent(owner_id=owner.id, name="assistant")
            )
            session = await SQLSessionRepository(db_session, engine).create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=1,
                    origin=SessionOrigin.IMPORTED,
                )
            )
            rows = [
                (root_id, None, "[]", 0, "call-0", started_at),
                (child_id, root_id, f'["{orphan_id}"]', 1, None, None),
                (orphan_id, None, "[]", 2, None, None),
            ]
            for (
                node_id,
                parent_id,
                secondary_parent_ids,
                index,
                external_id,
                node_started_at,
            ) in rows:
                await db_session.execute(
                    INSERT_NODE,
                    {
                        "id": node_id,
                        "session_id": session.id,
                        "parent_id": parent_id,
                        "secondary_parent_ids": secondary_parent_ids,
                        "index": index,
                        "external_id": external_id,
                        "started_at": node_started_at,
                        "created": created,
                    },
                )
            await db_session.commit()

        await alembic.upgrade(NODE_EXTERNAL_ID_REVISION)
        async with engine.connect() as connection:
            upgraded = (
                await connection.execute(
                    text(
                        "SELECT id, external_id, started_at, "
                        "parent_external_id, secondary_parent_external_ids "
                        "FROM session_node ORDER BY started_at, id"
                    )
                )
            ).all()
        identities = {row.id: (row.external_id, row.started_at) for row in upgraded}
        assert identities[root_id] == ("call-0", started_at)
        assert identities[child_id] == ("index-1", None)
        assert identities[orphan_id] == ("index-2", None)
        references = {
            row.id: (row.parent_external_id, row.secondary_parent_external_ids)
            for row in upgraded
        }
        assert references[root_id] == (None, [])
        assert references[child_id] == ("call-0", ["index-2"])
        assert references[orphan_id] == (None, [])
        async with engine.connect() as connection:
            columns = (await connection.execute(SESSION_NODE_COLUMNS)).scalars().all()
        assert "parent_id" not in columns
        assert "secondary_parent_ids" not in columns

        await alembic.downgrade(PREVIOUS_REVISION)

        assert await alembic.current_revisions() == [PREVIOUS_REVISION]
        async with engine.connect() as connection:
            downgraded = (
                await connection.execute(
                    text(
                        'SELECT id, "index", parent_id, secondary_parent_ids '
                        'FROM session_node ORDER BY "index"'
                    )
                )
            ).all()
        assert [row.id for row in downgraded] == [root_id, child_id, orphan_id]
        assert [row.index for row in downgraded] == [0, 1, 2]
        links = {
            row.id: (row.parent_id, row.secondary_parent_ids) for row in downgraded
        }
        assert links[root_id] == (None, [])
        assert links[child_id] == (root_id, [str(orphan_id)])
        assert links[orphan_id] == (None, [])
        async with engine.connect() as connection:
            columns = (await connection.execute(SESSION_NODE_COLUMNS)).scalars().all()
        assert "parent_external_id" not in columns
        assert "secondary_parent_external_ids" not in columns
    finally:
        await engine.dispose()
        await drop_test_database(settings)
