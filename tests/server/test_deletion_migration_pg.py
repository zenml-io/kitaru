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
"""The deletion rules migration refuses to downgrade."""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent

BEFORE_DELETION_RULES = "004_replay_result_session_id"
DELETION_RULES = "005_deletion_rules"


async def _seed_agents(engine: AsyncEngine, names: list[str]) -> None:
    """Store one account and one agent per name."""
    factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    async with factory() as session:
        owner = await SQLAccountRepository(session).create(
            Account(name=f"owner-{uuid.uuid4().hex[:8]}")
        )
        for name in names:
            await SQLAgentRepository(session).create(
                Agent(owner_id=owner.id, name=name)
            )
        await session.commit()


async def test_downgrade_past_deletion_rules_is_rejected() -> None:
    """Refuse the downgrade, roll the revision walk back, and keep data."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        await alembic.upgrade()
        head = await alembic.current_revisions()

        await _seed_agents(engine, ["assistant"])
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE agent SET deleted_at = now() WHERE name = 'assistant'")
            )
        await _seed_agents(engine, ["assistant"])

        # The walk runs in one transaction, so the refusal in the deletion
        # rules revision also rolls back the downgrades above it.
        with pytest.raises(RuntimeError, match="cannot be downgraded"):
            await alembic.downgrade(BEFORE_DELETION_RULES)
        assert await alembic.current_revisions() == head

        async with engine.connect() as connection:
            count = await connection.scalar(text("SELECT count(*) FROM agent"))
        assert count == 2
    finally:
        await engine.dispose()
        await drop_test_database(settings)


async def test_downgrade_to_deletion_rules_round_trips() -> None:
    """Downgrade the revisions above the deletion rules and upgrade back to head."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        await alembic.upgrade()
        head = await alembic.current_revisions()

        await _seed_agents(engine, ["assistant"])

        await alembic.downgrade(DELETION_RULES)
        assert await alembic.current_revisions() == [DELETION_RULES]

        await alembic.upgrade()
        assert await alembic.current_revisions() == head

        async with engine.connect() as connection:
            count = await connection.scalar(text("SELECT count(*) FROM agent"))
        assert count == 1
    finally:
        await engine.dispose()
        await drop_test_database(settings)
