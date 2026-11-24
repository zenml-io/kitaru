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
"""Cohort creation and deletion transaction serialization against PostgreSQL."""

import asyncio
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.cohort_repository import SQLCohortRepository
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import CohortVersionCreate
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import Session


async def _wait_until_blocked(
    factory: async_sessionmaker[AsyncSession], pid: int
) -> None:
    """Wait for a specific PostgreSQL transaction to block on another transaction."""
    async with factory() as observer:
        async with asyncio.timeout(5):
            while True:
                blockers = await observer.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid}
                )
                await observer.rollback()
                if blockers:
                    return
                await asyncio.sleep(0.01)


@pytest.mark.parametrize("explicit_baseline", [False, True])
async def test_delete_waits_until_creation_has_copied_baseline(
    explicit_baseline: bool,
) -> None:
    """Deletion cannot remove default or explicit baseline members during creation."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (seed, engine):
        owner = await SQLAccountRepository(seed).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed).create(
            Agent(owner_id=owner.id, name="agent")
        )
        cohort = await SQLCohortRepository(seed).create(
            Cohort(owner_id=owner.id, agent_id=agent.id, name="cohort")
        )
        sessions = SQLSessionRepository(seed, engine)
        members = [
            (
                await sessions.create(
                    Session(
                        owner_id=owner.id,
                        agent_id=agent.id,
                        number=number,
                        origin=SessionOrigin.RECORDED,
                    )
                )
            ).id
            for number in range(1, 3)
        ]
        actor = AuthContext(account=owner)
        baseline = await CohortVersionService(
            SQLCohortVersionRepository(seed), SQLCohortRepository(seed), sessions
        ).create_version(
            cohort.id, CohortVersionCreate(add_session_ids=members), actor=actor
        )
        await seed.commit()
        factory = async_sessionmaker(engine, expire_on_commit=False)
        reading = asyncio.Event()
        resume = asyncio.Event()

        class PausedMembershipRepository(SQLCohortVersionRepository):
            async def list_session_ids(
                self, cohort_version_id: uuid.UUID
            ) -> list[uuid.UUID]:
                reading.set()
                await resume.wait()
                return await super().list_session_ids(cohort_version_id)

        async def create_copy():
            async with factory() as db:
                service = CohortVersionService(
                    PausedMembershipRepository(db),
                    SQLCohortRepository(db),
                    SQLSessionRepository(db, engine),
                )
                version = await service.create_version(
                    cohort.id,
                    CohortVersionCreate(
                        baseline_id=baseline.id if explicit_baseline else None
                    ),
                    actor=actor,
                )
                await db.commit()
                return version

        async with factory() as deleting:
            deleting_pid = await deleting.scalar(text("SELECT pg_backend_pid()"))
            assert deleting_pid is not None
            deletion_service = CohortVersionService(
                SQLCohortVersionRepository(deleting),
                SQLCohortRepository(deleting),
                SQLSessionRepository(deleting, engine),
            )
            creating = asyncio.create_task(create_copy())
            await asyncio.wait_for(reading.wait(), timeout=5)
            deletion = asyncio.create_task(
                deletion_service.delete_version(baseline.id, actor=actor)
            )
            blocked = asyncio.create_task(_wait_until_blocked(factory, deleting_pid))
            try:
                done, _ = await asyncio.wait(
                    {deletion, blocked}, return_when=asyncio.FIRST_COMPLETED, timeout=6
                )
                assert blocked in done, (
                    "Deletion completed before the baseline was copied"
                )
                await blocked
                assert not deletion.done()
            finally:
                resume.set()
                blocked.cancel()
                await asyncio.gather(blocked, return_exceptions=True)
                await asyncio.wait_for(asyncio.gather(creating, deletion), timeout=5)
            await deleting.commit()
            copied = creating.result()
        async with factory() as verifying:
            repository = SQLCohortVersionRepository(verifying)
            assert copied.version == 2
            assert await repository.list_session_ids(copied.id) == members
            assert (
                await SQLCohortRepository(verifying).get(cohort.id)
            ).latest_version == 2


async def test_creation_waits_for_deletion_then_copies_surviving_version() -> None:
    """A waiting creation reads surviving membership after deletion commits."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (seed, engine):
        owner = await SQLAccountRepository(seed).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed).create(
            Agent(owner_id=owner.id, name="agent")
        )
        cohort = await SQLCohortRepository(seed).create(
            Cohort(owner_id=owner.id, agent_id=agent.id, name="cohort")
        )
        sessions = SQLSessionRepository(seed, engine)
        members = [
            (
                await sessions.create(
                    Session(
                        owner_id=owner.id,
                        agent_id=agent.id,
                        number=number,
                        origin=SessionOrigin.RECORDED,
                    )
                )
            ).id
            for number in range(1, 3)
        ]
        actor = AuthContext(account=owner)
        service = CohortVersionService(
            SQLCohortVersionRepository(seed), SQLCohortRepository(seed), sessions
        )
        await service.create_version(
            cohort.id, CohortVersionCreate(add_session_ids=members), actor=actor
        )
        latest = await service.create_version(
            cohort.id, CohortVersionCreate(remove_session_ids=members[:1]), actor=actor
        )
        await seed.commit()
        factory = async_sessionmaker(engine, expire_on_commit=False)
        deleting = asyncio.Event()
        resume = asyncio.Event()

        class PausedDeleteRepository(SQLCohortVersionRepository):
            async def delete(self, cohort_version_id: uuid.UUID) -> None:
                deleting.set()
                await resume.wait()
                await super().delete(cohort_version_id)

        async def delete_latest() -> None:
            async with factory() as db:
                service = CohortVersionService(
                    PausedDeleteRepository(db),
                    SQLCohortRepository(db),
                    SQLSessionRepository(db, engine),
                )
                await service.delete_version(latest.id, actor=actor)
                await db.commit()

        async with factory() as creating_session:
            creating_pid = await creating_session.scalar(
                text("SELECT pg_backend_pid()")
            )
            assert creating_pid is not None
            service = CohortVersionService(
                SQLCohortVersionRepository(creating_session),
                SQLCohortRepository(creating_session),
                SQLSessionRepository(creating_session, engine),
            )
            deletion = asyncio.create_task(delete_latest())
            await asyncio.wait_for(deleting.wait(), timeout=5)
            creation = asyncio.create_task(
                service.create_version(cohort.id, CohortVersionCreate(), actor=actor)
            )
            blocked = asyncio.create_task(_wait_until_blocked(factory, creating_pid))
            try:
                done, _ = await asyncio.wait(
                    {creation, blocked}, return_when=asyncio.FIRST_COMPLETED, timeout=6
                )
                assert blocked in done, (
                    "Creation copied a version before deletion committed"
                )
                await blocked
                assert not creation.done()
            finally:
                resume.set()
                blocked.cancel()
                await asyncio.gather(blocked, return_exceptions=True)
                await asyncio.wait_for(asyncio.gather(deletion, creation), timeout=5)
            await creating_session.commit()
            copied = creation.result()
        async with factory() as verifying:
            assert copied.version == 3
            assert (
                await SQLCohortVersionRepository(verifying).list_session_ids(copied.id)
                == members
            )
            assert (
                await SQLCohortRepository(verifying).get(cohort.id)
            ).latest_version == 3
