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
"""Concurrency tests for job settlement against PostgreSQL."""

import asyncio
import uuid
from datetime import UTC, datetime
from functools import partial

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.job import Job
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import AgentTask, Task


async def _complete_and_advance(session: AsyncSession, task_id: uuid.UUID) -> None:
    """Complete one task and advance its job within the same session."""
    task_repository = SQLTaskRepository(session)
    transitions = TaskTransitions(
        task_repository=task_repository,
        job_repository=SQLJobRepository(session),
        dispatcher=EventDispatcher(),
    )
    task = await task_repository.get(task_id)
    await transitions.apply_status(
        task, partial(Task.complete, result=None, now=datetime.now(UTC))
    )


async def test_advance_job_serializes_concurrent_task_completions() -> None:
    """Two tasks of one job completing in overlapping transactions still settle it.

    Regression test for a race in job settlement: it used to list the job's
    tasks before locking the job row, so two tasks completing concurrently
    could each read the other's still-uncommitted terminal status as live and
    both skip settlement, leaving the job running forever. Locking the job row
    first forces the second completion to wait for the first to commit, so its
    relist sees accurate, post-commit data.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        owner = await SQLAccountRepository(seed_session).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed_session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(seed_session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        job = await SQLJobRepository(seed_session).create(
            Job(owner_id=owner.id, kind=JobKind.SESSION_RUN, status=JobStatus.RUNNING)
        )

        task_ids = []
        for number in range(1, 3):
            task = await SQLTaskRepository(seed_session).create(
                AgentTask(
                    job_id=job.id,
                    agent_version_id=agent_version.id,
                    status=TaskStatus.RUNNING,
                    attempt=1,
                )
            )
            await SQLSessionRepository(seed_session, engine).create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=number,
                    origin=SessionOrigin.REPLAY,
                    task_id=task.id,
                )
            )
            task_ids.append(task.id)
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        session_a = session_factory()
        session_b = session_factory()
        try:
            # Complete and advance the first task, but do not commit yet:
            # with the fix this holds the job row's FOR UPDATE lock open,
            # exactly mirroring the first of two task completions still being
            # mid-transaction when the second one lands.
            await _complete_and_advance(session_a, task_ids[0])

            task_b = asyncio.create_task(_complete_and_advance(session_b, task_ids[1]))
            await asyncio.sleep(0.2)
            assert not task_b.done(), (
                "second completion did not block on the job row lock, "
                "settlement is not serialized"
            )

            await session_a.commit()
            await asyncio.wait_for(task_b, timeout=5.0)
            await session_b.commit()
        finally:
            await session_a.close()
            await session_b.close()

        async with session_factory() as verify_session:
            settled = await SQLJobRepository(verify_session).get(job.id)
            assert settled.status == JobStatus.COMPLETED
