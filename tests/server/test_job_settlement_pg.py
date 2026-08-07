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


def _build_transitions(session: AsyncSession) -> TaskTransitions:
    """Build task transitions on the SQL repositories of one session."""
    return TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=EventDispatcher(),
    )


async def _complete_task(session: AsyncSession, task_id: uuid.UUID) -> None:
    """Complete one task, enqueuing its job's settlement check."""
    task_repository = SQLTaskRepository(session)
    task = await task_repository.get(task_id)
    await _build_transitions(session).apply_status(
        task, partial(Task.complete, result=None, now=datetime.now(UTC))
    )


async def _settle_once(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """Run one settlement pass in its own transaction and return jobs advanced."""
    async with session_factory() as session:
        advanced = await _build_transitions(session).settle_queued_jobs(100)
        await session.commit()
        return advanced


async def test_concurrent_task_completions_settle_the_job_exactly_once() -> None:
    """Two tasks of one job completing concurrently still settle it exactly once.

    Regression test for a race in job settlement: advancing the job used to
    run inside the completing transaction, so two tasks completing
    concurrently contended for the job row's lock. Completion now only
    enqueues a settlement check and never touches the job row, so the second
    completion below must not block on the first one being left open. A
    settlement pass afterward claims both checks, deduped to the one job, and
    settles it exactly once.
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
            result_session = await SQLSessionRepository(seed_session, engine).create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=number,
                    origin=SessionOrigin.REPLAY,
                )
            )
            task = await SQLTaskRepository(seed_session).create(
                AgentTask(
                    job_id=job.id,
                    agent_version_id=agent_version.id,
                    status=TaskStatus.RUNNING,
                    attempt=1,
                    result_session_id=result_session.id,
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
            # Complete the first task but do not commit yet. Completion no
            # longer touches the job row, so the second completion below
            # must not block on this transaction staying open.
            await _complete_task(session_a, task_ids[0])

            task_b = asyncio.create_task(_complete_task(session_b, task_ids[1]))
            await asyncio.sleep(0.2)
            assert task_b.done(), (
                "second completion blocked on the first one's open "
                "transaction, completion is locking the job row again"
            )
            await task_b

            await session_a.commit()
            await session_b.commit()
        finally:
            await session_a.close()
            await session_b.close()

        assert await _settle_once(session_factory) == 1
        assert await _settle_once(session_factory) == 0

        async with session_factory() as verify_session:
            settled = await SQLJobRepository(verify_session).get(job.id)
            assert settled.status == JobStatus.COMPLETED
