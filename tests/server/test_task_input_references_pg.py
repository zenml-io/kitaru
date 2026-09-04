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
"""A task names its input rows by id, without a foreign key to them."""

import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin
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
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.job import Job
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import (
    AgentTask,
    AnalysisTask,
    EvaluationTask,
    ImportTask,
)


@dataclass
class Setup:
    """Repositories on one PostgreSQL session plus a stored parent tree."""

    session: AsyncSession
    engine: AsyncEngine
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID
    session_id: uuid.UUID
    job_id: uuid.UUID


@pytest.fixture
async def setup() -> AsyncGenerator[Setup, None]:
    """Provide the SQL repositories with every row a task can take as input."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        version = await SQLAgentVersionRepository(session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id, version=0)
        )
        stored = await SQLSessionRepository(session, engine).create(
            Session(
                owner_id=owner.id,
                agent_id=agent.id,
                number=1,
                origin=SessionOrigin.RECORDED,
            )
        )
        job = await SQLJobRepository(session).create(
            Job(owner_id=owner.id, kind=JobKind.REPLAY, status=JobStatus.PENDING)
        )
        yield Setup(
            session=session,
            engine=engine,
            owner_id=owner.id,
            agent_id=agent.id,
            agent_version_id=version.id,
            session_id=stored.id,
            job_id=job.id,
        )


async def test_task_names_a_missing_agent_version(setup: Setup) -> None:
    """Store an agent task whose agent version is already gone."""
    stored = await SQLTaskRepository(setup.session).create(
        AgentTask(job_id=setup.job_id, agent_version_id=uuid.uuid4(), inputs={})
    )
    assert isinstance(stored, AgentTask)
    assert stored.agent_version_id is not None


async def test_task_names_a_missing_input_session(setup: Setup) -> None:
    """Store an evaluation task whose input session is already gone."""
    stored = await SQLTaskRepository(setup.session).create(
        EvaluationTask(
            job_id=setup.job_id,
            plugin_version_id=uuid.uuid4(),
            input_session_id=uuid.uuid4(),
        )
    )
    assert isinstance(stored, EvaluationTask)
    assert stored.input_session_id is not None


async def test_task_names_a_missing_import(setup: Setup) -> None:
    """Store an import task whose import row is already gone."""
    stored = await SQLTaskRepository(setup.session).create(
        ImportTask(job_id=setup.job_id, import_id=uuid.uuid4())
    )
    assert isinstance(stored, ImportTask)
    assert stored.import_id is not None


async def test_analysis_task_names_missing_input_sessions(setup: Setup) -> None:
    """Store an analysis task whose input sessions are already gone."""
    stored = await SQLTaskRepository(setup.session).create(
        AnalysisTask(
            job_id=setup.job_id,
            plugin_version_id=uuid.uuid4(),
            agent_id=setup.agent_id,
            input_session_ids=[uuid.uuid4(), uuid.uuid4()],
        )
    )
    assert isinstance(stored, AnalysisTask)
    assert len(stored.input_session_ids) == 2


async def test_analysis_task_still_requires_its_agent(setup: Setup) -> None:
    """Reject an analysis task naming an agent that does not exist.

    Unlike the other input references, an analysis task's agent is a real
    foreign key.
    """
    with pytest.raises(IntegrityError):
        await SQLTaskRepository(setup.session).create(
            AnalysisTask(
                job_id=setup.job_id,
                plugin_version_id=uuid.uuid4(),
                agent_id=uuid.uuid4(),
                input_session_ids=[uuid.uuid4()],
            )
        )


async def test_create_many_names_missing_inputs(setup: Setup) -> None:
    """Store a batch of tasks whose inputs are already gone."""
    stored = await SQLTaskRepository(setup.session).create_many(
        [
            AgentTask(job_id=setup.job_id, agent_version_id=uuid.uuid4(), inputs={}),
            AgentTask(job_id=setup.job_id, agent_version_id=uuid.uuid4(), inputs={}),
        ]
    )
    assert len(stored) == 2


async def test_task_still_requires_its_job(setup: Setup) -> None:
    """Reject a task naming a job that does not exist."""
    # The job reference stays constrained, since a task belongs to its job
    # rather than merely pointing at it.
    with pytest.raises(IntegrityError):
        await SQLTaskRepository(setup.session).create(
            AgentTask(
                job_id=uuid.uuid4(),
                agent_version_id=setup.agent_version_id,
                inputs={},
            )
        )


async def test_deleting_an_input_leaves_the_task_in_place(setup: Setup) -> None:
    """Keep a stored task when the session it takes as input is deleted."""
    tasks = SQLTaskRepository(setup.session)
    stored = await tasks.create(
        EvaluationTask(
            job_id=setup.job_id,
            plugin_version_id=uuid.uuid4(),
            input_session_id=setup.session_id,
        )
    )
    await SQLSessionRepository(setup.session, setup.engine).delete(setup.session_id)
    await setup.session.flush()

    reloaded = await tasks.get(stored.id)
    assert reloaded.id == stored.id
