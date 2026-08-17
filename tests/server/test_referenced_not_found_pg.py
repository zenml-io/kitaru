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
"""Foreign key violations on a missing parent translate to not-found errors."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.investigation_repository import (
    SQLInvestigationRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import AgentVersionNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.investigation import Investigation
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import Session, SessionNotFound
from kitaru.server.domain.task import AgentTask

Setup = tuple[AsyncSession, AsyncEngine]


@pytest.fixture
async def setup() -> AsyncGenerator[Setup, None]:
    """Provide a PostgreSQL session and its engine on a fresh database."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        yield session, engine


async def _owner_id(session: AsyncSession) -> uuid.UUID:
    return (await SQLAccountRepository(session).create(Account(name="owner"))).id


async def test_session_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on a session insert."""
    session, engine = setup
    owner_id = await _owner_id(session)
    missing = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing} was not found"):
        await SQLSessionRepository(session, engine).create(
            Session(
                owner_id=owner_id,
                agent_id=missing,
                number=1,
                origin=SessionOrigin.RECORDED,
            )
        )


async def test_session_missing_agent_version(setup: Setup) -> None:
    """Translate the agent version foreign key on a session insert."""
    session, engine = setup
    owner_id = await _owner_id(session)
    agent = await SQLAgentRepository(session).create(
        Agent(owner_id=owner_id, name="assistant")
    )
    missing = uuid.uuid4()
    with pytest.raises(AgentVersionNotFound):
        await SQLSessionRepository(session, engine).create(
            Session(
                owner_id=owner_id,
                agent_id=agent.id,
                agent_version_id=missing,
                number=1,
                origin=SessionOrigin.RECORDED,
            )
        )


async def test_cohort_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on a cohort insert."""
    session, _ = setup
    owner_id = await _owner_id(session)
    with pytest.raises(AgentNotFound):
        await SQLCohortRepository(session).create(
            Cohort(owner_id=owner_id, name="cohort", agent_id=uuid.uuid4())
        )


async def test_experiment_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on an experiment insert."""
    session, _ = setup
    owner_id = await _owner_id(session)
    experiments = SQLExperimentRepository(session)
    config = await experiments.create_replay_config(
        ReplayConfig(
            owner_id=owner_id,
            tool_policy=ToolPolicy(default=PassthroughConfig()),
            evaluators=[],
        )
    )
    with pytest.raises(AgentNotFound):
        await experiments.create(
            Experiment(
                owner_id=owner_id,
                name="experiment",
                agent_id=uuid.uuid4(),
                replay_config_id=config.id,
            )
        )


async def test_experiment_run_missing_agent_version(setup: Setup) -> None:
    """Translate the agent version foreign key on an experiment run insert."""
    session, _ = setup
    owner_id = await _owner_id(session)
    agent = await SQLAgentRepository(session).create(
        Agent(owner_id=owner_id, name="assistant")
    )
    experiments = SQLExperimentRepository(session)
    config = await experiments.create_replay_config(
        ReplayConfig(
            owner_id=owner_id,
            tool_policy=ToolPolicy(default=PassthroughConfig()),
            evaluators=[],
        )
    )
    experiment = await experiments.create(
        Experiment(
            owner_id=owner_id,
            name="experiment",
            agent_id=agent.id,
            replay_config_id=config.id,
        )
    )
    cohort = await SQLCohortRepository(session).create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent.id)
    )
    cohort_version = await SQLCohortVersionRepository(session).create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=0), []
    )
    with pytest.raises(AgentVersionNotFound):
        await SQLExperimentRunRepository(session).create(
            ExperimentRun(
                owner_id=owner_id,
                experiment_id=experiment.id,
                number=1,
                cohort_version_id=cohort_version.id,
                agent_version_id=uuid.uuid4(),
            )
        )


async def test_investigation_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on an investigation insert."""
    session, _ = setup
    owner_id = await _owner_id(session)
    with pytest.raises(AgentNotFound):
        await SQLInvestigationRepository(session).create(
            Investigation(
                owner_id=owner_id,
                agent_id=uuid.uuid4(),
                name="investigation",
                total_sessions=0,
                completed_sessions=0,
            ),
            [],
        )


async def test_replay_missing_baseline_session(setup: Setup) -> None:
    """Translate the baseline session foreign key on replay inserts."""
    session, _ = setup
    owner_id = await _owner_id(session)
    job = await SQLJobRepository(session).create(
        Job(owner_id=owner_id, kind=JobKind.REPLAY)
    )
    config = await SQLExperimentRepository(session).create_replay_config(
        ReplayConfig(
            owner_id=owner_id,
            tool_policy=ToolPolicy(default=PassthroughConfig()),
            evaluators=[],
        )
    )
    missing = uuid.uuid4()
    replay = Replay(
        owner_id=owner_id,
        job_id=job.id,
        replay_config_id=config.id,
        baseline_session_id=missing,
    )
    replays = SQLReplayRepository(session)
    with pytest.raises(SessionNotFound, match=f"Session {missing} was not found"):
        await replays.create(replay)
    with pytest.raises(NotFoundError, match="Baseline session was not found"):
        await replays.create_many([replay])


async def test_task_missing_references(setup: Setup) -> None:
    """Translate the agent version foreign key on task inserts."""
    session, _ = setup
    owner_id = await _owner_id(session)
    job = await SQLJobRepository(session).create(
        Job(owner_id=owner_id, kind=JobKind.SESSION_RUN)
    )
    tasks = SQLTaskRepository(session)
    missing_version = uuid.uuid4()
    with pytest.raises(AgentVersionNotFound):
        await tasks.create(AgentTask(job_id=job.id, agent_version_id=missing_version))
    with pytest.raises(NotFoundError, match="Agent version was not found"):
        await tasks.create_many(
            [AgentTask(job_id=job.id, agent_version_id=missing_version)]
        )
