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
"""Creates whose parent row vanished translate the foreign key into not-found."""

import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.insight import TextInsightData
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.annotation_repository import (
    SQLAnnotationRepository,
)
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
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.investigation_repository import (
    SQLInvestigationRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.session_node_repository import (
    SQLSessionNodeRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import AgentVersion, AgentVersionNotFound
from kitaru.server.domain.annotation import Annotation
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import (
    CohortVersion,
    CohortVersionIdNotFound,
)
from kitaru.server.domain.experiment import Experiment, ExperimentNotFound
from kitaru.server.domain.experiment_run import ExperimentRun, ExperimentRunNotFound
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.investigation import (
    Investigation,
    InvestigationSession,
    InvestigationSessionNotFound,
    InvestigationSessionQuestion,
)
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig, default_tool_policy
from kitaru.server.domain.session import Session, SessionNotFound
from kitaru.server.domain.session_node import SessionNode


@dataclass
class Setup:
    """Repositories on one PostgreSQL session plus a stored parent tree."""

    session: AsyncSession
    engine: AsyncEngine
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID
    session_id: uuid.UUID
    replay_config_id: uuid.UUID
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    job_id: uuid.UUID


@pytest.fixture
async def setup() -> AsyncGenerator[Setup, None]:
    """Provide the SQL repositories with a stored agent, version, and session."""
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
        experiments = SQLExperimentRepository(session)
        config = await experiments.create_replay_config(
            ReplayConfig(
                owner_id=owner.id, tool_policy=default_tool_policy(), evaluators=[]
            )
        )
        experiment = await experiments.create(
            Experiment(
                owner_id=owner.id,
                name="exp",
                agent_id=agent.id,
                replay_config_id=config.id,
            )
        )
        cohort = await SQLCohortRepository(session).create(
            Cohort(owner_id=owner.id, name="cohort", agent_id=agent.id)
        )
        cohort_version = await SQLCohortVersionRepository(session).create(
            CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0), []
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
            replay_config_id=config.id,
            experiment_id=experiment.id,
            cohort_version_id=cohort_version.id,
            job_id=job.id,
        )


async def test_session_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on session create."""
    with pytest.raises(AgentNotFound):
        await SQLSessionRepository(setup.session, setup.engine).create(
            Session(
                owner_id=setup.owner_id,
                agent_id=uuid.uuid4(),
                number=2,
                origin=SessionOrigin.RECORDED,
            )
        )


async def test_session_missing_agent_version(setup: Setup) -> None:
    """Translate the agent version foreign key on session create."""
    with pytest.raises(AgentVersionNotFound):
        await SQLSessionRepository(setup.session, setup.engine).create(
            Session(
                owner_id=setup.owner_id,
                agent_id=setup.agent_id,
                agent_version_id=uuid.uuid4(),
                number=2,
                origin=SessionOrigin.RECORDED,
            )
        )


async def test_session_nodes_missing_session(setup: Setup) -> None:
    """Translate the session foreign key on node upsert."""
    session_id = uuid.uuid4()
    node = SessionNode(
        session_id=session_id,
        index=0,
        node_type=NodeType.LLM_CALL,
        name="call",
        status=NodeStatus.COMPLETED,
    )
    with pytest.raises(SessionNotFound):
        await SQLSessionNodeRepository(setup.session).upsert_batch(session_id, [node])


async def test_experiment_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on experiment create."""
    with pytest.raises(AgentNotFound):
        await SQLExperimentRepository(setup.session).create(
            Experiment(
                owner_id=setup.owner_id,
                name="other",
                agent_id=uuid.uuid4(),
                replay_config_id=setup.replay_config_id,
            )
        )


async def test_cohort_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on cohort create."""
    with pytest.raises(AgentNotFound):
        await SQLCohortRepository(setup.session).create(
            Cohort(owner_id=setup.owner_id, name="other", agent_id=uuid.uuid4())
        )


async def test_cohort_version_missing_session(setup: Setup) -> None:
    """Translate the session foreign key on cohort version create."""
    cohort = await SQLCohortRepository(setup.session).create(
        Cohort(owner_id=setup.owner_id, name="other", agent_id=setup.agent_id)
    )
    missing = uuid.uuid4()
    with pytest.raises(SessionNotFound, match="A referenced session was not found"):
        await SQLCohortVersionRepository(setup.session).create(
            CohortVersion(
                owner_id=setup.owner_id, cohort_id=cohort.id, session_count=2
            ),
            [setup.session_id, missing],
        )


def _investigation(owner_id: uuid.UUID, agent_id: uuid.UUID) -> Investigation:
    return Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        total_sessions=0,
        completed_sessions=0,
    )


def _link(investigation_id: uuid.UUID, session_id: uuid.UUID) -> InvestigationSession:
    return InvestigationSession(
        investigation_id=investigation_id,
        session_id=session_id,
        position=0,
        questions=[InvestigationSessionQuestion(key="cause", question="Why?")],
    )


async def test_investigation_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on investigation create."""
    investigation = _investigation(setup.owner_id, uuid.uuid4())
    with pytest.raises(AgentNotFound):
        await SQLInvestigationRepository(setup.session).create(
            investigation, [_link(investigation.id, setup.session_id)]
        )


async def test_investigation_missing_session(setup: Setup) -> None:
    """Translate the session foreign key on investigation create."""
    investigation = _investigation(setup.owner_id, setup.agent_id)
    missing = uuid.uuid4()
    with pytest.raises(SessionNotFound, match="A referenced session was not found"):
        await SQLInvestigationRepository(setup.session).create(
            investigation, [_link(investigation.id, missing)]
        )


def _run(
    setup: Setup,
    experiment_id: uuid.UUID | None = None,
    cohort_version_id: uuid.UUID | None = None,
    agent_version_id: uuid.UUID | None = None,
) -> ExperimentRun:
    return ExperimentRun(
        owner_id=setup.owner_id,
        experiment_id=experiment_id or setup.experiment_id,
        number=1,
        cohort_version_id=cohort_version_id or setup.cohort_version_id,
        agent_version_id=agent_version_id or setup.agent_version_id,
    )


async def test_experiment_run_missing_experiment(setup: Setup) -> None:
    """Translate the experiment foreign key on run create."""
    with pytest.raises(ExperimentNotFound):
        await SQLExperimentRunRepository(setup.session).create(
            _run(setup, experiment_id=uuid.uuid4())
        )


async def test_experiment_run_missing_cohort_version(setup: Setup) -> None:
    """Translate the cohort version foreign key on run create."""
    with pytest.raises(CohortVersionIdNotFound):
        await SQLExperimentRunRepository(setup.session).create(
            _run(setup, cohort_version_id=uuid.uuid4())
        )


async def test_experiment_run_missing_agent_version(setup: Setup) -> None:
    """Translate the agent version foreign key on run create."""
    with pytest.raises(AgentVersionNotFound):
        await SQLExperimentRunRepository(setup.session).create(
            _run(setup, agent_version_id=uuid.uuid4())
        )


async def test_replay_missing_baseline_session(setup: Setup) -> None:
    """Translate the baseline session foreign key on replay create."""
    with pytest.raises(SessionNotFound):
        await SQLReplayRepository(setup.session).create(
            Replay(
                owner_id=setup.owner_id,
                job_id=setup.job_id,
                replay_config_id=setup.replay_config_id,
                baseline_session_id=uuid.uuid4(),
            )
        )


async def test_replay_create_many_missing_baseline_session(setup: Setup) -> None:
    """Translate the baseline session foreign key on bulk replay create."""
    with pytest.raises(SessionNotFound):
        await SQLReplayRepository(setup.session).create_many(
            [
                Replay(
                    owner_id=setup.owner_id,
                    job_id=setup.job_id,
                    replay_config_id=setup.replay_config_id,
                    baseline_session_id=uuid.uuid4(),
                )
            ]
        )


async def test_replay_missing_experiment_run(setup: Setup) -> None:
    """Translate the experiment run foreign key on replay create."""
    with pytest.raises(ExperimentRunNotFound):
        await SQLReplayRepository(setup.session).create(
            Replay(
                owner_id=setup.owner_id,
                job_id=setup.job_id,
                replay_config_id=setup.replay_config_id,
                baseline_session_id=setup.session_id,
                experiment_run_id=uuid.uuid4(),
            )
        )


async def test_annotation_missing_session(setup: Setup) -> None:
    """Translate the session foreign key on annotation create."""
    with pytest.raises(SessionNotFound):
        await SQLAnnotationRepository(setup.session).create(
            Annotation(
                owner_id=setup.owner_id,
                session_id=uuid.uuid4(),
                question_key="cause",
                value="answer",
            )
        )


async def test_annotation_missing_investigation_session(setup: Setup) -> None:
    """Translate the investigation session foreign key on annotation create."""
    with pytest.raises(InvestigationSessionNotFound):
        await SQLAnnotationRepository(setup.session).create(
            Annotation(
                owner_id=setup.owner_id,
                session_id=setup.session_id,
                investigation_session_id=uuid.uuid4(),
                question_key="cause",
                value="answer",
            )
        )


async def test_insight_missing_agent(setup: Setup) -> None:
    """Translate the agent foreign key on insight batch create."""
    insight = Insight(
        owner_id=setup.owner_id,
        agent_id=uuid.uuid4(),
        title="insight",
        data=TextInsightData(content="root cause"),
    )
    with pytest.raises(AgentNotFound):
        await SQLInsightRepository(setup.session).create_many([insight])
