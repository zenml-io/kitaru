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
"""Contract tests for session repositories."""

import itertools
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text

from conftest import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeEvaluationRepository,
    FakeInvestigationRepository,
    FakeJobRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeTaskRepository,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.pagination import (
    LIST_QUERY_TIMEOUT_INFO_KEY,
    paginate,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
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
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.cohort_version_repository import (
    CohortVersionRepository,
)
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.investigation_repository import (
    InvestigationRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.base import QueryTimeoutError, ValidationError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.investigation import Investigation, InvestigationSession
from kitaru.server.domain.job import Job
from kitaru.server.domain.payload import Payload
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionInUse,
    SessionNotFound,
    SessionRollups,
)
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    NotExpression,
    OrExpression,
)

Setup = tuple[
    SessionRepository, uuid.UUID, uuid.UUID, TagRepository, EvaluationRepository
]
CohortSetup = tuple[
    SessionRepository, CohortRepository, CohortVersionRepository, uuid.UUID, uuid.UUID
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each session repository implementation and its collaborators.

    Yields the repository, an owner id, an agent id to attach sessions to, a
    tag repository, and an evaluation repository, the last two sharing the
    session repository's backend.
    """
    if request.param == "fake":
        tags = FakeTagRepository()
        evaluations = FakeEvaluationRepository()
        yield (
            FakeSessionRepository(tags=tags, evaluations=evaluations),
            uuid.uuid4(),
            uuid.uuid4(),
            tags,
            evaluations,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        yield (
            SQLSessionRepository(session, engine),
            owner.id,
            agent.id,
            SQLTagRepository(session),
            SQLEvaluationRepository(session),
        )


@pytest.fixture(params=["fake", "postgres"])
async def cohort_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[CohortSetup, None]:
    """Provide a cohort-aware session repository and its collaborators.

    Yields a session repository wired to cohort and cohort version
    repositories sharing its backend, an owner id, and an agent id.
    """
    if request.param == "fake":
        sessions = FakeSessionRepository()
        cohorts = FakeCohortRepository()
        cohort_versions = FakeCohortVersionRepository(
            cohorts=cohorts, sessions=sessions
        )
        yield sessions, cohorts, cohort_versions, uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        yield (
            SQLSessionRepository(session, engine),
            SQLCohortRepository(session),
            SQLCohortVersionRepository(session),
            owner.id,
            agent.id,
        )


InvestigationSetup = tuple[
    SessionRepository, InvestigationRepository, uuid.UUID, uuid.UUID
]


@pytest.fixture(params=["fake", "postgres"])
async def investigation_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[InvestigationSetup, None]:
    """Provide a session repository and its investigation collaborators.

    Yields the repository wired to an investigation repository sharing its
    backend, an owner id, and an agent id.
    """
    if request.param == "fake":
        sessions = FakeSessionRepository()
        investigations = FakeInvestigationRepository(session_repository=sessions)
        yield sessions, investigations, uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        yield (
            SQLSessionRepository(session, engine),
            SQLInvestigationRepository(session),
            owner.id,
            agent.id,
        )


ReplaySetup = tuple[
    SessionRepository, ReplayRepository, JobRepository, uuid.UUID, uuid.UUID, uuid.UUID
]


@pytest.fixture(params=["fake", "postgres"])
async def replay_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[ReplaySetup, None]:
    """Provide a session repository and its replay collaborators.

    Yields the repository wired to replay and job repositories sharing its
    backend, an owner id, an agent id, and a replay config id.
    """
    if request.param == "fake":
        sessions = FakeSessionRepository()
        jobs = FakeJobRepository()
        replays = FakeReplayRepository(sessions=sessions)
        yield sessions, replays, jobs, uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        experiments = SQLExperimentRepository(session)
        config = await experiments.create_replay_config(
            ReplayConfig(
                owner_id=owner.id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
        )
        yield (
            SQLSessionRepository(session, engine),
            SQLReplayRepository(session),
            SQLJobRepository(session),
            owner.id,
            agent.id,
            config.id,
        )


TaskSetup = tuple[
    SessionRepository, TaskRepository, JobRepository, uuid.UUID, uuid.UUID, uuid.UUID
]


@pytest.fixture(params=["fake", "postgres"])
async def task_setup(request: pytest.FixtureRequest) -> AsyncGenerator[TaskSetup, None]:
    """Provide a session repository and its task collaborators.

    Yields the repository wired to a task repository sharing its backend, a
    job repository, an owner id, an agent id, and an agent version id.
    """
    if request.param == "fake":
        sessions = FakeSessionRepository()
        tasks = FakeTaskRepository(sessions=sessions)
        jobs = FakeJobRepository(tasks=tasks)
        yield sessions, tasks, jobs, uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        agent_version = await SQLAgentVersionRepository(session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        yield (
            SQLSessionRepository(session, engine),
            SQLTaskRepository(session),
            SQLJobRepository(session),
            owner.id,
            agent.id,
            agent_version.id,
        )


async def test_create_sets_timestamps_and_defaults(setup: Setup) -> None:
    """Store a new session with both timestamps and default rollups."""
    repository, owner_id, agent_id, _, _ = setup
    session = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    assert session.owner_id == owner_id
    assert session.agent_id == agent_id
    assert session.status == SessionStatus.IN_PROGRESS
    assert session.cost is None
    assert session.tokens is None
    assert session.llm_call_count == 0
    assert session.tool_call_count == 0
    assert session.created is not None
    assert session.updated is not None


async def test_create_duplicate_imported_from_external_id(setup: Setup) -> None:
    """Reject a second session with the same imported_from and external id."""
    repository, owner_id, agent_id, _, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
            external_id="run-1",
        )
    )
    with pytest.raises(DuplicateSessionExternalId):
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=2,
                origin=SessionOrigin.IMPORTED,
                imported_from="langsmith",
                external_id="run-1",
            )
        )


async def test_create_allows_null_imported_from_and_external_id_repeatedly(
    setup: Setup,
) -> None:
    """Allow many sessions with no imported_from and external id."""
    repository, owner_id, agent_id, _, _ = setup
    first = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    second = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    assert first.id != second.id


async def test_get(setup: Setup) -> None:
    """Load a stored session by id."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_exclusive(setup: Setup) -> None:
    """Load a session with an exclusive lock without error."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    loaded = await repository.get(created.id, exclusive=True)
    assert loaded == created


async def test_query_filters_by_origin_and_status(setup: Setup) -> None:
    """Filter sessions by origin and status."""
    repository, owner_id, agent_id, _, _ = setup
    recorded = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
        )
    )

    sessions, next_cursor = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="origin", op=FilterOp.EQ, value="recorded")
        ),
        include_payloads=True,
    )
    assert next_cursor is None
    assert [s.id for s in sessions] == [recorded.id]

    sessions, next_cursor = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value="completed"
            )
        ),
        include_payloads=True,
    )
    assert next_cursor is None
    assert len(sessions) == 1
    assert sessions[0].status == SessionStatus.COMPLETED


async def test_query_filters_by_imported_from_and_external_id(setup: Setup) -> None:
    """Filter sessions by imported_from and external id together."""
    repository, owner_id, agent_id, _, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
            external_id="run-1",
        )
    )
    target = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
            external_id="run-2",
        )
    )

    sessions, next_cursor = await repository.query(
        SessionFilter(
            expression=AndExpression(
                operands=(
                    FilterCondition(
                        field="imported_from", op=FilterOp.EQ, value="langsmith"
                    ),
                    FilterCondition(field="external_id", op=FilterOp.EQ, value="run-2"),
                )
            )
        ),
        include_payloads=True,
    )
    assert next_cursor is None
    assert [s.id for s in sessions] == [target.id]


async def test_query_filters_by_date_bounds(setup: Setup) -> None:
    """Filter sessions by started_at/ended_at ordered comparisons."""
    repository, owner_id, agent_id, _, _ = setup
    early = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
            ended_at=datetime(2026, 1, 2, tzinfo=UTC),
        )
    )
    late = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
            started_at=datetime(2026, 6, 1, tzinfo=UTC),
            ended_at=datetime(2026, 6, 2, tzinfo=UTC),
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="started_at",
                op=FilterOp.GE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [late.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="started_at",
                op=FilterOp.LE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [early.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="ended_at",
                op=FilterOp.GE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [late.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="ended_at",
                op=FilterOp.LE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [early.id]


async def test_query_filters_by_cost_bounds(setup: Setup) -> None:
    """Filter sessions by cost ordered comparisons."""
    repository, owner_id, agent_id, _, _ = setup
    cheap = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    pricey = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.apply_rollups(cheap.id, SessionRollups(cost=Decimal("1.00")))
    await repository.apply_rollups(pricey.id, SessionRollups(cost=Decimal("9.00")))

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="cost", op=FilterOp.GE, value=Decimal("5.00")
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [pricey.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="cost", op=FilterOp.LE, value=Decimal("5.00")
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [cheap.id]


async def test_query_filters_by_has_evaluation(setup: Setup) -> None:
    """Filter sessions by whether they have a stored evaluation."""
    repository, owner_id, agent_id, _, evaluations = setup
    scored = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    unscored = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    await evaluations.merge_session_evaluations(
        scored.id,
        [
            Evaluation(
                owner_id=owner_id,
                session_id=scored.id,
                name="accuracy",
                data_type=EvaluationDataType.FLOAT,
                score=0.9,
            )
        ],
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="has_evaluation", op=FilterOp.EQ, value=True
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [scored.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="has_evaluation", op=FilterOp.EQ, value=False
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [unscored.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _, _ = setup
    created = [
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=i + 1,
                origin=SessionOrigin.RECORDED,
            )
        )
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Session] = []
    cursor = None
    while True:
        sessions, next_cursor = await repository.query(
            SessionFilter(cursor=cursor, size=2), include_payloads=True
        )
        collected.extend(sessions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({s.id for s in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    created.update_name("renamed")
    created.finish(
        status=SessionStatus.COMPLETED,
        outputs=Payload.from_json({"a": 1}),
        error=None,
        ended_at=None,
    )
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.status == SessionStatus.COMPLETED
    assert updated.outputs is not None
    assert updated.outputs.value == {"a": 1}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, owner_id, agent_id, _, _ = setup
    session = Session(
        owner_id=owner_id,
        agent_id=agent_id,
        number=1,
        origin=SessionOrigin.RECORDED,
    )
    with pytest.raises(SessionNotFound, match=f"Session {session.id} was not found"):
        await repository.update(session)


async def test_update_duplicate_external_id(setup: Setup) -> None:
    """Reject an update colliding on another session's imported_from + external id."""
    repository, owner_id, agent_id, _, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
            external_id="run-1",
        )
    )
    other = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
            external_id="run-2",
        )
    )
    other.external_id = "run-1"
    with pytest.raises(DuplicateSessionExternalId):
        await repository.update(other)


async def test_delete(setup: Setup) -> None:
    """Delete a stored session."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.delete(created.id)
    with pytest.raises(SessionNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_in_use_by_cohort_version(cohort_setup: CohortSetup) -> None:
    """Reject deleting a session that belongs to a cohort version."""
    sessions, cohorts, cohort_versions, owner_id, agent_id = cohort_setup
    member = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    cohort = await cohorts.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id)
    )
    await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=1),
        [member.id],
    )

    with pytest.raises(SessionInUse):
        await sessions.delete(member.id)


async def test_delete_restricted_by_investigation_membership(
    investigation_setup: InvestigationSetup,
) -> None:
    """Reject deleting a session linked to an investigation."""
    sessions, investigations, owner_id, agent_id = investigation_setup
    member = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        total_sessions=0,
        completed_sessions=0,
    )
    await investigations.create(
        investigation,
        [
            InvestigationSession(
                investigation_id=investigation.id,
                session_id=member.id,
                position=0,
                questions=[],
            )
        ],
    )

    with pytest.raises(SessionInUse, match=f"Session {member.id} is referenced"):
        await sessions.delete(member.id)


async def test_delete_restricted_by_replay_baseline(replay_setup: ReplaySetup) -> None:
    """Reject deleting a session that is a replay's baseline."""
    sessions, replays, jobs, owner_id, agent_id, replay_config_id = replay_setup
    baseline = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    job = await jobs.create(
        Job(owner_id=owner_id, kind=JobKind.REPLAY, status=JobStatus.PENDING)
    )
    await replays.create(
        Replay(
            owner_id=owner_id,
            job_id=job.id,
            replay_config_id=replay_config_id,
            baseline_session_id=baseline.id,
        )
    )

    with pytest.raises(SessionInUse, match=f"Session {baseline.id} is referenced"):
        await sessions.delete(baseline.id)


async def test_delete_restricted_by_replay_result(replay_setup: ReplaySetup) -> None:
    """Reject deleting a session that is a replay's result."""
    sessions, replays, jobs, owner_id, agent_id, replay_config_id = replay_setup
    baseline = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    result = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    job = await jobs.create(
        Job(owner_id=owner_id, kind=JobKind.REPLAY, status=JobStatus.PENDING)
    )
    await replays.create(
        Replay(
            owner_id=owner_id,
            job_id=job.id,
            replay_config_id=replay_config_id,
            baseline_session_id=baseline.id,
            result_session_id=result.id,
        )
    )

    with pytest.raises(SessionInUse, match=f"Session {result.id} is referenced"):
        await sessions.delete(result.id)


async def test_apply_rollups_accumulates_deltas(setup: Setup) -> None:
    """Add deltas atomically, coalescing null cost and tokens to zero."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.apply_rollups(
        created.id,
        SessionRollups(
            cost=Decimal("1.50"),
            input_tokens=10,
            output_tokens=5,
            llm_call_count=1,
        ),
    )
    await repository.apply_rollups(
        created.id,
        SessionRollups(cost=Decimal("0.50"), tool_call_count=1),
    )
    loaded = await repository.get(created.id)
    assert loaded.cost == Decimal("2.00")
    assert loaded.tokens is not None
    assert loaded.tokens.input_tokens == 10
    assert loaded.tokens.output_tokens == 5
    assert loaded.llm_call_count == 1
    assert loaded.tool_call_count == 1


async def test_apply_rollups_not_found(setup: Setup) -> None:
    """Raise for an unknown session id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound):
        await repository.apply_rollups(missing_id, SessionRollups())


async def test_query_filters_by_tag(setup: Setup) -> None:
    """Filter sessions linked to a tag through tag_link."""
    repository, owner_id, agent_id, tags, _ = setup
    tagged = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )

    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=tagged.id,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test")
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [tagged.id]


async def test_query_filters_by_tag_in_unions_names(setup: Setup) -> None:
    """Union sessions matching either of two tag names with an in condition."""
    repository, owner_id, agent_id, tags, _ = setup
    smoke = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    regression = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=3,
            origin=SessionOrigin.RECORDED,
        )
    )

    smoke_tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    regression_tag = await tags.create(Tag(owner_id=owner_id, name="regression"))
    await tags.create_link(
        TagLink(
            tag_id=smoke_tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=smoke.id,
        )
    )
    await tags.create_link(
        TagLink(
            tag_id=regression_tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=regression.id,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="tag", op=FilterOp.IN, value=["smoke-test", "regression"]
            )
        ),
        include_payloads=True,
    )
    assert {s.id for s in sessions} == {smoke.id, regression.id}


async def test_query_filters_by_not_tag_returns_untagged(setup: Setup) -> None:
    """Return untagged sessions when negating a tag eq condition."""
    repository, owner_id, agent_id, tags, _ = setup
    tagged = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    untagged = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )

    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=tagged.id,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=NotExpression(
                operand=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test")
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [untagged.id]


async def test_query_filters_by_is_null_on_name(setup: Setup) -> None:
    """Match only null-named sessions with is_null."""
    repository, owner_id, agent_id, _, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            name="run-1",
        )
    )
    unnamed = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(expression=FilterCondition(field="name", op=FilterOp.IS_NULL)),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [unnamed.id]


async def test_query_filters_by_cohort_version(cohort_setup: CohortSetup) -> None:
    """Filter sessions that are members of a cohort version."""
    sessions, cohorts, cohort_versions, owner_id, agent_id = cohort_setup
    member = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )
    await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    cohort = await cohorts.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=agent_id)
    )
    version = await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=1),
        [member.id],
    )

    matched, _ = await sessions.query(
        SessionFilter(
            expression=FilterCondition(
                field="cohort_version_id", op=FilterOp.EQ, value=version.id
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in matched] == [member.id]


async def test_query_filters_by_experiment_run() -> None:
    """Filter sessions produced as the results of a run's replays.

    Postgres-only: the run is reached through the replay row that links its
    result session directly.

    Matching through the replay's result session is what keeps a run's
    baseline sessions out of the result set, so that is asserted here.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        agent_version = await SQLAgentVersionRepository(session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        experiments = SQLExperimentRepository(session)
        config = await experiments.create_replay_config(
            ReplayConfig(
                owner_id=owner.id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
        )
        experiment = await experiments.create(
            Experiment(
                owner_id=owner.id,
                name="experiment",
                agent_id=agent.id,
                replay_config_id=config.id,
            )
        )
        cohort = await SQLCohortRepository(session).create(
            Cohort(owner_id=owner.id, name="cohort", agent_id=agent.id)
        )
        cohort_version = await SQLCohortVersionRepository(session).create(
            CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
            [],
        )
        run = await SQLExperimentRunRepository(session).create(
            ExperimentRun(
                owner_id=owner.id,
                experiment_id=experiment.id,
                number=1,
                cohort_version_id=cohort_version.id,
                agent_version_id=agent_version.id,
            )
        )

        repository = SQLSessionRepository(session, engine)
        jobs = SQLJobRepository(session)
        replays = SQLReplayRepository(session)

        # Sessions are unique per agent and number, and every session here
        # belongs to the same agent.
        session_numbers = itertools.count(1)

        async def create_session() -> Session:
            """Create a session for the shared agent.

            Returns:
                Stored session.
            """
            return await repository.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=next(session_numbers),
                    origin=SessionOrigin.RECORDED,
                )
            )

        async def replay_in_run(
            experiment_run_id: uuid.UUID | None,
        ) -> tuple[Session, Session]:
            """Replay one baseline session into a result session.

            Returns:
                Baseline and result session.
            """
            job = await jobs.create(
                Job(owner_id=owner.id, kind=JobKind.REPLAY, status=JobStatus.PENDING)
            )
            baseline = await create_session()
            result = await create_session()
            await replays.create(
                Replay(
                    owner_id=owner.id,
                    job_id=job.id,
                    experiment_run_id=experiment_run_id,
                    replay_config_id=config.id,
                    baseline_session_id=baseline.id,
                    result_session_id=result.id,
                )
            )
            return baseline, result

        baseline, result = await replay_in_run(run.id)
        loose_baseline, loose_result = await replay_in_run(None)

        sessions, _ = await repository.query(
            SessionFilter(
                expression=FilterCondition(
                    field="experiment_run_id", op=FilterOp.EQ, value=run.id
                )
            ),
            include_payloads=True,
        )
        assert [s.id for s in sessions] == [result.id]

        sessions, _ = await repository.query(
            SessionFilter(
                expression=FilterCondition(
                    field="experiment_run_id", op=FilterOp.EQ, value=uuid.uuid4()
                )
            ),
            include_payloads=True,
        )
        assert sessions == []

        sessions, _ = await repository.query(
            SessionFilter(
                expression=NotExpression(
                    operand=FilterCondition(
                        field="experiment_run_id", op=FilterOp.EQ, value=run.id
                    )
                )
            ),
            include_payloads=True,
        )
        assert {s.id for s in sessions} == {
            baseline.id,
            loose_baseline.id,
            loose_result.id,
        }


async def test_query_filters_by_and_expression(setup: Setup) -> None:
    """Narrow results by an and expression combining two conditions."""
    repository, owner_id, agent_id, _, _ = setup
    match = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=3,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=AndExpression(
                operands=(
                    FilterCondition(field="origin", op=FilterOp.EQ, value="recorded"),
                    FilterCondition(field="status", op=FilterOp.EQ, value="completed"),
                )
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [match.id]


async def test_query_filters_by_or_expression(setup: Setup) -> None:
    """Union results by an or expression combining two different fields."""
    repository, owner_id, agent_id, _, _ = setup
    by_status = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    by_imported_from = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.IMPORTED,
            imported_from="langsmith",
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=3,
            origin=SessionOrigin.RECORDED,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=OrExpression(
                operands=(
                    FilterCondition(field="status", op=FilterOp.EQ, value="completed"),
                    FilterCondition(
                        field="imported_from", op=FilterOp.EQ, value="langsmith"
                    ),
                )
            )
        ),
        include_payloads=True,
    )
    assert {s.id for s in sessions} == {by_status.id, by_imported_from.id}


async def test_query_filters_by_not_is_null_expression(setup: Setup) -> None:
    """Invert an is_null condition to return only rows with the field set."""
    repository, owner_id, agent_id, _, _ = setup
    named = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            name="run-1",
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=NotExpression(
                operand=FilterCondition(field="name", op=FilterOp.IS_NULL)
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [named.id]


async def test_query_filters_by_ne_excludes_null(setup: Setup) -> None:
    """Exclude null rows from a ne condition, proving three-valued semantics."""
    repository, owner_id, agent_id, _, _ = setup
    other_name = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            name="run-1",
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
            name="run-2",
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=3,
            origin=SessionOrigin.RECORDED,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="name", op=FilterOp.NE, value="run-2")
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [other_name.id]


async def test_query_filters_by_in_expression(setup: Setup) -> None:
    """Match multiple values with an in expression over an enum field."""
    repository, owner_id, agent_id, _, _ = setup
    completed = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    failed = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.FAILED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=3,
            origin=SessionOrigin.RECORDED,
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.IN, value=["completed", "failed"]
            )
        ),
        include_payloads=True,
    )
    assert {s.id for s in sessions} == {completed.id, failed.id}


async def test_query_filters_by_string_ops_on_name(setup: Setup) -> None:
    """Match name with startswith, endswith, and contains.

    SQL wildcards in the contains value are autoescaped.
    """
    repository, owner_id, agent_id, _, _ = setup
    web_run = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            name="web-agent-run",
        )
    )
    percent_run = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
            name="batch-0%_day",
        )
    )
    # Would spuriously match "0%_d" as an unescaped LIKE pattern (any char,
    # then "0", then any chars, then any single char, then "d") but must not
    # match it as a literal substring.
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=3,
            origin=SessionOrigin.RECORDED,
            name="foo0Xday",
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=4,
            origin=SessionOrigin.RECORDED,
            name="other",
        )
    )

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="name", op=FilterOp.STARTSWITH, value="web"
            )
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [web_run.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="name", op=FilterOp.ENDSWITH, value="run")
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [web_run.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="name", op=FilterOp.CONTAINS, value="0%_d")
        ),
        include_payloads=True,
    )
    assert [s.id for s in sessions] == [percent_run.id]


async def test_query_where_filter_persists_across_cursor(setup: Setup) -> None:
    """Keep a filter expression applied across every page of a cursor walk."""
    repository, owner_id, agent_id, _, _ = setup
    matching = [
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=i + 1,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
            )
        )
        for i in range(3)
    ]
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=4,
            origin=SessionOrigin.RECORDED,
        )
    )
    expected_order = list(reversed(matching))

    collected: list[Session] = []
    cursor = None
    while True:
        sessions, next_cursor = await repository.query(
            SessionFilter(
                expression=FilterCondition(
                    field="status", op=FilterOp.EQ, value="completed"
                ),
                cursor=cursor,
                size=1,
            ),
            include_payloads=True,
        )
        collected.extend(sessions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order


async def test_query_cursor_expression_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed after the filter expression changes."""
    repository, owner_id, agent_id, _, _ = setup
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=2,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    _, next_cursor = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value="completed"
            ),
            size=1,
        ),
        include_payloads=True,
    )
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(
            SessionFilter(cursor=next_cursor, size=1), include_payloads=True
        )


async def test_query_applies_list_query_timeout() -> None:
    """Set the statement timeout for the transaction when the session carries one."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        session.info[LIST_QUERY_TIMEOUT_INFO_KEY] = 3
        repository = SQLSessionRepository(session, engine)
        await repository.query(SessionFilter(), include_payloads=True)
        timeout = (await session.execute(text("SHOW statement_timeout"))).scalar_one()
        assert timeout == "3s"


async def test_query_without_list_query_timeout() -> None:
    """Leave the statement timeout untouched when the session carries none."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        repository = SQLSessionRepository(session, engine)
        await repository.query(SessionFilter(), include_payloads=True)
        timeout = (await session.execute(text("SHOW statement_timeout"))).scalar_one()
        assert timeout == "0"


async def test_query_translates_statement_timeout() -> None:
    """Raise the timeout domain error when the statement is canceled."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        session.info[LIST_QUERY_TIMEOUT_INFO_KEY] = 1
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        repository = SQLSessionRepository(session, engine)
        await repository.create(
            Session(
                owner_id=owner.id,
                agent_id=agent.id,
                number=1,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
            )
        )
        statement = select(SessionORM).where(func.pg_sleep(1.5).is_(None))
        with pytest.raises(QueryTimeoutError):
            await paginate(session, statement, SessionFilter(), id_column=SessionORM.id)


async def test_allocate_session_number_fake() -> None:
    """Bump session numbers per agent, restarting at 1 for each new agent."""
    repository = FakeSessionRepository()
    agent_id = uuid.uuid4()
    assert await repository.allocate_session_number(agent_id) == 1
    assert await repository.allocate_session_number(agent_id) == 2
    assert await repository.allocate_session_number(agent_id) == 3

    other_agent_id = uuid.uuid4()
    assert await repository.allocate_session_number(other_agent_id) == 1


async def test_allocate_session_number_pg() -> None:
    """Bump session numbers per agent, and raise for an unknown or deleted agent."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        other_agent = await agents.create(Agent(owner_id=owner.id, name="reviewer"))
        await session.commit()

        repository = SQLSessionRepository(session, engine)
        assert await repository.allocate_session_number(agent.id) == 1
        assert await repository.allocate_session_number(agent.id) == 2
        assert await repository.allocate_session_number(agent.id) == 3
        assert await repository.allocate_session_number(other_agent.id) == 1

        missing_agent_id = uuid.uuid4()
        with pytest.raises(AgentNotFound):
            await repository.allocate_session_number(missing_agent_id)

        await agents.mark_deleted(other_agent.id)
        await session.commit()
        with pytest.raises(AgentNotFound):
            await repository.allocate_session_number(other_agent.id)
