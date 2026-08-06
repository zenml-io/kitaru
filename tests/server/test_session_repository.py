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
    FakeSessionRepository,
    FakeTagRepository,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.filter import FilterOp
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
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.cohort_version_repository import (
    CohortVersionRepository,
)
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.base import QueryTimeoutError, ValidationError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.evaluation import Evaluation
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
    """Provide each session repository implementation, an owner id, an agent
    id to attach sessions to, a tag repository, and an evaluation repository,
    the last two sharing the session repository's backend."""
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
    """Provide a session repository wired to cohort and cohort version
    repositories sharing its backend, an owner id, and an agent id."""
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


async def test_create_sets_timestamps_and_defaults(setup: Setup) -> None:
    """Store a new session with both timestamps and default rollups."""
    repository, owner_id, agent_id, _, _ = setup
    session = await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            system_prompt="Follow the policy.",
        )
    )
    assert session.owner_id == owner_id
    assert session.agent_id == agent_id
    assert session.status == SessionStatus.IN_PROGRESS
    assert session.system_prompt == "Follow the policy."
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
        )
    )
    assert next_cursor is None
    assert [s.id for s in sessions] == [recorded.id]

    sessions, next_cursor = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value="completed"
            )
        )
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
        )
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
        )
    )
    assert [s.id for s in sessions] == [late.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="started_at",
                op=FilterOp.LE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        )
    )
    assert [s.id for s in sessions] == [early.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="ended_at",
                op=FilterOp.GE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        )
    )
    assert [s.id for s in sessions] == [late.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="ended_at",
                op=FilterOp.LE,
                value=datetime(2026, 3, 1, tzinfo=UTC),
            )
        )
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
        )
    )
    assert [s.id for s in sessions] == [pricey.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="cost", op=FilterOp.LE, value=Decimal("5.00")
            )
        )
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
        )
    )
    assert [s.id for s in sessions] == [scored.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(
                field="has_evaluation", op=FilterOp.EQ, value=False
            )
        )
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
            SessionFilter(cursor=cursor, size=2)
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
        status=SessionStatus.COMPLETED, outputs={"a": 1}, error=None, ended_at=None
    )
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.status == SessionStatus.COMPLETED
    assert updated.outputs == {"a": 1}
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
    """Reject an update that collides with another session's imported_from and
    external id."""
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
        )
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
        )
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
        )
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
        SessionFilter(expression=FilterCondition(field="name", op=FilterOp.IS_NULL))
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
        )
    )
    assert [s.id for s in matched] == [member.id]


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
        )
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
        )
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
        )
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
        )
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
        )
    )
    assert {s.id for s in sessions} == {completed.id, failed.id}


async def test_query_filters_by_string_ops_on_name(setup: Setup) -> None:
    """Match name with startswith, endswith, and contains, autoescaping SQL
    wildcards in the contains value."""
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
        )
    )
    assert [s.id for s in sessions] == [web_run.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="name", op=FilterOp.ENDSWITH, value="run")
        )
    )
    assert [s.id for s in sessions] == [web_run.id]

    sessions, _ = await repository.query(
        SessionFilter(
            expression=FilterCondition(field="name", op=FilterOp.CONTAINS, value="0%_d")
        )
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
            )
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
        )
    )
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(SessionFilter(cursor=next_cursor, size=1))


async def test_query_applies_list_query_timeout() -> None:
    """Set the statement timeout for the transaction when the session carries one."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        session.info[LIST_QUERY_TIMEOUT_INFO_KEY] = 3
        repository = SQLSessionRepository(session, engine)
        await repository.query(SessionFilter())
        timeout = (await session.execute(text("SHOW statement_timeout"))).scalar_one()
        assert timeout == "3s"


async def test_query_without_list_query_timeout() -> None:
    """Leave the statement timeout untouched when the session carries none."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        repository = SQLSessionRepository(session, engine)
        await repository.query(SessionFilter())
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
    """Bump session numbers per agent, and raise for an unknown agent."""
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
