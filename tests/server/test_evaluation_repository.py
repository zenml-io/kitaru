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
"""Contract tests for evaluation repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from conftest import (
    FakeEvaluationRepository,
    FakePluginRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType, EvaluationResult
from kitaru.server.adapters.db.orm.evaluation import EvaluationORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    SQLEvaluationRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.evaluation import Evaluation, EvaluationNotFound
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginInUse,
    PluginKind,
)
from kitaru.server.domain.session import Session

Setup = tuple[EvaluationRepository, uuid.UUID, uuid.UUID]
PluginSetup = tuple[PluginRepository, EvaluationRepository, uuid.UUID, uuid.UUID]

SOURCE = PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score")


def _evaluation(
    owner_id: uuid.UUID,
    session_id: uuid.UUID,
    name: str,
    score: float | bool | None = None,
    value: str | None = None,
    explanation: str | None = None,
    evaluator_version_id: uuid.UUID | None = None,
    passed: bool | None = None,
) -> Evaluation:
    """Build an unstored evaluation, deriving data_type via EvaluationResult."""
    result = EvaluationResult(
        name=name, score=score, value=value, explanation=explanation, passed=passed
    )
    return Evaluation(
        owner_id=owner_id,
        evaluator_version_id=evaluator_version_id,
        session_id=session_id,
        name=result.name,
        data_type=result.data_type,
        score=result.score,
        value=result.value,
        explanation=result.explanation,
        passed=result.passed,
    )


async def _create_owner_and_agent(session: AsyncSession) -> tuple[uuid.UUID, uuid.UUID]:
    """Create an account and an agent for a postgres-backed fixture.

    Returns:
        Owner id and agent id.
    """
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    agent = await SQLAgentRepository(session).create(
        Agent(owner_id=owner.id, name="assistant")
    )
    return owner.id, agent.id


async def _create_session_row(
    session: AsyncSession, owner_id: uuid.UUID, agent_id: uuid.UUID
) -> uuid.UUID:
    """Create a session row for a postgres-backed fixture.

    Returns:
        Id of the stored session.
    """
    stored = await SQLSessionRepository(session).create(
        Session(owner_id=owner_id, agent_id=agent_id, origin="recorded")
    )
    return stored.id


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each evaluation repository implementation, an owner id, and a
    session id to attach evaluations to."""
    if request.param == "fake":
        yield FakeEvaluationRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner_id, agent_id = await _create_owner_and_agent(session)
        session_id = await _create_session_row(session, owner_id, agent_id)
        yield SQLEvaluationRepository(session), owner_id, session_id


async def test_merge_inserts_new_evaluations(setup: Setup) -> None:
    """Insert evaluations that do not already exist."""
    repository, owner_id, session_id = setup
    evaluations = [
        _evaluation(owner_id, session_id, "accuracy", score=0.9),
        _evaluation(owner_id, session_id, "passed", score=True),
    ]
    stored = await repository.merge_session_evaluations(session_id, evaluations)
    assert [e.name for e in stored] == ["accuracy", "passed"]
    assert stored[0].data_type == EvaluationDataType.FLOAT
    assert stored[0].score == 0.9
    assert stored[1].data_type == EvaluationDataType.BOOL
    assert stored[1].score is True
    assert stored[0].created is not None
    assert stored[0].updated is not None


async def test_merge_overwrites_matching_name_and_keeps_id(setup: Setup) -> None:
    """Resending a name overwrites its score, value, data type, and
    explanation, while keeping the row id, owner, and creation time."""
    repository, owner_id, session_id = setup
    first = await repository.merge_session_evaluations(
        session_id,
        [
            _evaluation(
                owner_id, session_id, "accuracy", score=0.5, explanation="first pass"
            )
        ],
    )
    second = await repository.merge_session_evaluations(
        session_id,
        [
            _evaluation(
                owner_id,
                session_id,
                "accuracy",
                value="high",
                explanation="second pass",
            )
        ],
    )
    assert second[0].id == first[0].id
    assert second[0].owner_id == first[0].owner_id
    assert second[0].created == first[0].created
    assert second[0].data_type == EvaluationDataType.STR
    assert second[0].score is None
    assert second[0].value == "high"
    assert second[0].explanation == "second pass"


async def test_merge_round_trips_passed(setup: Setup) -> None:
    """Round-trip the pass flag through both true and false, defaulting to null."""
    repository, owner_id, session_id = setup
    stored = await repository.merge_session_evaluations(
        session_id,
        [
            _evaluation(owner_id, session_id, "a", score=1.0, passed=True),
            _evaluation(owner_id, session_id, "b", score=1.0, passed=False),
            _evaluation(owner_id, session_id, "c", score=1.0),
        ],
    )
    assert [e.passed for e in stored] == [True, False, None]


async def test_merge_overwrites_passed(setup: Setup) -> None:
    """Resending a name overwrites its pass flag, clearing it when omitted."""
    repository, owner_id, session_id = setup
    await repository.merge_session_evaluations(
        session_id, [_evaluation(owner_id, session_id, "a", score=1.0, passed=True)]
    )
    flipped = await repository.merge_session_evaluations(
        session_id, [_evaluation(owner_id, session_id, "a", score=1.0, passed=False)]
    )
    assert flipped[0].passed is False
    cleared = await repository.merge_session_evaluations(
        session_id, [_evaluation(owner_id, session_id, "a", score=1.0)]
    )
    assert cleared[0].passed is None


async def test_merge_preserves_request_order(setup: Setup) -> None:
    """Return stored evaluations in the same order the batch was sent."""
    repository, owner_id, session_id = setup
    evaluations = [
        _evaluation(owner_id, session_id, f"m{i}", score=float(i))
        for i in reversed(range(5))
    ]
    stored = await repository.merge_session_evaluations(session_id, evaluations)
    assert [e.name for e in stored] == [f"m{i}" for i in reversed(range(5))]


async def test_merge_empty_batch(setup: Setup) -> None:
    """Return an empty list for an empty batch."""
    repository, _, session_id = setup
    stored = await repository.merge_session_evaluations(session_id, [])
    assert stored == []


async def test_categorical_round_trips_both_channels(setup: Setup) -> None:
    """Round-trip a categorical evaluation carrying both score and value."""
    repository, owner_id, session_id = setup
    stored = await repository.merge_session_evaluations(
        session_id,
        [_evaluation(owner_id, session_id, "verdict", score=3.0, value="good")],
    )
    assert stored[0].data_type == EvaluationDataType.CATEGORICAL
    assert stored[0].score == 3.0
    assert stored[0].value == "good"


async def test_get(setup: Setup) -> None:
    """Load a stored evaluation by id."""
    repository, owner_id, session_id = setup
    stored = await repository.merge_session_evaluations(
        session_id, [_evaluation(owner_id, session_id, "a", score=1.0)]
    )
    item = await repository.get(stored[0].id)
    assert item.evaluation == stored[0]
    assert item.evaluator_name is None
    assert item.evaluator_version is None


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown evaluation id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        EvaluationNotFound, match=f"Evaluation {missing_id} was not found"
    ):
        await repository.get(missing_id)


@pytest.fixture(params=["fake", "postgres"])
async def session_pair(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[tuple[EvaluationRepository, uuid.UUID, uuid.UUID, uuid.UUID], None]:
    """Provide an evaluation repository, an owner id, and two distinct
    session ids to score."""
    if request.param == "fake":
        yield FakeEvaluationRepository(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner_id, agent_id = await _create_owner_and_agent(session)
        first_id = await _create_session_row(session, owner_id, agent_id)
        second_id = await _create_session_row(session, owner_id, agent_id)
        yield SQLEvaluationRepository(session), owner_id, first_id, second_id


async def test_query_filters_by_session_and_name(
    session_pair: tuple[EvaluationRepository, uuid.UUID, uuid.UUID, uuid.UUID],
) -> None:
    """Filter evaluations by session_id and name."""
    repository, owner_id, session_id, other_session_id = session_pair
    await repository.merge_session_evaluations(
        session_id, [_evaluation(owner_id, session_id, "a", score=1.0)]
    )
    await repository.merge_session_evaluations(
        other_session_id, [_evaluation(owner_id, other_session_id, "a", score=2.0)]
    )

    items, next_cursor = await repository.query(EvaluationFilter(session_id=session_id))
    assert next_cursor is None
    assert [item.evaluation.session_id for item in items] == [session_id]

    items, next_cursor = await repository.query(EvaluationFilter(name="a"))
    assert next_cursor is None
    assert len(items) == 2


async def test_query_filters_by_data_type(setup: Setup) -> None:
    """Filter evaluations by data_type."""
    repository, owner_id, session_id = setup
    await repository.merge_session_evaluations(
        session_id,
        [
            _evaluation(owner_id, session_id, "a", score=1.0),
            _evaluation(owner_id, session_id, "b", score=True),
        ],
    )
    items, next_cursor = await repository.query(
        EvaluationFilter(data_type=EvaluationDataType.BOOL)
    )
    assert next_cursor is None
    assert [item.evaluation.name for item in items] == ["b"]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, session_id = setup
    evaluations = [
        _evaluation(owner_id, session_id, f"m{i}", score=float(i)) for i in range(5)
    ]
    created = await repository.merge_session_evaluations(session_id, evaluations)
    expected_order = list(reversed(created))

    collected: list[Evaluation] = []
    cursor = None
    while True:
        items, next_cursor = await repository.query(
            EvaluationFilter(session_id=session_id, cursor=cursor, size=2)
        )
        collected.extend(item.evaluation for item in items)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({e.id for e in collected}) == 5


async def test_query_task_id_filter(setup: Setup) -> None:
    """Filter evaluations by task_id, which stays null for manual rows."""
    repository, owner_id, session_id = setup
    await repository.merge_session_evaluations(
        session_id, [_evaluation(owner_id, session_id, "a", score=1.0)]
    )
    items, _ = await repository.query(EvaluationFilter(task_id=uuid.uuid4()))
    assert items == []


@pytest.fixture(params=["fake", "postgres"])
async def plugin_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[PluginSetup, None]:
    """Provide a plugin repository and an evaluation repository sharing one
    backend, an owner id, and a session id."""
    if request.param == "fake":
        plugin_repository = FakePluginRepository()
        evaluation_repository = FakeEvaluationRepository(
            plugin_repository=plugin_repository
        )
        yield plugin_repository, evaluation_repository, uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner_id, agent_id = await _create_owner_and_agent(session)
        session_id = await _create_session_row(session, owner_id, agent_id)
        yield (
            SQLPluginRepository(session),
            SQLEvaluationRepository(session),
            owner_id,
            session_id,
        )


async def test_denormalized_evaluator_name_and_version(
    plugin_setup: PluginSetup,
) -> None:
    """Join the evaluator name and version from the referenced plugin version."""
    plugin_repository, evaluation_repository, owner_id, session_id = plugin_setup
    plugin = await plugin_repository.create(
        Plugin(owner_id=owner_id, kind=PluginKind.EVALUATOR, name="accuracy-scorer")
    )
    version = await plugin_repository.create_version(
        plugin.id, SOURCE, display_version="v1"
    )
    stored = await evaluation_repository.merge_session_evaluations(
        session_id,
        [
            _evaluation(
                owner_id,
                session_id,
                "accuracy",
                score=0.8,
                evaluator_version_id=version.id,
            )
        ],
    )

    item = await evaluation_repository.get(stored[0].id)
    assert item.evaluator_name == "accuracy-scorer"
    assert item.evaluator_version == version.version

    items, _ = await evaluation_repository.query(
        EvaluationFilter(evaluator_version_id=version.id)
    )
    assert items[0].evaluator_name == "accuracy-scorer"
    assert items[0].evaluator_version == version.version


async def test_evaluator_delete_restricted_by_stored_evaluation(
    plugin_setup: PluginSetup,
) -> None:
    """Reject deleting an evaluator with a stored evaluation."""
    plugin_repository, evaluation_repository, owner_id, session_id = plugin_setup
    plugin = await plugin_repository.create(
        Plugin(owner_id=owner_id, kind=PluginKind.EVALUATOR, name="accuracy-scorer")
    )
    version = await plugin_repository.create_version(
        plugin.id, SOURCE, display_version="v1"
    )
    await evaluation_repository.merge_session_evaluations(
        session_id,
        [
            _evaluation(
                owner_id,
                session_id,
                "accuracy",
                score=0.8,
                evaluator_version_id=version.id,
            )
        ],
    )

    with pytest.raises(PluginInUse):
        await plugin_repository.delete(plugin.id)


async def test_check_constraint_rejects_mismatched_columns() -> None:
    """Surface the CHECK constraint violation on a direct bad write."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner_id, agent_id = await _create_owner_and_agent(session)
        session_id = await _create_session_row(session, owner_id, agent_id)
        bad_row = EvaluationORM(
            owner_id=owner_id,
            session_id=session_id,
            name="bad",
            data_type="float",
            numerical_value=None,
            string_value="oops",
        )
        session.add(bad_row)
        with pytest.raises(IntegrityError):
            await session.flush()
