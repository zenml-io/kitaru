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
"""Contract tests for annotation repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest

from conftest import (
    FakeAnnotationRepository,
    FakeInvestigationRepository,
    FakeSessionRepository,
    create_session,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.annotation import AnnotationSelector, AnnotationSelectorPart
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.investigation import QuestionItem
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.annotation_repository import (
    SQLAnnotationRepository,
)
from kitaru.server.adapters.db.repositories.investigation_repository import (
    SQLInvestigationRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.annotation_repository import (
    AnnotationRepository,
)
from kitaru.server.application.interfaces.investigation_repository import (
    InvestigationRepository,
)
from kitaru.server.application.models.annotation import AnnotationFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.annotation import Annotation, AnnotationNotFound
from kitaru.server.domain.investigation import Investigation, InvestigationSession
from kitaru.server.domain.session import Session
from kitaru.server.filtering import FilterCondition

MakeInvestigationSession = Callable[[uuid.UUID], Awaitable[tuple[uuid.UUID, uuid.UUID]]]

Setup = tuple[
    AnnotationRepository,
    InvestigationRepository,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    MakeInvestigationSession,
]


async def _link_investigation_session(
    investigations: InvestigationRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> tuple[uuid.UUID, uuid.UUID]:
    """Create a one-question, one-session investigation and link the session.

    Args:
        investigations: Investigation repository to store the investigation in.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the session belongs to.
        session_id: Id of the session to link.

    Returns:
        Id of the created investigation and id of its session link.
    """
    investigation = Investigation(
        owner_id=owner_id,
        agent_id=agent_id,
        name="investigation",
        questions=[QuestionItem(key="root_cause", question="root_cause")],
        total_sessions=0,
        completed_sessions=0,
    )
    created = await investigations.create(
        investigation,
        [
            InvestigationSession(
                investigation_id=investigation.id, session_id=session_id, position=0
            )
        ],
    )
    linked = await investigations.get_session_by_session_id(created.id, session_id)
    return created.id, linked.id


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each annotation repository implementation, an investigation
    repository sharing its backend, an owner id, a factory for session ids,
    and a factory linking a session into a fresh one-question investigation."""
    if request.param == "fake":
        investigations = FakeInvestigationRepository()
        annotations = FakeAnnotationRepository(investigations=investigations)
        sessions = FakeSessionRepository()
        owner_id = uuid.uuid4()
        agent_id = uuid.uuid4()

        async def make_session_id() -> uuid.UUID:
            created = await create_session(sessions, owner_id, agent_id=agent_id)
            return created.id

        async def make_investigation_session(
            session_id: uuid.UUID,
        ) -> tuple[uuid.UUID, uuid.UUID]:
            return await _link_investigation_session(
                investigations, owner_id, agent_id, session_id
            )

        yield (
            annotations,
            investigations,
            owner_id,
            make_session_id,
            make_investigation_session,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        sessions_repository = SQLSessionRepository(session)
        investigations = SQLInvestigationRepository(session)
        annotations = SQLAnnotationRepository(session)

        async def make_session_id() -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id, agent_id=agent.id, origin=SessionOrigin.RECORDED
                )
            )
            return created.id

        async def make_investigation_session(
            session_id: uuid.UUID,
        ) -> tuple[uuid.UUID, uuid.UUID]:
            return await _link_investigation_session(
                investigations, owner.id, agent.id, session_id
            )

        yield (
            annotations,
            investigations,
            owner.id,
            make_session_id,
            make_investigation_session,
        )


def _answer(
    owner_id: uuid.UUID,
    session_id: uuid.UUID,
    investigation_session_id: uuid.UUID,
    question_key: str = "root_cause",
    **overrides: Any,
) -> Annotation:
    """Build an investigation answer annotation.

    Args:
        owner_id: Id of the owning account.
        session_id: Id of the linked session.
        investigation_session_id: Id of the investigation session being answered.
        question_key: Key of the question being answered.
        **overrides: Additional annotation fields.

    Returns:
        Annotation ready to pass to create().
    """
    values: dict[str, Any] = {
        "owner_id": owner_id,
        "session_id": session_id,
        "investigation_session_id": investigation_session_id,
        "question_key": question_key,
        "value": "answer",
    }
    values.update(overrides)
    return Annotation(**values)


async def _create_manual_annotation(
    repository: AnnotationRepository,
    owner_id: uuid.UUID,
    session_id: uuid.UUID,
    **overrides: Any,
) -> Annotation:
    """Store a manual annotation with no investigation link.

    Args:
        repository: Annotation repository under test.
        owner_id: Id of the owning account.
        session_id: Id of the session being annotated.
        **overrides: Additional annotation fields.

    Returns:
        Stored annotation.
    """
    values: dict[str, Any] = {
        "owner_id": owner_id,
        "session_id": session_id,
        "value": "note",
    }
    values.update(overrides)
    return await repository.create(Annotation(**values))


async def test_create_manual_sets_timestamps(setup: Setup) -> None:
    """Persist a manual annotation with timestamps set and no investigation link."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await _create_manual_annotation(annotations, owner_id, session_id)
    assert created.owner_id == owner_id
    assert created.session_id == session_id
    assert created.investigation_session_id is None
    assert created.question_key is None
    assert created.created is not None
    assert created.updated is not None


async def test_create_investigation_answer(setup: Setup) -> None:
    """Persist an investigation answer tied to a linked session."""
    annotations, _, owner_id, make_session_id, make_investigation_session = setup
    session_id = await make_session_id()
    _, investigation_session_id = await make_investigation_session(session_id)
    created = await annotations.create(
        _answer(owner_id, session_id, investigation_session_id)
    )
    assert created.session_id == session_id
    assert created.investigation_session_id == investigation_session_id
    assert created.question_key == "root_cause"


async def test_create_upserts_same_question(setup: Setup) -> None:
    """Replace the value and selector of an existing answer instead of conflicting."""
    annotations, _, owner_id, make_session_id, make_investigation_session = setup
    session_id = await make_session_id()
    _, investigation_session_id = await make_investigation_session(session_id)
    first = await annotations.create(
        _answer(
            owner_id,
            session_id,
            investigation_session_id,
            value="first answer",
        )
    )
    second = await annotations.create(
        _answer(
            owner_id,
            session_id,
            investigation_session_id,
            selector=AnnotationSelector(part=AnnotationSelectorPart.OUTPUT),
            value="second answer",
        )
    )
    assert second.id == first.id
    assert second.value == "second answer"
    assert second.selector == AnnotationSelector(part=AnnotationSelectorPart.OUTPUT)
    assert first.updated is not None
    assert second.updated is not None
    assert second.updated >= first.updated
    loaded = await annotations.get(first.id)
    assert loaded == second


async def test_create_manual_never_conflicts(setup: Setup) -> None:
    """Insert two manual annotations on the same session without upserting."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    first = await _create_manual_annotation(annotations, owner_id, session_id)
    second = await _create_manual_annotation(annotations, owner_id, session_id)
    assert first.id != second.id


async def test_get(setup: Setup) -> None:
    """Load a stored annotation by id."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await _create_manual_annotation(annotations, owner_id, session_id)
    loaded = await annotations.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown annotation id."""
    annotations, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AnnotationNotFound, match=f"Annotation {missing_id} was not found"
    ):
        await annotations.get(missing_id)


async def test_query_filters_by_session_id(setup: Setup) -> None:
    """Filter annotations scoped to one session."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    other_session_id = await make_session_id()
    matching = await _create_manual_annotation(annotations, owner_id, session_id)
    await _create_manual_annotation(annotations, owner_id, other_session_id)
    results, _ = await annotations.query(
        AnnotationFilter(
            expression=FilterCondition(
                field="session_id", op=FilterOp.EQ, value=session_id
            )
        )
    )
    assert [annotation.id for annotation in results] == [matching.id]


async def test_query_filters_by_investigation_session_id(setup: Setup) -> None:
    """Filter annotations scoped to one investigation session."""
    annotations, _, owner_id, make_session_id, make_investigation_session = setup
    session_id = await make_session_id()
    _, investigation_session_id = await make_investigation_session(session_id)
    answer = await annotations.create(
        _answer(owner_id, session_id, investigation_session_id)
    )
    await _create_manual_annotation(annotations, owner_id, session_id)
    results, _ = await annotations.query(
        AnnotationFilter(
            expression=FilterCondition(
                field="investigation_session_id",
                op=FilterOp.EQ,
                value=investigation_session_id,
            )
        )
    )
    assert [annotation.id for annotation in results] == [answer.id]


async def test_query_filters_manual_by_null_investigation_session_id(
    setup: Setup,
) -> None:
    """Filter manual annotations via the investigation_session_id is_null op."""
    annotations, _, owner_id, make_session_id, make_investigation_session = setup
    session_id = await make_session_id()
    manual = await _create_manual_annotation(annotations, owner_id, session_id)
    _, investigation_session_id = await make_investigation_session(session_id)
    await annotations.create(_answer(owner_id, session_id, investigation_session_id))
    results, _ = await annotations.query(
        AnnotationFilter(
            expression=FilterCondition(
                field="investigation_session_id", op=FilterOp.IS_NULL
            )
        )
    )
    assert [annotation.id for annotation in results] == [manual.id]


async def test_query_filters_by_investigation_id(setup: Setup) -> None:
    """Filter annotations scoped to one investigation through the session link."""
    annotations, _, owner_id, make_session_id, make_investigation_session = setup
    session_id = await make_session_id()
    other_session_id = await make_session_id()
    investigation_id, investigation_session_id = await make_investigation_session(
        session_id
    )
    _, other_investigation_session_id = await make_investigation_session(
        other_session_id
    )
    answer = await annotations.create(
        _answer(owner_id, session_id, investigation_session_id)
    )
    await annotations.create(
        _answer(owner_id, other_session_id, other_investigation_session_id)
    )
    results, _ = await annotations.query(
        AnnotationFilter(
            expression=FilterCondition(
                field="investigation_id", op=FilterOp.EQ, value=investigation_id
            )
        )
    )
    assert [annotation.id for annotation in results] == [answer.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = [
        await _create_manual_annotation(annotations, owner_id, session_id)
        for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Annotation] = []
    cursor = None
    while True:
        results, next_cursor = await annotations.query(
            AnnotationFilter(cursor=cursor, size=2)
        )
        collected.extend(results)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert [annotation.id for annotation in collected] == [
        annotation.id for annotation in expected_order
    ]
    assert len({annotation.id for annotation in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist a value change and renew the updated timestamp."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await _create_manual_annotation(annotations, owner_id, session_id)
    created.update_value(True)
    updated = await annotations.update(created)
    assert updated.value is True
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await annotations.get(created.id)
    assert loaded == updated


async def test_update_leaves_selector_untouched(setup: Setup) -> None:
    """Persist only the value change, ignoring other fields on the incoming entity."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    original_selector = AnnotationSelector(part=AnnotationSelectorPart.INPUT)
    created = await _create_manual_annotation(
        annotations, owner_id, session_id, selector=original_selector
    )
    created.selector = AnnotationSelector(part=AnnotationSelectorPart.OUTPUT)
    created.update_value("revised")
    updated = await annotations.update(created)
    assert updated.selector == original_selector
    assert updated.value == "revised"


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown annotation id."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    annotation = Annotation(
        owner_id=owner_id,
        session_id=session_id,
        value="x",
    )
    with pytest.raises(
        AnnotationNotFound, match=f"Annotation {annotation.id} was not found"
    ):
        await annotations.update(annotation)


async def test_delete(setup: Setup) -> None:
    """Delete a stored annotation."""
    annotations, _, owner_id, make_session_id, _ = setup
    session_id = await make_session_id()
    created = await _create_manual_annotation(annotations, owner_id, session_id)
    await annotations.delete(created.id)
    with pytest.raises(AnnotationNotFound):
        await annotations.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown annotation id."""
    annotations, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AnnotationNotFound, match=f"Annotation {missing_id} was not found"
    ):
        await annotations.delete(missing_id)


async def test_investigation_delete_cascades_answers_but_keeps_manual(
    setup: Setup,
) -> None:
    """Drop an investigation's answers on delete, keep manual annotations."""
    (
        annotations,
        investigations,
        owner_id,
        make_session_id,
        make_investigation_session,
    ) = setup
    session_id = await make_session_id()
    investigation_id, investigation_session_id = await make_investigation_session(
        session_id
    )
    answer = await annotations.create(
        _answer(owner_id, session_id, investigation_session_id)
    )
    manual = await _create_manual_annotation(annotations, owner_id, session_id)

    await investigations.delete(investigation_id)

    with pytest.raises(AnnotationNotFound):
        await annotations.get(answer.id)
    loaded_manual = await annotations.get(manual.id)
    assert loaded_manual == manual
