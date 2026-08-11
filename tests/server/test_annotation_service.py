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
"""Tests for annotation use cases."""

import uuid
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAnnotationRepository,
    FakeInvestigationRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    create_agent,
    create_session,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.investigation import InvestigationStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.application.models.annotation import (
    AnnotationFilter,
    InvestigationAnswerCreate,
    ManualAnnotationCreate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.annotation_service import AnnotationService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.annotation import AnnotationNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.investigation import (
    Investigation,
    InvestigationSession,
    InvestigationSessionNotFound,
)
from kitaru.server.domain.session import SessionNotFound
from kitaru.server.domain.session_node import SessionNode
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def session_node_repository(
    session_repository: FakeSessionRepository,
) -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository(sessions=session_repository)


@pytest.fixture
def investigation_repository() -> FakeInvestigationRepository:
    """Provide a fake investigation repository."""
    return FakeInvestigationRepository()


@pytest.fixture
def annotation_repository(
    investigation_repository: FakeInvestigationRepository,
) -> FakeAnnotationRepository:
    """Provide a fake annotation repository wired to the investigation fake."""
    return FakeAnnotationRepository(investigations=investigation_repository)


@pytest.fixture
def service(
    annotation_repository: FakeAnnotationRepository,
    investigation_repository: FakeInvestigationRepository,
    session_repository: FakeSessionRepository,
    session_node_repository: FakeSessionNodeRepository,
) -> AnnotationService:
    """Provide an annotation service backed by fake repositories."""
    return AnnotationService(
        repository=annotation_repository,
        investigation_repository=investigation_repository,
        session_repository=session_repository,
        session_node_repository=session_node_repository,
    )


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


@pytest.fixture
async def session_id(
    session_repository: FakeSessionRepository, agent_id: uuid.UUID
) -> uuid.UUID:
    """Provide a session id belonging to the agent."""
    session = await create_session(
        session_repository, ACTOR.account.id, agent_id=agent_id
    )
    return session.id


async def _link_investigation_session(
    investigation_repository: FakeInvestigationRepository,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> tuple[uuid.UUID, uuid.UUID]:
    """Create a one-session investigation and link the session to it.

    Args:
        investigation_repository: Fake investigation repository to store into.
        agent_id: Id of the agent the session belongs to.
        session_id: Id of the session to link.

    Returns:
        Id of the created investigation and id of its session link.
    """
    investigation = Investigation(
        owner_id=ACTOR.account.id,
        agent_id=agent_id,
        name="investigation",
        total_sessions=0,
        completed_sessions=0,
    )
    created = await investigation_repository.create(
        investigation,
        [
            InvestigationSession(
                investigation_id=investigation.id, session_id=session_id, position=0
            )
        ],
    )
    linked = await investigation_repository.get_session_by_session_id(
        created.id, session_id
    )
    return created.id, linked.id


async def test_create_manual_annotation(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """Create a manual annotation with no investigation link."""
    annotation = await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"),
        actor=ACTOR,
    )
    assert annotation.owner_id == ACTOR.account.id
    assert annotation.session_id == session_id
    assert annotation.investigation_session_id is None
    assert annotation.selector is None
    assert annotation.value == "note"
    assert annotation.created is not None


async def test_create_manual_annotation_missing_session(
    service: AnnotationService,
) -> None:
    """Raise when the session does not exist."""
    with pytest.raises(SessionNotFound):
        await service.create_manual_annotation(
            ManualAnnotationCreate(
                session_id=uuid.uuid4(),
                value="note",
            ),
            actor=ACTOR,
        )


async def test_create_manual_annotation_invalid_selector_node(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """Reject a selector naming a node outside the session."""
    node_id = uuid.uuid4()
    with pytest.raises(
        ValidationError, match=f"Node {node_id} does not belong to session {session_id}"
    ):
        await service.create_manual_annotation(
            ManualAnnotationCreate(
                session_id=session_id,
                selector=AnnotationSelector(node_id=node_id),
                value="note",
            ),
            actor=ACTOR,
        )


async def test_create_manual_annotation_valid_selector_node(
    service: AnnotationService,
    session_node_repository: FakeSessionNodeRepository,
    session_id: uuid.UUID,
) -> None:
    """Accept a selector naming a node that belongs to the session."""
    node = SessionNode(
        session_id=session_id,
        index=0,
        node_type=NodeType.LLM_CALL,
        name="call",
        status=NodeStatus.COMPLETED,
    )
    await session_node_repository.upsert_batch(session_id, [node])
    annotation = await service.create_manual_annotation(
        ManualAnnotationCreate(
            session_id=session_id,
            selector=AnnotationSelector(node_id=node.id),
            value="note",
        ),
        actor=ACTOR,
    )
    assert annotation.selector == AnnotationSelector(node_id=node.id)


async def test_get_annotation(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """Load a stored annotation by id."""
    created = await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"),
        actor=ACTOR,
    )
    loaded = await service.get_annotation(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_annotation_not_found(service: AnnotationService) -> None:
    """Raise for an unknown annotation id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        AnnotationNotFound, match=f"Annotation {missing_id} was not found"
    ):
        await service.get_annotation(missing_id, actor=ACTOR)


async def test_list_annotations(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """List annotations and filter by session id."""
    matching = await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"),
        actor=ACTOR,
    )

    annotations, next_cursor = await service.list_annotations(
        AnnotationFilter(), actor=ACTOR
    )
    assert next_cursor is None
    assert [annotation.id for annotation in annotations] == [matching.id]

    annotations, _ = await service.list_annotations(
        AnnotationFilter(
            expression=FilterCondition(
                field="session_id", op=FilterOp.EQ, value=uuid.uuid4()
            )
        ),
        actor=ACTOR,
    )
    assert annotations == []


async def test_update_annotation(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """Set a new value on an annotation."""
    created = await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"),
        actor=ACTOR,
    )
    updated = await service.update_annotation(created.id, True, actor=ACTOR)
    assert updated.value is True
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated


async def test_update_annotation_not_found(service: AnnotationService) -> None:
    """Raise for an unknown annotation id."""
    with pytest.raises(AnnotationNotFound):
        await service.update_annotation(uuid.uuid4(), "x", actor=ACTOR)


async def test_delete_annotation(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """Delete a stored annotation."""
    created = await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"),
        actor=ACTOR,
    )
    await service.delete_annotation(created.id, actor=ACTOR)
    with pytest.raises(AnnotationNotFound):
        await service.get_annotation(created.id, actor=ACTOR)


async def test_delete_annotation_not_found(service: AnnotationService) -> None:
    """Raise for an unknown annotation id."""
    with pytest.raises(AnnotationNotFound):
        await service.delete_annotation(uuid.uuid4(), actor=ACTOR)


async def test_create_investigation_answer(
    service: AnnotationService,
    investigation_repository: FakeInvestigationRepository,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> None:
    """Answer a linked session, deriving session_id from the link."""
    investigation_id, investigation_session_id = await _link_investigation_session(
        investigation_repository, agent_id, session_id
    )
    annotation = await service.create_investigation_answer(
        InvestigationAnswerCreate(
            investigation_session_id=investigation_session_id,
            value="a retry loop",
        ),
        actor=ACTOR,
    )
    assert annotation.session_id == session_id
    assert annotation.investigation_session_id == investigation_session_id

    investigation = await investigation_repository.get(investigation_id)
    assert investigation.status is InvestigationStatus.IN_PROGRESS
    assert investigation.started_at is not None


async def test_create_investigation_answer_missing_link(
    service: AnnotationService,
) -> None:
    """Raise when no investigation session has the given id."""
    with pytest.raises(InvestigationSessionNotFound):
        await service.create_investigation_answer(
            InvestigationAnswerCreate(
                investigation_session_id=uuid.uuid4(),
                value="x",
            ),
            actor=ACTOR,
        )


async def test_create_investigation_answer_invalid_selector_node(
    service: AnnotationService,
    investigation_repository: FakeInvestigationRepository,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> None:
    """Reject a selector naming a node outside the linked session."""
    _, investigation_session_id = await _link_investigation_session(
        investigation_repository, agent_id, session_id
    )
    node_id = uuid.uuid4()
    with pytest.raises(
        ValidationError, match=f"Node {node_id} does not belong to session {session_id}"
    ):
        await service.create_investigation_answer(
            InvestigationAnswerCreate(
                investigation_session_id=investigation_session_id,
                selector=AnnotationSelector(node_id=node_id),
                value="x",
            ),
            actor=ACTOR,
        )


async def test_create_investigation_answer_never_conflicts(
    service: AnnotationService,
    investigation_repository: FakeInvestigationRepository,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> None:
    """Create a separate annotation for each answer to the same session."""
    _, investigation_session_id = await _link_investigation_session(
        investigation_repository, agent_id, session_id
    )
    first = await service.create_investigation_answer(
        InvestigationAnswerCreate(
            investigation_session_id=investigation_session_id,
            value="first answer",
        ),
        actor=ACTOR,
    )
    second = await service.create_investigation_answer(
        InvestigationAnswerCreate(
            investigation_session_id=investigation_session_id,
            value="second answer",
        ),
        actor=ACTOR,
    )
    assert second.id != first.id
    annotations, _ = await service.list_annotations(
        AnnotationFilter(
            expression=FilterCondition(
                field="investigation_session_id",
                op=FilterOp.EQ,
                value=investigation_session_id,
            )
        ),
        actor=ACTOR,
    )
    assert {annotation.id for annotation in annotations} == {first.id, second.id}


async def test_create_investigation_answer_second_answer_leaves_started_at(
    service: AnnotationService,
    investigation_repository: FakeInvestigationRepository,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> None:
    """Leave the investigation in progress on a second answer."""
    investigation_id, investigation_session_id = await _link_investigation_session(
        investigation_repository, agent_id, session_id
    )
    await service.create_investigation_answer(
        InvestigationAnswerCreate(
            investigation_session_id=investigation_session_id,
            value="a retry loop",
        ),
        actor=ACTOR,
    )
    started_at = (await investigation_repository.get(investigation_id)).started_at
    await service.create_investigation_answer(
        InvestigationAnswerCreate(
            investigation_session_id=investigation_session_id,
            value=False,
        ),
        actor=ACTOR,
    )
    investigation = await investigation_repository.get(investigation_id)
    assert investigation.status is InvestigationStatus.IN_PROGRESS
    assert investigation.started_at == started_at


async def test_create_manual_annotation_tracks_annotation_created(
    annotation_repository: FakeAnnotationRepository,
    investigation_repository: FakeInvestigationRepository,
    session_repository: FakeSessionRepository,
    session_node_repository: FakeSessionNodeRepository,
    session_id: uuid.UUID,
) -> None:
    """Fire ANNOTATION_CREATED for an annotation outside any investigation."""
    analytics = _RecordingAnalytics()
    service = AnnotationService(
        repository=annotation_repository,
        investigation_repository=investigation_repository,
        session_repository=session_repository,
        session_node_repository=session_node_repository,
        analytics=analytics,
    )

    await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"), actor=ACTOR
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.ANNOTATION_CREATED
    assert properties == {"investigation_answer": False, "has_selector": False}


async def test_create_investigation_answer_tracks_annotation_created(
    annotation_repository: FakeAnnotationRepository,
    investigation_repository: FakeInvestigationRepository,
    session_repository: FakeSessionRepository,
    session_node_repository: FakeSessionNodeRepository,
    agent_id: uuid.UUID,
    session_id: uuid.UUID,
) -> None:
    """Fire ANNOTATION_CREATED flagged as an investigation answer."""
    analytics = _RecordingAnalytics()
    service = AnnotationService(
        repository=annotation_repository,
        investigation_repository=investigation_repository,
        session_repository=session_repository,
        session_node_repository=session_node_repository,
        analytics=analytics,
    )
    _, investigation_session_id = await _link_investigation_session(
        investigation_repository, agent_id, session_id
    )

    await service.create_investigation_answer(
        InvestigationAnswerCreate(
            investigation_session_id=investigation_session_id,
            value="a retry loop",
        ),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.ANNOTATION_CREATED
    assert properties == {"investigation_answer": True, "has_selector": False}


async def test_create_manual_annotation_without_analytics_tracker(
    service: AnnotationService, session_id: uuid.UUID
) -> None:
    """Create an annotation normally when no analytics tracker is configured."""
    annotation = await service.create_manual_annotation(
        ManualAnnotationCreate(session_id=session_id, value="note"), actor=ACTOR
    )
    assert annotation.owner_id == ACTOR.account.id
