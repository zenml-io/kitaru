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
"""Tests for investigation use cases."""

import uuid
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeInvestigationRepository,
    FakeSessionRepository,
    create_agent,
    create_session,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.investigation import (
    InvestigationSessionHighlight,
    InvestigationSessionQuestion,
    InvestigationSessionVerdict,
    InvestigationStatus,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.investigation import (
    InvestigationCreate,
    InvestigationFilter,
    InvestigationSessionFilter,
    InvestigationSessionInput,
    InvestigationUpdate,
)
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.investigation import (
    IllegalInvestigationStatusTransition,
    InvestigationNotFound,
    InvestigationSessionNotFound,
)
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
def investigation_repository() -> FakeInvestigationRepository:
    """Provide a fake investigation repository."""
    return FakeInvestigationRepository()


@pytest.fixture
def service(
    investigation_repository: FakeInvestigationRepository,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> InvestigationService:
    """Provide an investigation service backed by fake repositories."""
    return InvestigationService(
        repository=investigation_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


@pytest.fixture
async def session_ids(
    session_repository: FakeSessionRepository, agent_id: uuid.UUID
) -> list[uuid.UUID]:
    """Provide two session ids belonging to the agent."""
    sessions = [
        await create_session(session_repository, ACTOR.account.id, agent_id=agent_id)
        for _ in range(2)
    ]
    return [session.id for session in sessions]


async def test_create_investigation(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Assign session position from list order and initialize progress counts."""
    highlights = [
        InvestigationSessionHighlight(
            selector=AnnotationSelector(), description="Retried without backoff."
        )
    ]
    investigation = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            description="curator rationale",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_ids[0],
                    questions=[
                        InvestigationSessionQuestion(
                            key="cause",
                            question="What caused it?",
                            highlights=highlights,
                        )
                    ],
                ),
                InvestigationSessionInput(
                    session_id=session_ids[1],
                    questions=[
                        InvestigationSessionQuestion(
                            key="cause", question="What caused it?"
                        )
                    ],
                ),
            ],
        ),
        actor=ACTOR,
    )
    assert investigation.owner_id == ACTOR.account.id
    assert investigation.agent_id == agent_id
    assert investigation.name == "investigation"
    assert investigation.description == "curator rationale"
    assert investigation.status is InvestigationStatus.PENDING
    assert investigation.total_sessions == 2
    assert investigation.completed_sessions == 0
    assert investigation.created is not None

    sessions, _ = await service.list_investigation_sessions(
        InvestigationSessionFilter(investigation_id=investigation.id), actor=ACTOR
    )
    assert [session.session_id for session in sessions] == session_ids
    assert [session.position for session in sessions] == [0, 1]
    assert sessions[0].questions[0].highlights == highlights
    assert sessions[1].questions[0].highlights == []


async def test_create_investigation_missing_agent(
    service: InvestigationService,
) -> None:
    """Raise when the agent does not exist."""
    with pytest.raises(AgentNotFound):
        await service.create_investigation(
            InvestigationCreate(
                agent_id=uuid.uuid4(), name="investigation", sessions=[]
            ),
            actor=ACTOR,
        )


async def test_create_investigation_missing_session(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Reject a linked session id that does not exist."""
    missing_session_id = uuid.uuid4()
    with pytest.raises(
        ValidationError, match=f"Session {missing_session_id} was not found"
    ):
        await service.create_investigation(
            InvestigationCreate(
                agent_id=agent_id,
                name="investigation",
                sessions=[
                    InvestigationSessionInput(
                        session_id=missing_session_id, questions=[]
                    )
                ],
            ),
            actor=ACTOR,
        )


async def test_create_investigation_session_wrong_agent(
    service: InvestigationService,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
) -> None:
    """Reject a linked session that belongs to a different agent."""
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    other_session = await create_session(
        session_repository, ACTOR.account.id, agent_id=other_agent.id
    )
    with pytest.raises(
        ValidationError,
        match=f"Session {other_session.id} does not belong to agent {agent_id}",
    ):
        await service.create_investigation(
            InvestigationCreate(
                agent_id=agent_id,
                name="investigation",
                sessions=[
                    InvestigationSessionInput(session_id=other_session.id, questions=[])
                ],
            ),
            actor=ACTOR,
        )


async def test_create_investigation_duplicate_session_ids(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Reject a session list naming the same session twice."""
    with pytest.raises(
        ValidationError, match="Investigation session list contains duplicate ids"
    ):
        await service.create_investigation(
            InvestigationCreate(
                agent_id=agent_id,
                name="investigation",
                sessions=[
                    InvestigationSessionInput(session_id=session_ids[0], questions=[]),
                    InvestigationSessionInput(session_id=session_ids[0], questions=[]),
                ],
            ),
            actor=ACTOR,
        )


async def test_get_investigation(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Load a stored investigation by id."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    loaded = await service.get_investigation(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_investigation_not_found(service: InvestigationService) -> None:
    """Raise for an unknown investigation id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        InvestigationNotFound, match=f"Investigation {missing_id} was not found"
    ):
        await service.get_investigation(missing_id, actor=ACTOR)


async def test_list_investigations(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """List investigations and filter by status."""
    for name in ["alpha", "beta"]:
        await service.create_investigation(
            InvestigationCreate(agent_id=agent_id, name=name, sessions=[]),
            actor=ACTOR,
        )

    investigations, next_cursor = await service.list_investigations(
        InvestigationFilter(), actor=ACTOR
    )
    assert next_cursor is None
    assert [investigation.name for investigation in investigations] == [
        "beta",
        "alpha",
    ]

    investigations, _ = await service.list_investigations(
        InvestigationFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=InvestigationStatus.COMPLETED
            )
        ),
        actor=ACTOR,
    )
    assert investigations == []


async def test_list_investigations_filters_by_agent_id(
    service: InvestigationService,
    agent_repository: FakeAgentRepository,
    agent_id: uuid.UUID,
) -> None:
    """Filter investigations scoped to one agent."""
    matching = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    await service.create_investigation(
        InvestigationCreate(agent_id=other_agent.id, name="other", sessions=[]),
        actor=ACTOR,
    )

    investigations, _ = await service.list_investigations(
        InvestigationFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        ),
        actor=ACTOR,
    )
    assert [investigation.id for investigation in investigations] == [matching.id]


async def test_update_investigation_name(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Update an investigation's name."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    updated = await service.update_investigation(
        created.id, InvestigationUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated


async def test_update_investigation_description(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Update an investigation's description without touching its name."""
    created = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            description="old",
            sessions=[],
        ),
        actor=ACTOR,
    )
    updated = await service.update_investigation(
        created.id, InvestigationUpdate(description="new"), actor=ACTOR
    )
    assert updated.name == "investigation"
    assert updated.description == "new"


async def test_update_investigation_omitted_fields_unchanged(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Leave every field unchanged when the command sets none of it."""
    created = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            description="old",
            sessions=[],
        ),
        actor=ACTOR,
    )
    updated = await service.update_investigation(
        created.id, InvestigationUpdate(), actor=ACTOR
    )
    assert updated.name == "investigation"
    assert updated.description == "old"


async def test_update_investigation_status(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Move an investigation through in_progress to completed."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    updated = await service.update_investigation(
        created.id,
        InvestigationUpdate(status=InvestigationStatus.IN_PROGRESS),
        actor=ACTOR,
    )
    assert updated.status is InvestigationStatus.IN_PROGRESS
    assert updated.started_at is not None
    assert updated.ended_at is None
    updated = await service.update_investigation(
        created.id,
        InvestigationUpdate(status=InvestigationStatus.COMPLETED),
        actor=ACTOR,
    )
    assert updated.status is InvestigationStatus.COMPLETED
    assert updated.ended_at is not None


async def test_update_investigation_status_illegal_transition(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Reject moving a completed investigation backwards."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    await service.update_investigation(
        created.id,
        InvestigationUpdate(status=InvestigationStatus.COMPLETED),
        actor=ACTOR,
    )
    with pytest.raises(IllegalInvestigationStatusTransition):
        await service.update_investigation(
            created.id,
            InvestigationUpdate(status=InvestigationStatus.IN_PROGRESS),
            actor=ACTOR,
        )


async def test_update_investigation_cannot_clear_status(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Reject clearing the investigation status with an explicit null."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    with pytest.raises(ValidationError, match="Investigation status cannot be cleared"):
        await service.update_investigation(
            created.id, InvestigationUpdate(status=None), actor=ACTOR
        )


async def test_update_investigation_cannot_clear_name(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Reject clearing the investigation name with an explicit null."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    with pytest.raises(ValidationError, match="Investigation name cannot be cleared"):
        await service.update_investigation(
            created.id, InvestigationUpdate(name=None), actor=ACTOR
        )


async def test_update_investigation_not_found(
    service: InvestigationService,
) -> None:
    """Raise for an unknown investigation id."""
    with pytest.raises(InvestigationNotFound):
        await service.update_investigation(
            uuid.uuid4(), InvestigationUpdate(name="x"), actor=ACTOR
        )


async def test_delete_investigation(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Delete a stored investigation."""
    created = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    await service.delete_investigation(created.id, actor=ACTOR)
    with pytest.raises(InvestigationNotFound):
        await service.get_investigation(created.id, actor=ACTOR)


async def test_delete_investigation_not_found(
    service: InvestigationService,
) -> None:
    """Raise for an unknown investigation id."""
    with pytest.raises(InvestigationNotFound):
        await service.delete_investigation(uuid.uuid4(), actor=ACTOR)


async def test_list_investigation_sessions_not_found(
    service: InvestigationService,
) -> None:
    """Raise for an unknown investigation id."""
    with pytest.raises(InvestigationNotFound):
        await service.list_investigation_sessions(
            InvestigationSessionFilter(investigation_id=uuid.uuid4()), actor=ACTOR
        )


async def test_update_investigation_session_verdict(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Set a linked session's verdict without completing the investigation."""
    investigation = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(session_id=session_ids[0], questions=[]),
                InvestigationSessionInput(session_id=session_ids[1], questions=[]),
            ],
        ),
        actor=ACTOR,
    )
    session = await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[0],
        InvestigationSessionVerdict.ACCEPTABLE,
        actor=ACTOR,
    )
    assert session.verdict is InvestigationSessionVerdict.ACCEPTABLE
    reloaded = await service.get_investigation(investigation.id, actor=ACTOR)
    assert reloaded.status is InvestigationStatus.PENDING
    assert reloaded.completed_sessions == 1
    assert reloaded.ended_at is None


async def test_update_investigation_session_verdict_replaces_earlier_verdict(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Replace a linked session's earlier verdict."""
    investigation = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(session_id=session_ids[0], questions=[]),
                InvestigationSessionInput(session_id=session_ids[1], questions=[]),
            ],
        ),
        actor=ACTOR,
    )
    await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[0],
        InvestigationSessionVerdict.UNCERTAIN,
        actor=ACTOR,
    )
    session = await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[0],
        InvestigationSessionVerdict.PROBLEMATIC,
        actor=ACTOR,
    )
    assert session.verdict is InvestigationSessionVerdict.PROBLEMATIC
    reloaded = await service.get_investigation(investigation.id, actor=ACTOR)
    assert reloaded.completed_sessions == 1


async def test_update_investigation_session_verdict_clear(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Clear a linked session's verdict."""
    investigation = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(session_id=session_ids[0], questions=[]),
                InvestigationSessionInput(session_id=session_ids[1], questions=[]),
            ],
        ),
        actor=ACTOR,
    )
    await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[0],
        InvestigationSessionVerdict.ACCEPTABLE,
        actor=ACTOR,
    )
    session = await service.update_investigation_session_verdict(
        investigation.id, session_ids[0], None, actor=ACTOR
    )
    assert session.verdict is None
    reloaded = await service.get_investigation(investigation.id, actor=ACTOR)
    assert reloaded.completed_sessions == 0


async def test_update_investigation_session_verdict_does_not_complete_investigation(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Leave the investigation status untouched once every link has a verdict."""
    investigation = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(session_id=session_ids[0], questions=[]),
                InvestigationSessionInput(session_id=session_ids[1], questions=[]),
            ],
        ),
        actor=ACTOR,
    )
    await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[0],
        InvestigationSessionVerdict.ACCEPTABLE,
        actor=ACTOR,
    )
    await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[1],
        InvestigationSessionVerdict.PROBLEMATIC,
        actor=ACTOR,
    )
    reloaded = await service.get_investigation(investigation.id, actor=ACTOR)
    assert reloaded.status is InvestigationStatus.PENDING
    assert reloaded.completed_sessions == 2
    assert reloaded.ended_at is None


async def test_update_investigation_session_verdict_investigation_not_found(
    service: InvestigationService,
) -> None:
    """Raise for an unknown investigation id."""
    with pytest.raises(InvestigationNotFound):
        await service.update_investigation_session_verdict(
            uuid.uuid4(),
            uuid.uuid4(),
            InvestigationSessionVerdict.ACCEPTABLE,
            actor=ACTOR,
        )


async def test_update_investigation_session_verdict_session_not_found(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Raise when no linked session matches the investigation and session pair."""
    investigation = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    with pytest.raises(InvestigationSessionNotFound):
        await service.update_investigation_session_verdict(
            investigation.id,
            uuid.uuid4(),
            InvestigationSessionVerdict.ACCEPTABLE,
            actor=ACTOR,
        )


async def test_update_investigation_session_verdict_leaves_questions_untouched(
    service: InvestigationService, agent_id: uuid.UUID, session_ids: list[uuid.UUID]
) -> None:
    """Leave a linked session's questions, set at create, untouched by the verdict."""
    highlights = [
        InvestigationSessionHighlight(
            selector=AnnotationSelector(), description="Retried without backoff."
        )
    ]
    questions = [
        InvestigationSessionQuestion(
            key="cause", question="What caused it?", highlights=highlights
        )
    ]
    investigation = await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_ids[0], questions=questions
                )
            ],
        ),
        actor=ACTOR,
    )
    session = await service.update_investigation_session_verdict(
        investigation.id,
        session_ids[0],
        InvestigationSessionVerdict.ACCEPTABLE,
        actor=ACTOR,
    )
    assert session.verdict is InvestigationSessionVerdict.ACCEPTABLE
    assert session.questions == questions


async def test_create_investigation_tracks_investigation_created(
    investigation_repository: FakeInvestigationRepository,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    session_ids: list[uuid.UUID],
) -> None:
    """Fire INVESTIGATION_CREATED with the session count."""
    analytics = _RecordingAnalytics()
    service = InvestigationService(
        repository=investigation_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
        analytics=analytics,
    )

    await service.create_investigation(
        InvestigationCreate(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(session_id=session_id, questions=[])
                for session_id in session_ids
            ],
        ),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.INVESTIGATION_CREATED
    assert properties == {"session_count": 2}


async def test_create_investigation_without_analytics_tracker(
    service: InvestigationService, agent_id: uuid.UUID
) -> None:
    """Create an investigation normally when no analytics tracker is configured."""
    investigation = await service.create_investigation(
        InvestigationCreate(agent_id=agent_id, name="investigation", sessions=[]),
        actor=ACTOR,
    )
    assert investigation.owner_id == ACTOR.account.id
