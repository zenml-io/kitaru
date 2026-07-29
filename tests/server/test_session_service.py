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
"""Tests for session use cases."""

import uuid

import pytest

from conftest import FakeSessionRepository, create_session
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.session import (
    IllegalSessionStatusTransition,
    SessionNotFound,
    SessionStatusCannotBeCleared,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def service(repository: FakeSessionRepository) -> SessionService:
    """Provide a session service backed by the fake repository."""
    return SessionService(repository=repository)


async def test_create_session_defaults_status_in_progress(
    service: SessionService,
) -> None:
    """Default a session with no status to in_progress."""
    agent_id = uuid.uuid4()
    session = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    assert session.agent_id == agent_id
    assert session.owner_id == ACTOR.account.id
    assert session.status == SessionStatus.IN_PROGRESS
    assert session.created is not None
    assert session.updated is not None


async def test_create_session_honors_explicit_status(
    service: SessionService,
) -> None:
    """Store an explicit initial status, for example an imported session."""
    session = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
        ),
        actor=ACTOR,
    )
    assert session.status == SessionStatus.COMPLETED


async def test_get_session(service: SessionService) -> None:
    """Load a stored session by id."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    loaded = await service.get_session(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await service.get_session(missing_id, actor=ACTOR)


async def test_list_sessions_scoped_by_agent(service: SessionService) -> None:
    """List only the sessions of the requested agent."""
    agent_id = uuid.uuid4()
    other_agent_id = uuid.uuid4()
    first = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED), actor=ACTOR
    )
    second = await service.create_session(
        SessionCreate(agent_id=agent_id, origin=SessionOrigin.RECORDED), actor=ACTOR
    )
    await service.create_session(
        SessionCreate(agent_id=other_agent_id, origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )

    sessions, next_cursor = await service.list_sessions(
        SessionFilter(agent_id=agent_id), actor=ACTOR
    )
    assert next_cursor is None
    assert {session.id for session in sessions} == {first.id, second.id}


async def test_update_session_clears_outputs_and_expected_with_explicit_null(
    service: SessionService,
) -> None:
    """Clear outputs and expected with an explicit null passed alongside status."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            outputs={"answer": 42},
            expected={"answer": 42},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.COMPLETED, outputs=None, expected=None),
        actor=ACTOR,
    )
    assert updated.outputs is None
    assert updated.expected is None
    assert updated.status == SessionStatus.COMPLETED


async def test_update_session_omitted_fields_unchanged(
    service: SessionService,
) -> None:
    """Leave outputs and expected unchanged when the command omits them."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            outputs={"answer": 42},
            expected={"answer": 42},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.outputs == {"answer": 42}
    assert updated.expected == {"answer": 42}
    assert updated.name == "renamed"
    assert updated.status == SessionStatus.IN_PROGRESS


async def test_update_session_metadata_replaced_whole(
    service: SessionService,
) -> None:
    """Replace metadata whole rather than merging keys."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            metadata={"a": 1, "b": 2},
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(metadata={"c": 3}), actor=ACTOR
    )
    assert updated.metadata == {"c": 3}


async def test_update_session_metadata_null_clears(service: SessionService) -> None:
    """Clear metadata to an empty dict with an explicit null."""
    created = await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED, metadata={"a": 1}
        ),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id, SessionUpdate(metadata=None), actor=ACTOR
    )
    assert updated.metadata == {}


async def test_update_session_status_transition_completes_session(
    service: SessionService,
) -> None:
    """Move an in_progress session to completed via the update endpoint."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    updated = await service.update_session(
        created.id,
        SessionUpdate(status=SessionStatus.COMPLETED, error=None),
        actor=ACTOR,
    )
    assert updated.status == SessionStatus.COMPLETED
    assert updated.ended_at is None


async def test_update_session_status_cannot_be_cleared(
    service: SessionService,
) -> None:
    """Reject an explicit null status."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    with pytest.raises(SessionStatusCannotBeCleared):
        await service.update_session(
            created.id, SessionUpdate(status=None), actor=ACTOR
        )


async def test_update_session_rejects_terminal_back_to_in_progress(
    service: SessionService,
) -> None:
    """Reject moving a terminal session back to in_progress."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.update_session(
        created.id, SessionUpdate(status=SessionStatus.FAILED), actor=ACTOR
    )
    with pytest.raises(IllegalSessionStatusTransition):
        await service.update_session(
            created.id, SessionUpdate(status=SessionStatus.IN_PROGRESS), actor=ACTOR
        )


async def test_update_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.update_session(uuid.uuid4(), SessionUpdate(name="x"), actor=ACTOR)


async def test_delete_session(service: SessionService) -> None:
    """Delete a stored session."""
    created = await service.create_session(
        SessionCreate(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED),
        actor=ACTOR,
    )
    await service.delete_session(created.id, actor=ACTOR)
    with pytest.raises(SessionNotFound):
        await service.get_session(created.id, actor=ACTOR)


async def test_delete_session_not_found(service: SessionService) -> None:
    """Raise for an unknown session id."""
    with pytest.raises(SessionNotFound):
        await service.delete_session(uuid.uuid4(), actor=ACTOR)


async def test_create_session_duplicate_external_id_conflict(
    service: SessionService,
) -> None:
    """Reject a duplicate provider and external id pair."""
    await service.create_session(
        SessionCreate(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.IMPORTED,
            provider="langsmith",
            external_id="run-1",
        ),
        actor=ACTOR,
    )
    with pytest.raises(Exception, match="already registered"):
        await service.create_session(
            SessionCreate(
                agent_id=uuid.uuid4(),
                origin=SessionOrigin.IMPORTED,
                provider="langsmith",
                external_id="run-1",
            ),
            actor=ACTOR,
        )


async def test_create_session_helper_defaults(
    repository: FakeSessionRepository,
) -> None:
    """Store a session through the create_session test helper."""
    owner_id = uuid.uuid4()
    session = await create_session(repository, owner_id, agent_id=uuid.uuid4())
    assert session.owner_id == owner_id
    assert session.status == SessionStatus.IN_PROGRESS
