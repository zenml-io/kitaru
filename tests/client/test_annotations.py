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
"""Round-trip tests for the annotations SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeAnnotationRepository,
    FakeInvestigationRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    asgi_api_client,
    build_payload_offload_service,
    override_idempotency,
)
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.annotation import (
    AnnotationListParams,
    AnnotationResponse,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationSessionInput,
    InvestigationSessionQuestion,
)
from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_agent_service,
    get_annotation_service,
    get_investigation_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.annotation_service import AnnotationService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
QUESTION_KEY = "cause"


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_repository = FakeAgentRepository()
    session_repository = FakeSessionRepository()
    session_node_repository = FakeSessionNodeRepository(sessions=session_repository)
    investigation_repository = FakeInvestigationRepository()
    annotation_repository = FakeAnnotationRepository(
        investigations=investigation_repository
    )
    app.dependency_overrides[get_agent_service] = lambda: AgentService(
        repository=agent_repository
    )
    app.dependency_overrides[get_session_service] = lambda: SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(agent_repository),
        replay_repository=FakeReplayRepository(),
        payload_offload=build_payload_offload_service(),
    )
    app.dependency_overrides[get_investigation_service] = lambda: InvestigationService(
        repository=investigation_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[get_annotation_service] = lambda: AnnotationService(
        repository=annotation_repository,
        investigation_repository=investigation_repository,
        session_repository=session_repository,
        session_node_repository=session_node_repository,
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def _make_agent(api_client: KitaruAPIClient) -> uuid.UUID:
    """Create an agent through the SDK."""
    agent = await api_client.agents.create(
        AgentCreateRequest(name=f"assistant-{uuid.uuid4().hex[:8]}")
    )
    return agent.id


async def _make_session(api_client: KitaruAPIClient, agent_id: uuid.UUID) -> uuid.UUID:
    """Create a session on the given agent through the SDK."""
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
        )
    )
    return session.id


async def _make_investigation_session(
    api_client: KitaruAPIClient, agent_id: uuid.UUID, session_id: uuid.UUID
) -> uuid.UUID:
    """Create an investigation linking the session, return its link id."""
    investigation = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id,
                    questions=[
                        InvestigationSessionQuestion(
                            key=QUESTION_KEY, question="What caused it?"
                        )
                    ],
                )
            ],
        )
    )
    page = await api_client.investigations.list_sessions(investigation.id)
    return page.items[0].id


async def test_create_manual(api_client: KitaruAPIClient) -> None:
    """Create a manual annotation through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    annotation = await api_client.annotations.create(
        ManualAnnotationCreateRequest(session_id=session_id, value="note")
    )
    assert isinstance(annotation, AnnotationResponse)
    assert annotation.owner_id == ACCOUNT.id
    assert annotation.session_id == session_id
    assert annotation.investigation_session_id is None
    assert annotation.value == "note"


async def test_create_manual_missing_session(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.annotations.create(
            ManualAnnotationCreateRequest(
                session_id=uuid.uuid4(),
                value="note",
            )
        )


async def test_create_investigation_answer(api_client: KitaruAPIClient) -> None:
    """Answer an investigation session through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    investigation_session_id = await _make_investigation_session(
        api_client, agent_id, session_id
    )
    annotation = await api_client.annotations.create(
        InvestigationAnswerCreateRequest(
            investigation_session_id=investigation_session_id,
            question_key=QUESTION_KEY,
            value="a retry loop",
        )
    )
    assert annotation.session_id == session_id
    assert annotation.investigation_session_id == investigation_session_id
    assert annotation.question_key == QUESTION_KEY


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an annotation by id through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    created = await api_client.annotations.create(
        ManualAnnotationCreateRequest(session_id=session_id, value="note")
    )
    loaded = await api_client.annotations.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.annotations.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate annotations through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    for value in ["first", "second", "third"]:
        await api_client.annotations.create(
            ManualAnnotationCreateRequest(
                session_id=session_id,
                value=value,
            )
        )

    page = await api_client.annotations.list(AnnotationListParams(size=2))
    assert len(page.items) == 2

    collected = [item async for item in api_client.annotations.iter()]
    assert len(collected) == 3


async def test_update(api_client: KitaruAPIClient) -> None:
    """Set a new value on an annotation through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    created = await api_client.annotations.create(
        ManualAnnotationCreateRequest(session_id=session_id, value="note")
    )
    updated = await api_client.annotations.update(
        created.id,
        AnnotationUpdateRequest(value=True),
    )
    assert updated.value is True


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an annotation through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    created = await api_client.annotations.create(
        ManualAnnotationCreateRequest(session_id=session_id, value="note")
    )
    await api_client.annotations.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.annotations.get(created.id)
