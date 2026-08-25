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
"""Round-trip tests for the investigations SDK resource."""

import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeInvestigationRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    asgi_api_client,
    build_payload_offload_service,
    override_idempotency,
)
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationResponse,
    InvestigationSessionHighlight,
    InvestigationSessionInput,
    InvestigationSessionQuestion,
    InvestigationSessionResponse,
    InvestigationSessionsListParams,
    InvestigationSessionUpdateRequest,
    InvestigationSessionVerdict,
    InvestigationStatus,
    InvestigationUpdateRequest,
)
from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_agent_service,
    get_investigation_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


def _question(**overrides: Any) -> InvestigationSessionQuestion:
    """Build a minimal investigation session question.

    Args:
        **overrides: Additional question fields.

    Returns:
        Question ready to pass to InvestigationSessionInput.
    """
    values: dict[str, Any] = {"key": "cause", "question": "What caused it?"}
    values.update(overrides)
    return InvestigationSessionQuestion(**values)


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
    investigation_repository = FakeInvestigationRepository()
    app.dependency_overrides[get_agent_service] = lambda: AgentService(
        repository=agent_repository
    )
    app.dependency_overrides[get_session_service] = lambda: SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(agent_repository),
        replay_repository=FakeReplayRepository(),
        payload_offload=build_payload_offload_service().service,
    )
    app.dependency_overrides[get_investigation_service] = lambda: InvestigationService(
        repository=investigation_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
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


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an investigation with its sessions through the SDK."""
    agent_id = await _make_agent(api_client)
    session_ids = [await _make_session(api_client, agent_id) for _ in range(2)]
    investigation = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            description="curator rationale",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id, questions=[_question()]
                )
                for session_id in session_ids
            ],
        )
    )
    assert isinstance(investigation, InvestigationResponse)
    assert investigation.owner_id == ACCOUNT.id
    assert investigation.agent_id == agent_id
    assert investigation.name == "investigation"
    assert investigation.status is InvestigationStatus.PENDING
    assert investigation.total_sessions == 2
    assert investigation.completed_sessions == 0


async def test_create_missing_agent(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.investigations.create(
            InvestigationCreateRequest(
                agent_id=uuid.uuid4(), name="investigation", sessions=[]
            )
        )


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an investigation by id through the SDK."""
    agent_id = await _make_agent(api_client)
    created = await api_client.investigations.create(
        InvestigationCreateRequest(agent_id=agent_id, name="investigation", sessions=[])
    )
    loaded = await api_client.investigations.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.investigations.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate investigations through the SDK."""
    agent_id = await _make_agent(api_client)
    for name in ["alpha", "beta", "gamma"]:
        await api_client.investigations.create(
            InvestigationCreateRequest(agent_id=agent_id, name=name, sessions=[])
        )

    page = await api_client.investigations.list(InvestigationListParams(size=2))
    assert len(page.items) == 2

    collected = [item.name async for item in api_client.investigations.iter()]
    assert collected == ["gamma", "beta", "alpha"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an investigation through the SDK."""
    agent_id = await _make_agent(api_client)
    created = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            description="old",
            sessions=[],
        )
    )
    updated = await api_client.investigations.update(
        created.id, InvestigationUpdateRequest(description="new")
    )
    assert updated.description == "new"

    updated = await api_client.investigations.update(
        created.id, InvestigationUpdateRequest(status=InvestigationStatus.COMPLETED)
    )
    assert updated.status is InvestigationStatus.COMPLETED
    assert updated.ended_at is not None


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an investigation through the SDK."""
    agent_id = await _make_agent(api_client)
    created = await api_client.investigations.create(
        InvestigationCreateRequest(agent_id=agent_id, name="investigation", sessions=[])
    )
    await api_client.investigations.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.investigations.get(created.id)


async def test_list_sessions_and_iter_sessions(api_client: KitaruAPIClient) -> None:
    """List and iterate an investigation's sessions, ordered by position."""
    agent_id = await _make_agent(api_client)
    session_ids = [await _make_session(api_client, agent_id) for _ in range(3)]
    created = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id, questions=[_question()]
                )
                for session_id in session_ids
            ],
        )
    )

    page = await api_client.investigations.list_sessions(created.id)
    assert isinstance(page.items[0], InvestigationSessionResponse)
    assert [item.session_id for item in page.items] == session_ids

    collected = [
        item.session_id
        async for item in api_client.investigations.iter_sessions(
            created.id, InvestigationSessionsListParams(size=1)
        )
    ]
    assert collected == session_ids


async def test_update_session(api_client: KitaruAPIClient) -> None:
    """Set an investigation session verdict through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    created = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id, questions=[_question()]
                )
            ],
        )
    )
    updated = await api_client.investigations.update_session(
        created.id,
        session_id,
        InvestigationSessionUpdateRequest(
            verdict=InvestigationSessionVerdict.ACCEPTABLE
        ),
    )
    assert updated.verdict is InvestigationSessionVerdict.ACCEPTABLE

    reloaded = await api_client.investigations.get(created.id)
    assert reloaded.status is InvestigationStatus.PENDING
    assert reloaded.completed_sessions == 1


async def test_update_session_clears_verdict(api_client: KitaruAPIClient) -> None:
    """Clear an investigation session verdict through the SDK."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    created = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id, questions=[_question()]
                )
            ],
        )
    )
    await api_client.investigations.update_session(
        created.id,
        session_id,
        InvestigationSessionUpdateRequest(
            verdict=InvestigationSessionVerdict.PROBLEMATIC
        ),
    )
    updated = await api_client.investigations.update_session(
        created.id,
        session_id,
        InvestigationSessionUpdateRequest(verdict=None),
    )
    assert updated.verdict is None

    reloaded = await api_client.investigations.get(created.id)
    assert reloaded.completed_sessions == 0


async def test_create_session_with_question(api_client: KitaruAPIClient) -> None:
    """Set a question on a session input and read it back."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    created = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id, questions=[_question()]
                )
            ],
        )
    )

    page = await api_client.investigations.list_sessions(created.id)
    assert page.items[0].questions[0].key == "cause"
    assert page.items[0].questions[0].question == "What caused it?"


async def test_create_session_with_highlights(api_client: KitaruAPIClient) -> None:
    """Set highlights on a session question and read them back."""
    agent_id = await _make_agent(api_client)
    session_id = await _make_session(api_client, agent_id)
    highlights = [
        InvestigationSessionHighlight(
            selector=AnnotationSelector(path="/output"), description="Root cause"
        )
    ]
    created = await api_client.investigations.create(
        InvestigationCreateRequest(
            agent_id=agent_id,
            name="investigation",
            sessions=[
                InvestigationSessionInput(
                    session_id=session_id, questions=[_question(highlights=highlights)]
                )
            ],
        )
    )

    page = await api_client.investigations.list_sessions(created.id)
    assert page.items[0].questions[0].highlights == highlights
