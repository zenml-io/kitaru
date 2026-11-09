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
"""Round-trip tests for atomic session ingestion."""

import uuid
from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from conftest import asgi_api_client
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionIngestRequest,
    SessionNodeCreateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru_test_support import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    build_payload_store,
    override_idempotency,
)

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


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
    session_repository = FakeSessionRepository()
    node_repository = FakeSessionNodeRepository()
    payload_store = build_payload_store().store
    session_service = SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(FakeAgentRepository()),
        replay_repository=FakeReplayRepository(),
        import_repository=FakeImportRepository(),
        payload_store=payload_store,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=FakeTaskRepository(),
        payload_store=payload_store,
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_ingest_round_trip(api_client: KitaruAPIClient) -> None:
    """Create the complete imported session with rollups through the SDK."""
    request = SessionIngestRequest(
        session=SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            inputs={"prompt": "hello"},
            outputs={"answer": "world"},
            imported_from="langfuse",
            external_id="project:session",
        ),
        nodes=[
            SessionNodeCreateRequest(
                external_id="llm-1",
                node_type=NodeType.LLM_CALL,
                name="model call",
                status=NodeStatus.COMPLETED,
                inputs="hello",
                outputs="world",
                attributes=None,
                tokens=TokenUsage(input_tokens=3, output_tokens=5),
                cost=Decimal("0.125"),
            )
        ],
    )
    created = await api_client.sessions.ingest(request)
    assert created.external_id == "project:session"
    assert created.llm_call_count == 1
    assert created.tokens is not None
    assert created.tokens.input_tokens == 3
    assert created.tokens.output_tokens == 5
    assert created.cost == Decimal("0.125")
    full = await api_client.sessions.get_with_nodes(created.id)
    assert full.session.inputs == {"prompt": "hello"}
    assert [node.outputs for node in full.nodes] == ["world"]
    with pytest.raises(APIError) as caught:
        await api_client.sessions.ingest(request)
    assert caught.value.status_code == 409
