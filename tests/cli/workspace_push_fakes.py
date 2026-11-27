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
"""Fake-backed servers and seeded records for workspace push tests."""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import SecretStr

from conftest import asgi_api_client
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
)
from kitaru.api_models.v1.insight import (
    InsightInput,
    TextInsightData,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionIngestRequest,
    SessionNodeCreateRequest,
)
from kitaru.cli.workspace_push import (
    push_workspace,
)
from kitaru.cli.workspace_push_report import (
    PushReport,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_agent_service,
    get_agent_version_service,
    get_connection_service,
    get_insight_service,
    get_secret_service,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.connection_service import ConnectionService
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru_test_support import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeConnectionRepository,
    FakeImportRepository,
    FakeInsightRepository,
    FakeReplayRepository,
    FakeSecretRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    build_payload_store,
    override_idempotency,
)

SOURCE_URL = "http://localhost:8000"
DESTINATION_URL = "https://acme.cloud.example.com"
SECRET_VALUE = "sk-lf-do-not-print"
INSIGHT_NAMESPACE = "kitaru.insights/v1"


def build_counts(
    *, copied: int = 0, already_present: int = 0, skipped: int = 0, failed: int = 0
) -> dict[str, int]:
    """Build the outcome counts of one kind of record."""
    return {
        "copied": copied,
        "already_present": already_present,
        "skipped": skipped,
        "failed": failed,
    }


def _provide(service: Any) -> Callable[[], Any]:
    """Build a dependency override returning one shared service."""
    return lambda: service


def build_workspace_client() -> KitaruAPIClient:
    """Build a client routed to one fake-backed in-process server."""
    account = Account(id=uuid.uuid4(), name="ann")
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agents = FakeAgentRepository()
    agent_versions = FakeAgentVersionRepository(agents)
    connections = FakeConnectionRepository()
    secrets = FakeSecretRepository(connections=connections)
    sessions = FakeSessionRepository()
    payload_store = build_payload_store().store
    session_service = SessionService(
        repository=sessions,
        task_repository=FakeTaskRepository(),
        agent_version_repository=agent_versions,
        replay_repository=FakeReplayRepository(),
        import_repository=FakeImportRepository(),
        payload_store=payload_store,
    )
    node_service = SessionNodeService(
        repository=FakeSessionNodeRepository(),
        session_repository=sessions,
        task_repository=FakeTaskRepository(),
        payload_store=payload_store,
    )
    services: dict[Any, Any] = {
        get_agent_service: AgentService(repository=agents),
        get_agent_version_service: AgentVersionService(repository=agent_versions),
        get_secret_service: SecretService(repository=secrets),
        get_connection_service: ConnectionService(
            repository=connections, secret_repository=secrets
        ),
        get_session_service: session_service,
        get_session_node_service: node_service,
        get_insight_service: InsightService(
            repository=FakeInsightRepository(), agent_repository=agents
        ),
    }
    for dependency, service in services.items():
        app.dependency_overrides[dependency] = _provide(service)
    app.dependency_overrides[authorize] = lambda: AuthContext(account=account)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=account)
    override_idempotency(app, account)
    return asgi_api_client(app)


@dataclass
class Seed:
    """Ids of the records seeded on the source."""

    agent_id: uuid.UUID
    imported_session_id: uuid.UUID
    recorded_session_id: uuid.UUID
    in_progress_session_id: uuid.UUID
    child_node_id: uuid.UUID
    insight_id: uuid.UUID
    stray_id: uuid.UUID


def build_node(external_id: str, parent: str | None = None) -> SessionNodeCreateRequest:
    """Build one completed LLM call node."""
    return SessionNodeCreateRequest(
        external_id=external_id,
        parent_external_id=parent,
        node_type=NodeType.LLM_CALL,
        name=f"call {external_id}",
        status=NodeStatus.COMPLETED,
        inputs=[{"role": "user", "content": "hello"}],
        outputs=[{"role": "assistant", "content": "world"}],
        output_text_selector="/0/content",
        model="claude-haiku-4-5",
        attributes={"span": external_id},
    )


async def ingest_session(
    client: KitaruAPIClient,
    agent_id: uuid.UUID,
    agent_version_id: uuid.UUID | None = None,
    **fields: Any,
) -> SessionResponse:
    """Ingest a completed two-node session on a server."""
    values: dict[str, Any] = {
        "agent_id": agent_id,
        "agent_version_id": agent_version_id,
        "origin": SessionOrigin.IMPORTED,
        "status": SessionStatus.COMPLETED,
        "name": "Weather request",
        "inputs": {"question": "weather?"},
        "outputs": {"answer": "rain"},
        "metadata": {"environment": "production"},
    }
    values.update(fields)
    return await client.sessions.ingest(
        SessionIngestRequest(
            session=SessionCreateRequest(**values),
            nodes=[build_node("root"), build_node("child", parent="root")],
        )
    )


async def seed_workspace(source: KitaruAPIClient) -> Seed:
    """Fill the source with one agent and every kind of copied record."""
    agent = await source.agents.create(
        AgentCreateRequest(name="support", description="Support agent")
    )
    version = await source.agents.create_version(
        agent.id,
        AgentVersionCreateRequest(
            display_version="v1", run_spec=RunSpec(command="python agent.py")
        ),
    )
    await source.connections.create(
        ConnectionCreateRequest(
            name="langfuse-prod",
            provider="langfuse",
            env={"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"},
            secrets={
                "LANGFUSE_PUBLIC_KEY": SecretStr("pk-lf"),
                "LANGFUSE_SECRET_KEY": SecretStr(SECRET_VALUE),
            },
            default=True,
        )
    )
    await source.connections.create(
        ConnectionCreateRequest(
            name="internal-tracing",
            provider="internal",
            env={"BASE_URL": "https://tracing.example.com"},
        )
    )
    imported = await ingest_session(
        source,
        agent.id,
        version.id,
        imported_from="langfuse",
        external_id="trace-42",
        framework="pydantic-ai",
    )
    # A recorded session accepts nodes only while it is in progress.
    recorded = await ingest_session(
        source,
        agent.id,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.IN_PROGRESS,
    )
    await source.sessions.update(
        recorded.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )
    in_progress = await source.sessions.create(
        SessionCreateRequest(
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.IN_PROGRESS,
            inputs=None,
            outputs=None,
        )
    )
    nodes = [node async for node in source.sessions.iter_nodes(imported.id)]
    child = next(node for node in nodes if node.external_id == "child")
    stray_id = uuid.uuid4()
    [insight] = await source.insights.create(
        agent.id,
        [
            InsightInput(
                name="tool-retries",
                title="Tool retries",
                description=f"Seen in {imported.id}.",
                data=TextInsightData(content=f"Start with session {recorded.id}."),
                metadata={
                    INSIGHT_NAMESPACE: {
                        "contributing_session_ids": [
                            str(imported.id),
                            str(recorded.id),
                            str(in_progress.id),
                        ],
                        "evidence": [
                            {
                                "session_id": str(imported.id),
                                "node_id": str(child.id),
                                "signal": "retry",
                            }
                        ],
                        "context": {
                            "agent_id": str(agent.id),
                            "server_url": SOURCE_URL,
                            "source_import": {"import_id": str(stray_id)},
                        },
                        "investigation_prompt": f"Run `kitaru login {SOURCE_URL}`. "
                        f'Finding data: {{"node":"{child.id}"}}',
                    }
                },
            )
        ],
    )
    return Seed(
        agent_id=agent.id,
        imported_session_id=imported.id,
        recorded_session_id=recorded.id,
        in_progress_session_id=in_progress.id,
        child_node_id=child.id,
        insight_id=insight.id,
        stray_id=stray_id,
    )


async def push(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    *,
    agent_names: list[str] | None = None,
    dry_run: bool = False,
) -> PushReport:
    """Push the source workspace to the destination."""
    return await push_workspace(
        source,
        destination,
        source_url=SOURCE_URL,
        destination_url=DESTINATION_URL,
        agent_names=agent_names,
        dry_run=dry_run,
    )


async def count_records(client: KitaruAPIClient) -> dict[str, int]:
    """Count every kind of pushed record stored on a server."""
    agents = [agent async for agent in client.agents.iter()]
    versions = 0
    for agent in agents:
        versions += len([v async for v in client.agents.iter_versions(agent.id)])
    return {
        "agents": len(agents),
        "agent_versions": versions,
        "connections": len([c async for c in client.connections.iter()]),
        "sessions": len([s async for s in client.sessions.iter()]),
        "insights": len([i async for i in client.insights.iter()]),
    }
