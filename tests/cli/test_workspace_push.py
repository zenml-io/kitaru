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
"""Workspace push between two in-process servers through the SDK."""

import json
import uuid
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import SecretStr

from conftest import asgi_api_client
from kitaru.api_models.v1.agent import AgentCreateRequest, AgentListParams
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
)
from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightListParams,
    TextInsightData,
)
from kitaru.api_models.v1.secret import SecretWithValuesResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionListParams,
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
    SessionNodeListParams,
)
from kitaru.cli import app as app_module
from kitaru.cli.output import CLIError
from kitaru.cli.workspace_push import (
    PUSH_IMPORTED_FROM,
    get_session_identity,
    push_workspace,
)
from kitaru.cli.workspace_push_report import PushReport, build_push_result
from kitaru.cli.workspace_references import PUSH_METADATA_KEY
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


def _provide(service: Any) -> Callable[[], Any]:
    """Build a dependency override returning one shared service."""
    return lambda: service


def _build_workspace_client() -> KitaruAPIClient:
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


@pytest.fixture
async def source() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client for the server pushed from."""
    async with _build_workspace_client() as client:
        yield client


@pytest.fixture
async def destination() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client for the server pushed to."""
    async with _build_workspace_client() as client:
        yield client


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


def _build_node(
    external_id: str, parent: str | None = None
) -> SessionNodeCreateRequest:
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


async def _ingest(
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
            nodes=[_build_node("root"), _build_node("child", parent="root")],
        )
    )


async def _seed(source: KitaruAPIClient) -> Seed:
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
            secrets={"LANGFUSE_SECRET_KEY": SecretStr(SECRET_VALUE)},
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
    imported = await _ingest(
        source,
        agent.id,
        version.id,
        imported_from="langfuse",
        external_id="trace-42",
        framework="pydantic-ai",
    )
    # A recorded session accepts nodes only while it is in progress.
    recorded = await _ingest(
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


async def _push(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    *,
    agent_names: list[str] | None = None,
    copy_secrets: bool = True,
    dry_run: bool = False,
) -> PushReport:
    """Push the source workspace to the destination."""
    return await push_workspace(
        source,
        destination,
        source_url=SOURCE_URL,
        destination_url=DESTINATION_URL,
        agent_names=agent_names,
        copy_secrets=copy_secrets,
        dry_run=dry_run,
    )


async def _count_records(client: KitaruAPIClient) -> dict[str, int]:
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


async def test_push_copies_records_and_rewrites_insight_references(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A push recreates the workspace and points insights at the copies."""
    seed = await _seed(source)

    report = await _push(source, destination)

    assert report.counts == {
        "agents": {"copied": 1, "already_present": 0, "skipped": 0, "failed": 0},
        "agent_versions": {
            "copied": 1,
            "already_present": 0,
            "skipped": 0,
            "failed": 0,
        },
        "connections": {
            "copied": 2,
            "already_present": 0,
            "skipped": 0,
            "failed": 0,
        },
        "sessions": {"copied": 2, "already_present": 0, "skipped": 1, "failed": 0},
        "insights": {"copied": 1, "already_present": 0, "skipped": 0, "failed": 0},
    }
    assert report.skipped == [
        {
            "kind": "sessions",
            "record": str(seed.in_progress_session_id),
            "reason": "The session is still in progress on the source.",
        }
    ]
    [agent] = [agent async for agent in destination.agents.iter()]
    assert (agent.name, agent.description) == ("support", "Support agent")
    assert agent.id != seed.agent_id
    [version] = [v async for v in destination.agents.iter_versions(agent.id)]
    assert (version.version, version.display_version) == (1, "v1")

    sessions = {
        (session.imported_from, session.external_id): session
        async for session in destination.sessions.iter(
            SessionListParams(include_payloads=True)
        )
    }
    imported = sessions[("langfuse", "trace-42")]
    recorded = sessions[(PUSH_IMPORTED_FROM, str(seed.recorded_session_id))]
    assert imported.agent_id == agent.id
    assert imported.agent_version_id == version.id
    assert imported.origin == SessionOrigin.IMPORTED
    assert imported.framework == "pydantic-ai"
    assert imported.metadata == {"environment": "production"}
    assert recorded.origin == SessionOrigin.RECORDED
    full = await destination.sessions.get_with_nodes(imported.id)
    assert full.session.inputs == {"question": "weather?"}
    assert full.session.outputs == {"answer": "rain"}
    nodes = {node.external_id: node for node in full.nodes}
    assert nodes["child"].parent_external_id == "root"
    assert nodes["child"].outputs == [{"role": "assistant", "content": "world"}]
    assert nodes["child"].output_text_selector == "/0/content"
    assert nodes["child"].attributes == {"span": "child"}
    assert nodes["child"].id != seed.child_node_id

    [insight] = [insight async for insight in destination.insights.iter()]
    assert insight.agent_id == agent.id
    assert insight.description == f"Seen in {imported.id}."
    assert insight.data == TextInsightData(content=f"Start with session {recorded.id}.")
    assert insight.metadata[INSIGHT_NAMESPACE] == {
        "contributing_session_ids": [
            str(imported.id),
            str(recorded.id),
            str(seed.in_progress_session_id),
        ],
        "evidence": [
            {
                "session_id": str(imported.id),
                "node_id": str(nodes["child"].id),
                "signal": "retry",
            }
        ],
        "context": {
            "agent_id": str(agent.id),
            "server_url": DESTINATION_URL,
            "source_import": {"import_id": str(seed.stray_id)},
        },
        "investigation_prompt": f"Run `kitaru login {DESTINATION_URL}`. "
        f'Finding data: {{"node":"{nodes["child"].id}"}}',
    }
    unmapped = sorted(
        [
            {"id": str(seed.in_progress_session_id), "kind": "session"},
            {"id": str(seed.stray_id), "kind": "unknown"},
        ],
        key=lambda reference: reference["id"],
    )
    assert insight.metadata[PUSH_METADATA_KEY] == {
        "source_insight_id": str(seed.insight_id),
        "source_server": SOURCE_URL,
        "unmapped_references": unmapped,
    }
    assert report.unmapped_references == [
        {
            "insight": "tool-retries",
            "source_insight_id": str(seed.insight_id),
            "references": unmapped,
        }
    ]


async def test_push_twice_copies_nothing_the_second_time(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A repeated push finds every record already on the destination."""
    await _seed(source)
    await _push(source, destination)
    before = await _count_records(destination)

    report = await _push(source, destination)

    assert await _count_records(destination) == before
    assert before == {
        "agents": 1,
        "agent_versions": 1,
        "connections": 2,
        "sessions": 2,
        "insights": 1,
    }
    assert {kind: counts["copied"] for kind, counts in report.counts.items()} == (
        dict.fromkeys(report.counts, 0)
    )
    assert {
        kind: counts["already_present"] for kind, counts in report.counts.items()
    } == before
    assert report.failures == 0


async def test_push_again_copies_only_new_records_and_maps_old_sessions(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A later push adds new records and resolves references to earlier copies."""
    seed = await _seed(source)
    await _push(source, destination)
    added = await _ingest(
        source, seed.agent_id, imported_from="langfuse", external_id="trace-43"
    )
    await source.insights.create(
        seed.agent_id,
        [
            InsightInput(
                name="follow-up",
                title="Follow-up",
                data=TextInsightData(
                    content=f"{seed.imported_session_id} then {added.id}"
                ),
            )
        ],
    )

    report = await _push(source, destination)

    assert report.counts["sessions"] == {
        "copied": 1,
        "already_present": 2,
        "skipped": 1,
        "failed": 0,
    }
    assert report.counts["insights"]["copied"] == 1
    assert report.counts["insights"]["already_present"] == 1
    assert report.unmapped_references == []
    sessions = {
        session.external_id: session.id async for session in destination.sessions.iter()
    }
    [follow_up] = [
        insight
        async for insight in destination.insights.iter()
        if insight.name == "follow-up"
    ]
    assert follow_up.data == TextInsightData(
        content=f"{sessions['trace-42']} then {sessions['trace-43']}"
    )


async def test_push_reports_connections_whose_secret_values_stay_hidden(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A connection copies without the values the source does not return."""
    await _seed(source)

    report = await _push(source, destination)

    assert report.incomplete_connections == [
        {"connection": "langfuse-prod", "missing_keys": ["LANGFUSE_SECRET_KEY"]}
    ]
    connections = {
        connection.name: connection
        async for connection in destination.connections.iter(ConnectionListParams())
    }
    assert connections["langfuse-prod"].env == {
        "LANGFUSE_BASE_URL": "https://cloud.langfuse.com"
    }
    assert connections["langfuse-prod"].secret_keys == []
    assert connections["langfuse-prod"].default is True
    assert connections["internal-tracing"].provider == "internal"
    result = build_push_result(report)
    assert any("incomplete_connections" in warning for warning in result.warnings)
    assert any("--set-secret" in action for action in result.next_actions)
    assert SECRET_VALUE not in json.dumps([result.item, result.text, result.warnings])


async def test_push_copies_secret_values_a_source_returns(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Values a source does return reach the destination and no output."""
    await _seed(source)

    async def get_secret(
        secret_id: uuid.UUID, include_values: bool = False
    ) -> SecretWithValuesResponse:
        assert include_values is True
        return SecretWithValuesResponse.model_construct(
            id=secret_id, values={"LANGFUSE_SECRET_KEY": SecretStr(SECRET_VALUE)}
        )

    monkeypatch.setattr(source.secrets, "get", get_secret)

    report = await _push(source, destination)

    assert report.incomplete_connections == []
    connections = {
        connection.name: connection
        async for connection in destination.connections.iter()
    }
    assert connections["langfuse-prod"].secret_keys == ["LANGFUSE_SECRET_KEY"]
    result = build_push_result(report)
    assert SECRET_VALUE not in json.dumps([result.item, result.text, result.warnings])


async def test_push_without_copy_secrets_skips_connections_holding_secrets(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Only a connection without secret values is copied by default."""
    await _seed(source)

    report = await _push(source, destination, copy_secrets=False)

    assert report.counts["connections"] == {
        "copied": 1,
        "already_present": 0,
        "skipped": 1,
        "failed": 0,
    }
    assert [connection.name async for connection in destination.connections.iter()] == [
        "internal-tracing"
    ]
    [skipped] = [item for item in report.skipped if item["kind"] == "connections"]
    assert skipped["record"] == "langfuse-prod"
    assert "--copy-secrets" in skipped["reason"]
    assert any(
        "--copy-secrets" in action for action in build_push_result(report).next_actions
    )


async def test_push_keeps_the_default_connection_of_the_destination(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A copied connection does not displace the destination's default."""
    await _seed(source)
    await destination.connections.create(
        ConnectionCreateRequest(
            name="langfuse-cloud", provider="langfuse", default=True
        )
    )

    report = await _push(source, destination)

    assert report.failures == 0
    defaults = {
        connection.name: connection.default
        async for connection in destination.connections.iter()
    }
    assert defaults == {
        "langfuse-cloud": True,
        "langfuse-prod": False,
        "internal-tracing": False,
    }


async def test_push_dry_run_writes_nothing(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A dry run counts what a push would copy and leaves the destination empty."""
    await _seed(source)

    report = await _push(source, destination, dry_run=True)

    assert {kind: counts["copied"] for kind, counts in report.counts.items()} == {
        "agents": 1,
        "agent_versions": 1,
        "connections": 2,
        "sessions": 2,
        "insights": 1,
    }
    assert report.counts["sessions"]["skipped"] == 1
    assert await _count_records(destination) == dict.fromkeys(report.counts, 0)
    result = build_push_result(report)
    assert result.item["dry_run"] is True
    assert result.next_actions[0] == "Rerun without --dry-run to copy the records."
    assert result.text is not None
    assert result.text.startswith(f"Would copy from {SOURCE_URL}")


async def test_push_limits_agents_by_name(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Only the named agents are copied with their sessions and insights."""
    await _seed(source)
    other = await source.agents.create(AgentCreateRequest(name="billing"))
    await _ingest(source, other.id, imported_from="langfuse", external_id="trace-1")

    report = await _push(source, destination, agent_names=["billing"])

    assert [
        agent.name async for agent in destination.agents.iter(AgentListParams())
    ] == ["billing"]
    assert report.counts["sessions"]["copied"] == 1
    assert report.counts["insights"]["copied"] == 0
    assert [
        insight async for insight in destination.insights.iter(InsightListParams())
    ] == []


async def test_push_rejects_an_unknown_agent_before_writing(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Naming an agent the source lacks fails without touching the destination."""
    await _seed(source)

    with pytest.raises(CLIError) as raised:
        await _push(source, destination, agent_names=["support", "missing"])

    assert raised.value.kind == "not_found"
    assert "missing" in raised.value.message
    assert await _count_records(destination) == dict.fromkeys(
        ("agents", "agent_versions", "connections", "sessions", "insights"), 0
    )


async def test_push_matches_an_agent_that_already_exists_by_name(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Sessions land under the destination agent of the same name."""
    await _seed(source)
    existing = await destination.agents.create(AgentCreateRequest(name="support"))
    await _ingest(
        destination, existing.id, imported_from="langfuse", external_id="trace-42"
    )

    report = await _push(source, destination)

    assert report.counts["agents"]["already_present"] == 1
    assert report.counts["sessions"] == {
        "copied": 1,
        "already_present": 1,
        "skipped": 1,
        "failed": 0,
    }
    assert {session.agent_id async for session in destination.sessions.iter()} == {
        existing.id
    }


async def test_push_resolves_a_session_created_after_the_destination_listing(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A conflict on ingest maps the session the destination already holds."""
    await _seed(source)
    existing = await destination.agents.create(AgentCreateRequest(name="support"))
    present = await _ingest(
        destination, existing.id, imported_from="langfuse", external_id="trace-42"
    )

    async def iterate_nothing(
        params: SessionListParams | None = None,
    ) -> AsyncGenerator[SessionResponse, None]:
        for session in ():
            yield session

    monkeypatch.setattr(destination.sessions, "iter", iterate_nothing)

    report = await _push(source, destination)

    assert report.counts["sessions"] == {
        "copied": 1,
        "already_present": 1,
        "skipped": 1,
        "failed": 0,
    }
    [insight] = [insight async for insight in destination.insights.iter()]
    assert insight.description == f"Seen in {present.id}."


async def test_push_matches_versions_by_number_and_appends_the_rest(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A version is created only under the number its source carries."""
    seed = await _seed(source)
    for number in range(2, 6):
        await source.agents.create_version(
            seed.agent_id, AgentVersionCreateRequest(display_version=f"v{number}")
        )
    existing = await destination.agents.create(AgentCreateRequest(name="support"))
    for _ in range(3):
        await destination.agents.create_version(
            existing.id, AgentVersionCreateRequest()
        )

    report = await _push(source, destination)

    assert report.counts["agent_versions"] == {
        "copied": 2,
        "already_present": 3,
        "skipped": 0,
        "failed": 0,
    }
    versions = {
        version.version: version.display_version
        async for version in destination.agents.iter_versions(existing.id)
    }
    assert versions == {1: None, 2: None, 3: None, 4: "v4", 5: "v5"}


async def test_push_copies_the_nodes_of_a_recorded_session(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A settled recorded session arrives with its nodes."""
    seed = await _seed(source)

    await _push(source, destination)

    [recorded] = [
        session
        async for session in destination.sessions.iter()
        if session.imported_from == PUSH_IMPORTED_FROM
    ]
    nodes = [
        node
        async for node in destination.sessions.iter_nodes(
            recorded.id, SessionNodeListParams()
        )
    ]
    assert {node.external_id for node in nodes} == {"root", "child"}
    assert recorded.external_id == str(seed.recorded_session_id)
    assert recorded.status == SessionStatus.COMPLETED


def test_session_identity_keeps_source_identity_and_fills_gaps() -> None:
    """The copy keeps a source identity and derives a stable one otherwise."""
    session_id = uuid.uuid4()

    def build(**fields: Any) -> SessionResponse:
        return SessionResponse.model_construct(id=session_id, **fields)

    assert get_session_identity(
        build(imported_from="langfuse", external_id="trace-42")
    ) == ("langfuse", "trace-42")
    assert get_session_identity(build(imported_from=None, external_id=None)) == (
        PUSH_IMPORTED_FROM,
        str(session_id),
    )
    assert get_session_identity(build(imported_from=None, external_id="run-7")) == (
        PUSH_IMPORTED_FROM,
        "run-7",
    )
    assert get_session_identity(build(imported_from="langfuse", external_id=None)) == (
        "langfuse",
        str(session_id),
    )


def test_push_result_fails_when_a_record_failed() -> None:
    """A failed record turns the report into a retryable partial failure."""
    report = PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)
    report.count("sessions", "copied", 3)
    report.fail("sessions", "abc", "HTTP 500: boom")

    with pytest.raises(CLIError) as raised:
        build_push_result(report)

    assert raised.value.kind == "partial_failure"
    assert raised.value.retryable is True
    assert raised.value.details["counts"]["sessions"]["failed"] == 1
    assert raised.value.details["failed"] == [
        {"kind": "sessions", "record": "abc", "error": "HTTP 500: boom"}
    ]


def test_push_result_names_what_is_not_copied() -> None:
    """A successful push tells the caller what remains to recreate."""
    result = build_push_result(PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False))

    assert result.item["not_copied"][:2] == ["cohorts", "evaluators"]
    assert "cohorts, evaluators" in result.next_actions[-1]
    assert DESTINATION_URL in result.next_actions[0]


def test_report_bounds_its_detail_lists() -> None:
    """Counts stay complete when more records are skipped than are listed."""
    report = PushReport(SOURCE_URL, DESTINATION_URL, dry_run=False)
    for index in range(250):
        report.skip("sessions", str(index), "The session is still in progress.")

    assert report.counts["sessions"]["skipped"] == 250
    assert len(report.skipped) == 200
    assert report.details_truncated is True


def test_push_command_requires_a_stored_destination_credential(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The source's environment key is never used for the destination."""
    monkeypatch.setenv("KITARU_API_KEY", "KITKEY_source")

    code = app_module.main(
        [
            "--output",
            "json",
            "--server",
            SOURCE_URL,
            "workspace",
            "push",
            "--to",
            DESTINATION_URL,
        ]
    )

    error = json.loads(capsys.readouterr().err)["error"]
    assert code == 3
    assert error["kind"] == "authentication_failed"
    assert DESTINATION_URL in error["message"]
    assert f"kitaru login {DESTINATION_URL}" in error["hint"]


def test_push_command_rejects_the_source_as_destination(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Pushing a server to itself is refused."""
    code = app_module.main(
        [
            "--output",
            "json",
            "--server",
            SOURCE_URL,
            "workspace",
            "push",
            "--to",
            f"{SOURCE_URL}/",
        ]
    )

    assert code == 2
    assert json.loads(capsys.readouterr().err)["error"]["kind"] == "invalid_arguments"
