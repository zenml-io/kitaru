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

import asyncio
import json
from collections.abc import AsyncGenerator

import pytest
from pydantic import SecretStr
from tests.cli.workspace_push_fakes import (
    DESTINATION_URL,
    INSIGHT_NAMESPACE,
    SECRET_VALUE,
    SOURCE_URL,
    build_counts,
    build_workspace_client,
    count_records,
    ingest_session,
    push,
    seed_workspace,
)

from kitaru.api_models.v1.agent import AgentCreateRequest, AgentListParams
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionUpdateRequest,
)
from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightListParams,
    TextInsightData,
)
from kitaru.api_models.v1.session import (
    SessionListParams,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    SessionIngestRequest,
    SessionNodeListParams,
)
from kitaru.cli.output import CLIError
from kitaru.cli.workspace_push import (
    PUSH_IMPORTED_FROM,
)
from kitaru.cli.workspace_push_references import PUSH_METADATA_KEY
from kitaru.cli.workspace_push_report import (
    KINDS,
    build_push_result,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, AuthenticationError


@pytest.fixture
async def source() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client for the server pushed from."""
    async with build_workspace_client() as client:
        yield client


@pytest.fixture
async def destination() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide a client for the server pushed to."""
    async with build_workspace_client() as client:
        yield client


async def test_push_copies_records_and_rewrites_insight_references(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A push recreates the workspace and points insights at the copies."""
    seed = await seed_workspace(source)

    report = await push(source, destination)

    assert report.counts == {
        "agents": build_counts(copied=1),
        "agent_versions": build_counts(copied=1),
        "connections": build_counts(copied=2),
        "sessions": build_counts(copied=2, skipped=1),
        "insights": build_counts(copied=1),
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
    await seed_workspace(source)
    await push(source, destination)
    before = await count_records(destination)

    report = await push(source, destination)

    assert await count_records(destination) == before
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
    seed = await seed_workspace(source)
    await push(source, destination)
    added = await ingest_session(
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

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(
        copied=1, already_present=2, skipped=1
    )
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


async def test_push_copies_connections_without_their_secret_values(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A connection arrives with its non-secret values and names what it lacks."""
    await seed_workspace(source)

    report = await push(source, destination)

    command = (
        f"kitaru --server {DESTINATION_URL} connection update langfuse-prod "
        "--set-secret LANGFUSE_PUBLIC_KEY=<value> "
        "--set-secret LANGFUSE_SECRET_KEY=<value>"
    )
    assert report.counts["connections"] == build_counts(copied=2)
    assert report.incomplete_connections == [
        {
            "connection": "langfuse-prod",
            "missing_keys": ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"],
            "command": command,
        }
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
    assert f"`{command}`" in result.next_actions
    assert any("replaces every stored" in action for action in result.next_actions)
    assert SECRET_VALUE not in json.dumps([result.item, result.text, result.warnings])


async def test_push_reports_missing_secret_keys_until_they_are_set(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A rerun keeps naming the keys an earlier copy still lacks."""
    await seed_workspace(source)
    await push(source, destination)
    [copy] = [
        connection
        async for connection in destination.connections.iter()
        if connection.name == "langfuse-prod"
    ]
    await destination.connections.update(
        copy.id,
        ConnectionUpdateRequest(secrets={"LANGFUSE_PUBLIC_KEY": SecretStr("pk")}),
    )

    partial = await push(source, destination)

    assert partial.counts["connections"] == build_counts(already_present=2)
    assert partial.incomplete_connections == [
        {
            "connection": "langfuse-prod",
            "missing_keys": ["LANGFUSE_SECRET_KEY"],
            "command": f"kitaru --server {DESTINATION_URL} connection update "
            "langfuse-prod --set-secret LANGFUSE_PUBLIC_KEY=<value> "
            "--set-secret LANGFUSE_SECRET_KEY=<value>",
        }
    ]

    await destination.connections.update(
        copy.id,
        ConnectionUpdateRequest(
            secrets={
                "LANGFUSE_PUBLIC_KEY": SecretStr("pk"),
                "LANGFUSE_SECRET_KEY": SecretStr("sk"),
            }
        ),
    )
    complete = await push(source, destination)

    assert complete.incomplete_connections == []
    assert not any(
        "incomplete_connections" in warning
        for warning in build_push_result(complete).warnings
    )


async def test_push_keeps_the_default_connection_of_the_destination(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A copied connection does not displace the destination's default."""
    await seed_workspace(source)
    await destination.connections.create(
        ConnectionCreateRequest(
            name="langfuse-cloud", provider="langfuse", default=True
        )
    )

    report = await push(source, destination)

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
    await seed_workspace(source)

    report = await push(source, destination, dry_run=True)

    assert {kind: counts["copied"] for kind, counts in report.counts.items()} == {
        "agents": 1,
        "agent_versions": 1,
        "connections": 2,
        "sessions": 2,
        "insights": 1,
    }
    assert report.counts["sessions"]["skipped"] == 1
    assert await count_records(destination) == dict.fromkeys(KINDS, 0)
    assert [item["connection"] for item in report.incomplete_connections] == [
        "langfuse-prod"
    ]
    result = build_push_result(report)
    assert any("would lack secret values" in warning for warning in result.warnings)
    assert result.item["dry_run"] is True
    assert result.next_actions[0] == "Rerun without --dry-run to copy the records."
    assert result.text is not None
    assert result.text.startswith(f"Would copy from {SOURCE_URL}")


async def test_push_limits_agents_by_name(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Only the named agents are copied with their sessions and insights."""
    await seed_workspace(source)
    other = await source.agents.create(AgentCreateRequest(name="billing"))
    await ingest_session(
        source, other.id, imported_from="langfuse", external_id="trace-1"
    )

    report = await push(source, destination, agent_names=["billing"])

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
    await seed_workspace(source)

    with pytest.raises(CLIError) as raised:
        await push(source, destination, agent_names=["support", "missing"])

    assert raised.value.kind == "not_found"
    assert "missing" in raised.value.message
    assert await count_records(destination) == dict.fromkeys(KINDS, 0)


async def test_push_matches_an_agent_that_already_exists_by_name(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """Sessions land under the destination agent of the same name."""
    await seed_workspace(source)
    existing = await destination.agents.create(AgentCreateRequest(name="support"))
    await ingest_session(
        destination, existing.id, imported_from="langfuse", external_id="trace-42"
    )

    report = await push(source, destination)

    assert report.counts["agents"]["already_present"] == 1
    assert report.counts["sessions"] == build_counts(
        copied=1, already_present=1, skipped=1
    )
    assert {session.agent_id async for session in destination.sessions.iter()} == {
        existing.id
    }


async def test_push_resolves_a_session_created_after_the_destination_listing(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A conflict on ingest maps the session the destination already holds."""
    await seed_workspace(source)
    existing = await destination.agents.create(AgentCreateRequest(name="support"))
    present = await ingest_session(
        destination, existing.id, imported_from="langfuse", external_id="trace-42"
    )

    async def iterate_nothing(
        params: SessionListParams | None = None,
    ) -> AsyncGenerator[SessionResponse, None]:
        return
        yield

    monkeypatch.setattr(destination.sessions, "iter", iterate_nothing)

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(
        copied=1, already_present=1, skipped=1
    )
    [insight] = [insight async for insight in destination.insights.iter()]
    assert insight.description == f"Seen in {present.id}."


async def test_push_matches_versions_by_number_and_appends_the_rest(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A version is created only under the number its source carries."""
    seed = await seed_workspace(source)
    for number in range(2, 6):
        await source.agents.create_version(
            seed.agent_id, AgentVersionCreateRequest(display_version=f"v{number}")
        )
    existing = await destination.agents.create(AgentCreateRequest(name="support"))
    for _ in range(3):
        await destination.agents.create_version(
            existing.id, AgentVersionCreateRequest()
        )

    report = await push(source, destination)

    assert report.counts["agent_versions"] == build_counts(copied=2, already_present=3)
    versions = {
        version.version: version.display_version
        async for version in destination.agents.iter_versions(existing.id)
    }
    assert versions == {1: None, 2: None, 3: None, 4: "v4", 5: "v5"}


async def test_push_copies_the_nodes_of_a_recorded_session(
    source: KitaruAPIClient, destination: KitaruAPIClient
) -> None:
    """A settled recorded session arrives with its nodes."""
    seed = await seed_workspace(source)

    await push(source, destination)

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


async def test_push_copies_many_sessions_and_isolates_one_failure(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent copies all land, and one rejected copy fails alone."""
    agent = await source.agents.create(AgentCreateRequest(name="bulk"))
    for index in range(30):
        await ingest_session(
            source, agent.id, imported_from="langfuse", external_id=f"trace-{index}"
        )
    ingest = destination.sessions.ingest
    running = 0
    peak = 0

    async def ingest_most(request: SessionIngestRequest) -> SessionResponse:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        try:
            await asyncio.sleep(0)
            if request.session.external_id == "trace-7":
                raise APIError(500, "boom")
            return await ingest(request)
        finally:
            running -= 1

    monkeypatch.setattr(destination.sessions, "ingest", ingest_most)

    report = await push(source, destination)

    assert report.counts["sessions"] == build_counts(copied=29, failed=1)
    assert [item["error"] for item in report.failed] == ["HTTP 500: boom"]
    assert 1 < peak <= 8
    monkeypatch.undo()
    retried = await push(source, destination)
    assert retried.counts["sessions"] == build_counts(copied=1, already_present=29)
    assert len([s async for s in destination.sessions.iter()]) == 30


async def test_push_stops_on_a_rejected_credential(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A credential rejected mid-copy ends the push with that one error."""
    await seed_workspace(source)

    async def reject(request: SessionIngestRequest) -> SessionResponse:
        raise AuthenticationError(401, "Token expired")

    monkeypatch.setattr(destination.sessions, "ingest", reject)

    with pytest.raises(AuthenticationError):
        await push(source, destination)
