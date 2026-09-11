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
"""Session import and read-only inspection CLI behavior."""

import json
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.imports import (
    ApiImportSource,
    BlobImportSource,
    ImportResponse,
)
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.session import (
    SessionListParams,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import SessionNodeListParams
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskOnFailure,
    TaskResponse,
    TaskStatus,
)
from kitaru.cli import app as app_module
from kitaru.cli import sessions
from kitaru.cli.output import CLIError
from kitaru.client.exceptions import APIError


@dataclass
class StubModel:
    """Small response exposing the Pydantic serialization surface."""

    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), **self.values}


def _job(status: JobStatus = JobStatus.PENDING) -> JobResponse:
    """Build one job response for import tests."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        kind=JobKind.IMPORT,
        status=status,
        started_at=now if status is not JobStatus.PENDING else None,
        ended_at=now
        if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
        else None,
        error="import failed" if status is JobStatus.FAILED else None,
    )


def _task(
    job: JobResponse,
    *,
    status: TaskStatus = TaskStatus.COMPLETED,
    kind: TaskKind = TaskKind.IMPORTER,
    result: Any = None,
) -> TaskResponse:
    """Build one terminal task response for import tests."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return TaskResponse(
        id=uuid.uuid4(),
        job_id=job.id,
        kind=kind,
        status=status,
        on_failure=TaskOnFailure.ABORT,
        attempt=1,
        labels={},
        error="parser crashed" if status is TaskStatus.FAILED else None,
        result=result,
        created=now,
        updated=now,
    )


class StubImportClient:
    """Protocol-shaped client recording the two import mutation phases."""

    def __init__(self, *, create_error: Exception | None = None) -> None:
        self.importer = SimpleNamespace(id=uuid.uuid4(), name="jsonl", latest_version=2)
        self.importer_version = SimpleNamespace(
            id=uuid.uuid4(), importer_id=self.importer.id, version=2
        )
        self.agent = SimpleNamespace(
            id=uuid.uuid4(), name="assistant", latest_version=3
        )
        self.agent_version = SimpleNamespace(
            id=uuid.uuid4(), agent_id=self.agent.id, version=3
        )
        self.evaluator = SimpleNamespace(
            id=uuid.uuid4(), name="quality", latest_version=3
        )
        self.evaluator_version = SimpleNamespace(
            id=uuid.uuid4(), evaluator_id=self.evaluator.id, version=3
        )
        self.analyzer = SimpleNamespace(
            id=uuid.uuid4(), name="clustering", latest_version=2
        )
        self.analyzer_version = SimpleNamespace(
            id=uuid.uuid4(), analyzer_id=self.analyzer.id, version=2
        )
        self.blob = SimpleNamespace(
            id=uuid.uuid4(), sha256="a" * 64, size=7, media_type="application/jsonl"
        )
        self.connection = SimpleNamespace(id=uuid.uuid4(), name="langfuse-prod")
        self.job = _job()
        now = datetime(2026, 8, 3, tzinfo=UTC)
        self.import_response = ImportResponse(
            id=uuid.uuid4(),
            owner_id=self.job.owner_id,
            job_id=self.job.id,
            agent_id=self.agent.id,
            agent_version_id=self.agent_version.id,
            importer_version_id=self.importer_version.id,
            source=BlobImportSource(blob_id=self.blob.id),
            params={},
            evaluators=[],
            analyzers=[],
            created=now,
            updated=now,
        )
        self.uploads: list[tuple[bytes, str, str | None]] = []
        self.requests: list[Any] = []
        self.create_idempotency_keys: list[str | None] = []
        self.lookup_calls: list[str] = []
        self.job_get_calls: list[uuid.UUID] = []
        self.create_error = create_error
        self.importers = self._Importers(self)
        self.agents = self._Agents(self)
        self.evaluators = self._Evaluators(self)
        self.analyzers = self._Analyzers(self)
        self.blobs = self._Blobs(self)
        self.connections = self._Connections(self)
        self.imports = self._Imports(self)
        self.jobs = self._Jobs(self)

    class _Importers:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.lookup_calls.append("importer")
            yield self.owner.importer

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.lookup_calls.append("importer")
            return SimpleNamespace(items=[self.owner.importer], next_cursor=None)

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            assert parent_id == self.owner.importer.id
            assert version == self.owner.importer_version.version
            return self.owner.importer_version

    class _Connections:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.lookup_calls.append("connection")
            return SimpleNamespace(items=[self.owner.connection], next_cursor=None)

    class _Agents:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.lookup_calls.append("agent")
            yield self.owner.agent

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.lookup_calls.append("agent")
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

        async def iter_versions(self, parent_id: uuid.UUID):
            assert parent_id == self.owner.agent.id
            yield self.owner.agent_version

    class _Evaluators:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            self.owner.lookup_calls.append("evaluator")
            return SimpleNamespace(items=[self.owner.evaluator], next_cursor=None)

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            assert parent_id == self.owner.evaluator.id
            assert version == self.owner.evaluator_version.version
            return self.owner.evaluator_version

    class _Analyzers:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def list(self, params: Any) -> Any:
            self.owner.lookup_calls.append("analyzer")
            return SimpleNamespace(items=[self.owner.analyzer], next_cursor=None)

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            assert parent_id == self.owner.analyzer.id
            assert version == self.owner.analyzer_version.version
            return self.owner.analyzer_version

    class _Blobs:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def upload(
            self, content: bytes, media_type: str, filename: str | None
        ) -> Any:
            self.owner.uploads.append((content, media_type, filename))
            return self.owner.blob

    class _Imports:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def create(
            self, request: Any, idempotency_key: str | None = None
        ) -> ImportResponse:
            self.owner.requests.append(request)
            self.owner.create_idempotency_keys.append(idempotency_key)
            if self.owner.create_error is not None:
                raise self.owner.create_error
            return self.owner.import_response

    class _Jobs:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def get(self, job_id: uuid.UUID) -> JobResponse:
            assert job_id == self.owner.job.id
            self.owner.job_get_calls.append(job_id)
            return self.owner.job


class StubSessions:
    """Session resource fake that records bounded list requests."""

    def __init__(self) -> None:
        self.session_id = uuid.uuid4()
        self.session = StubModel(self.session_id, {"name": "demo"})
        self.node = StubModel(uuid.uuid4(), {"index": 0, "inputs": None})
        self.list_calls: list[SessionListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.node_calls: list[tuple[uuid.UUID, SessionNodeListParams]] = []

    async def list(self, params: SessionListParams) -> Any:
        self.list_calls.append(params)
        return SimpleNamespace(items=[self.session], next_cursor="next-session")

    async def get(self, session_id: uuid.UUID) -> StubModel:
        self.get_calls.append(session_id)
        return self.session

    async def list_nodes(
        self, session_id: uuid.UUID, params: SessionNodeListParams
    ) -> Any:
        self.node_calls.append((session_id, params))
        return SimpleNamespace(items=[self.node], next_cursor="next-node")


class StubAgents:
    """Agent resource fake supporting exact-name session filtering."""

    def __init__(self) -> None:
        self.agent = SimpleNamespace(id=uuid.uuid4(), name="assistant")
        self.list_calls: list[Any] = []

    async def list(self, params: Any) -> Any:
        """Return the one exact-name match."""
        assert params.size == 2
        self.list_calls.append(params)
        return SimpleNamespace(items=[self.agent], next_cursor=None)


async def test_session_list_and_get_return_standard_envelopes() -> None:
    """Session reads preserve complete records and server pagination metadata."""
    resource = StubSessions()
    client = SimpleNamespace(sessions=resource)

    listed = await sessions.list_sessions(
        client,
        size=7,
        cursor="cursor",
        sort="created:asc",
        filter='{"field":"status","op":"eq","value":"completed"}',
    )
    params = resource.list_calls[0]
    assert isinstance(params, SessionListParams)
    dumped_params = params.model_dump(mode="json")
    assert {key: dumped_params[key] for key in ("cursor", "size", "sort")} == {
        "cursor": "cursor",
        "size": 7,
        "sort": "created:asc",
    }
    assert json.loads(dumped_params["filter"]) == {
        "field": "status",
        "op": "eq",
        "value": "completed",
    }
    assert listed.items == [{"id": str(resource.session_id), "name": "demo"}]
    assert listed.page == {
        "limit": 7,
        "next_cursor": "next-session",
        "truncated": True,
    }

    fetched = await sessions.get_session(client, resource.session_id)
    assert resource.get_calls == [resource.session_id]
    assert fetched.item == {"id": str(resource.session_id), "name": "demo"}


async def test_session_list_combines_typed_and_raw_filters() -> None:
    """Friendly session filters compose with the complete raw filter escape hatch."""
    resource = StubSessions()
    agents = StubAgents()
    client = SimpleNamespace(sessions=resource, agents=agents)
    started_after = datetime(2026, 8, 1, tzinfo=UTC)

    await sessions.list_sessions(
        client,
        size=20,
        cursor=None,
        sort="created:desc",
        filter='{"field":"name","op":"contains","value":"demo"}',
        status=SessionStatus.COMPLETED,
        agent="assistant",
        origin=SessionOrigin.IMPORTED,
        imported_from="langfuse",
        tag="baseline",
        started_after=started_after,
        started_before=None,
    )

    encoded = resource.list_calls[0].model_dump(mode="json")["filter"]
    combined = json.loads(encoded)
    conditions = combined["and"]
    assert conditions == [
        {"field": "name", "op": "contains", "value": "demo"},
        {"field": "status", "op": "eq", "value": "completed"},
        {"field": "agent_id", "op": "eq", "value": str(agents.agent.id)},
        {"field": "origin", "op": "eq", "value": "imported"},
        {"field": "imported_from", "op": "eq", "value": "langfuse"},
        {"field": "tag", "op": "eq", "value": "baseline"},
        {
            "field": "started_at",
            "op": "ge",
            "value": "2026-08-01T00:00:00Z",
        },
    ]
    assert len(agents.list_calls) == 1


async def test_session_agent_uuid_filter_needs_no_agent_lookup() -> None:
    """Exact agent UUID filters go directly to the session list request."""
    resource = StubSessions()
    agent_id = uuid.uuid4()
    agents = StubAgents()
    client = SimpleNamespace(sessions=resource, agents=agents)

    await sessions.list_sessions(
        client,
        size=20,
        cursor=None,
        sort="created:desc",
        filter=None,
        agent=str(agent_id),
    )

    assert agents.list_calls == []
    encoded = resource.list_calls[0].model_dump(mode="json")["filter"]
    assert json.loads(encoded) == {
        "field": "agent_id",
        "op": "eq",
        "value": str(agent_id),
    }


def test_invalid_list_values_are_concise_and_option_named(capsys) -> None:
    """Local list validation avoids raw Pydantic diagnostics and documentation URLs."""
    assert app_module.main(["session", "list", "--filter", "nope"]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["kind"] == "invalid_arguments"
    assert (
        payload["error"]["message"]
        == "--filter must be a valid JSON filter expression."
    )
    assert "errors.pydantic.dev" not in payload["error"]["message"]


async def test_session_nodes_controls_payload_flag_and_filters() -> None:
    """Node reads forward pagination, payload inclusion, and filter expressions."""
    resource = StubSessions()
    client = SimpleNamespace(sessions=resource)

    result = await sessions.list_session_nodes(
        client,
        resource.session_id,
        size=3,
        cursor="node-cursor",
        include_payloads=True,
        filter=json.dumps(
            {"field": "node_type", "op": "in", "value": ["llm_call", "tool_call"]}
        ),
    )

    session_id, params = resource.node_calls[0]
    assert session_id == resource.session_id
    assert isinstance(params, SessionNodeListParams)
    assert params.model_dump(mode="json") == {
        "cursor": "node-cursor",
        "size": 3,
        "include_payloads": True,
        "filter": json.dumps(
            {"field": "node_type", "op": "in", "value": ["llm_call", "tool_call"]}
        ),
        "sort": "position:asc",
    }
    assert result.items == [{"id": str(resource.node.id), "index": 0, "inputs": None}]
    assert result.page == {
        "limit": 3,
        "next_cursor": "next-node",
        "truncated": True,
    }


def test_session_list_and_get_argv_use_bounded_resource_calls(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered list and get leaves forward exact bounded requests."""
    resource = StubSessions()
    client = SimpleNamespace(sessions=resource)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "session",
                "list",
                "--size",
                "2",
                "--cursor",
                "current",
                "--sort",
                "created:asc",
            ]
        )
        == 0
    )
    listed = json.loads(capsys.readouterr().out)
    assert listed["command"] == "session.list"
    assert listed["page"] == {
        "limit": 2,
        "next_cursor": "next-session",
        "truncated": True,
    }
    assert resource.list_calls[0].cursor == "current"
    assert resource.list_calls[0].sort == "created:asc"

    assert app_module.main(["session", "get", str(resource.session_id)]) == 0
    fetched = json.loads(capsys.readouterr().out)
    assert fetched["command"] == "session.get"
    assert fetched["item"]["id"] == str(resource.session_id)
    assert resource.get_calls == [resource.session_id]


@pytest.mark.parametrize(
    "filters",
    [
        None,
        {"field": "node_type", "op": "in", "value": ["llm_call", "tool_call"]},
        {
            "or": [
                {"field": "node_type", "op": "eq", "value": "llm_call"},
                {"not": {"field": "node_type", "op": "eq", "value": "span"}},
            ]
        },
    ],
)
def test_session_nodes_argv_passes_exact_uuid_and_payload_flag(
    filters: dict[str, Any] | None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The registered leaf maps argv into the bounded node helper."""
    resource = StubSessions()
    client = SimpleNamespace(sessions=resource)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "session",
                "nodes",
                str(resource.session_id),
                "--size",
                "1",
                "--include-payloads",
                *(["--filter", json.dumps(filters)] if filters else []),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "session.nodes"
    assert payload["page"]["limit"] == 1
    session_id, params = resource.node_calls[0]
    assert session_id == resource.session_id
    assert params.include_payloads is True
    assert (
        params.filter.model_dump(mode="json", by_alias=True) if params.filter else None
    ) == filters


async def test_session_import_uploads_once_and_returns_exact_created_receipt(
    tmp_path: Path,
) -> None:
    """Import resolves exact versions before submitting the uploaded blob."""
    payload = tmp_path / "private-input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@latest",
        agent="assistant@3",
        params='{"secret_value":"not-for-receipt"}',
        join_on="/metadata/conversation~1id",
        media_type="application/jsonl",
        wait=False,
        interval=None,
        timeout=None,
    )

    assert client.uploads == [(b'{"x":1}', "application/jsonl", payload.name)]
    [request] = client.requests
    assert request.model_dump(mode="json") == {
        "importer": "jsonl",
        "agent_id": str(client.agent.id),
        "agent_version_id": str(client.agent_version.id),
        "version": 2,
        "source": {"type": "blob", "blob_id": str(client.blob.id)},
        "payload_blob_id": None,
        "params": {
            "secret_value": "not-for-receipt",
            "join_on": "/metadata/conversation~1id",
        },
        "evaluators": [],
        "analyzers": [],
        "max_sessions": None,
    }
    assert client.job_get_calls == [client.job.id]
    assert result.event == "created"
    assert result.item["operation"] == "session_import"
    assert result.item["terminal"] is False
    assert result.item["import_id"] == str(client.import_response.id)
    assert result.item["job"]["id"] == str(client.job.id)
    assert "evaluators" not in result.item
    assert "analyzers" not in result.item
    assert result.item["importer"] == {
        "id": str(client.importer.id),
        "name": "jsonl",
        "version_id": str(client.importer_version.id),
        "version": 2,
    }
    assert result.item["agent"]["version"] == 3
    assert result.item["blob"] == {
        "id": str(client.blob.id),
        "sha256": "a" * 64,
        "size": 7,
        "media_type": "application/jsonl",
    }
    assert str(payload) not in repr(result)
    assert "secret_value" not in repr(result)
    assert result.next_actions[-1] == "kitaru session list"


async def test_session_import_forwards_evaluators(tmp_path: Path) -> None:
    """Import resolves exact evaluator versions and sends their configs."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        evaluators=["quality@3"],
        evaluator_params=['quality@3={"threshold": 0.8}'],
        media_type="application/jsonl",
        wait=False,
        interval=None,
        timeout=None,
    )

    [request] = client.requests
    assert request.model_dump(mode="json")["evaluators"] == [
        {
            "evaluator": "quality",
            "version": 3,
            "params": {"threshold": 0.8},
            "connection_id": None,
        }
    ]
    assert result.item["evaluators"] == [
        {
            "id": str(client.evaluator.id),
            "name": "quality",
            "version_id": str(client.evaluator_version.id),
            "version": 3,
        }
    ]


async def test_session_import_forwards_evaluator_connections(tmp_path: Path) -> None:
    """Import resolves a connection for each selected evaluator token."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        evaluators=["quality@3"],
        evaluator_connections=["quality@3=langfuse-prod"],
        media_type="application/jsonl",
        wait=False,
        interval=None,
        timeout=None,
    )

    [request] = client.requests
    assert request.evaluators[0].connection_id == client.connection.id
    assert result.item["evaluators"][0]["connection"] == {
        "id": str(client.connection.id),
        "name": "langfuse-prod",
    }


async def test_session_import_forwards_analyzers(tmp_path: Path) -> None:
    """Import resolves exact analyzer versions and sends their configs."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        analyzers=["clustering@2"],
        analyzer_params=['clustering@2={"min_size": 5}'],
        media_type="application/jsonl",
        wait=False,
        interval=None,
        timeout=None,
    )

    [request] = client.requests
    assert request.model_dump(mode="json")["analyzers"] == [
        {
            "analyzer": "clustering",
            "version": 2,
            "params": {"min_size": 5},
            "connection_id": None,
            "min_sessions": None,
        }
    ]
    assert result.item["analyzers"] == [
        {
            "id": str(client.analyzer.id),
            "name": "clustering",
            "version_id": str(client.analyzer_version.id),
            "version": 2,
        }
    ]


async def test_session_import_forwards_analyzer_connections(tmp_path: Path) -> None:
    """Import resolves a connection for each selected analyzer token."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        analyzers=["clustering@2"],
        analyzer_connections=["clustering@2=langfuse-prod"],
        media_type="application/jsonl",
        wait=False,
        interval=None,
        timeout=None,
    )

    [request] = client.requests
    assert request.analyzers[0].connection_id == client.connection.id
    assert result.item["analyzers"][0]["connection"] == {
        "id": str(client.connection.id),
        "name": "langfuse-prod",
    }


async def test_session_import_rejects_evaluator_params_without_evaluator(
    tmp_path: Path,
) -> None:
    """Evaluator parameters without a selected evaluator fail before upload."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            evaluator_params=['quality@3={"threshold": 0.8}'],
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.uploads == []


async def test_session_import_rejects_evaluator_connection_without_evaluator(
    tmp_path: Path,
) -> None:
    """Evaluator connections require a selected evaluator token."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            evaluator_connections=["quality@3=langfuse-prod"],
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.uploads == []


async def test_session_import_rejects_analyzer_params_without_analyzer(
    tmp_path: Path,
) -> None:
    """Analyzer parameters without a selected analyzer fail before upload."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            analyzer_params=['clustering@2={"min_size": 5}'],
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.uploads == []


async def test_session_import_rejects_analyzer_connection_without_analyzer(
    tmp_path: Path,
) -> None:
    """Analyzer connections require a selected analyzer token."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            analyzer_connections=["clustering@2=langfuse-prod"],
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.uploads == []


@pytest.mark.parametrize(
    ("wait", "interval", "timeout"),
    [(False, 1.0, None), (False, None, 4.0), (True, float("nan"), None)],
)
async def test_session_import_rejects_invalid_wait_flags_before_upload(
    tmp_path: Path, wait: bool, interval: float | None, timeout: float | None
) -> None:
    """Invalid local wait controls cannot leave a remote blob behind."""
    payload = tmp_path / "input"
    payload.write_bytes(b"")
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/octet-stream",
            wait=wait,
            interval=interval,
            timeout=timeout,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.uploads == []
    assert client.requests == []


async def test_session_import_rejects_non_file_without_upload(tmp_path: Path) -> None:
    """A missing or non-regular payload fails before either mutation phase."""
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            tmp_path / "missing-private-path",
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/octet-stream",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert str(tmp_path) not in error.value.message
    assert client.lookup_calls == []
    assert client.uploads == []


async def test_session_import_tags_require_wait_before_mutation(
    tmp_path: Path,
) -> None:
    """Post-import tagging requires a settled task before any mutation."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError, match="--tag requires --wait"):
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            tags=["baseline", "discovery"],
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert client.uploads == []
    assert client.requests == []


async def test_tag_imported_sessions_links_every_tag_once() -> None:
    """Account credentials apply each requested tag to each imported session."""
    task_id = uuid.uuid4()
    imported = [SimpleNamespace(id=uuid.uuid4()), SimpleNamespace(id=uuid.uuid4())]
    existing = SimpleNamespace(id=uuid.uuid4(), name="baseline")

    class Tags:
        def __init__(self) -> None:
            self.created: list[Any] = []
            self.links: list[tuple[uuid.UUID, Any]] = []

        async def iter(self, params: Any):
            if params.filter.value == "baseline":
                yield existing

        async def create(self, request: Any) -> Any:
            tag = SimpleNamespace(id=uuid.uuid4(), name=request.name)
            self.created.append(tag)
            return tag

        async def create_link(self, tag_id: uuid.UUID, request: Any) -> None:
            self.links.append((tag_id, request))

    class ImportedSessions:
        async def iter(self, params: Any):
            assert params.filter.field == "task_id"
            assert params.filter.value == str(task_id)
            for session in imported:
                yield session

    tags = Tags()
    client = SimpleNamespace(tags=tags, sessions=ImportedSessions())

    count = await sessions._tag_imported_sessions(
        client, task_id, ["baseline", "discovery"]
    )

    assert count == 2
    assert [tag.name for tag in tags.created] == ["discovery"]
    assert len(tags.links) == 4
    assert {link.resource_id for _, link in tags.links} == {
        session.id for session in imported
    }


def test_payload_read_error_suppresses_private_path_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sanitized payload read failures do not chain path-bearing OS errors."""
    payload = tmp_path / "private-input.jsonl"
    payload.write_bytes(b"payload")

    def fail_read(_: Path) -> bytes:
        raise OSError(13, "permission denied", str(payload))

    monkeypatch.setattr(Path, "read_bytes", fail_read)

    with pytest.raises(CLIError) as error:
        sessions._read_payload(payload)

    rendered = "".join(
        traceback.format_exception(
            type(error.value), error.value, error.value.__traceback__
        )
    )
    assert error.value.__suppress_context__ is True
    assert str(payload) not in rendered


async def test_session_import_reports_job_create_partial_failure(
    tmp_path: Path,
) -> None:
    """A successful upload is retained and disclosed when job creation fails."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient(create_error=APIError(422, "invalid import request"))

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "partial_failure"
    assert error.value.details == {
        "operation": "session_import",
        "job_created": False,
        "blob": sessions._blob_metadata(client.blob),
        "error": {"status_code": 422, "detail": "invalid import request"},
    }
    assert len(client.uploads) == 1
    assert len(client.requests) == 1


async def test_waited_session_import_returns_validated_stats_and_task_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A waited import validates its sole task and exposes exact retrieval."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()
    terminal = _job(JobStatus.COMPLETED)
    task = _task(
        terminal,
        result={"created": 4, "skipped": 2, "failed": 0, "failures": []},
    )
    events: list[tuple[str, Any]] = []
    tag_calls: list[tuple[uuid.UUID, list[str]]] = []

    async def wait_for_terminal_tasks(*args, **kwargs):
        assert args[1] == client.job.id
        assert kwargs == {
            "interval": 2.0,
            "timeout": 300.0,
            "initial_job": client.job,
        }
        return terminal, [task]

    monkeypatch.setattr(
        sessions.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    monkeypatch.setattr(
        sessions, "emit_event", lambda event, item: events.append((event, item))
    )

    async def tag_imported_sessions(
        client: Any, task_id: uuid.UUID, names: list[str]
    ) -> int:
        tag_calls.append((task_id, names))
        return 4

    monkeypatch.setattr(sessions, "_tag_imported_sessions", tag_imported_sessions)

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        tags=["baseline", "discovery"],
        media_type="application/jsonl",
        wait=True,
        interval=None,
        timeout=None,
    )

    assert events[0][0] == "created"
    assert result.event == "terminal"
    assert result.item["terminal"] is True
    assert result.item["task"] == {
        "id": str(task.id),
        "kind": "importer",
        "status": "completed",
        "error": None,
    }
    assert result.item["stats"] == {
        "created": 4,
        "skipped": 2,
        "failed": 0,
        "failures": [],
        "limit_reached": False,
    }
    assert result.item["tags"] == ["baseline", "discovery"]
    assert result.item["tagged_session_count"] == 4
    assert tag_calls == [(task.id, ["baseline", "discovery"])]
    assert not hasattr(client.requests[0], "tags")
    assert result.warnings == ["2 duplicate session(s) were skipped."]
    assert str(task.id) in result.next_actions[0]
    assert '"field":"task_id"' in result.next_actions[0]


def _run_terminal_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    job: JobResponse,
    tasks: list[TaskResponse],
) -> tuple[int, dict[str, Any]]:
    """Invoke a waited CLI import against a settled remote job."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    async def wait_for_terminal_tasks(*args, **kwargs):
        assert args[1] == client.job.id
        return job, tasks

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    monkeypatch.setattr(
        sessions.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    exit_code = app_module.main(
        [
            "session",
            "import",
            str(payload),
            "--importer",
            "jsonl@2",
            "--agent",
            "assistant@3",
            "--wait",
        ]
    )
    captured = capsys.readouterr()
    events = [json.loads(line) for line in captured.out.splitlines()]
    assert events[0]["event"] == "created"
    if exit_code:
        return exit_code, json.loads(captured.err)["error"]
    assert captured.err == ""
    assert events[-1]["event"] == "terminal"
    return exit_code, events[-1]


def test_terminal_import_warns_about_failed_items(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Failed items in a completed import are warnings, not a failure."""
    job = _job(JobStatus.COMPLETED)
    task = _task(
        job,
        result={
            "created": 1,
            "skipped": 0,
            "failed": 2,
            "failures": [
                {"line": 3, "external_id": "trace-3", "error": "missing sessionId"},
                {"line": 5, "external_id": None, "error": "missing sessionId"},
            ],
        },
    )
    exit_code, receipt = _run_terminal_import(
        tmp_path, monkeypatch, capsys, job, [task]
    )

    assert exit_code == 0
    assert receipt["item"]["stats"]["failed"] == 2
    assert receipt["warnings"] == [
        "2 item(s) failed to import.",
        "line 3 (trace-3): missing sessionId",
        "line 5: missing sessionId",
    ]
    assert str(task.id) in receipt["next_actions"][0]


def test_terminal_import_warns_about_reaching_the_session_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Reaching the session limit in a completed import is a warning, not a failure."""
    job = _job(JobStatus.COMPLETED)
    task = _task(
        job,
        result={
            "created": 2,
            "skipped": 0,
            "failed": 0,
            "failures": [],
            "limit_reached": True,
        },
    )
    exit_code, receipt = _run_terminal_import(
        tmp_path, monkeypatch, capsys, job, [task]
    )

    assert exit_code == 0
    assert receipt["item"]["stats"]["limit_reached"] is True
    assert receipt["warnings"] == [
        "Import stopped after reaching the limit of 2 session(s)."
    ]
    assert str(task.id) in receipt["next_actions"][0]


@pytest.mark.parametrize(
    ("job_status", "task_status", "stats", "kind"),
    [
        (
            JobStatus.FAILED,
            TaskStatus.FAILED,
            {"created": 1, "skipped": 0, "failed": 1, "failures": []},
            "remote_failed",
        ),
        (JobStatus.CANCELED, TaskStatus.CANCELED, None, "remote_canceled"),
    ],
)
def test_terminal_import_preserves_remote_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    job_status: JobStatus,
    task_status: TaskStatus,
    stats: dict[str, Any] | None,
    kind: str,
) -> None:
    """Remote settlement retains the enriched receipt."""
    job = _job(job_status)
    task = _task(job, status=task_status, result=stats)
    exit_code, error = _run_terminal_import(tmp_path, monkeypatch, capsys, job, [task])

    assert exit_code != 0
    assert error["kind"] == kind
    receipt = error["details"]["receipt"]
    assert receipt["task"]["id"] == str(task.id)
    assert receipt["task"]["error"] == task.error
    if stats is not None:
        assert receipt["stats"]["failed"] == 1
    assert str(task.id) in error["details"]["next_actions"][0]


@pytest.mark.parametrize(
    ("job_status", "task_status", "kind"),
    [
        (JobStatus.FAILED, TaskStatus.FAILED, "remote_failed"),
        (JobStatus.CANCELED, TaskStatus.CANCELED, "remote_canceled"),
    ],
)
def test_terminal_import_ignores_malformed_remote_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    job_status: JobStatus,
    task_status: TaskStatus,
    kind: str,
) -> None:
    """Malformed diagnostic results do not replace the remote terminal outcome."""
    job = _job(job_status)
    task = _task(job, status=task_status, result={"diagnostic": "worker stopped"})

    exit_code, error = _run_terminal_import(tmp_path, monkeypatch, capsys, job, [task])

    assert exit_code != 0
    assert error["kind"] == kind
    assert "stats" not in error["details"]["receipt"]


def test_session_import_argv_registers_streaming_created_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The registered leaf forwards exact import options and emits one created event."""
    payload = tmp_path / "payload.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "session",
                "import",
                str(payload),
                "--importer",
                "jsonl@2",
                "--agent",
                "assistant@3",
                "--join-on",
                "/metadata/customer~1case_id",
                "--evaluator",
                "quality@3",
                "--analyzer",
                "clustering@2",
                "--analyzer-connection",
                "clustering@2=langfuse-prod",
                "--media-type",
                "application/jsonl",
                "--max-sessions",
                "5",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert captured.err == ""
    assert document["command"] == "session.import"
    assert document["event"] == "created"
    assert document["item"]["job"]["id"] == str(client.job.id)
    assert document["item"]["import_id"] == str(client.import_response.id)
    assert client.uploads == [(b'{"x":1}', "application/jsonl", "payload.jsonl")]
    assert client.requests[0].params == {"join_on": "/metadata/customer~1case_id"}
    assert [config.evaluator for config in client.requests[0].evaluators] == ["quality"]
    assert [config.analyzer for config in client.requests[0].analyzers] == [
        "clustering"
    ]
    assert client.requests[0].analyzers[0].connection_id == client.connection.id
    assert client.requests[0].max_sessions == 5


@pytest.mark.parametrize("join_on", ["metadata.case_id", "/metadata/case~2id"])
async def test_session_import_rejects_invalid_join_pointer_before_upload(
    tmp_path: Path, join_on: str
) -> None:
    """Reject a malformed join pointer before uploading the payload."""
    payload = tmp_path / "payload.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
            join_on=join_on,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.uploads == []


@pytest.mark.parametrize(
    "tasks",
    [
        [],
        None,
    ],
)
def test_terminal_import_rejects_missing_or_malformed_completed_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tasks: list[TaskResponse] | None,
) -> None:
    """Completed receipts reject invalid task sets and malformed statistics."""
    job = _job(JobStatus.COMPLETED)
    observed = tasks if tasks is not None else [_task(job, result={"created": "bad"})]

    exit_code, error = _run_terminal_import(
        tmp_path, monkeypatch, capsys, job, observed
    )

    assert exit_code != 0
    assert error["kind"] == "internal_error"


@pytest.mark.parametrize("extra_kind", [TaskKind.ANALYZER, TaskKind.EVALUATOR])
@pytest.mark.parametrize("importer_first", [True, False])
def test_terminal_import_accepts_followup_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    extra_kind: TaskKind,
    importer_first: bool,
) -> None:
    """Read importer statistics even when a job includes follow-up tasks."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(job, result={"created": 3, "skipped": 0, "failed": 0})
    extra = _task(job, kind=extra_kind, result=[])
    tasks = [importer, extra] if importer_first else [extra, importer]

    exit_code, result = _run_terminal_import(tmp_path, monkeypatch, capsys, job, tasks)

    assert exit_code == 0
    assert result["item"]["task"]["id"] == str(importer.id)
    assert result["item"]["stats"]["created"] == 3


def test_terminal_import_accepts_a_skipped_analyzer_followup_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A skipped analyzer follow-up task does not affect the import receipt."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(job, result={"created": 3, "skipped": 0, "failed": 0})
    analyzer = _task(job, kind=TaskKind.ANALYZER, status=TaskStatus.SKIPPED)
    tasks = [importer, analyzer]

    exit_code, result = _run_terminal_import(tmp_path, monkeypatch, capsys, job, tasks)

    assert exit_code == 0
    assert result["item"]["task"]["id"] == str(importer.id)
    assert result["item"]["stats"]["created"] == 3


@pytest.mark.parametrize("importer_count", [0, 2])
def test_terminal_import_requires_exactly_one_importer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    importer_count: int,
) -> None:
    """Follow-up tasks do not hide missing or duplicate importer tasks."""
    job = _job(JobStatus.COMPLETED)
    tasks = [_task(job, kind=TaskKind.ANALYZER, result=[])] + [
        _task(job, result={"created": 3, "skipped": 0, "failed": 0})
        for _ in range(importer_count)
    ]
    exit_code, error = _run_terminal_import(tmp_path, monkeypatch, capsys, job, tasks)
    assert exit_code != 0
    assert error["kind"] == "internal_error"
    assert "exactly one importer task" in error["message"]


def test_terminal_import_selects_importer_task_among_evaluator_tasks() -> None:
    """Evaluator tasks appended to the import job do not hide the importer task."""
    job = _job(JobStatus.COMPLETED)
    importer_task = _task(
        job, result={"created": 2, "skipped": 0, "failed": 0, "failures": []}
    )
    evaluator_tasks = [_task(job, kind=TaskKind.EVALUATOR) for _ in range(3)]

    result = sessions._terminal_import_result(
        job, [*evaluator_tasks, importer_task], identity={}
    )

    assert result.item["task"]["id"] == str(importer_task.id)
    assert result.item["stats"]["created"] == 2


@pytest.mark.parametrize(
    "filter_value", ["not-json", '{"or": []}', '{"field": "node_type"}']
)
def test_session_nodes_rejects_invalid_filter(
    filter_value: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Invalid filter shapes fail before any SDK request."""
    resource = StubSessions()

    @asynccontextmanager
    async def fake_open_client():
        yield SimpleNamespace(sessions=resource)

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    assert (
        app_module.main(
            ["session", "nodes", str(resource.session_id), "--filter", filter_value]
        )
        != 0
    )
    assert resource.node_calls == []
    assert json.loads(capsys.readouterr().err)["error"]["kind"] == "invalid_arguments"


def test_resolve_time_option_relative_duration_resolves_near_now() -> None:
    """A relative --since duration resolves to a UTC timestamp near now."""
    before = datetime.now(UTC) - timedelta(days=7)
    resolved = sessions._resolve_time_option("7d", "--since")
    after = datetime.now(UTC) - timedelta(days=7)

    assert resolved is not None
    parsed = datetime.fromisoformat(resolved)
    assert before <= parsed <= after


async def test_session_import_api_query_merges_options_and_uploads_nothing() -> None:
    """An API import builds the merged query and performs no upload."""
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        None,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        media_type=None,
        wait=False,
        interval=None,
        timeout=None,
        since="2026-08-01T00:00:00Z",
        trace_ids=["trace-1", "trace-2"],
        query='{"project_id":"proj-1"}',
    )

    assert client.uploads == []
    [request] = client.requests
    expected_query = {
        "project_id": "proj-1",
        "since": "2026-08-01T00:00:00Z",
        "trace_ids": ["trace-1", "trace-2"],
    }
    assert isinstance(request.source, ApiImportSource)
    assert (
        request.source.query.model_dump(mode="json", exclude_unset=True)
        == expected_query
    )
    assert result.item["query"] == expected_query
    assert "blob" not in result.item


async def test_session_import_carries_a_resolved_connection() -> None:
    """A named connection resolves to the id sent on the API source."""
    client = StubImportClient()

    result = await sessions.import_sessions(
        client,
        None,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
        media_type=None,
        wait=False,
        interval=None,
        timeout=None,
        since="2026-08-01T00:00:00Z",
        connection="langfuse-prod",
    )

    [request] = client.requests
    assert isinstance(request.source, ApiImportSource)
    assert request.source.connection_id == client.connection.id
    assert result.item["connection"] == {
        "id": str(client.connection.id),
        "name": "langfuse-prod",
    }


async def test_session_import_rejects_a_connection_with_a_payload_file(
    tmp_path: Path,
) -> None:
    """A blob import carries no connection."""
    client = StubImportClient()
    payload = tmp_path / "traces.jsonl"
    payload.write_text("{}\n", encoding="utf-8")

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type=None,
            wait=False,
            interval=None,
            timeout=None,
            connection="langfuse-prod",
        )

    assert error.value.kind == "invalid_arguments"
    assert client.lookup_calls == []
    assert client.uploads == []


async def test_session_import_query_clash_rejected_before_remote_call() -> None:
    """A --query key already set by --since is rejected before any lookup."""
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            None,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type=None,
            wait=False,
            interval=None,
            timeout=None,
            since="2026-08-01T00:00:00Z",
            query='{"since":"2026-08-02T00:00:00Z"}',
        )

    assert error.value.kind == "invalid_arguments"
    assert client.lookup_calls == []
    assert client.uploads == []
    assert client.requests == []


async def test_session_import_rejects_an_inverted_query_before_remote_call() -> None:
    """An inverted window merged from --since and --query is rejected locally."""
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            None,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type=None,
            wait=False,
            interval=None,
            timeout=None,
            since="2026-08-02T00:00:00Z",
            query='{"until":"2026-08-01T00:00:00Z"}',
        )

    assert error.value.kind == "invalid_arguments"
    assert client.lookup_calls == []
    assert client.uploads == []
    assert client.requests == []


async def test_session_import_rejects_path_combined_with_since(
    tmp_path: Path,
) -> None:
    """FILE and an API selection option cannot be combined."""
    payload = tmp_path / "payload.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
            since="7d",
        )

    assert error.value.kind == "invalid_arguments"
    assert client.lookup_calls == []
    assert client.uploads == []


async def test_session_import_rejects_media_type_without_path() -> None:
    """--media-type only applies to an uploaded file."""
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            None,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
            since="7d",
        )

    assert error.value.kind == "invalid_arguments"
    assert client.lookup_calls == []
    assert client.uploads == []


async def test_session_import_rejects_neither_path_nor_api_selection() -> None:
    """Omitting both FILE and every API selection option is rejected."""
    client = StubImportClient()

    with pytest.raises(CLIError) as error:
        await sessions.import_sessions(
            client,
            None,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type=None,
            wait=False,
            interval=None,
            timeout=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert client.lookup_calls == []
    assert client.uploads == []
