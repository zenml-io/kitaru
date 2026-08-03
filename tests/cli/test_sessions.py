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
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.job import JobResponse, JobStatus
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
        self.blob = SimpleNamespace(
            id=uuid.uuid4(), sha256="a" * 64, size=7, media_type="application/jsonl"
        )
        self.job = _job()
        self.uploads: list[tuple[bytes, str, str | None]] = []
        self.requests: list[Any] = []
        self.lookup_calls: list[str] = []
        self.create_error = create_error
        self.importers = self._Importers(self)
        self.agents = self._Agents(self)
        self.blobs = self._Blobs(self)
        self.imports = self._Imports(self)

    class _Importers:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.lookup_calls.append("importer")
            yield self.owner.importer

        async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
            assert parent_id == self.owner.importer.id
            assert version == self.owner.importer_version.version
            return self.owner.importer_version

    class _Agents:
        def __init__(self, owner: "StubImportClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.lookup_calls.append("agent")
            yield self.owner.agent

        async def iter_versions(self, parent_id: uuid.UUID):
            assert parent_id == self.owner.agent.id
            yield self.owner.agent_version

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

        async def create(self, request: Any) -> JobResponse:
            self.owner.requests.append(request)
            if self.owner.create_error is not None:
                raise self.owner.create_error
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
        provider="langfuse",
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
        {"field": "provider", "op": "eq", "value": "langfuse"},
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


async def test_session_nodes_controls_payload_flag_only() -> None:
    """Node reads expose only cursor pagination and the explicit payload flag."""
    resource = StubSessions()
    client = SimpleNamespace(sessions=resource)

    result = await sessions.list_session_nodes(
        client,
        resource.session_id,
        size=3,
        cursor="node-cursor",
        include_payloads=True,
    )

    session_id, params = resource.node_calls[0]
    assert session_id == resource.session_id
    assert isinstance(params, SessionNodeListParams)
    assert params.model_dump(mode="json") == {
        "cursor": "node-cursor",
        "size": 3,
        "include_payloads": True,
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


def test_session_nodes_argv_passes_exact_uuid_and_payload_flag(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
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
        "payload_blob_id": str(client.blob.id),
        "params": {"secret_value": "not-for-receipt"},
    }
    assert result.event == "created"
    assert result.item["operation"] == "session_import"
    assert result.item["terminal"] is False
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

    result = await sessions.import_sessions(
        client,
        payload,
        importer="jsonl@2",
        agent="assistant@3",
        params=None,
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
    }
    assert result.warnings == ["2 duplicate session(s) were skipped."]
    assert str(task.id) in result.next_actions[0]
    assert '"field":"task_id"' in result.next_actions[0]


@pytest.mark.parametrize(
    ("job_status", "task_status", "stats", "kind"),
    [
        (
            JobStatus.COMPLETED,
            TaskStatus.COMPLETED,
            {"created": 1, "skipped": 0, "failed": 1, "failures": []},
            "partial_failure",
        ),
        (
            JobStatus.FAILED,
            TaskStatus.FAILED,
            {"created": 1, "skipped": 0, "failed": 1, "failures": []},
            "remote_failed",
        ),
        (JobStatus.CANCELED, TaskStatus.CANCELED, None, "remote_canceled"),
    ],
)
def test_terminal_import_preserves_partial_and_remote_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    job_status: JobStatus,
    task_status: TaskStatus,
    stats: dict[str, Any] | None,
    kind: str,
) -> None:
    """Item failures and remote settlement retain the enriched receipt."""
    job = _job(job_status)
    task = _task(job, status=task_status, result=stats)
    monkeypatch.setattr(sessions, "emit_event", lambda *args: None)

    def terminal_job_error(job: JobResponse, receipt: dict[str, Any]) -> CLIError:
        remote_kind = (
            "remote_failed" if job.status is JobStatus.FAILED else "remote_canceled"
        )
        return CLIError(remote_kind, "remote outcome", details={"receipt": receipt})

    monkeypatch.setattr(sessions.receipts, "terminal_job_error", terminal_job_error)

    with pytest.raises(CLIError) as error:
        sessions._terminal_import_result(job, [task], identity={"blob": {}})

    assert error.value.kind == kind
    receipt = error.value.details["receipt"]
    assert receipt["task"]["id"] == str(task.id)
    assert receipt["task"]["error"] == task.error
    if stats is not None:
        assert receipt["stats"]["failed"] == 1
    assert str(task.id) in error.value.details["next_actions"][0]


@pytest.mark.parametrize(
    ("job_status", "task_status", "kind"),
    [
        (JobStatus.FAILED, TaskStatus.FAILED, "remote_failed"),
        (JobStatus.CANCELED, TaskStatus.CANCELED, "remote_canceled"),
    ],
)
def test_terminal_import_ignores_malformed_remote_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    job_status: JobStatus,
    task_status: TaskStatus,
    kind: str,
) -> None:
    """Malformed diagnostic results do not replace the remote terminal outcome."""
    job = _job(job_status)
    task = _task(job, status=task_status, result={"diagnostic": "worker stopped"})

    def terminal_job_error(job: JobResponse, receipt: dict[str, Any]) -> CLIError:
        return CLIError(kind, "remote outcome", details={"receipt": receipt})

    monkeypatch.setattr(sessions.receipts, "terminal_job_error", terminal_job_error)

    with pytest.raises(CLIError) as error:
        sessions._terminal_import_result(job, [task], identity={})

    assert error.value.kind == kind
    assert "stats" not in error.value.details["receipt"]


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
                "--media-type",
                "application/jsonl",
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
    assert client.uploads == [(b'{"x":1}', "application/jsonl", "payload.jsonl")]


@pytest.mark.parametrize(
    "tasks",
    [
        [],
        None,
    ],
)
def test_terminal_import_rejects_missing_or_malformed_completed_result(
    tasks: list[TaskResponse] | None,
) -> None:
    """Completed receipts reject invalid task sets and malformed statistics."""
    job = _job(JobStatus.COMPLETED)
    observed = tasks if tasks is not None else [_task(job, result={"created": "bad"})]

    with pytest.raises(CLIError) as error:
        sessions._terminal_import_result(job, observed, identity={})

    assert error.value.kind == "internal_error"
