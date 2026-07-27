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
"""Tests for the importer contract and the import flow."""

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from kitaru.api_models.v1.jobs import (
    MAX_IMPORT_FAILURES,
    ImportStats,
    JobKind,
    JobSpecImporter,
    JobSpecPayload,
    JobSpecPlugin,
    JobSpecResponse,
)
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionProvider,
    SessionResponse,
    SessionStatus,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, ConflictError
from kitaru.job.importer import (
    NODE_BATCH_SIZE,
    ParsedItem,
    ParsedNode,
    ParsedSession,
    SessionImportError,
    call_parser,
    flatten_nodes,
    run,
    session_request,
)

NOW = datetime.now(UTC)

AGENT_ID = uuid.uuid4()

GENERIC_PLUGIN_CODE = """
from kitaru.job.importer import ParseFailure, ParsedNode, ParsedSession


def _node(children):
    return ParsedNode(
        node_type="span",
        name="trace",
        children=[
            ParsedNode(node_type="llm_call", name=f"call-{i}")
            for i in range(children)
        ],
    )


def parse(payload, params):
    for item in params["items"]:
        kind = item["kind"]
        if kind == "session":
            yield ParsedSession(
                external_id=item["external_id"],
                nodes=[_node(item.get("children", 0))],
            )
        elif kind == "failure":
            yield ParseFailure(
                line=item["line"],
                external_id=item.get("external_id"),
                error=item["error"],
            )
        elif kind == "raise":
            raise RuntimeError(item["message"])
"""


def write_plugin(tmp_path: Path, code: str = GENERIC_PLUGIN_CODE) -> Path:
    """Write importer code to a file without a suffix, as the cache does."""
    path = tmp_path / ("a" * 64)
    path.write_text(code)
    return path


def make_importer(items: list[dict[str, Any]] | None = None) -> JobSpecImporter:
    """Build the importer of an import job spec."""
    return JobSpecImporter(
        plugin=JobSpecPlugin(
            format=PluginFormat.INLINE,
            entrypoint="parse",
            blob_id=uuid.uuid4(),
            sha256="0" * 64,
        ),
        payload=JobSpecPayload(blob_id=uuid.uuid4(), sha256="1" * 64),
        provider=SessionProvider.OTLP,
        agent_id=AGENT_ID,
        params={"items": items or []},
    )


def make_spec(job_id: uuid.UUID, importer: JobSpecImporter | None) -> JobSpecResponse:
    """Build an import job spec."""
    return JobSpecResponse(
        job_id=job_id,
        kind=JobKind.IMPORT,
        inputs=None,
        override=None,
        tool_policy=None,
        scorer=None,
        importer=importer,
        run=None,
        secret_env={},
        input_session_id=None,
        name=None,
    )


def make_session(session_id: uuid.UUID) -> SessionResponse:
    """Build an imported session."""
    return SessionResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=AGENT_ID,
        agent_version_id=None,
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
        name=None,
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="ext",
        metadata={},
        provider=SessionProvider.OTLP,
        framework=None,
        adapter_version=None,
        log_uri=None,
        scores={},
        cost=None,
        tokens=None,
        llm_call_count=0,
        tool_call_count=0,
        created=NOW,
        updated=NOW,
    )


class FakeJobsResource:
    """Fake jobs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get_spec(self, job_id: uuid.UUID) -> JobSpecResponse:
        """Return the configured spec."""
        return self._client.spec


class FakeSessionsResource:
    """Fake sessions resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def create(self, request: SessionCreateRequest) -> SessionResponse:
        """Record the request and return a created session."""
        self._client.session_requests.append(request)
        error = self._client.create_errors.get(request.external_id or "")
        if error is not None:
            raise error
        return make_session(uuid.uuid4())


class FakeSessionNodesResource:
    """Fake session nodes resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def upsert(
        self, session_id: uuid.UUID, request: SessionNodeBatchRequest
    ) -> None:
        """Record the batch."""
        self._client.node_batches.append((session_id, request))
        if self._client.ingest_error is not None:
            raise self._client.ingest_error


class FakeClient:
    """Fake API client implementing the resource methods the flow uses."""

    def __init__(
        self,
        spec: JobSpecResponse,
        create_errors: dict[str, APIError] | None = None,
        ingest_error: APIError | None = None,
    ) -> None:
        """Initialize the client."""
        self.spec = spec
        self.create_errors = create_errors or {}
        self.ingest_error = ingest_error
        self.session_requests: list[SessionCreateRequest] = []
        self.node_batches: list[tuple[uuid.UUID, SessionNodeBatchRequest]] = []
        self.jobs = FakeJobsResource(self)
        self.sessions = FakeSessionsResource(self)
        self.session_nodes = FakeSessionNodesResource(self)


async def run_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fake: FakeClient,
    job_id: uuid.UUID,
) -> Path:
    """Materialize the job env and run the import flow, returning the result path."""
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", str(write_plugin(tmp_path)))
    payload_path = tmp_path / "payload"
    payload_path.write_bytes(b"")
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", str(payload_path))
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_JOB_RESULT_PATH", str(result_path))
    await run(cast(KitaruAPIClient, fake), job_id)
    return result_path


def read_stats(path: Path) -> ImportStats:
    """Parse the stats the import flow wrote."""
    return ImportStats.model_validate_json(path.read_text())


def test_parsed_session_rejects_a_non_terminal_status() -> None:
    """Reject a parsed session that is still in progress."""
    with pytest.raises(ValueError, match="Imported sessions cannot be in progress"):
        ParsedSession(external_id="ext", status=SessionStatus.IN_PROGRESS)


def test_call_parser_passes_params_as_a_dict() -> None:
    """Call the parser with payload and params as a positional dict, not kwargs."""
    calls: list[tuple[bytes, dict[str, Any]]] = []

    def parser(payload: bytes, params: dict[str, Any]) -> Iterator[ParsedItem]:
        calls.append((payload, params))
        return iter([])

    list(call_parser(parser, b"data", {"a": 1}))
    assert calls == [(b"data", {"a": 1})]


def test_call_parser_wraps_a_raising_parser() -> None:
    """Wrap a parser that raises in a SessionImportError."""

    def parser(payload: bytes, params: dict[str, Any]) -> Iterator[ParsedItem]:
        raise ValueError("bad payload")

    with pytest.raises(SessionImportError, match="Importer raised ValueError"):
        call_parser(parser, b"data", {})


def test_flatten_nodes_walks_parent_before_child() -> None:
    """Assign ids, parent links, and sequence in depth first walk order."""
    tree = [
        ParsedNode(
            node_type=NodeType.SPAN,
            name="trace",
            children=[
                ParsedNode(
                    node_type=NodeType.LLM_CALL,
                    name="plan",
                    children=[
                        ParsedNode(node_type=NodeType.TOOL_CALL, name="get_weather")
                    ],
                ),
                ParsedNode(node_type=NodeType.LLM_CALL, name="answer"),
            ],
        ),
        ParsedNode(node_type=NodeType.SPAN, name="cleanup"),
    ]
    requests = flatten_nodes(tree)

    assert [request.name for request in requests] == [
        "trace",
        "plan",
        "get_weather",
        "answer",
        "cleanup",
    ]
    assert [request.sequence for request in requests] == [0, 1, 2, 3, 4]
    assert len({request.id for request in requests}) == 5
    assert requests[0].parent_id is None
    assert requests[1].parent_id == requests[0].id
    assert requests[2].parent_id == requests[1].id
    assert requests[3].parent_id == requests[0].id
    assert requests[4].parent_id is None


def test_flatten_nodes_maps_the_node_fields() -> None:
    """Carry the parsed node fields into the ingest request."""
    node = ParsedNode(
        node_type=NodeType.TOOL_CALL,
        name="get_weather",
        status=NodeStatus.FAILED,
        error="upstream 503",
        started_at=NOW,
        ended_at=NOW,
        inputs={"city": "Berlin"},
        outputs=None,
        tool_name="get_weather",
        external_id="span-1",
        attributes={"retries": 2},
        metadata={"tenant": "acme"},
    )
    request = flatten_nodes([node])[0]

    assert request.node_type is NodeType.TOOL_CALL
    assert request.status is NodeStatus.FAILED
    assert request.error == "upstream 503"
    assert request.inputs == {"city": "Berlin"}
    assert request.tool_name == "get_weather"
    assert request.external_id == "span-1"
    assert request.attributes == {"retries": 2}
    assert request.metadata == {"tenant": "acme"}


def test_session_request_maps_the_session_fields() -> None:
    """Bind a parsed session to the agent and provider of the spec."""
    parsed = ParsedSession(
        external_id="trace-1",
        name="Berlin weather",
        inputs={"question": "weather?"},
        outputs={"answer": "18C"},
        expected={"answer": "18C"},
        started_at=NOW,
        ended_at=NOW,
        metadata={"tenant": "acme"},
    )
    request = session_request(make_importer(), parsed)

    assert request.agent_id == AGENT_ID
    assert request.origin is SessionOrigin.IMPORTED
    assert request.provider is SessionProvider.OTLP
    assert request.status is SessionStatus.COMPLETED
    assert request.external_id == "trace-1"
    assert request.name == "Berlin weather"
    assert request.inputs == {"question": "weather?"}
    assert request.outputs == {"answer": "18C"}
    assert request.expected == {"answer": "18C"}
    assert request.metadata == {"tenant": "acme"}


async def test_run_creates_and_batches_nodes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Create every parsed session and ingest its tree in batches."""
    job_id = uuid.uuid4()
    items = [
        {"kind": "session", "external_id": "trace-1", "children": NODE_BATCH_SIZE + 3}
    ]
    fake = FakeClient(make_spec(job_id, make_importer(items)))

    result_path = await run_import(tmp_path, monkeypatch, fake, job_id)

    stats = read_stats(result_path)
    assert (stats.created, stats.skipped, stats.failed) == (1, 0, 0)
    assert [request.external_id for request in fake.session_requests] == ["trace-1"]
    assert [len(batch.nodes) for _, batch in fake.node_batches] == [NODE_BATCH_SIZE, 4]
    sequences = [
        node.sequence for _, batch in fake.node_batches for node in batch.nodes
    ]
    assert sequences == list(range(NODE_BATCH_SIZE + 4))


async def test_run_counts_a_conflict_as_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Count a session the server already holds as skipped."""
    job_id = uuid.uuid4()
    items = [
        {"kind": "session", "external_id": "trace-1"},
        {"kind": "session", "external_id": "trace-2"},
    ]
    fake = FakeClient(
        make_spec(job_id, make_importer(items)),
        create_errors={"trace-2": ConflictError(409, "duplicate")},
    )

    result_path = await run_import(tmp_path, monkeypatch, fake, job_id)

    stats = read_stats(result_path)
    assert (stats.created, stats.skipped, stats.failed) == (1, 1, 0)
    assert stats.failures == []
    assert len(fake.node_batches) == 1


async def test_run_counts_a_create_error_as_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sample a create failure under the position of the session in the stream."""
    job_id = uuid.uuid4()
    items = [
        {"kind": "session", "external_id": "trace-1"},
        {"kind": "session", "external_id": "trace-2"},
    ]
    fake = FakeClient(
        make_spec(job_id, make_importer(items)),
        create_errors={"trace-2": APIError(500, "boom")},
    )

    result_path = await run_import(tmp_path, monkeypatch, fake, job_id)

    stats = read_stats(result_path)
    assert (stats.created, stats.skipped, stats.failed) == (1, 0, 1)
    assert stats.failures[0].line == 2
    assert stats.failures[0].external_id == "trace-2"
    assert "boom" in stats.failures[0].error


async def test_run_counts_an_ingest_error_as_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Count a session whose node ingest failed as failed, not created."""
    job_id = uuid.uuid4()
    items = [{"kind": "session", "external_id": "trace-1", "children": 1}]
    fake = FakeClient(
        make_spec(job_id, make_importer(items)),
        ingest_error=APIError(422, "unknown parent"),
    )

    result_path = await run_import(tmp_path, monkeypatch, fake, job_id)

    stats = read_stats(result_path)
    assert (stats.created, stats.skipped, stats.failed) == (0, 0, 1)
    assert stats.failures[0].external_id == "trace-1"


async def test_run_counts_parse_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Count reported parse failures without touching the server."""
    job_id = uuid.uuid4()
    items = [
        {
            "kind": "failure",
            "line": 3,
            "external_id": "trace-3",
            "error": "Invalid JSON",
        },
        {"kind": "session", "external_id": "trace-1"},
    ]
    fake = FakeClient(make_spec(job_id, make_importer(items)))

    result_path = await run_import(tmp_path, monkeypatch, fake, job_id)

    stats = read_stats(result_path)
    assert (stats.created, stats.skipped, stats.failed) == (1, 0, 1)
    assert stats.failures[0].line == 3
    assert stats.failures[0].external_id == "trace-3"
    assert stats.failures[0].error == "Invalid JSON"


async def test_run_bounds_the_failure_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Count every failure but keep only the first samples."""
    job_id = uuid.uuid4()
    items = [
        {"kind": "failure", "line": line, "error": f"bad line {line}"}
        for line in range(MAX_IMPORT_FAILURES + 5)
    ]
    fake = FakeClient(make_spec(job_id, make_importer(items)))

    result_path = await run_import(tmp_path, monkeypatch, fake, job_id)

    stats = read_stats(result_path)
    assert stats.failed == MAX_IMPORT_FAILURES + 5
    assert len(stats.failures) == MAX_IMPORT_FAILURES
    assert stats.failures[0].line == 0
    assert stats.failures[-1].line == MAX_IMPORT_FAILURES - 1


async def test_run_streams_until_the_parser_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Process what the parser yielded before it raised, then propagate."""
    job_id = uuid.uuid4()
    items = [
        {"kind": "session", "external_id": "trace-1"},
        {"kind": "session", "external_id": "trace-2"},
        {"kind": "raise", "message": "truncated export"},
    ]
    fake = FakeClient(make_spec(job_id, make_importer(items)))
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", str(write_plugin(tmp_path)))
    payload_path = tmp_path / "payload"
    payload_path.write_bytes(b"")
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", str(payload_path))
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_JOB_RESULT_PATH", str(result_path))

    with pytest.raises(RuntimeError, match="truncated export"):
        await run(cast(KitaruAPIClient, fake), job_id)

    assert [request.external_id for request in fake.session_requests] == [
        "trace-1",
        "trace-2",
    ]
    assert not result_path.exists()


async def test_run_rejects_another_kind() -> None:
    """Reject a job spec without an importer."""
    job_id = uuid.uuid4()
    fake = FakeClient(make_spec(job_id, None))
    with pytest.raises(SessionImportError, match="is not an import job"):
        await run(cast(KitaruAPIClient, fake), job_id)


async def test_run_without_plugin_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject an import job without a materialized code path."""
    job_id = uuid.uuid4()
    monkeypatch.delenv("KITARU_JOB_PLUGIN_PATH", raising=False)
    fake = FakeClient(make_spec(job_id, make_importer()))
    with pytest.raises(SessionImportError, match="KITARU_JOB_PLUGIN_PATH is not set"):
        await run(cast(KitaruAPIClient, fake), job_id)


async def test_run_rejects_a_missing_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a payload the worker did not materialize."""
    job_id = uuid.uuid4()
    fake = FakeClient(make_spec(job_id, make_importer()))
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", str(write_plugin(tmp_path)))
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", str(tmp_path / "absent"))
    with pytest.raises(SessionImportError, match="Failed to read the payload"):
        await run(cast(KitaruAPIClient, fake), job_id)
