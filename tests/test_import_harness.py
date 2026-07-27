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
"""Tests for the import job harness."""

import subprocess
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from kitaru.api_models.v1.jobs import (
    MAX_IMPORT_FAILURES,
    JobKind,
    JobSpecImporter,
    JobSpecPayload,
    JobSpecPlugin,
    JobSpecResponse,
    JobUpdateRequest,
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
from kitaru.importing import (
    ParsedItem,
    ParsedNode,
    ParsedSession,
    ParseFailure,
    SessionImportError,
    flatten_nodes,
    session_request,
)
from kitaru.imports import (
    NODE_BATCH_SIZE,
    PLUGIN_MODULE_NAME,
    import_job,
    import_sessions,
    load_plugin_parser,
    read_payload,
)
from kitaru.plugin_loader import required_env

NOW = datetime.now(UTC)

EXAMPLE_ROOT = Path(__file__).resolve().parent.parent / "importer_example"
IMPORTER_FILE = EXAMPLE_ROOT / "importer.py"
TRACE_FILE = EXAMPLE_ROOT / "trace.jsonl"

PLUGIN_CODE = """
def parse(payload):
    return []


NOT_CALLABLE = "text"
"""

AGENT_ID = uuid.uuid4()


def make_importer(params: dict | None = None) -> JobSpecImporter:
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
        params=params or {},
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


def parsed_session(external_id: str, children: int = 0) -> ParsedSession:
    """Build a parsed session with one root span and its children."""
    return ParsedSession(
        external_id=external_id,
        nodes=[
            ParsedNode(
                node_type=NodeType.SPAN,
                name="trace",
                children=[
                    ParsedNode(node_type=NodeType.LLM_CALL, name=f"call-{index}")
                    for index in range(children)
                ],
            )
        ],
    )


class FakeJobsResource:
    """Fake jobs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get_spec(self, job_id: uuid.UUID) -> JobSpecResponse:
        """Return the configured spec."""
        return self._client.spec

    async def update(self, job_id: uuid.UUID, request: JobUpdateRequest) -> None:
        """Record the update."""
        self._client.updates.append(request)


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
        session_id = uuid.uuid4()
        self._client.sessions_by_external_id[request.external_id or ""] = session_id
        return make_session(session_id)


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
    """Fake API client implementing the resource methods the harness uses."""

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
        self.updates: list[JobUpdateRequest] = []
        self.session_requests: list[SessionCreateRequest] = []
        self.sessions_by_external_id: dict[str, uuid.UUID] = {}
        self.node_batches: list[tuple[uuid.UUID, SessionNodeBatchRequest]] = []
        self.jobs = FakeJobsResource(self)
        self.sessions = FakeSessionsResource(self)
        self.session_nodes = FakeSessionNodesResource(self)


def write_plugin(tmp_path: Path, code: str = PLUGIN_CODE) -> Path:
    """Write importer code to a file without a suffix, as the cache does."""
    path = tmp_path / ("a" * 64)
    path.write_text(code)
    return path


def test_required_env_reads_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the value of a set variable."""
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", "/payload")
    assert required_env("KITARU_JOB_PAYLOAD_PATH", SessionImportError) == "/payload"


def test_required_env_rejects_a_missing_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a variable that is not set."""
    monkeypatch.delenv("KITARU_JOB_PAYLOAD_PATH", raising=False)
    with pytest.raises(SessionImportError, match="KITARU_JOB_PAYLOAD_PATH is not set"):
        required_env("KITARU_JOB_PAYLOAD_PATH", SessionImportError)


def test_module_entrypoint_reports_a_missing_environment(tmp_path: Path) -> None:
    """Exit non-zero naming the missing variable when run as a module."""
    result = subprocess.run(
        [sys.executable, "-m", "kitaru.imports"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        check=False,
    )

    assert result.returncode == 1
    assert "KITARU_JOB_ID is not set" in result.stderr


def test_plugin_models_are_the_harness_models() -> None:
    """Share the model classes between the harness and the loaded importer."""
    load_plugin_parser(IMPORTER_FILE, "parse")
    module = sys.modules[PLUGIN_MODULE_NAME]

    assert module.ParsedSession is ParsedSession
    assert module.ParseFailure is ParseFailure


def test_load_plugin_parser_imports_a_suffixless_file(tmp_path: Path) -> None:
    """Import the entrypoint of a cached code file."""
    assert load_plugin_parser(write_plugin(tmp_path), "parse")(b"") == []


def test_load_plugin_parser_missing_attribute(tmp_path: Path) -> None:
    """Reject an entrypoint the code does not define."""
    with pytest.raises(SessionImportError, match="has no attribute 'missing'"):
        load_plugin_parser(write_plugin(tmp_path), "missing")


def test_load_plugin_parser_not_callable(tmp_path: Path) -> None:
    """Reject an entrypoint that is not callable."""
    with pytest.raises(SessionImportError, match="is not callable"):
        load_plugin_parser(write_plugin(tmp_path), "NOT_CALLABLE")


def test_load_plugin_parser_import_error(tmp_path: Path) -> None:
    """Reject code that raises while importing."""
    path = write_plugin(tmp_path, "raise RuntimeError('boom')\n")
    with pytest.raises(SessionImportError, match="RuntimeError: boom"):
        load_plugin_parser(path, "parse")


def test_read_payload_rejects_a_missing_file(tmp_path: Path) -> None:
    """Reject a payload the worker did not materialize."""
    with pytest.raises(SessionImportError, match="Failed to read the payload"):
        read_payload(tmp_path / "absent")


def test_parsed_session_rejects_a_non_terminal_status() -> None:
    """Reject a parsed session that is still in progress."""
    with pytest.raises(ValueError, match="Imported sessions cannot be in progress"):
        ParsedSession(external_id="ext", status=SessionStatus.IN_PROGRESS)


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


async def test_import_sessions_creates_and_batches_nodes() -> None:
    """Create every parsed session and ingest its tree in batches."""
    fake = FakeClient(make_spec(uuid.uuid4(), make_importer()))
    parsed = [parsed_session("trace-1", children=NODE_BATCH_SIZE + 3)]

    stats = await import_sessions(
        cast(KitaruAPIClient, fake), make_importer(), iter(parsed)
    )

    assert (stats.created, stats.skipped, stats.failed) == (1, 0, 0)
    assert [request.external_id for request in fake.session_requests] == ["trace-1"]
    assert [len(batch.nodes) for _, batch in fake.node_batches] == [NODE_BATCH_SIZE, 4]
    sequences = [
        node.sequence for _, batch in fake.node_batches for node in batch.nodes
    ]
    assert sequences == list(range(NODE_BATCH_SIZE + 4))


async def test_import_sessions_counts_a_conflict_as_skipped() -> None:
    """Count a session the server already holds as skipped."""
    fake = FakeClient(
        make_spec(uuid.uuid4(), make_importer()),
        create_errors={"trace-2": ConflictError(409, "duplicate")},
    )
    parsed = [parsed_session("trace-1"), parsed_session("trace-2")]

    stats = await import_sessions(
        cast(KitaruAPIClient, fake), make_importer(), iter(parsed)
    )

    assert (stats.created, stats.skipped, stats.failed) == (1, 1, 0)
    assert stats.failures == []
    assert len(fake.node_batches) == 1


async def test_import_sessions_counts_a_create_error_as_failed() -> None:
    """Sample a create failure under the position of the session in the stream."""
    fake = FakeClient(
        make_spec(uuid.uuid4(), make_importer()),
        create_errors={"trace-2": APIError(500, "boom")},
    )
    parsed = [parsed_session("trace-1"), parsed_session("trace-2")]

    stats = await import_sessions(
        cast(KitaruAPIClient, fake), make_importer(), iter(parsed)
    )

    assert (stats.created, stats.skipped, stats.failed) == (1, 0, 1)
    assert stats.failures[0].line == 2
    assert stats.failures[0].external_id == "trace-2"
    assert "boom" in stats.failures[0].error


async def test_import_sessions_counts_an_ingest_error_as_failed() -> None:
    """Count a session whose node ingest failed as failed, not created."""
    fake = FakeClient(
        make_spec(uuid.uuid4(), make_importer()),
        ingest_error=APIError(422, "unknown parent"),
    )
    parsed = [parsed_session("trace-1", children=1)]

    stats = await import_sessions(
        cast(KitaruAPIClient, fake), make_importer(), iter(parsed)
    )

    assert (stats.created, stats.skipped, stats.failed) == (0, 0, 1)
    assert stats.failures[0].external_id == "trace-1"


async def test_import_sessions_counts_parse_failures() -> None:
    """Count reported parse failures without touching the server."""
    fake = FakeClient(make_spec(uuid.uuid4(), make_importer()))
    parsed: list[ParsedItem] = [
        ParseFailure(line=3, external_id="trace-3", error="Invalid JSON"),
        parsed_session("trace-1"),
    ]

    stats = await import_sessions(
        cast(KitaruAPIClient, fake), make_importer(), iter(parsed)
    )

    assert (stats.created, stats.skipped, stats.failed) == (1, 0, 1)
    assert stats.failures[0].line == 3
    assert stats.failures[0].external_id == "trace-3"
    assert stats.failures[0].error == "Invalid JSON"


async def test_import_sessions_bounds_the_failure_sample() -> None:
    """Count every failure but keep only the first samples."""
    fake = FakeClient(make_spec(uuid.uuid4(), make_importer()))
    parsed: list[ParsedItem] = [
        ParseFailure(line=line, error=f"bad line {line}")
        for line in range(MAX_IMPORT_FAILURES + 5)
    ]

    stats = await import_sessions(
        cast(KitaruAPIClient, fake), make_importer(), iter(parsed)
    )

    assert stats.failed == MAX_IMPORT_FAILURES + 5
    assert len(stats.failures) == MAX_IMPORT_FAILURES
    assert stats.failures[0].line == 0
    assert stats.failures[-1].line == MAX_IMPORT_FAILURES - 1


async def test_import_sessions_streams_until_the_parser_raises() -> None:
    """Process what the parser yielded before it raised, then propagate."""
    fake = FakeClient(make_spec(uuid.uuid4(), make_importer()))

    def parsed() -> Iterator[ParsedItem]:
        yield parsed_session("trace-1")
        yield parsed_session("trace-2")
        raise RuntimeError("truncated export")

    with pytest.raises(RuntimeError, match="truncated export"):
        await import_sessions(cast(KitaruAPIClient, fake), make_importer(), parsed())

    assert [request.external_id for request in fake.session_requests] == [
        "trace-1",
        "trace-2",
    ]


async def test_import_job_records_the_stats(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the registered importer over the payload and patch the stats."""
    job_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", str(write_plugin(tmp_path)))
    payload = tmp_path / "payload.jsonl"
    payload.write_bytes(b"{}\n")
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", str(payload))
    fake = FakeClient(make_spec(job_id, make_importer()))

    stats = await import_job(cast(KitaruAPIClient, fake), job_id)

    assert (stats.created, stats.skipped, stats.failed) == (0, 0, 0)
    assert [update.stats for update in fake.updates] == [stats]


async def test_import_job_rejects_another_kind() -> None:
    """Reject a job spec without an importer."""
    job_id = uuid.uuid4()
    fake = FakeClient(make_spec(job_id, None))
    with pytest.raises(SessionImportError, match="is not an import job"):
        await import_job(cast(KitaruAPIClient, fake), job_id)


def test_example_importer_parses_the_example_trace() -> None:
    """Parse the example trace into three sessions and one failure."""
    parse = load_plugin_parser(IMPORTER_FILE, "parse")
    items = list(parse(TRACE_FILE.read_bytes()))
    sessions = [item for item in items if isinstance(item, ParsedSession)]
    failures = [item for item in items if isinstance(item, ParseFailure)]

    assert [session.external_id for session in sessions] == [
        "trace-2026-07-20-001",
        "trace-2026-07-20-002",
        "trace-2026-07-20-004",
    ]
    assert [len(flatten_nodes(session.nodes)) for session in sessions] == [5, 3, 4]
    assert len(failures) == 1
    assert failures[0].line == 3
    assert "Invalid JSON" in failures[0].error


def test_example_importer_nests_tool_calls_under_the_llm_call() -> None:
    """Nest the tool calls of a trace under the LLM call that requested them."""
    parse = load_plugin_parser(IMPORTER_FILE, "parse")
    items = list(parse(TRACE_FILE.read_bytes()))
    session = next(item for item in items if isinstance(item, ParsedSession))
    requests = flatten_nodes(session.nodes)

    assert [request.node_type for request in requests] == [
        NodeType.SPAN,
        NodeType.LLM_CALL,
        NodeType.TOOL_CALL,
        NodeType.TOOL_CALL,
        NodeType.LLM_CALL,
    ]
    assert requests[2].parent_id == requests[1].id
    assert requests[2].tool_name == "get_weather"
    assert requests[1].model == "gpt-4o-mini"
    assert requests[1].tokens is not None
    assert requests[1].cost is not None
