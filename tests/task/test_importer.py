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
"""Tests for importer parsing, translation, and streaming flow."""

import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.imports import MAX_IMPORT_FAILURES, ImportFailure
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.api_models.v1.task import ImportTaskDetails, PackagePluginSpec, PayloadSpec
from kitaru.client.exceptions import APIError
from kitaru.task import importer as importer_module
from kitaru.task.importer import (
    NODE_BATCH_SIZE,
    ParsedNode,
    ParsedSession,
    SessionImportError,
    call_parser,
    flatten_nodes,
    session_request,
)


def parsed_session(
    external_id: str = "external-1",
    *,
    nodes: list[ParsedNode] | None = None,
) -> ParsedSession:
    """Build a parsed session with explicit contract fields."""
    return ParsedSession(
        status=SessionStatus.COMPLETED,
        name="Imported",
        inputs={"prompt": "hello"},
        outputs={"answer": "world"},
        expected={"answer": "world"},
        error=None,
        started_at=datetime(2026, 7, 29, 10, tzinfo=UTC),
        ended_at=datetime(2026, 7, 29, 10, 1, tzinfo=UTC),
        external_id=external_id,
        metadata={"source": "fixture"},
        nodes=nodes or [],
    )


def import_details() -> ImportTaskDetails:
    """Build package importer task details."""
    return ImportTaskDetails(
        kind="importer",
        plugin=PackagePluginSpec(
            type="package",
            entrypoint="package:parse",
            requirement="package==1.0",
        ),
        payload=PayloadSpec(blob_id=uuid.uuid4(), sha256="a" * 64),
        provider="fixture",
        agent_id=uuid.uuid4(),
        params={"mode": "strict"},
    )


def test_call_parser_streams_items_and_wraps_midstream_crash() -> None:
    """Preserve yielded items, then wrap a generator failure on advancement."""
    first = parsed_session()

    def parse(payload: bytes, params: dict[str, Any]):
        assert payload == b"payload"
        assert params == {"mode": "strict"}
        yield first
        raise ValueError("bad line")

    items = call_parser(parse, b"payload", {"mode": "strict"})
    assert next(items) is first
    with pytest.raises(SessionImportError, match="bad line"):
        next(items)


def test_call_parser_wraps_failure_before_first_item() -> None:
    """Wrap a parser that raises instead of returning an iterator."""

    def parse(payload: bytes, params: dict[str, Any]):
        raise ValueError("cannot parse")

    with pytest.raises(SessionImportError, match="cannot parse"):
        next(call_parser(parse, b"payload", {}))


def test_call_parser_rejects_unknown_item() -> None:
    """Reject values outside the public parser return union."""

    def parse(payload: bytes, params: dict[str, Any]):
        yield {"not": "a parsed item"}

    with pytest.raises(SessionImportError, match="ParsedSession"):
        next(call_parser(parse, b"payload", {}))


def test_session_request_translates_import_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Add importer identity, provider, origin, and task link."""
    task_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    details = import_details()
    parsed = parsed_session()
    request = session_request(details, parsed)
    assert request.agent_id == details.agent_id
    assert request.origin == SessionOrigin.IMPORTED
    assert request.task_id == task_id
    assert request.provider == "fixture"
    assert request.external_id == "external-1"
    assert request.inputs == {"prompt": "hello"}
    assert request.status == SessionStatus.COMPLETED


def test_flatten_nodes_assigns_depth_first_indexes_and_parents() -> None:
    """Flatten multiple roots with every parent before its children."""
    grandchild = ParsedNode(
        node_type=NodeType.TOOL_CALL,
        name="grandchild",
        status=NodeStatus.COMPLETED,
        tool_name="lookup",
    )
    child = ParsedNode(
        node_type=NodeType.SPAN,
        name="child",
        status=NodeStatus.COMPLETED,
        children=[grandchild],
    )
    roots = [
        ParsedNode(
            node_type=NodeType.LLM_CALL,
            name="root",
            status=NodeStatus.COMPLETED,
            model="gpt",
            children=[child],
        ),
        ParsedNode(
            node_type=NodeType.SPAN,
            name="second-root",
            status=NodeStatus.FAILED,
            error="failed",
        ),
    ]
    flattened = flatten_nodes(roots)
    assert [(node.index, node.parent_index, node.name) for node in flattened] == [
        (0, None, "root"),
        (1, 0, "child"),
        (2, 1, "grandchild"),
        (3, None, "second-root"),
    ]
    assert flattened[0].model == "gpt"
    assert flattened[2].tool_name == "lookup"
    assert all(node.secondary_parent_indexes == [] for node in flattened)


async def test_run_streams_failures_conflicts_and_node_batches(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Count each outcome and ingest successful sessions in bounded batches."""
    task_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    payload_path = tmp_path / "payload"
    payload_path.write_bytes(b"raw")
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    details = import_details()
    roots = [
        ParsedNode(
            node_type=NodeType.SPAN,
            name=f"node-{index}",
            status=NodeStatus.COMPLETED,
        )
        for index in range(NODE_BATCH_SIZE * 2 + 1)
    ]

    def parse(payload: bytes, params: dict[str, Any]):
        assert payload == b"raw"
        assert params == {"mode": "strict"}
        yield ImportFailure(line=3, external_id="bad", error="bad record")
        yield parsed_session("conflict")
        yield parsed_session("create-error")
        yield parsed_session("success", nodes=roots)
        yield parsed_session("ingest-error", nodes=roots[:1])

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID):
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    class Sessions:
        def __init__(self) -> None:
            self.external_by_id: dict[uuid.UUID, str] = {}
            self.batch_sizes: list[int] = []

        async def create(self, request):
            if request.external_id == "conflict":
                raise APIError(409, "already imported")
            if request.external_id == "create-error":
                raise APIError(503, "unavailable")
            session_id = uuid.uuid4()
            self.external_by_id[session_id] = request.external_id
            return SimpleNamespace(id=session_id)

        async def ingest_nodes(self, session_id: uuid.UUID, request):
            if self.external_by_id[session_id] == "ingest-error":
                raise APIError(422, "invalid nodes")
            self.batch_sizes.append(len(request.nodes))

    sessions = Sessions()
    client: Any = SimpleNamespace(tasks=Tasks(), sessions=sessions)
    monkeypatch.setattr(importer_module, "load_source_ref", lambda ref, label: parse)

    await importer_module.run(client, str(task_id))

    assert sessions.batch_sizes == [200, 200, 1]
    result = json.loads(result_path.read_text())
    assert result["created"] == 1
    assert result["skipped"] == 1
    assert result["failed"] == 3
    assert [failure["external_id"] for failure in result["failures"]] == [
        "bad",
        "create-error",
        "ingest-error",
    ]


async def test_run_writes_partial_stats_before_parser_crash(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Persist progress and re-raise when a parser crashes mid-stream."""
    task_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    payload_path = tmp_path / "payload"
    payload_path.write_bytes(b"raw")
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    details = import_details()

    def parse(payload: bytes, params: dict[str, Any]):
        yield parsed_session("created")
        raise ValueError("truncated document")

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID):
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    class Sessions:
        async def create(self, request):
            return SimpleNamespace(id=uuid.uuid4())

        async def ingest_nodes(self, session_id: uuid.UUID, request):
            pytest.fail("empty node tree should not produce a batch")

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
    monkeypatch.setattr(importer_module, "load_source_ref", lambda ref, label: parse)

    with pytest.raises(SessionImportError, match="truncated document"):
        await importer_module.run(client, str(task_id))
    result = json.loads(result_path.read_text())
    assert result["created"] == 1
    assert result["failed"] == 1
    assert result["failures"][0]["line"] is None
    assert "truncated document" in result["failures"][0]["error"]


async def test_run_caps_failure_samples_without_losing_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Count every parser-reported failure but retain only bounded samples."""
    task_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    payload_path = tmp_path / "payload"
    payload_path.write_bytes(b"raw")
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    details = import_details()

    def parse(payload: bytes, params: dict[str, Any]):
        for line in range(MAX_IMPORT_FAILURES + 5):
            yield ImportFailure(line=line, external_id=None, error=f"bad {line}")

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID):
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    client: Any = SimpleNamespace(tasks=Tasks())
    monkeypatch.setattr(importer_module, "load_source_ref", lambda ref, label: parse)

    await importer_module.run(client, str(task_id))
    result = json.loads(result_path.read_text())
    assert result["failed"] == MAX_IMPORT_FAILURES + 5
    assert len(result["failures"]) == MAX_IMPORT_FAILURES
