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

import json
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest
from task_fixtures import (
    TaskAppFixture,
    build_task_app,
    create_script_plugin_version,
    start_task,
)

from conftest import (
    create_agent_task,
    create_agent_version,
    create_blob,
    create_import_task,
    create_job,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.imports import ImportFailure, ImportStats
from kitaru.api_models.v1.session import SessionListParams, SessionOrigin, SessionStatus
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeListParams,
)
from kitaru.api_models.v1.task import ImportTaskDetails
from kitaru.client.exceptions import APIError
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind
from kitaru.task.importer import (
    MAX_IMPORT_FAILURES,
    NODE_BATCH_SIZE,
    ParsedNode,
    ParsedSession,
    SessionImportError,
    call_parser,
    flatten_nodes,
    run,
    session_request,
)


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async for value in build_task_app():
        yield value


def _node(name: str, children: list[ParsedNode] | None = None) -> ParsedNode:
    return ParsedNode(
        node_type=NodeType.LLM_CALL,
        name=name,
        status=NodeStatus.COMPLETED,
        inputs=None,
        outputs=None,
        attributes=None,
        children=children or [],
    )


def _parsed_session(
    external_id: str, nodes: list[ParsedNode] | None = None
) -> ParsedSession:
    return ParsedSession(
        status=SessionStatus.COMPLETED,
        name=external_id,
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=external_id,
        metadata={},
        nodes=nodes or [],
    )


def test_call_parser_is_lazy() -> None:
    """Not advance the parser until the caller iterates."""
    started = False

    def parser(payload: bytes, params: dict) -> Any:
        nonlocal started
        started = True
        yield _parsed_session("a")

    iterator = call_parser(parser, b"", {})
    assert started is False
    next(iterator)
    assert started is True


def test_call_parser_wraps_start_failure() -> None:
    """Wrap an exception raised while constructing the parser's iterator."""

    def parser(payload: bytes, params: dict) -> Any:
        raise ValueError("bad payload")
        yield  # pragma: no cover

    with pytest.raises(SessionImportError, match="bad payload"):
        next(call_parser(parser, b"", {}))


def test_call_parser_wraps_mid_stream_crash() -> None:
    """Yield items until the parser crashes, then wrap the crash."""

    def parser(payload: bytes, params: dict) -> Any:
        yield _parsed_session("a")
        raise ValueError("boom")

    iterator = call_parser(parser, b"", {})
    first = next(iterator)
    assert isinstance(first, ParsedSession)
    with pytest.raises(SessionImportError, match="boom"):
        next(iterator)


def test_call_parser_rejects_unknown_item() -> None:
    """Raise SessionImportError when the parser yields an unsupported item type."""

    def parser(payload: bytes, params: dict) -> Any:
        yield {"not": "a parsed item"}

    with pytest.raises(SessionImportError, match="ParsedSession"):
        next(call_parser(parser, b"", {}))


def test_flatten_nodes_assigns_depth_first_indexes_and_parents() -> None:
    """Assign indexes and parent indexes in depth-first order."""
    tree = [
        _node(
            "root",
            children=[
                _node("child-1", children=[_node("grandchild")]),
                _node("child-2"),
            ],
        ),
        _node("second-root"),
    ]
    flattened = flatten_nodes(tree)
    by_name = {request.name: request for request in flattened}

    assert [request.index for request in flattened] == [0, 1, 2, 3, 4]
    assert by_name["root"].parent_index is None
    assert by_name["child-1"].parent_index == by_name["root"].index
    assert by_name["grandchild"].parent_index == by_name["child-1"].index
    assert by_name["child-2"].parent_index == by_name["root"].index
    assert by_name["second-root"].parent_index is None


def test_session_request_maps_fields() -> None:
    """Build a session create request from importer details and a parsed item."""
    agent_id = uuid.uuid4()
    task_id = uuid.uuid4()
    importer = ImportTaskDetails(
        plugin={
            "type": "package",
            "entrypoint": "acme.parser:parse",
            "requirement": "acme==1.0",
        },
        payload={"blob_id": uuid.uuid4(), "sha256": "a" * 64},
        provider="acme",
        agent_id=agent_id,
        params={},
    )
    parsed = ParsedSession(
        status=SessionStatus.FAILED,
        name="imported-1",
        inputs={"a": 1},
        outputs={"b": 2},
        expected=None,
        error="boom",
        started_at=None,
        ended_at=None,
        external_id="ext-1",
        metadata={"k": "v"},
        nodes=[],
    )

    request = session_request(importer, parsed, task_id)

    assert request.agent_id == agent_id
    assert request.origin == SessionOrigin.IMPORTED
    assert request.status == SessionStatus.FAILED
    assert request.name == "imported-1"
    assert request.inputs == {"a": 1}
    assert request.outputs == {"b": 2}
    assert request.error == "boom"
    assert request.external_id == "ext-1"
    assert request.metadata == {"k": "v"}
    assert request.provider == "acme"
    assert request.task_id == task_id


_PARSER_SCRIPT = """
import json

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ParsedNode, ParsedSession


def parse(payload: bytes, params: dict):
    config = json.loads(payload)
    yield ImportFailure(line=1, external_id="bad-1", error="unparsable item")

    nodes = [
        ParsedNode(
            external_id=f"node-{i}",
            node_type=NodeType.LLM_CALL,
            name=f"call-{i}",
            status=NodeStatus.COMPLETED,
            inputs=None,
            outputs=None,
            attributes=None,
        )
        for i in range(config["node_count"])
    ]
    yield ParsedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=nodes,
    )
    yield ParsedSession(
        status=SessionStatus.COMPLETED,
        name="session-1-dup",
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[],
    )
"""

_CRASHING_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ParsedSession


def parse(payload: bytes, params: dict):
    yield ParsedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[],
    )
    raise RuntimeError("parser exploded")
"""

_SINGLE_SESSION_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ParsedSession


def parse(payload: bytes, params: dict):
    yield ParsedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[],
    )
"""

_SINGLE_SESSION_WITH_NODE_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ParsedNode, ParsedSession


def parse(payload: bytes, params: dict):
    yield ParsedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[
            ParsedNode(
                node_type=NodeType.LLM_CALL,
                name="call-1",
                status=NodeStatus.COMPLETED,
                inputs=None,
                outputs=None,
                attributes=None,
            )
        ],
    )
"""

_MANY_FAILURES_PARSER_SCRIPT = """
from kitaru.api_models.v1.imports import ImportFailure


def parse(payload: bytes, params: dict):
    for line in range(params["failure_count"]):
        yield ImportFailure(line=line, external_id=None, error=f"bad {line}")
"""


async def _create_importer_task(
    task_app: TaskAppFixture,
    script: str,
    tmp_path: Path,
    params: dict[str, Any] | None = None,
) -> tuple[uuid.UUID, Path]:
    """Register a script importer plugin and a running import task for it.

    Args:
        task_app: Task app fixture to register the task against.
        script: Parser script source written to the plugin file.
        tmp_path: Temporary directory the plugin file is written under.
        params: Parameters passed to the importer task.

    Returns:
        Id of the running import task and the path of its plugin file.
    """
    version = await create_script_plugin_version(
        task_app,
        PluginKind.IMPORTER,
        entrypoint="parse",
        name="acme-importer",
        provider="acme",
    )
    job = await create_job(task_app.services.jobs, task_app.agent.owner_id)
    payload_blob = await create_blob(task_app.services.blobs, task_app.agent.owner_id)
    task = await create_import_task(
        task_app.services.tasks,
        job.id,
        plugin_version_id=version.id,
        payload_blob_id=payload_blob.id,
        agent_id=task_app.agent.id,
        params=params or {},
    )
    await start_task(task_app, task.id)

    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text(script)
    return task.id, plugin_path


async def test_importer_flow_batches_nodes_and_dedups(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Batch node ingestion over NODE_BATCH_SIZE and skip a duplicate session."""
    node_count = NODE_BATCH_SIZE + 50
    task_id, plugin_path = await _create_importer_task(
        task_app, _PARSER_SCRIPT, tmp_path, params={"node_count": node_count}
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"node_count": node_count}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    call_count = 0
    original_ingest = task_app.client.sessions.ingest_nodes

    async def counting_ingest(session_id: uuid.UUID, batch: Any) -> Any:
        nonlocal call_count
        call_count += 1
        return await original_ingest(session_id, batch)

    monkeypatch.setattr(task_app.client.sessions, "ingest_nodes", counting_ingest)

    await run(task_app.client, str(task_id))

    assert call_count == 2
    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 1
    assert written.skipped == 1
    assert written.failed == 1
    assert written.failures == [
        ImportFailure(line=1, external_id="bad-1", error="unparsable item")
    ]

    sessions_page = await task_app.client.sessions.list(
        SessionListParams(
            filter=FilterCondition(
                field="external_id", op=FilterOp.EQ, value="session-1"
            )
        )
    )
    nodes_page = await task_app.client.sessions.list_nodes(
        sessions_page.items[0].id, SessionNodeListParams(size=1000)
    )
    assert len(nodes_page.items) == node_count


async def test_importer_flow_mid_stream_crash_writes_partial_stats(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write the stats gathered so far and re-raise on a parser crash."""
    task_id, plugin_path = await _create_importer_task(
        task_app, _CRASHING_PARSER_SCRIPT, tmp_path
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    with pytest.raises(SessionImportError, match="parser exploded"):
        await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 1
    assert written.failed == 1
    assert "parser exploded" in written.failures[0].error


async def test_importer_flow_records_non_conflict_create_error(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Record a failure, not a skip, on a non-conflict session creation error."""
    task_id, plugin_path = await _create_importer_task(
        task_app, _SINGLE_SESSION_PARSER_SCRIPT, tmp_path
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    async def failing_create(request: Any) -> Any:
        raise APIError(httpx.codes.SERVICE_UNAVAILABLE, "backend unavailable")

    monkeypatch.setattr(task_app.client.sessions, "create", failing_create)

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 0
    assert written.skipped == 0
    assert written.failed == 1
    assert written.failures[0].external_id == "session-1"


async def test_importer_flow_records_ingest_nodes_error(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Record a failure when node ingestion fails after the session is created."""
    task_id, plugin_path = await _create_importer_task(
        task_app, _SINGLE_SESSION_WITH_NODE_PARSER_SCRIPT, tmp_path
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    async def failing_ingest(session_id: uuid.UUID, batch: Any) -> Any:
        raise APIError(httpx.codes.UNPROCESSABLE_ENTITY, "invalid nodes")

    monkeypatch.setattr(task_app.client.sessions, "ingest_nodes", failing_ingest)

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 0
    assert written.failed == 1
    assert written.failures[0].external_id == "session-1"


async def test_run_caps_failure_samples_without_losing_count(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cap stored failure samples while the failed count keeps growing."""
    failure_count = MAX_IMPORT_FAILURES + 5
    task_id, plugin_path = await _create_importer_task(
        task_app,
        _MANY_FAILURES_PARSER_SCRIPT,
        tmp_path,
        params={"failure_count": failure_count},
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.failed == failure_count
    assert len(written.failures) == MAX_IMPORT_FAILURES


async def test_importer_flow_rejects_non_importer_task(
    task_app: TaskAppFixture,
) -> None:
    """Raise SessionImportError when the task spec is not an importer task."""
    job = await create_job(task_app.services.jobs, task_app.agent.owner_id)
    version = await create_agent_version(
        task_app.services.agent_versions,
        agent_id=task_app.agent.id,
        owner_id=task_app.agent.owner_id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    task = await create_agent_task(
        task_app.services.tasks, job.id, agent_version_id=version.id
    )
    with pytest.raises(SessionImportError, match="not an importer task"):
        await run(task_app.client, str(task.id))
