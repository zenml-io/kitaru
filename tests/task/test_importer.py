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

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from pathlib import Path
from typing import Any, cast

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
    create_import,
    create_import_task,
    create_job,
    imported_node,
    imported_session,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.imports import ImportFailure, ImportQuery, ImportStats
from kitaru.api_models.v1.session import SessionListParams, SessionOrigin, SessionStatus
from kitaru.api_models.v1.session_node import (
    SessionNodeListParams,
)
from kitaru.api_models.v1.task import (
    ApiImportSourceSpec,
    ImportTaskDetails,
    PackagePluginSpec,
    ScriptPluginSpec,
    TaskKind,
    TaskSpecResponse,
)
from kitaru.client.exceptions import APIError
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind
from kitaru.task.importer import (
    MAX_IMPORT_FAILURES,
    NODE_BATCH_SIZE,
    ImportedSession,
    SessionImportError,
    _resolve_importer,
    call_fetcher,
    call_parser,
    flatten_nodes,
    gather_bounded,
    retry_rate_limited,
    run,
    session_request,
)


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async for value in build_task_app():
        yield value


async def test_call_parser_is_lazy() -> None:
    """Not advance the parser until the caller iterates."""
    started = False

    def parser(payload: bytes, params: dict) -> Any:
        nonlocal started
        started = True
        yield imported_session("a")

    iterator = call_parser(parser, b"", {})
    assert started is False
    await anext(iterator)
    assert started is True


async def test_call_parser_wraps_start_failure() -> None:
    """Wrap an exception raised while constructing the parser's iterator."""

    def parser(payload: bytes, params: dict) -> Any:
        raise ValueError("bad payload")
        yield  # pragma: no cover

    with pytest.raises(SessionImportError, match="bad payload"):
        await anext(call_parser(parser, b"", {}))


async def test_call_parser_wraps_mid_stream_crash() -> None:
    """Yield items until the parser crashes, then wrap the crash."""

    def parser(payload: bytes, params: dict) -> Any:
        yield imported_session("a")
        raise ValueError("boom")

    iterator = call_parser(parser, b"", {})
    first = await anext(iterator)
    assert isinstance(first, ImportedSession)
    with pytest.raises(SessionImportError, match="boom"):
        await anext(iterator)


async def test_call_parser_rejects_unknown_item() -> None:
    """Raise SessionImportError when the parser yields an unsupported item type."""

    def parser(payload: bytes, params: dict) -> Any:
        yield {"not": "a imported item"}

    with pytest.raises(SessionImportError, match="ImportedSession"):
        await anext(call_parser(parser, b"", {}))


async def test_call_parser_accepts_an_async_parser() -> None:
    """Advance an async parser with anext instead of next."""

    async def parser(payload: bytes, params: dict) -> Any:
        yield imported_session("a")
        yield imported_session("b")

    items = [item async for item in call_parser(parser, b"", {})]

    assert [item.external_id for item in items] == ["a", "b"]


async def test_call_fetcher_is_lazy() -> None:
    """Not advance the fetcher until the caller iterates."""
    started = False

    async def fetcher(query: dict) -> AsyncIterator[bytes]:
        nonlocal started
        started = True
        yield b"payload"

    iterator = call_fetcher(fetcher, {})
    assert started is False
    await anext(iterator)
    assert started is True


async def test_call_fetcher_wraps_start_failure() -> None:
    """Wrap an exception raised while calling the fetcher."""

    def fetcher(query: dict) -> Any:
        raise ValueError("bad query")

    with pytest.raises(SessionImportError, match="bad query"):
        await anext(call_fetcher(fetcher, {}))


async def test_call_fetcher_wraps_mid_stream_crash() -> None:
    """Yield payloads until the fetcher crashes, then wrap the crash."""

    async def fetcher(query: dict) -> AsyncIterator[bytes]:
        yield b"first"
        raise ValueError("boom")

    iterator = call_fetcher(fetcher, {})
    first = await anext(iterator)
    assert first == b"first"
    with pytest.raises(SessionImportError, match="boom"):
        await anext(iterator)


async def test_call_fetcher_rejects_non_bytes_item() -> None:
    """Raise SessionImportError when the fetcher yields an item that is not bytes."""

    async def fetcher(query: dict) -> Any:
        yield "not bytes"

    with pytest.raises(SessionImportError, match="not bytes"):
        await anext(call_fetcher(fetcher, {}))


async def test_call_fetcher_accepts_a_sync_fetcher() -> None:
    """Advance a sync fetcher with next instead of anext."""

    def fetcher(query: dict) -> Any:
        yield b"first"
        yield b"second"

    payloads = [payload async for payload in call_fetcher(fetcher, {})]

    assert payloads == [b"first", b"second"]


def _script_details(entrypoint: str) -> ImportTaskDetails:
    return ImportTaskDetails(
        plugin=ScriptPluginSpec(
            entrypoint=entrypoint, blob_id=uuid.uuid4(), sha256="x"
        ),
        source=ApiImportSourceSpec(query=ImportQuery(trace_ids=[])),
        agent_id=uuid.uuid4(),
        params={},
    )


def test_resolve_importer_callable_parses_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plain callable entrypoint is the parser and has no fetcher."""
    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text("def parse(payload, params):\n    return []\n")
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))

    parser, fetcher = _resolve_importer(_script_details("parse"))

    assert parser(b"", {}) == []
    assert fetcher is None


def test_resolve_importer_object_with_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An importer object exposes its parse and fetch methods."""
    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text(
        "class Importer:\n"
        "    def parse(self, payload, params):\n"
        "        return [payload]\n"
        "    def fetch(self, query):\n"
        "        return query\n"
        "importer = Importer()\n"
    )
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))

    parser, fetcher = _resolve_importer(_script_details("importer"))

    assert parser(b"x", {}) == [b"x"]
    assert fetcher is not None
    assert fetcher({"a": 1}) == {"a": 1}


def test_resolve_importer_package_plugin() -> None:
    """Load a package plugin's entrypoint by module:attribute."""
    details = ImportTaskDetails(
        plugin=PackagePluginSpec(entrypoint="json:dumps", requirement="pkg==1.0"),
        source=ApiImportSourceSpec(query=ImportQuery(trace_ids=[])),
        agent_id=uuid.uuid4(),
        params={},
    )

    parser, fetcher = _resolve_importer(details)

    assert cast(Any, parser)({"a": 1}) == '{"a": 1}'
    assert fetcher is None


def test_resolve_importer_rejects_a_non_importer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An entrypoint that is neither callable nor an importer is rejected."""
    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text("importer = 42\n")
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))

    with pytest.raises(SessionImportError, match="neither callable nor an importer"):
        _resolve_importer(_script_details("importer"))


async def test_gather_bounded_limits_in_flight_and_keeps_order() -> None:
    in_flight = 0
    peak = 0

    async def _work(value: int) -> int:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return value

    results = await gather_bounded((_work(value) for value in range(6)), 2)
    assert results == list(range(6))
    assert peak == 2


async def test_retry_rate_limited_sleeps_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    calls = 0

    async def _call() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("429")
        return "done"

    def _retry_after(exc: Exception) -> float | None:
        return 2.5 if str(exc) == "429" else None

    assert await retry_rate_limited(_call, _retry_after) == "done"
    assert sleeps == [2.5, 2.5]


async def test_retry_rate_limited_gives_up_and_passes_other_errors() -> None:
    async def _limited() -> None:
        raise RuntimeError("429")

    async def _other() -> None:
        raise ValueError("boom")

    def _retry_after(exc: Exception) -> float | None:
        return 0 if str(exc) == "429" else None

    with pytest.raises(RuntimeError):
        await retry_rate_limited(_limited, _retry_after, max_retries=2)
    with pytest.raises(ValueError):
        await retry_rate_limited(_other, _retry_after)


def test_flatten_nodes_assigns_depth_first_indexes_and_parents() -> None:
    """Assign indexes and parent indexes in depth-first order."""
    tree = [
        imported_node(
            "root",
            children=[
                imported_node("child-1", children=[imported_node("grandchild")]),
                imported_node("child-2"),
            ],
        ),
        imported_node("second-root"),
    ]
    flattened = flatten_nodes(tree)
    by_name = {request.name: request for request in flattened}

    assert [request.index for request in flattened] == [0, 1, 2, 3, 4]
    assert by_name["root"].parent_index is None
    assert by_name["child-1"].parent_index == by_name["root"].index
    assert by_name["grandchild"].parent_index == by_name["child-1"].index
    assert by_name["child-2"].parent_index == by_name["root"].index
    assert by_name["second-root"].parent_index is None


def test_flatten_nodes_preserves_explicit_wire_indexes() -> None:
    """Keep the flat Kitaru JSONL node representation unchanged."""
    nodes = [
        imported_node("child").model_copy(update={"index": 7, "parent_index": 4}),
        imported_node("root").model_copy(update={"index": 4}),
    ]

    flattened = flatten_nodes(nodes)

    assert [node.index for node in flattened] == [4, 7]
    assert flattened[1].parent_index == 4


def test_flatten_nodes_handles_deep_acyclic_tree() -> None:
    """Flatten deep plugin trees without depending on Python recursion depth."""
    root = imported_node("0")
    parent = root
    for index in range(1, 1_200):
        child = imported_node(str(index))
        parent.children.append(child)
        parent = child

    flattened = flatten_nodes([root])

    assert len(flattened) == 1_200
    assert [node.name for node in flattened] == [str(i) for i in range(1_200)]
    assert [node.parent_index for node in flattened] == [None, *range(1_199)]
    for node in flattened:
        json.loads(node.model_dump_json())


@pytest.mark.parametrize("cycle_length", [1, 2])
def test_flatten_nodes_rejects_object_cycles(cycle_length: int) -> None:
    """Reject cyclic plugin objects instead of traversing them forever."""
    root = imported_node("root")
    tail = root
    if cycle_length == 2:
        tail = imported_node("child")
        root.children.append(tail)
    tail.children.append(root)

    with pytest.raises(SessionImportError, match="cycle"):
        flatten_nodes([root])


def test_flatten_nodes_allows_shared_child_outside_ancestor_path() -> None:
    """Preserve repeated subtrees that do not form an ancestor cycle."""
    child = imported_node("shared")

    flattened = flatten_nodes(
        [
            imported_node("left", children=[child]),
            imported_node("right", children=[child]),
        ]
    )

    assert [node.name for node in flattened] == ["left", "shared", "right", "shared"]
    assert [node.parent_index for node in flattened] == [None, 0, None, 2]


def test_session_request_maps_fields() -> None:
    """Build a session create request from a imported item."""
    agent_id = uuid.uuid4()
    parsed = ImportedSession(
        status=SessionStatus.FAILED,
        name="imported-1",
        inputs={"a": 1},
        outputs={"b": 2},
        error="boom",
        started_at=None,
        ended_at=None,
        external_id="ext-1",
        metadata={"k": "v"},
        framework="langgraph",
        nodes=[],
    )

    request = session_request(parsed, agent_id, "acme")

    assert request.agent_id == agent_id
    assert request.origin == SessionOrigin.IMPORTED
    assert request.status == SessionStatus.FAILED
    assert request.name == "imported-1"
    assert request.inputs == {"a": 1}
    assert request.outputs == {"b": 2}
    assert request.error == "boom"
    assert request.external_id == "ext-1"
    assert request.metadata == {"k": "v"}
    assert request.imported_from == "acme"
    assert request.framework == "langgraph"


def test_session_request_carries_an_explicit_origin() -> None:
    """Use the origin the caller passes instead of the imported default."""
    request = session_request(
        imported_session("ext-1"), uuid.uuid4(), "acme", SessionOrigin.REPLAY
    )

    assert request.origin == SessionOrigin.REPLAY
    assert request.imported_from == "acme"


_PARSER_SCRIPT = """
import json

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession


def parse(payload: bytes, params: dict):
    config = json.loads(payload)
    yield ImportFailure(line=1, external_id="bad-1", error="unparsable item")

    nodes = [
        ImportedNode(
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
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=nodes,
    )
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1-dup",
        inputs=None,
        outputs=None,
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
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
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
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
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
from kitaru.task.importer import ImportedNode, ImportedSession


def parse(payload: bytes, params: dict):
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[
            ImportedNode(
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
    max_sessions: int | None = None,
) -> tuple[uuid.UUID, Path]:
    """Register a script importer plugin and a running import task for it.

    Args:
        task_app: Task app fixture to register the task against.
        script: Parser script source written to the plugin file.
        tmp_path: Temporary directory the plugin file is written under.
        params: Parameters passed to the importer task.
        max_sessions: Maximum number of sessions created by the import.

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
    payload = await create_blob(task_app.services.blobs, task_app.agent.owner_id)
    import_ = await create_import(
        task_app.services.imports,
        task_app.agent.owner_id,
        task_app.agent.id,
        job_id=job.id,
        importer_version_id=version.id,
        payload_blob_id=payload.id,
        params=params,
        max_sessions=max_sessions,
    )
    task = await create_import_task(
        task_app.services.tasks, job.id, import_id=import_.id
    )
    await start_task(task_app, task.id)

    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text(script)
    return task.id, plugin_path


async def _create_api_source_task(
    task_app: TaskAppFixture,
    script: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    query: dict[str, Any] | None = None,
    max_sessions: int | None = None,
) -> uuid.UUID:
    """Stub a running import task spec sourced from an API fetcher.

    Args:
        task_app: Task app fixture the task spec is built against.
        script: Parser and fetcher script source written to the plugin file.
        tmp_path: Temporary directory the plugin file is written under.
        monkeypatch: Fixture used to stub the plugin path and task spec.
        query: Query passed to the fetch entrypoint.
        max_sessions: Maximum number of sessions created by the import.

    Returns:
        Id of the stubbed running import task.
    """
    plugin_path = tmp_path / "importer.py"
    plugin_path.write_text(script)
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))

    task_id = uuid.uuid4()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.IMPORTER,
        timeout_seconds=30,
        run=None,
        env={},
        secret_env={},
        details=ImportTaskDetails(
            plugin=ScriptPluginSpec(
                entrypoint="importer", blob_id=uuid.uuid4(), sha256="x"
            ),
            source=ApiImportSourceSpec(
                query=ImportQuery.model_validate(query or {"trace_ids": []})
            ),
            agent_id=task_app.agent.id,
            params={},
            max_sessions=max_sessions,
        ),
    )

    async def fake_get_spec(requested_task_id: uuid.UUID) -> TaskSpecResponse:
        assert requested_task_id == task_id
        return spec

    monkeypatch.setattr(task_app.client.tasks, "get_spec", fake_get_spec)
    return task_id


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


_MANY_SESSIONS_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    for i in range(params["session_count"]):
        yield ImportedSession(
            status=SessionStatus.COMPLETED,
            name=f"session-{i}",
            inputs=None,
            outputs=None,
            error=None,
            started_at=None,
            ended_at=None,
            external_id=f"session-{i}",
            metadata={},
            nodes=[],
        )
"""

_DUPLICATE_THEN_MANY_SESSIONS_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-0",
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-0",
        metadata={},
        nodes=[],
    )
    for i in range(params["session_count"]):
        yield ImportedSession(
            status=SessionStatus.COMPLETED,
            name=f"session-{i}",
            inputs=None,
            outputs=None,
            error=None,
            started_at=None,
            ended_at=None,
            external_id=f"session-{i}",
            metadata={},
            nodes=[],
        )
"""


async def test_run_stops_creating_sessions_at_max_sessions(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stop creating sessions once the import reaches its session limit."""
    session_count = 5
    max_sessions = 2
    task_id, plugin_path = await _create_importer_task(
        task_app,
        _MANY_SESSIONS_PARSER_SCRIPT,
        tmp_path,
        params={"session_count": session_count},
        max_sessions=max_sessions,
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"session_count": session_count}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == max_sessions
    assert written.skipped == 0
    assert written.failed == 0
    assert written.limit_reached is True


async def test_run_duplicates_do_not_consume_the_session_limit(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skip a duplicate without counting it against the session limit."""
    session_count = 3
    max_sessions = 2
    task_id, plugin_path = await _create_importer_task(
        task_app,
        _DUPLICATE_THEN_MANY_SESSIONS_PARSER_SCRIPT,
        tmp_path,
        params={"session_count": session_count},
        max_sessions=max_sessions,
    )
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"session_count": session_count}))
    result_path = tmp_path / "result.json"

    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_PAYLOAD_PATH", str(payload_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == max_sessions
    assert written.skipped == 1
    assert written.failed == 0
    assert written.limit_reached is True


_API_FETCH_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    external_id = payload.decode()
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name=external_id,
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=external_id,
        metadata={},
        nodes=[],
    )


async def fetch(query: dict):
    for trace_id in query["trace_ids"]:
        yield trace_id.encode()


class _Importer:
    def parse(self, payload, params):
        return parse(payload, params)

    async def fetch(self, query):
        async for payload in fetch(query):
            yield payload


importer = _Importer()
"""

_ASYNC_PARSER_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession


async def parse(payload: bytes, params: dict):
    external_id = payload.decode()
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name=external_id,
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=external_id,
        metadata={},
        nodes=[],
    )


async def fetch(query: dict):
    for trace_id in query["trace_ids"]:
        yield trace_id.encode()


class _Importer:
    def parse(self, payload, params):
        return parse(payload, params)

    async def fetch(self, query):
        async for payload in fetch(query):
            yield payload


importer = _Importer()
"""

_API_FETCH_CRASHING_SCRIPT = """
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession


def parse(payload: bytes, params: dict):
    yield ImportedSession(
        status=SessionStatus.COMPLETED,
        name="session-1",
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id="session-1",
        metadata={},
        nodes=[],
    )


async def fetch(query: dict):
    yield b"first"
    raise RuntimeError("fetcher exploded")


class _Importer:
    def parse(self, payload, params):
        return parse(payload, params)

    async def fetch(self, query):
        async for payload in fetch(query):
            yield payload


importer = _Importer()
"""


async def test_run_with_api_source_parses_every_fetched_payload(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fetch payloads from an API source with the query and parse every one."""
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    task_id = await _create_api_source_task(
        task_app,
        _API_FETCH_PARSER_SCRIPT,
        tmp_path,
        monkeypatch,
        query={"trace_ids": ["a", "b", "c"]},
    )

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 3
    assert written.failed == 0


async def test_run_with_api_source_and_an_async_parser(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the import flow to completion with an async parser."""
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    task_id = await _create_api_source_task(
        task_app,
        _ASYNC_PARSER_SCRIPT,
        tmp_path,
        monkeypatch,
        query={"trace_ids": ["a", "b", "c"]},
    )

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 3
    assert written.failed == 0


async def test_run_with_api_source_mid_stream_fetch_crash_writes_partial_stats(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write the stats gathered so far and re-raise on a fetcher crash."""
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    task_id = await _create_api_source_task(
        task_app, _API_FETCH_CRASHING_SCRIPT, tmp_path, monkeypatch
    )

    with pytest.raises(SessionImportError, match="fetcher exploded"):
        await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 1
    assert written.failed == 1
    assert "fetcher exploded" in written.failures[0].error


async def test_run_with_api_source_stops_fetching_at_max_sessions(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stop pulling further payloads once the import reaches its session limit."""
    from kitaru.task import importer as importer_module

    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    task_id = await _create_api_source_task(
        task_app,
        _API_FETCH_PARSER_SCRIPT,
        tmp_path,
        monkeypatch,
        query={"trace_ids": ["a", "b", "c", "d", "e"]},
        max_sessions=2,
    )

    fetched = 0
    original_call_fetcher = importer_module.call_fetcher

    async def counting_call_fetcher(
        fetcher: Any, query: dict[str, Any]
    ) -> AsyncIterator[bytes]:
        nonlocal fetched
        async for payload in original_call_fetcher(fetcher, query):
            fetched += 1
            yield payload

    monkeypatch.setattr(importer_module, "call_fetcher", counting_call_fetcher)

    await run(task_app.client, str(task_id))

    written = ImportStats.model_validate(json.loads(result_path.read_text()))
    assert written.created == 2
    assert written.limit_reached is True
    assert fetched == 3


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
