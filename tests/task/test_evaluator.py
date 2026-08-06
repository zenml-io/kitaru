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
"""Tests for the evaluator contract and the evaluation flow."""

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from importlib.resources import as_file, files
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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
    create_evaluation_task,
    create_job,
)
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.api_models.v1.task import EvaluationTaskDetails, PackagePluginSpec
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind
from kitaru.task import evaluator as evaluator_module
from kitaru.task.evaluator import EvaluationError, SessionView, call_evaluator, run
from kitaru.task.plugins import load_plugin_entrypoint


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async for value in build_task_app():
        yield value


def _session_view() -> SessionView:
    """Build a minimal valid SessionView for evaluator contract tests."""
    now = datetime.now(UTC)
    session = SessionResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        number=1,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        inputs=None,
        outputs=None,
        metadata={},
        llm_call_count=0,
        tool_call_count=0,
        created=now,
        updated=now,
    )
    return SessionView(session=session, nodes=[])


def test_call_evaluator_single_result() -> None:
    """Normalize a single EvaluationResult into a one-element list."""
    view = _session_view()

    def evaluate(session: SessionView, **params: object) -> EvaluationResult:
        assert session is view
        return EvaluationResult(name="quality", score=0.9)

    results = call_evaluator("quality-check", evaluate, view, {})
    assert [item.name for item in results] == ["quality"]


def test_call_evaluator_list_result() -> None:
    """Pass a list of results through unchanged."""
    view = _session_view()

    def evaluate(session: SessionView, **params: object) -> list[EvaluationResult]:
        return [
            EvaluationResult(name="a", score=1.0),
            EvaluationResult(name="b", score=2.0),
        ]

    results = call_evaluator("multi", evaluate, view, {})
    assert [item.name for item in results] == ["a", "b"]


def test_call_evaluator_empty_list_raises() -> None:
    """Raise EvaluationError when the evaluator returns no results."""
    view = _session_view()

    def evaluate(session: SessionView, **params: object) -> list[EvaluationResult]:
        return []

    with pytest.raises(EvaluationError, match="quality-check"):
        call_evaluator("quality-check", evaluate, view, {})


def test_call_evaluator_duplicate_names_raise() -> None:
    """Raise EvaluationError when two results share a name."""
    view = _session_view()

    def evaluate(session: SessionView, **params: object) -> list[EvaluationResult]:
        return [
            EvaluationResult(name="a", score=1.0),
            EvaluationResult(name="a", score=2.0),
        ]

    with pytest.raises(EvaluationError, match="duplicate"):
        call_evaluator("dup", evaluate, view, {})


def test_call_evaluator_non_evaluation_result_raises() -> None:
    """Raise EvaluationError when the evaluator returns a non-EvaluationResult value."""
    view = _session_view()

    def evaluate(session: SessionView, **params: object) -> list[Any]:
        return [{"name": "a", "score": 1.0}]

    with pytest.raises(EvaluationError, match="non-EvaluationResult"):
        call_evaluator("quality-check", evaluate, view, {})


def test_call_evaluator_raising_evaluator_wrapped() -> None:
    """Wrap an evaluator's exception in EvaluationError."""
    view = _session_view()

    def evaluate(session: SessionView, **params: object) -> EvaluationResult:
        raise ValueError("boom")

    with pytest.raises(EvaluationError, match="raised an error"):
        call_evaluator("broken", evaluate, view, {})


def test_call_evaluator_passes_params() -> None:
    """Pass the params dict through as keyword arguments."""
    view = _session_view()
    received = {}

    def evaluate(session: SessionView, threshold: float) -> EvaluationResult:
        received["threshold"] = threshold
        return EvaluationResult(name="a", score=1.0)

    call_evaluator("with-params", evaluate, view, {"threshold": 0.5})
    assert received["threshold"] == 0.5


def test_packaged_built_in_source_loads_through_script_contract() -> None:
    """Run packaged diagnostic and policy entrypoints through the plugin loader."""
    resource = files("kitaru._default_plugins").joinpath("evaluators.py")
    with as_file(resource) as path:
        view = _session_view()
        diagnostics = load_plugin_entrypoint(path, "session_diagnostics", "Evaluator")
        budget = load_plugin_entrypoint(path, "resource_budget", "Evaluator")

        first = call_evaluator("kitaru/session-diagnostics", diagnostics, view, {})
        repeated = call_evaluator("kitaru/session-diagnostics", diagnostics, view, {})
        policy = call_evaluator(
            "kitaru/resource-budget", budget, view, {"max_nodes": 0}
        )

    assert [result.model_dump(mode="json") for result in first] == [
        result.model_dump(mode="json") for result in repeated
    ]
    assert {result.name for result in first} >= {"input_sha256", "terminality"}
    assert {result.name for result in policy} >= {
        "config_sha256",
        "node_count_budget",
    }


async def test_run_fetches_session_and_nodes_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Start the session and node fetches together instead of one after the other."""
    session_id = uuid.uuid4()
    task_id = uuid.uuid4()
    details = EvaluationTaskDetails(
        evaluator_name="quality-check",
        params={"threshold": 0.5},
        plugin=PackagePluginSpec(
            entrypoint="package:evaluate", requirement="package==1.0"
        ),
        input_session_id=session_id,
    )
    session = SessionResponse.model_construct(id=session_id)
    node = SessionNodeResponse.model_construct(
        id=uuid.uuid4(), session_id=session_id, index=0
    )
    both_started = asyncio.Event()
    release = asyncio.Event()
    started: set[str] = set()

    async def mark_started(name: str) -> None:
        started.add(name)
        if len(started) == 2:
            both_started.set()
        await release.wait()

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID) -> Any:
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    class Sessions:
        async def get(self, requested_id: uuid.UUID) -> SessionResponse:
            assert requested_id == session_id
            await mark_started("session")
            return session

        async def iter_nodes(
            self, requested_id: uuid.UUID, params: SessionNodeListParams
        ) -> AsyncGenerator[SessionNodeResponse, None]:
            assert requested_id == session_id
            assert params.include_payloads is True
            await mark_started("nodes")
            yield node

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
    captured: list[object] = []

    def evaluate(view: SessionView, **params: object) -> EvaluationResult:
        assert view.session is session
        assert view.nodes == [node]
        assert params == {"threshold": 0.5}
        return EvaluationResult(name="quality", score=1.0)

    monkeypatch.setattr(
        evaluator_module, "load_source_ref", lambda ref, label: evaluate
    )
    monkeypatch.setattr(evaluator_module, "write_task_result", captured.append)

    task = asyncio.create_task(evaluator_module.run(client, str(task_id)))
    await asyncio.wait_for(both_started.wait(), timeout=1)
    assert started == {"session", "nodes"}
    release.set()
    await task
    assert captured == [[EvaluationResult(name="quality", score=1.0)]]


async def test_evaluator_flow_end_to_end(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the evaluator flow against a script plugin and a real session."""
    client = task_app.client

    session = await client.sessions.create(
        SessionCreateRequest(
            agent_id=task_app.agent.id,
            origin=SessionOrigin.RECORDED,
            inputs={"prompt": "hi"},
            outputs={"answer": "hello"},
            metadata={},
        )
    )
    await client.sessions.ingest_nodes(
        session.id,
        SessionNodeBatchRequest(
            nodes=[
                SessionNodeCreateRequest(
                    index=0,
                    node_type=NodeType.LLM_CALL,
                    name="call-1",
                    status=NodeStatus.COMPLETED,
                    inputs={"prompt": "hi"},
                    outputs={"answer": "hello"},
                    attributes=None,
                )
            ]
        ),
    )

    version = await create_script_plugin_version(
        task_app, PluginKind.EVALUATOR, entrypoint="evaluate", name="length-check"
    )
    job = await create_job(task_app.services.jobs, task_app.agent.owner_id)
    task = await create_evaluation_task(
        task_app.services.tasks,
        job.id,
        plugin_version_id=version.id,
        input_session_id=session.id,
        params={"field": "answer"},
    )
    await start_task(task_app, task.id)

    plugin_path = tmp_path / "evaluator.py"
    plugin_path.write_text(
        "from kitaru.task.evaluator import EvaluationResult, SessionView\n\n\n"
        "def evaluate(session: SessionView, field: str) -> EvaluationResult:\n"
        "    node = session.nodes[0]\n"
        "    answer = node.outputs[field]\n"
        "    return EvaluationResult(name='length', score=float(len(answer)))\n"
    )
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    await run(client, str(task.id))

    written = json.loads(result_path.read_text())
    assert written == [
        {
            "name": "length",
            "score": 5.0,
            "value": None,
            "explanation": None,
            "passed": None,
        }
    ]


async def test_evaluator_flow_rejects_non_evaluator_task(
    task_app: TaskAppFixture,
) -> None:
    """Raise EvaluationError when the task spec is not an evaluator task."""
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
    with pytest.raises(EvaluationError, match="not an evaluator task"):
        await run(task_app.client, str(task.id))
