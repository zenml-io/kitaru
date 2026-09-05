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
"""Tests for the analyzer contract and the analysis flow."""

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from task_fixtures import TaskAppFixture, build_task_app

from conftest import create_agent_task, create_agent_version, create_job
from kitaru.api_models.v1.insight import InsightInput, TextInsightData
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.api_models.v1.task import (
    AnalysisTaskDetails,
    PackagePluginSpec,
    ScriptPluginSpec,
)
from kitaru.server.domain.agent_version import RunSpec
from kitaru.task import analyzer as analyzer_module
from kitaru.task.analyzer import AnalysisError, SessionView, call_analyzer, run


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async for value in build_task_app():
        yield value


def _session_views(count: int = 1) -> list[SessionView]:
    """Build minimal valid SessionViews for analyzer contract tests."""
    now = datetime.now(UTC)
    views = []
    for _ in range(count):
        session = SessionDetailResponse(
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
        views.append(SessionView(session=session, nodes=[]))
    return views


async def test_call_analyzer_single_result() -> None:
    """Normalize a single InsightInput into a one-element list."""
    views = _session_views()

    def analyze(sessions: list[SessionView], **params: object) -> InsightInput:
        assert sessions is views
        return InsightInput(
            name="summary", title="Summary", data=TextInsightData(content="ok")
        )

    results = await call_analyzer("summary-check", analyze, views, {})
    assert [item.name for item in results] == ["summary"]


async def test_call_analyzer_list_result() -> None:
    """Pass a list of results through unchanged."""
    views = _session_views()

    def analyze(sessions: list[SessionView], **params: object) -> list[InsightInput]:
        return [
            InsightInput(name="a", title="A", data=TextInsightData(content="1")),
            InsightInput(name="b", title="B", data=TextInsightData(content="2")),
        ]

    results = await call_analyzer("multi", analyze, views, {})
    assert [item.name for item in results] == ["a", "b"]


async def test_call_analyzer_empty_list_raises() -> None:
    """Raise AnalysisError when the analyzer returns no results."""
    views = _session_views()

    def analyze(sessions: list[SessionView], **params: object) -> list[InsightInput]:
        return []

    with pytest.raises(AnalysisError, match="summary-check"):
        await call_analyzer("summary-check", analyze, views, {})


async def test_call_analyzer_duplicate_names_raise() -> None:
    """Raise AnalysisError when two results share a name."""
    views = _session_views()

    def analyze(sessions: list[SessionView], **params: object) -> list[InsightInput]:
        return [
            InsightInput(name="a", title="A", data=TextInsightData(content="1")),
            InsightInput(name="a", title="A2", data=TextInsightData(content="2")),
        ]

    with pytest.raises(AnalysisError, match="duplicate"):
        await call_analyzer("dup", analyze, views, {})


async def test_call_analyzer_non_insight_input_raises() -> None:
    """Raise AnalysisError when the analyzer returns a non-InsightInput value."""
    views = _session_views()

    def analyze(sessions: list[SessionView], **params: object) -> list[Any]:
        return [{"name": "a"}]

    with pytest.raises(AnalysisError, match="non-InsightInput"):
        await call_analyzer("summary-check", analyze, views, {})


async def test_call_analyzer_raising_analyzer_wrapped() -> None:
    """Wrap an analyzer's exception in AnalysisError."""
    views = _session_views()

    def analyze(sessions: list[SessionView], **params: object) -> InsightInput:
        raise ValueError("boom")

    with pytest.raises(AnalysisError, match="raised an error"):
        await call_analyzer("broken", analyze, views, {})


async def test_call_analyzer_passes_params() -> None:
    """Pass the params dict through as keyword arguments."""
    views = _session_views()
    received = {}

    def analyze(sessions: list[SessionView], threshold: float) -> InsightInput:
        received["threshold"] = threshold
        return InsightInput(name="a", title="A", data=TextInsightData(content="ok"))

    await call_analyzer("with-params", analyze, views, {"threshold": 0.5})
    assert received["threshold"] == 0.5


async def test_call_analyzer_awaits_an_async_analyzer() -> None:
    """Await the result of an async analyzer."""
    views = _session_views()

    async def analyze(sessions: list[SessionView], **params: object) -> InsightInput:
        assert sessions is views
        return InsightInput(
            name="summary", title="Summary", data=TextInsightData(content="ok")
        )

    results = await call_analyzer("summary-check", analyze, views, {})
    assert [item.name for item in results] == ["summary"]


async def test_call_analyzer_raising_async_analyzer_wrapped() -> None:
    """Wrap an async analyzer's exception in AnalysisError."""
    views = _session_views()

    async def analyze(sessions: list[SessionView], **params: object) -> InsightInput:
        raise ValueError("boom")

    with pytest.raises(AnalysisError, match="raised an error"):
        await call_analyzer("broken", analyze, views, {})


async def test_run_loads_from_a_source_ref_and_fetches_sessions_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Load an installed analyzer and fetch every session with its nodes in order."""
    session_ids = [uuid.uuid4(), uuid.uuid4()]
    task_id = uuid.uuid4()
    details = AnalysisTaskDetails(
        analyzer_name="spread",
        params={"field": "answer"},
        plugin=PackagePluginSpec(
            entrypoint="package:analyze", requirement="package==1.0"
        ),
        agent_id=uuid.uuid4(),
        input_session_ids=session_ids,
    )
    sessions = {
        session_id: SessionDetailResponse.model_construct(id=session_id)
        for session_id in session_ids
    }
    nodes = {
        session_id: SessionNodeResponse.model_construct(
            id=uuid.uuid4(), session_id=session_id, index=0
        )
        for session_id in session_ids
    }
    calls: list[uuid.UUID] = []

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID) -> Any:
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    class Sessions:
        async def get_with_nodes(
            self, requested_id: uuid.UUID
        ) -> SessionWithNodesResponse:
            calls.append(requested_id)
            return SessionWithNodesResponse(
                session=sessions[requested_id], nodes=[nodes[requested_id]]
            )

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
    captured: list[object] = []

    def analyze(views: list[SessionView], **params: object) -> InsightInput:
        assert [view.session for view in views] == [
            sessions[session_id] for session_id in session_ids
        ]
        assert params == {"field": "answer"}
        return InsightInput(
            name="spread", title="Spread", data=TextInsightData(content=str(len(views)))
        )

    monkeypatch.setattr(analyzer_module, "load_source_ref", lambda ref, label: analyze)
    monkeypatch.setattr(analyzer_module, "write_task_result", captured.append)

    await analyzer_module.run(client, str(task_id))

    assert calls == session_ids
    assert captured == [
        [InsightInput(name="spread", title="Spread", data=TextInsightData(content="2"))]
    ]


async def test_analyzer_flow_end_to_end(
    task_app: TaskAppFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the analysis flow against a script plugin and real sessions."""
    client = task_app.client

    sessions = []
    for prompt, answer in [("hi", "hello"), ("bye", "goodbye!")]:
        session = await client.sessions.create(
            SessionCreateRequest(
                agent_id=task_app.agent.id,
                origin=SessionOrigin.RECORDED,
                inputs={"prompt": prompt},
                outputs={"answer": answer},
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
                        inputs={"prompt": prompt},
                        outputs={"answer": answer},
                        attributes=None,
                    )
                ]
            ),
        )
        sessions.append(session)

    task_id = uuid.uuid4()
    details = AnalysisTaskDetails(
        analyzer_name="length-spread",
        params={"field": "answer"},
        plugin=ScriptPluginSpec(
            entrypoint="analyze", blob_id=uuid.uuid4(), sha256="unused"
        ),
        agent_id=task_app.agent.id,
        input_session_ids=[session.id for session in sessions],
    )

    async def fake_get_spec(requested_task_id: uuid.UUID) -> Any:
        assert requested_task_id == task_id
        return SimpleNamespace(details=details)

    monkeypatch.setattr(client.tasks, "get_spec", fake_get_spec)

    plugin_path = tmp_path / "analyzer.py"
    plugin_path.write_text(
        "from kitaru.api_models.v1.insight import TextInsightData\n"
        "from kitaru.task.analyzer import InsightInput, SessionView\n\n\n"
        "def analyze(sessions: list[SessionView], field: str) -> InsightInput:\n"
        "    lengths = [len(view.nodes[0].outputs[field]) for view in sessions]\n"
        "    return InsightInput(\n"
        "        name='length-spread',\n"
        "        title='Length spread',\n"
        "        data=TextInsightData(content=str(sorted(lengths))),\n"
        "    )\n"
    )
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    await run(client, str(task_id))

    written = json.loads(result_path.read_text())
    assert written == [
        {
            "name": "length-spread",
            "title": "Length spread",
            "description": None,
            "data": {"type": "text", "content": "[5, 8]"},
            "metadata": {},
        }
    ]


async def test_analyzer_flow_rejects_non_analyzer_task(
    task_app: TaskAppFixture,
) -> None:
    """Raise AnalysisError when the task spec is not an analyzer task."""
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
    with pytest.raises(AnalysisError, match="not an analyzer task"):
        await run(task_app.client, str(task.id))
