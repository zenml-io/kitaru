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
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from task_fixtures import TaskAppFixture, build_task_app

from conftest import create_agent_task, create_agent_version, create_job
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.insight import InsightInput, TextInsightData
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionListParams,
    SessionOrigin,
    SessionResponse,
)
from kitaru.api_models.v1.task import (
    AnalysisTaskDetails,
    PackagePluginSpec,
    ScriptPluginSpec,
)
from kitaru.client.resources.sessions import SessionsResource
from kitaru.server.domain.agent_version import RunSpec
from kitaru.task import analyzer as analyzer_module
from kitaru.task.analyzer import AnalysisError, call_analyzer, run


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async for value in build_task_app():
        yield value


async def test_call_analyzer_single_result() -> None:
    """Normalize a single InsightInput into a one-element list."""
    session_ids = [uuid.uuid4()]

    def analyze(sessions: list[uuid.UUID], **params: object) -> InsightInput:
        assert sessions is session_ids
        return InsightInput(
            name="summary", title="Summary", data=TextInsightData(content="ok")
        )

    results = await call_analyzer("summary-check", analyze, session_ids, {})
    assert [item.name for item in results] == ["summary"]


async def test_call_analyzer_list_result() -> None:
    """Pass a list of results through unchanged."""
    session_ids = [uuid.uuid4()]

    def analyze(sessions: list[uuid.UUID], **params: object) -> list[InsightInput]:
        return [
            InsightInput(name="a", title="A", data=TextInsightData(content="1")),
            InsightInput(name="b", title="B", data=TextInsightData(content="2")),
        ]

    results = await call_analyzer("multi", analyze, session_ids, {})
    assert [item.name for item in results] == ["a", "b"]


async def test_call_analyzer_empty_list_accepted() -> None:
    """Accept an analysis that finds no useful insights."""
    session_ids = [uuid.uuid4()]

    def analyze(sessions: list[uuid.UUID], **params: object) -> list[InsightInput]:
        return []

    assert await call_analyzer("summary-check", analyze, session_ids, {}) == []


async def test_call_analyzer_duplicate_names_raise() -> None:
    """Raise AnalysisError when two results share a name."""
    session_ids = [uuid.uuid4()]

    def analyze(sessions: list[uuid.UUID], **params: object) -> list[InsightInput]:
        return [
            InsightInput(name="a", title="A", data=TextInsightData(content="1")),
            InsightInput(name="a", title="A2", data=TextInsightData(content="2")),
        ]

    with pytest.raises(AnalysisError, match="duplicate"):
        await call_analyzer("dup", analyze, session_ids, {})


async def test_call_analyzer_non_insight_input_raises() -> None:
    """Raise AnalysisError when the analyzer returns a non-InsightInput value."""
    session_ids = [uuid.uuid4()]

    def analyze(sessions: list[uuid.UUID], **params: object) -> list[Any]:
        return [{"name": "a"}]

    with pytest.raises(AnalysisError, match="non-InsightInput"):
        await call_analyzer("summary-check", analyze, session_ids, {})


async def test_call_analyzer_raising_analyzer_wrapped() -> None:
    """Wrap an analyzer's exception in AnalysisError."""
    session_ids = [uuid.uuid4()]

    def analyze(sessions: list[uuid.UUID], **params: object) -> InsightInput:
        raise ValueError("boom")

    with pytest.raises(AnalysisError, match="raised an error"):
        await call_analyzer("broken", analyze, session_ids, {})


async def test_call_analyzer_passes_params() -> None:
    """Pass the params dict through as keyword arguments."""
    session_ids = [uuid.uuid4()]
    received = {}

    def analyze(sessions: list[uuid.UUID], threshold: float) -> InsightInput:
        received["threshold"] = threshold
        return InsightInput(name="a", title="A", data=TextInsightData(content="ok"))

    await call_analyzer("with-params", analyze, session_ids, {"threshold": 0.5})
    assert received["threshold"] == 0.5


async def test_call_analyzer_awaits_an_async_analyzer() -> None:
    """Await the result of an async analyzer."""
    session_ids = [uuid.uuid4()]

    async def analyze(sessions: list[uuid.UUID], **params: object) -> InsightInput:
        assert sessions is session_ids
        return InsightInput(
            name="summary", title="Summary", data=TextInsightData(content="ok")
        )

    results = await call_analyzer("summary-check", analyze, session_ids, {})
    assert [item.name for item in results] == ["summary"]


async def test_call_analyzer_raising_async_analyzer_wrapped() -> None:
    """Wrap an async analyzer's exception in AnalysisError."""
    session_ids = [uuid.uuid4()]

    async def analyze(sessions: list[uuid.UUID], **params: object) -> InsightInput:
        raise ValueError("boom")

    with pytest.raises(AnalysisError, match="raised an error"):
        await call_analyzer("broken", analyze, session_ids, {})


async def test_run_passes_all_session_ids_without_fetching_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Follow metadata pages in order and pass only IDs to a package analyzer."""
    session_ids = [uuid.UUID(int=index) for index in range(1001, 0, -1)]
    task_id = uuid.uuid4()
    import_id = uuid.uuid4()
    details = AnalysisTaskDetails(
        analyzer_name="spread",
        params={"field": "answer"},
        plugin=PackagePluginSpec(
            entrypoint="package:analyze", requirement="package==1.0"
        ),
        agent_id=uuid.uuid4(),
        import_id=import_id,
    )
    sessions = [
        SessionResponse.model_construct(id=session_id) for session_id in session_ids
    ]

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID) -> Any:
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    listed: list[SessionListParams] = []

    async def list_sessions(params: SessionListParams) -> Page[SessionResponse]:
        listed.append(params)
        if params.cursor is None:
            return Page(items=sessions[:1000], next_cursor="second-page")
        assert params.cursor == "second-page"
        return Page(items=sessions[1000:], next_cursor=None)

    async def reject_detail_fetch(requested_id: uuid.UUID) -> None:
        pytest.fail(f"The analyzer runner fetched details for {requested_id}")

    client: Any = SimpleNamespace(tasks=Tasks())
    client.sessions = SessionsResource(client)
    monkeypatch.setattr(client.sessions, "list", list_sessions)
    monkeypatch.setattr(client.sessions, "get_with_nodes", reject_detail_fetch)
    captured: list[object] = []

    def analyze(received_ids: list[uuid.UUID], **params: object) -> InsightInput:
        assert received_ids == session_ids
        assert params == {"field": "answer"}
        return InsightInput(
            name="spread",
            title="Spread",
            data=TextInsightData(content=str(len(received_ids))),
        )

    monkeypatch.setattr(analyzer_module, "load_source_ref", lambda ref, label: analyze)
    monkeypatch.setattr(analyzer_module, "write_task_result", captured.append)

    await analyzer_module.run(client, str(task_id))

    assert [params.cursor for params in listed] == [None, "second-page"]
    for params in listed:
        assert params.size == 1000
        assert not params.include_payloads
        assert json.loads(params.model_dump(mode="json")["filter"]) == {
            "field": "import_id",
            "op": "eq",
            "value": str(import_id),
        }
    assert captured == [
        [
            InsightInput(
                name="spread", title="Spread", data=TextInsightData(content="1001")
            )
        ]
    ]


@pytest.mark.parametrize("source", ["script", "package"])
@pytest.mark.parametrize("async_callable", [False, True])
async def test_analyzer_flow_end_to_end(
    task_app: TaskAppFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    async_callable: bool,
) -> None:
    """Script and package callables receive IDs only from the specified import."""
    client = task_app.client

    sessions = []
    for _ in range(4):
        session = await client.sessions.create(
            SessionCreateRequest(
                agent_id=task_app.agent.id,
                origin=SessionOrigin.RECORDED,
                inputs=None,
                outputs=None,
                metadata={},
            )
        )
        sessions.append(session)
    # Only an import task can create sessions carrying an import id, so stamp
    # the import onto the stored rows directly.
    import_id = uuid.uuid4()
    for session in sessions[:2]:
        stored = task_app.services.sessions._sessions[session.id]
        stored.import_id = import_id
    task_app.services.sessions._sessions[sessions[2].id].import_id = uuid.uuid4()

    task_id = uuid.uuid4()
    module_name = f"contract_analyzer_{source}_{async_callable}"
    details = AnalysisTaskDetails(
        analyzer_name="session-ids",
        params={"prefix": "imported"},
        plugin=(
            ScriptPluginSpec(
                entrypoint="analyze", blob_id=uuid.uuid4(), sha256="unused"
            )
            if source == "script"
            else PackagePluginSpec(
                entrypoint=f"{module_name}:analyze",
                requirement="contract-analyzer==1.0",
            )
        ),
        agent_id=task_app.agent.id,
        import_id=import_id,
    )

    async def fake_get_spec(requested_task_id: uuid.UUID) -> Any:
        assert requested_task_id == task_id
        return SimpleNamespace(details=details)

    monkeypatch.setattr(client.tasks, "get_spec", fake_get_spec)

    async def reject_detail_fetch(requested_id: uuid.UUID) -> None:
        pytest.fail(f"The analyzer runner fetched details for {requested_id}")

    monkeypatch.setattr(client.sessions, "get_with_nodes", reject_detail_fetch)
    monkeypatch.syspath_prepend(str(tmp_path))
    plugin_path = tmp_path / f"{module_name}.py"
    definition = "async def" if async_callable else "def"
    plugin_path.write_text(
        "import uuid\n"
        "from kitaru.api_models.v1.insight import TextInsightData\n"
        "from kitaru.task.analyzer import InsightInput\n\n\n"
        f"{definition} analyze(\n"
        "    session_ids: list[uuid.UUID], prefix: str\n"
        ") -> InsightInput:\n"
        "    assert all(isinstance(item, uuid.UUID) for item in session_ids)\n"
        "    ids = ','.join(sorted(str(item) for item in session_ids))\n"
        "    content = prefix + ':' + ids\n"
        "    return InsightInput(\n"
        "        name='session-ids',\n"
        "        title='Session IDs',\n"
        "        data=TextInsightData(content=content),\n"
        "    )\n"
    )
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", str(plugin_path))
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))

    await run(client, str(task_id))

    written = json.loads(result_path.read_text())
    assert written == [
        {
            "name": "session-ids",
            "title": "Session IDs",
            "description": None,
            "data": {
                "type": "text",
                "content": "imported:"
                + ",".join(sorted(str(session.id) for session in sessions[:2])),
            },
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
