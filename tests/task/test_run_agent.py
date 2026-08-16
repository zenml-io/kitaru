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
"""Tests for the run function contract and the run-agent flow."""

import uuid
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import pytest
from task_fixtures import TaskAppFixture, build_task_app

from conftest import create_agent_task, create_agent_version, create_job
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.task import FunctionAgentTaskDetails
from kitaru.server.domain.agent_version import CommandRunSpec
from kitaru.task import run_agent as run_agent_module
from kitaru.task.plugins import PluginLoadError
from kitaru.task.run_agent import AgentRunError, call_run_function, run


@pytest.fixture
async def task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async for value in build_task_app():
        yield value


def test_call_run_function_returns_the_external_id() -> None:
    """Pass through the run function's returned external id."""

    def run_function(inputs: Any, replay_id: str | None) -> str:
        assert inputs == {"prompt": "hi"}
        assert replay_id == "replay-1"
        return "external-1"

    result = call_run_function("plugin:run", run_function, {"prompt": "hi"}, "replay-1")
    assert result == "external-1"


def test_call_run_function_raising_wrapped() -> None:
    """Wrap the run function's exception in AgentRunError, naming the function."""

    def run_function(inputs: Any, replay_id: str | None) -> str:
        raise ValueError("boom")

    with pytest.raises(AgentRunError, match="raised an error") as excinfo:
        call_run_function("plugin:run", run_function, None, None)
    assert "plugin:run" in str(excinfo.value)


def test_call_run_function_non_string_result_raises() -> None:
    """Raise AgentRunError when the run function returns a non-string value."""

    def run_function(inputs: Any, replay_id: str | None) -> Any:
        return 123

    with pytest.raises(AgentRunError, match="did not return a non-empty string"):
        call_run_function("plugin:run", run_function, None, None)


def test_call_run_function_empty_string_result_raises() -> None:
    """Raise AgentRunError when the run function returns an empty string."""

    def run_function(inputs: Any, replay_id: str | None) -> str:
        return ""

    with pytest.raises(AgentRunError, match="did not return a non-empty string"):
        call_run_function("plugin:run", run_function, None, None)


async def test_run_loads_calls_and_creates_the_placeholder_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Load the entrypoint, call it, and create the REPLAY placeholder session."""
    task_id = uuid.uuid4()
    replay_id = uuid.uuid4()
    details = FunctionAgentTaskDetails(
        entrypoint="plugin:run", inputs={"prompt": "hi"}, replay_id=replay_id
    )
    received: list[tuple[Any, str | None]] = []

    def run_function(inputs: Any, run_replay_id: str | None) -> str:
        received.append((inputs, run_replay_id))
        return "external-session-1"

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID) -> Any:
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    created: list[SessionCreateRequest] = []

    class Sessions:
        async def create(self, request: SessionCreateRequest) -> Any:
            created.append(request)
            return SimpleNamespace()

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
    captured: list[object] = []

    monkeypatch.setattr(
        run_agent_module, "load_source_ref", lambda ref, label: run_function
    )
    monkeypatch.setattr(run_agent_module, "write_task_result", captured.append)

    await run_agent_module.run(client, str(task_id))

    assert received == [({"prompt": "hi"}, str(replay_id))]
    assert created == [
        SessionCreateRequest(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="external-session-1",
            inputs=None,
            outputs=None,
        )
    ]
    assert captured == ["external-session-1"]


async def test_run_without_a_replay_id_passes_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass None to the run function when the task carries no replay id."""
    task_id = uuid.uuid4()
    details = FunctionAgentTaskDetails(entrypoint="plugin:run", inputs=None)
    received: list[str | None] = []

    def run_function(inputs: Any, run_replay_id: str | None) -> str:
        received.append(run_replay_id)
        return "external-session-1"

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID) -> Any:
            return SimpleNamespace(details=details)

    class Sessions:
        async def create(self, request: SessionCreateRequest) -> Any:
            return SimpleNamespace()

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())

    monkeypatch.setattr(
        run_agent_module, "load_source_ref", lambda ref, label: run_function
    )
    monkeypatch.setattr(run_agent_module, "write_task_result", lambda value: None)

    await run_agent_module.run(client, str(task_id))

    assert received == [None]


async def test_run_wraps_a_plugin_load_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap a PluginLoadError from the entrypoint load into AgentRunError."""
    task_id = uuid.uuid4()
    details = FunctionAgentTaskDetails(entrypoint="plugin:run", inputs=None)

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID) -> Any:
            return SimpleNamespace(details=details)

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=None)

    def raise_load_error(ref: str, label: str) -> Any:
        raise PluginLoadError("could not import 'plugin'")

    monkeypatch.setattr(run_agent_module, "load_source_ref", raise_load_error)

    with pytest.raises(AgentRunError, match="could not import 'plugin'"):
        await run_agent_module.run(client, str(task_id))


async def test_run_rejects_non_function_agent_task(
    task_app: TaskAppFixture,
) -> None:
    """Raise AgentRunError when the task spec is not a function agent task."""
    job = await create_job(task_app.services.jobs, task_app.agent.owner_id)
    version = await create_agent_version(
        task_app.services.agent_versions,
        agent_id=task_app.agent.id,
        owner_id=task_app.agent.owner_id,
        run_spec=CommandRunSpec(command="run.sh", timeout_seconds=60),
    )
    task = await create_agent_task(
        task_app.services.tasks, job.id, agent_version_id=version.id
    )
    with pytest.raises(AgentRunError, match="not a function agent task"):
        await run(task_app.client, str(task.id))
