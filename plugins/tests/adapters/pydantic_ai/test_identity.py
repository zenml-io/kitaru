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
"""Client-side agent identity validation for the PydanticAI adapter."""

import json
import uuid
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel

import kitaru_pydantic_ai.capability as capability_module
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.replay_config import PassthroughConfig, ToolPolicy
from kitaru_pydantic_ai import KitaruAgent


class _FakeSessions:
    def __init__(self, session_id: uuid.UUID) -> None:
        self._session_id = session_id
        self.created: list[Any] = []
        self.updated: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        self.created.append(request)
        return SimpleNamespace(id=self._session_id)

    async def update(self, session_id: uuid.UUID, request: Any) -> None:
        self.updated.append((session_id, request))

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        del session_id, request
        return []


class _FakeClient:
    instances: ClassVar[list["_FakeClient"]] = []
    next_replay: ClassVar[ReplayResponse | None] = None

    def __init__(self, **_: Any) -> None:
        self.session_id = uuid.uuid4()
        self.sessions = _FakeSessions(self.session_id)
        self.replay = type(self).next_replay
        self.replays = SimpleNamespace(get=self._get_replay)
        self.closed = False
        type(self).instances.append(self)

    async def _get_replay(self, replay_id: uuid.UUID) -> ReplayResponse:
        assert self.replay is not None
        assert replay_id == self.replay.id
        return self.replay

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _fake_client(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "KITARU_API_KEY",
        "KITARU_API_TOKEN",
        "KITARU_API_URL",
        "KITARU_AGENT_ID",
        "KITARU_TASK_ID",
        "KITARU_TASK_INPUTS",
        "KITARU_SESSION_NAME",
        "KITARU_REPLAY_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    _FakeClient.instances.clear()
    _FakeClient.next_replay = None
    monkeypatch.setattr(capability_module, "KitaruAPIClient", _FakeClient)


def _forbid_model_call(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
    raise AssertionError("model must not be called before identity validation")


def test_standalone_run_sync_without_agent_fails_before_client_or_model() -> None:
    agent = KitaruAgent(Agent(FunctionModel(_forbid_model_call)))

    with pytest.raises(ValueError, match="not inside a Kitaru worker task") as exc_info:
        agent.run_sync("hello")

    message = str(exc_info.value)
    assert "agent_id=<agent UUID>" in message
    assert "kitaru agent get <name>" in message
    assert _FakeClient.instances == []


async def test_standalone_run_and_iter_without_agent_fail_before_client() -> None:
    agent = KitaruAgent(Agent(FunctionModel(_forbid_model_call)))

    with pytest.raises(ValueError, match="agent_id=<agent UUID>"):
        await agent.run("hello")

    with pytest.raises(ValueError, match="agent_id=<agent UUID>"):
        async with agent.iter("hello"):
            pass

    assert _FakeClient.instances == []


async def test_kitaru_agent_id_env_var_alone_does_not_supply_agent_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_AGENT_ID", str(uuid.uuid4()))
    agent = KitaruAgent(Agent(FunctionModel(_forbid_model_call)))

    with pytest.raises(ValueError, match="agent_id=<agent UUID>"):
        await agent.run("hello")

    assert _FakeClient.instances == []


async def test_whitespace_task_id_does_not_bypass_agent_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_TASK_ID", "   ")
    agent = KitaruAgent(Agent(FunctionModel(_forbid_model_call)))

    with pytest.raises(ValueError, match="not inside a Kitaru worker task"):
        await agent.run("hello")

    assert _FakeClient.instances == []


async def test_standalone_run_accepts_agent_version_id_without_agent_id() -> None:
    version_id = uuid.uuid4()
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_version_id=version_id,
    )

    result = await agent.run("hello")

    assert result.output == "success (no tool calls)"
    request = _FakeClient.instances[0].sessions.created[0]
    assert request.agent_id is None
    assert request.agent_version_id == version_id


async def test_worker_replay_without_agent_id_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = capability_module.datetime.now(capability_module.UTC)
    replay = ReplayResponse(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        experiment_run_id=None,
        baseline_session_id=uuid.uuid4(),
        result_session_id=None,
        override=None,
        tool_policy=ToolPolicy(default=PassthroughConfig(), tools={}),
        evaluators=[],
        evaluate_baselines=False,
        baseline_evaluation_mode=BaselineEvaluationMode.NONE,
        status=ReplayStatus.PENDING,
        error=None,
        created=now,
        updated=now,
    )
    _FakeClient.next_replay = replay
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps("replayed prompt"))
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))

    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("replayed")])

    agent = KitaruAgent(Agent(FunctionModel(model)))

    result = await agent.run("ignored caller prompt")

    assert result.output == "replayed"
    request = _FakeClient.instances[0].sessions.created[0]
    assert request.agent_id is None
    assert request.agent_version_id is None
