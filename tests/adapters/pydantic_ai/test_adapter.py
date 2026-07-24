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
"""Focused contract tests for the PydanticAI adapter."""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from datetime import date
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from pydantic_ai import Agent
from pydantic_ai.agent import WrapperAgent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    NativeToolCallPart,
    NativeToolReturnPart,
    SystemPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel

import kitaru.adapters.pydantic_ai.capability as capability_module
from kitaru.adapters.pydantic_ai import (
    KitaruAgent,
    ToolPolicyError,
    ToolPolicyMissError,
)
from kitaru.api_models.v1.replays import (
    HistoryPolicy,
    LLMPolicy,
    PassthroughPolicy,
    ReplayOverride,
    ReplaySpecResponse,
    ReplaySpecRun,
    ScorerConfig,
    ScoringPolicy,
    StaticCase,
    StaticMatchMode,
    StaticPolicy,
    ToolLookupResponse,
    ToolPolicyConfig,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_nodes import NodeStatus, NodeType
from kitaru.api_models.v1.sessions import SessionStatus
from kitaru.hashing import tool_call_cache_key


class _FakeSessions:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.created: list[Any] = []
        self.updated: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        self.created.append(request)
        self._client.events.append("session:create")
        return SimpleNamespace(id=self._client.session_id)

    async def update(self, session_id: uuid.UUID, request: Any) -> Any:
        self.updated.append((session_id, request))
        self._client.events.append("session:update")
        return None


class _FakeSessionNodes:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.upserts: list[tuple[uuid.UUID, Any]] = []

    async def upsert(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        self.upserts.append((session_id, request))
        self._client.events.append("nodes:upsert")
        return []


class _FakeReplays:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.lookups: list[tuple[uuid.UUID, Any]] = []

    async def get_spec(self, replay_id: uuid.UUID) -> ReplaySpecResponse:
        assert self._client.spec is not None
        assert replay_id == self._client.spec.replay_id
        return self._client.spec

    async def tool_lookup(self, replay_id: uuid.UUID, request: Any) -> Any:
        self.lookups.append((replay_id, request))
        return self._client.lookup_response


class _FakeClient:
    instances: ClassVar[list["_FakeClient"]] = []
    next_spec: ClassVar[ReplaySpecResponse | None] = None
    next_lookup_response: ClassVar[ToolLookupResponse] = ToolLookupResponse(
        found=False, result=None
    )

    def __init__(self, **_: Any) -> None:
        self.session_id = uuid.uuid4()
        self.spec = type(self).next_spec
        self.lookup_response = type(self).next_lookup_response
        self.events: list[str] = []
        self.sessions = _FakeSessions(self)
        self.session_nodes = _FakeSessionNodes(self)
        self.replays = _FakeReplays(self)
        self.closed = False
        type(self).instances.append(self)

    async def close(self) -> None:
        self.closed = True
        self.events.append("client:close")


@pytest.fixture(autouse=True)
def _fake_client(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "KITARU_API_KEY",
        "KITARU_API_URL",
        "KITARU_INPUTS",
        "KITARU_OVERRIDE",
        "KITARU_REPLAY_ID",
        "KITARU_SESSION_ID_FILE",
        "KITARU_SESSION_NAME",
    ):
        monkeypatch.delenv(name, raising=False)
    _FakeClient.instances.clear()
    _FakeClient.next_spec = None
    _FakeClient.next_lookup_response = ToolLookupResponse(found=False, result=None)
    monkeypatch.setattr(capability_module, "KitaruAPIClient", _FakeClient)


def _scoring_policy() -> ScoringPolicy:
    return ScoringPolicy(
        scorers=[ScorerConfig(name="score", source="test:score")],
        pass_threshold=0.5,
    )


def _replay_spec(
    policy: Any,
    *,
    inputs: Any = "replayed prompt",
    override: ReplayOverride | None = None,
    tools: dict[str, Any] | None = None,
) -> ReplaySpecResponse:
    replay_id = uuid.uuid4()
    return ReplaySpecResponse(
        replay_id=replay_id,
        inputs=inputs,
        override=override,
        tool_policy=ToolPolicyConfig(default=policy, tools=tools or {}),
        scoring_policy=_scoring_policy(),
        score_baselines=False,
        run=ReplaySpecRun(
            command="python agent.py",
            working_dir=None,
            env={},
            timeout_seconds=30,
        ),
        secret_env={},
        original_session_id=uuid.uuid4(),
    )


def _set_replay(monkeypatch: pytest.MonkeyPatch, spec: ReplaySpecResponse) -> None:
    _FakeClient.next_spec = spec
    monkeypatch.setenv("KITARU_REPLAY_ID", str(spec.replay_id))


def _nodes(client: _FakeClient) -> list[Any]:
    return [node for _, batch in client.session_nodes.upserts for node in batch.nodes]


def _tool_agent(
    real_calls: list[dict[str, Any]], returned_results: list[Any]
) -> Agent[None, str]:
    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        for message in reversed(messages):
            if isinstance(message, ModelRequest):
                for part in message.parts:
                    if isinstance(part, ToolReturnPart):
                        returned_results.append(part.content)
                        return ModelResponse(parts=[TextPart("finished")])
        return ModelResponse(
            parts=[ToolCallPart("lookup", {"city": "Paris", "units": "metric"})]
        )

    agent = Agent(FunctionModel(model, model_name="tools"))

    @agent.tool_plain
    def lookup(city: str, units: str) -> dict[str, Any]:
        arguments = {"city": city, "units": units}
        real_calls.append(arguments)
        return {"source": "real", **arguments}

    return agent


def test_uses_pydantic_ai_wrapper_agent() -> None:
    original = Agent(TestModel(call_tools=[]))

    wrapped = KitaruAgent(
        original,
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    assert isinstance(wrapped, WrapperAgent)
    assert wrapped.wrapped is original


def test_constructor_validates_configuration() -> None:
    agent = Agent(TestModel(call_tools=[]))
    with pytest.raises(ValueError, match="KITARU_API_URL"):
        KitaruAgent(agent, agent_id=uuid.uuid4())
    with pytest.raises(ValueError, match="batch_size"):
        KitaruAgent(
            agent, agent_id=uuid.uuid4(), api_url="http://kitaru.test", batch_size=0
        )


def test_run_sync_preserves_result_and_records_lifecycle(tmp_path: Any) -> None:
    original = Agent(TestModel(call_tools=[]))
    session_file = tmp_path / "session-id"
    os_environ = pytest.MonkeyPatch()
    os_environ.setenv("KITARU_SESSION_ID_FILE", str(session_file))
    try:
        wrapped = KitaruAgent(
            original,
            agent_id=uuid.uuid4(),
            api_url="http://kitaru.test",
            session_name="recorded",
        )
        result = wrapped.run_sync("hello")
    finally:
        os_environ.undo()

    client = _FakeClient.instances[0]
    assert result.output == "success (no tool calls)"
    assert client.sessions.created[0].inputs == "hello"
    assert client.sessions.created[0].name == "recorded"
    assert client.sessions.updated[0][1].status is SessionStatus.COMPLETED
    assert session_file.read_text() == str(client.session_id)
    assert client.closed
    assert client.events == [
        "session:create",
        "nodes:upsert",
        "nodes:upsert",
        "session:update",
        "client:close",
    ]
    nodes = _nodes(client)
    assert [(node.node_type, node.status, node.sequence) for node in nodes] == [
        (NodeType.SPAN, NodeStatus.IN_PROGRESS, 0),
        (NodeType.LLM_CALL, NodeStatus.COMPLETED, 1),
        (NodeType.SPAN, NodeStatus.COMPLETED, 0),
    ]
    assert nodes[0].id == nodes[-1].id
    llm = next(node for node in nodes if node.node_type is NodeType.LLM_CALL)
    assert llm.cost is None

    original.run_sync("not recorded")
    assert len(_FakeClient.instances) == 1


async def test_setup_error_fails_created_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("KITARU_SESSION_ID_FILE", str(tmp_path))
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    with pytest.raises(IsADirectoryError):
        await agent.run("prompt")

    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert client.sessions.updated[0][1].error
    assert client.closed


async def test_replay_resolves_input_and_replaces_request_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observed["messages"] = messages
        observed["instructions"] = info.instructions
        observed["settings"] = info.model_settings
        return ModelResponse(parts=[TextPart("ok")])

    spec = _replay_spec(
        PassthroughPolicy(),
        inputs="spec prompt",
        override=ReplayOverride(
            system_prompt="replacement system",
            model_params={"temperature": 0.2},
        ),
    )
    _set_replay(monkeypatch, spec)
    monkeypatch.setenv("KITARU_INPUTS", json.dumps("environment prompt"))
    agent = KitaruAgent(
        Agent(
            FunctionModel(model, model_name="original"),
            system_prompt="original system",
            model_settings={"temperature": 0.9},
        ),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    result = await agent.run("caller prompt")

    assert result.output == "ok"
    assert observed["instructions"] is None
    assert observed["settings"] == {"temperature": 0.2}
    system_prompts = [
        part.content
        for message in observed["messages"]
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, SystemPromptPart)
    ]
    assert system_prompts == ["replacement system"]
    user_prompts = [
        part.content
        for message in observed["messages"]
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]
    assert user_prompts == ["environment prompt"]
    client = _FakeClient.instances[0]
    assert client.sessions.created[0].inputs == "environment prompt"
    assert client.sessions.created[0].replay_id == spec.replay_id


async def test_replay_json_input_is_encoded_and_recorded_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_prompts: list[Any] = []
    replay_input = {"question": "What is Kitaru?", "options": [1, 2]}

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        seen_prompts.extend(
            part.content
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, UserPromptPart)
        )
        return ModelResponse(parts=[TextPart("ok")])

    spec = _replay_spec(PassthroughPolicy(), inputs=replay_input)
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    await agent.run("caller prompt")

    assert len(seen_prompts) == 1
    assert json.loads(seen_prompts[0]) == replay_input
    client = _FakeClient.instances[0]
    assert client.sessions.created[0].inputs == replay_input
    assert _nodes(client)[0].inputs == replay_input


async def test_live_json_input_is_encoded_and_recorded_original() -> None:
    seen_prompts: list[Any] = []
    live_input = {"question": "What is Kitaru?", "enabled": True}

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        seen_prompts.extend(
            part.content
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, UserPromptPart)
        )
        return ModelResponse(parts=[TextPart("ok")])

    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    await agent.run(cast(Any, live_input))

    assert len(seen_prompts) == 1
    assert json.loads(seen_prompts[0]) == live_input
    client = _FakeClient.instances[0]
    assert client.sessions.created[0].inputs == live_input
    assert _nodes(client)[0].inputs == live_input


async def test_concurrent_runs_on_one_wrapper_keep_state_isolated() -> None:
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    results = await asyncio.gather(agent.run("first"), agent.run("second"))

    assert [result.output for result in results] == [
        "success (no tool calls)",
        "success (no tool calls)",
    ]
    assert len(_FakeClient.instances) == 2
    assert {client.sessions.created[0].inputs for client in _FakeClient.instances} == {
        "first",
        "second",
    }
    assert all(
        client.sessions.updated[0][1].status is SessionStatus.COMPLETED
        and client.closed
        for client in _FakeClient.instances
    )


async def test_replay_uses_spec_input_when_environment_input_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_prompts: list[Any] = []

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        seen_prompts.extend(
            part.content
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, UserPromptPart)
        )
        return ModelResponse(parts=[TextPart("ok")])

    spec = _replay_spec(PassthroughPolicy(), inputs="large spec input")
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    await agent.run("caller prompt")

    assert seen_prompts == ["large spec input"]


async def test_non_replay_environment_override_replaces_model_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_settings: list[Any] = []

    def model(_: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observed_settings.append(info.model_settings)
        return ModelResponse(parts=[TextPart("ok")])

    monkeypatch.setenv(
        "KITARU_OVERRIDE",
        ReplayOverride(model_params={"temperature": 0.1}).model_dump_json(),
    )
    agent = KitaruAgent(
        Agent(
            FunctionModel(model),
            model_settings={"temperature": 0.9, "max_tokens": 100},
        ),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    await agent.run("prompt")

    assert observed_settings == [{"temperature": 0.1}]


async def test_mapping_model_override_replaces_exact_requested_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_called = False

    def original(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        nonlocal original_called
        original_called = True
        return ModelResponse(parts=[TextPart("original")])

    replacement = FunctionModel(
        lambda _messages, _info: ModelResponse(parts=[TextPart("replacement")]),
        model_name="replacement",
    )
    monkeypatch.setattr(capability_module, "infer_model", lambda value: replacement)
    spec = _replay_spec(
        PassthroughPolicy(),
        override=ReplayOverride(model={"function:original": "provider:new"}),
    )
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(original, model_name="original")),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    result = await agent.run("prompt")

    assert result.output == "replacement"
    assert not original_called
    llm = next(
        node
        for node in _nodes(_FakeClient.instances[0])
        if node.node_type is NodeType.LLM_CALL
    )
    assert llm.requested_model == "function:original"
    assert llm.model == "replacement"


@pytest.mark.parametrize(
    ("policy", "expected_result", "real_call_count", "mocked"),
    [
        (
            PassthroughPolicy(),
            {"source": "real", "city": "Paris", "units": "metric"},
            1,
            False,
        ),
        (
            StaticPolicy(
                cases=[
                    StaticCase(match={"city": "London"}, result={"source": "wrong"}),
                    StaticCase(
                        match={"city": "Paris"},
                        match_mode=StaticMatchMode.SUBSET,
                        result={"source": "static"},
                    ),
                ]
            ),
            {"source": "static"},
            0,
            True,
        ),
        (HistoryPolicy(), {"source": "history"}, 0, True),
    ],
)
async def test_tool_policies(
    monkeypatch: pytest.MonkeyPatch,
    policy: Any,
    expected_result: Any,
    real_call_count: int,
    mocked: bool,
) -> None:
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    spec = _replay_spec(policy)
    _set_replay(monkeypatch, spec)
    if isinstance(policy, HistoryPolicy):
        _FakeClient.next_lookup_response = ToolLookupResponse(
            found=True, result=expected_result
        )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    result = await agent.run("weather")

    assert result.output == "finished"
    assert returned_results == [expected_result]
    assert len(real_calls) == real_call_count
    client = _FakeClient.instances[0]
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.outputs == expected_result
    assert tool.attributes.get("mocked", False) is mocked
    if isinstance(policy, HistoryPolicy):
        lookup = client.replays.lookups[0][1]
        arguments = {"city": "Paris", "units": "metric"}
        assert lookup.inputs == arguments
        assert lookup.cache_key == tool_call_cache_key("lookup", arguments)


@pytest.mark.parametrize(
    ("on_miss", "error_type", "expected_tool_status"),
    [
        (ToolPolicyOnMiss.FAIL, ToolPolicyMissError, NodeStatus.FAILED),
        (ToolPolicyOnMiss.ERROR_RESULT, None, NodeStatus.FAILED),
        (ToolPolicyOnMiss.PASSTHROUGH, None, NodeStatus.COMPLETED),
    ],
)
async def test_static_miss_behavior(
    monkeypatch: pytest.MonkeyPatch,
    on_miss: ToolPolicyOnMiss,
    error_type: type[BaseException] | None,
    expected_tool_status: NodeStatus,
) -> None:
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    spec = _replay_spec(
        StaticPolicy(
            cases=[StaticCase(match={"city": "London"}, result="unused")],
            on_miss=on_miss,
        )
    )
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    if error_type is not None:
        with pytest.raises(error_type, match="No static result"):
            await agent.run("weather")
    else:
        result = await agent.run("weather")
        assert result.output == "finished"

    client = _FakeClient.instances[0]
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.status is expected_tool_status
    if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
        assert returned_results == [{"error": "No static result for tool 'lookup'"}]
        assert real_calls == []
    elif on_miss is ToolPolicyOnMiss.PASSTHROUGH:
        assert len(real_calls) == 1


@pytest.mark.parametrize(
    ("policy", "fails"),
    [
        (PassthroughPolicy(), False),
        (StaticPolicy(cases=[StaticCase(result={"mocked": True})]), False),
        (HistoryPolicy(), False),
        (LLMPolicy(model="provider:model"), True),
    ],
)
async def test_provider_native_tools_are_observed_but_not_mocked(
    monkeypatch: pytest.MonkeyPatch,
    policy: Any,
    fails: bool,
) -> None:
    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                NativeToolCallPart(
                    "web_search", {"query": "Kitaru"}, tool_call_id="native-1"
                ),
                NativeToolReturnPart(
                    "web_search",
                    {"answer": "recorded"},
                    tool_call_id="native-1",
                ),
                TextPart("done"),
            ]
        )

    spec = _replay_spec(policy)
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    if fails:
        with pytest.raises(ToolPolicyError, match="provider-native tool"):
            await agent.run("search")
    else:
        result = await agent.run("search")
        assert result.output == "done"

    client = _FakeClient.instances[0]
    native = next(
        node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL
    )
    assert native.external_id == "native-1"
    assert native.inputs == {"query": "Kitaru"}
    assert native.outputs == {"answer": "recorded"}
    assert native.attributes == {"provider_native": True}
    assert "mocked" not in native.attributes
    assert client.replays.lookups == []


async def test_unpaired_provider_native_call_rejects_llm_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[
                NativeToolCallPart(
                    "web_search", {"query": "Kitaru"}, tool_call_id="native-1"
                ),
                TextPart("done"),
            ]
        )

    spec = _replay_spec(LLMPolicy(model="provider:model"))
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    with pytest.raises(ToolPolicyError, match="provider-native tool"):
        await agent.run("search")

    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    llm = next(node for node in _nodes(client) if node.node_type is NodeType.LLM_CALL)
    assert llm.attributes["provider_native_calls"][0]["tool_name"] == "web_search"


async def test_history_policy_normalizes_validated_tool_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    returned_results: list[Any] = []

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        for message in reversed(messages):
            if isinstance(message, ModelRequest):
                for part in message.parts:
                    if isinstance(part, ToolReturnPart):
                        returned_results.append(part.content)
                        return ModelResponse(parts=[TextPart("finished")])
        return ModelResponse(parts=[ToolCallPart("lookup_day", {"day": "2026-07-24"})])

    original = Agent(FunctionModel(model))

    @original.tool_plain
    def lookup_day(day: date) -> str:
        return day.isoformat()

    spec = _replay_spec(HistoryPolicy())
    _set_replay(monkeypatch, spec)
    _FakeClient.next_lookup_response = ToolLookupResponse(
        found=True, result={"source": "history"}
    )
    agent = KitaruAgent(
        original,
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    result = await agent.run("lookup")

    assert result.output == "finished"
    assert returned_results == [{"source": "history"}]
    client = _FakeClient.instances[0]
    lookup = client.replays.lookups[0][1]
    json_args = {"day": "2026-07-24"}
    assert lookup.inputs == json_args
    assert lookup.cache_key == tool_call_cache_key("lookup_day", json_args)
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.inputs == json_args


async def test_fully_consumed_stream_completes_session() -> None:
    async def stream_model(_: list[ModelMessage], __: AgentInfo) -> AsyncIterator[str]:
        yield "hel"
        yield "lo"

    agent = KitaruAgent(
        Agent(FunctionModel(stream_function=stream_model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    async with agent.run_stream("prompt") as stream:
        output = await stream.get_output()

    assert output == "hello"
    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.COMPLETED
    assert client.closed


async def test_failing_stream_fails_session() -> None:
    async def stream_model(_: list[ModelMessage], __: AgentInfo) -> AsyncIterator[str]:
        yield "started"
        raise RuntimeError("stream failed")

    agent = KitaruAgent(
        Agent(FunctionModel(stream_function=stream_model)),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    with pytest.raises(RuntimeError, match="stream failed"):
        async with agent.run_stream("prompt") as stream:
            await stream.get_output()

    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert client.sessions.updated[0][1].error == "stream failed"
    assert client.closed


async def test_per_tool_llm_policy_fails_clearly_without_executing_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    spec = _replay_spec(
        PassthroughPolicy(),
        tools={"lookup": LLMPolicy(model="provider:model")},
    )
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
        api_url="http://kitaru.test",
    )

    with pytest.raises(
        ToolPolicyError,
        match="Tool policy 'llm' is not supported by the PydanticAI adapter",
    ):
        await agent.run("weather")

    assert real_calls == []
    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.status is NodeStatus.FAILED
