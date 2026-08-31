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
"""Focused contract tests for the PydanticAI adapter plugin."""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from importlib.metadata import version
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
    RequestUsage,
    SystemPromptPart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel

import kitaru_pydantic_ai.capability as capability_module
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
    ToolLookupMatch,
    ToolLookupResponse,
)
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    LLMConfig,
    PassthroughConfig,
    ReplayOverride,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.api_models.v1.task import AgentTaskDetails
from kitaru.cache_keys import compute_tool_cache_key
from kitaru_pydantic_ai import (
    KitaruAgent,
    PydanticAIUsageSummary,
    ToolPolicyError,
    ToolPolicyMissError,
)


class _FakeSessions:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.created: list[Any] = []
        self.updated: list[tuple[uuid.UUID, Any]] = []
        self.node_batches: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        self.created.append(request)
        self._client.events.append("session:create")
        return SimpleNamespace(id=self._client.session_id)

    async def update(self, session_id: uuid.UUID, request: Any) -> Any:
        if self._client.update_error is not None:
            raise self._client.update_error
        self.updated.append((session_id, request))
        self._client.events.append("session:update")
        return None

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        if self._client.ingest_error is not None and (
            self._client.ingest_error_after is None
            or len(self.node_batches) >= self._client.ingest_error_after
        ):
            raise self._client.ingest_error
        self.node_batches.append((session_id, request))
        self._client.events.append("nodes:upsert")
        return []


class _FakeTasks:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client

    async def get_spec(self, task_id: uuid.UUID) -> Any:
        assert task_id == self._client.task_id
        return SimpleNamespace(details=AgentTaskDetails(inputs=self._client.inputs))


class _FakeReplays:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.lookups: list[tuple[uuid.UUID, Any]] = []

    async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
        assert self._client.replay is not None
        assert replay_id == self._client.replay.id
        return self._client.replay

    async def tool_lookup(self, replay_id: uuid.UUID, request: Any) -> Any:
        self.lookups.append((replay_id, request))
        if self._client.lookup_responses:
            return self._client.lookup_responses.pop(0)
        return self._client.lookup_response


class _FakeClient:
    instances: ClassVar[list["_FakeClient"]] = []
    next_fixture: ClassVar["_ReplayFixture | None"] = None
    next_lookup_response: ClassVar[ToolLookupResponse] = ToolLookupResponse(match=None)
    next_lookup_responses: ClassVar[list[ToolLookupResponse] | None] = None
    next_ingest_error: ClassVar[BaseException | None] = None
    next_ingest_error_after: ClassVar[int | None] = None
    next_update_error: ClassVar[BaseException | None] = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.session_id = uuid.uuid4()
        fixture = type(self).next_fixture
        self.task_id = fixture.task_id if fixture else None
        self.replay = fixture.replay if fixture else None
        self.inputs = fixture.inputs if fixture else None
        self.lookup_response = type(self).next_lookup_response
        self.lookup_responses = list(type(self).next_lookup_responses or [])
        self.ingest_error = type(self).next_ingest_error
        self.ingest_error_after = type(self).next_ingest_error_after
        self.update_error = type(self).next_update_error
        self.events: list[str] = []
        self.sessions = _FakeSessions(self)
        self.tasks = _FakeTasks(self)
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
        "KITARU_API_TOKEN",
        "KITARU_API_URL",
        "KITARU_TASK_ID",
        "KITARU_TASK_INPUTS",
        "KITARU_SESSION_NAME",
        "KITARU_REPLAY_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    _FakeClient.instances.clear()
    _FakeClient.next_fixture = None
    _FakeClient.next_lookup_response = ToolLookupResponse(match=None)
    _FakeClient.next_lookup_responses = None
    _FakeClient.next_ingest_error = None
    _FakeClient.next_ingest_error_after = None
    _FakeClient.next_update_error = None
    monkeypatch.setattr(capability_module, "KitaruAPIClient", _FakeClient)


@dataclass
class _ReplayFixture:
    """Canonical task and replay state used by adapter tests."""

    task_id: uuid.UUID
    replay: ReplayResponse
    inputs: Any


def _replay_spec(
    policy: Any,
    *,
    inputs: Any = "replayed prompt",
    override: ReplayOverride | None = None,
    tools: dict[str, Any] | None = None,
) -> _ReplayFixture:
    now = capability_module.datetime.now(capability_module.UTC)
    return _ReplayFixture(
        task_id=uuid.uuid4(),
        inputs=inputs,
        replay=ReplayResponse(
            id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            experiment_run_id=None,
            baseline_session_id=uuid.uuid4(),
            result_session_id=None,
            override=override,
            tool_policy=ToolPolicy(default=policy, tools=tools or {}),
            evaluators=[],
            evaluate_baselines=False,
            baseline_evaluation_mode=BaselineEvaluationMode.NONE,
            status=ReplayStatus.PENDING,
            error=None,
            created=now,
            updated=now,
        ),
    )


def _set_replay(monkeypatch: pytest.MonkeyPatch, spec: _ReplayFixture) -> None:
    _FakeClient.next_fixture = spec
    monkeypatch.setenv("KITARU_TASK_ID", str(spec.task_id))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps(spec.inputs))
    monkeypatch.setenv("KITARU_REPLAY_ID", str(spec.replay.id))


def _nodes(client: _FakeClient) -> list[Any]:
    return [node for _, batch in client.sessions.node_batches for node in batch.nodes]


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


def test_adapter_version_matches_distribution() -> None:
    assert version("kitaru-pydantic-ai") == capability_module.ADAPTER_VERSION


def test_uses_pydantic_ai_wrapper_agent() -> None:
    original = Agent(TestModel(call_tools=[]))

    wrapped = KitaruAgent(
        original,
        agent_id=uuid.uuid4(),
    )

    assert isinstance(wrapped, WrapperAgent)
    assert wrapped.wrapped is original


async def test_task_bound_run_leaves_agent_identity_for_server_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _replay_spec(PassthroughConfig())
    _set_replay(monkeypatch, spec)
    wrapped = KitaruAgent(
        Agent(TestModel(call_tools=[])),
    )

    await wrapped.run("ignored prompt")

    request = _FakeClient.instances[0].sessions.created[0]
    assert request.agent_id is None
    assert request.agent_version_id is None


def test_constructor_validates_batch_size() -> None:
    agent = Agent(TestModel(call_tools=[]))
    with pytest.raises(ValueError, match="batch_size"):
        KitaruAgent(agent, agent_id=uuid.uuid4(), batch_size=0)


def test_uses_default_api_client() -> None:
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
    )

    agent.run_sync("hello")

    assert _FakeClient.instances[0].kwargs == {}


def test_subset_static_case_with_non_mapping_match_does_not_match() -> None:
    """Treat malformed subset cases as misses instead of crashing the adapter."""
    case = StaticCase(
        match=["Paris"],
        match_mode=StaticMatchMode.SUBSET,
        result="unused",
    )

    assert not capability_module._case_matches(case, {"city": "Paris"})


def test_run_sync_preserves_result_and_records_lifecycle() -> None:
    original = Agent(TestModel(call_tools=[]))
    wrapped = KitaruAgent(
        original,
        agent_id=uuid.uuid4(),
        session_name="recorded",
    )
    result = wrapped.run_sync("hello")

    client = _FakeClient.instances[0]
    assert result.output == "success (no tool calls)"
    assert client.sessions.created[0].inputs == "hello"
    assert client.sessions.created[0].name == "recorded"
    assert client.sessions.updated[0][1].status is SessionStatus.COMPLETED
    assert client.closed
    assert client.events == [
        "session:create",
        "nodes:upsert",
        "nodes:upsert",
        "session:update",
        "client:close",
    ]
    nodes = _nodes(client)
    assert [(node.node_type, node.status, node.index) for node in nodes] == [
        (NodeType.SPAN, NodeStatus.IN_PROGRESS, 0),
        (NodeType.LLM_CALL, NodeStatus.COMPLETED, 1),
        (NodeType.SPAN, NodeStatus.COMPLETED, 0),
    ]
    assert nodes[0].index == nodes[-1].index
    llm = next(node for node in nodes if node.node_type is NodeType.LLM_CALL)
    assert llm.model_provider == "test"
    assert llm.cost is None
    assert llm.input_text_selector == "/0/parts/0/content"
    assert llm.output_text_selector == "/parts/0/content"
    assert nodes[-1].input_text_selector == ""
    assert nodes[-1].output_text_selector == ""

    original.run_sync("not recorded")
    assert len(_FakeClient.instances) == 1


def test_calculates_cost_from_the_bundled_pricing_catalog() -> None:
    response = ModelResponse(
        parts=[TextPart("resolved")],
        usage=RequestUsage(
            input_tokens=650,
            output_tokens=1241,
            cache_read_tokens=0,
            details={"reasoning_tokens": 1000},
        ),
        model_name="gpt-5-nano-2025-08-07",
        provider_name="openai",
    )

    cost, attributes = capability_module._calculate_cost(
        response,
        "gpt-5-nano-2025-08-07",
        response.timestamp,
        None,
        True,
    )

    assert cost == Decimal("0.0005289")
    assert attributes == {"cost": {"status": "estimated", "source": "genai-prices"}}


async def test_replay_records_cost_from_bundled_pricing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[TextPart("resolved")],
            usage=RequestUsage(input_tokens=650, output_tokens=1241),
            provider_name="openai",
        )

    model_name = "gpt-5-nano-2025-08-07"
    _set_replay(monkeypatch, _replay_spec(PassthroughConfig()))
    agent = KitaruAgent(
        Agent(FunctionModel(model, model_name=model_name)),
        agent_id=uuid.uuid4(),
    )

    await agent.run("ignored prompt")

    llm = next(
        node
        for node in _nodes(_FakeClient.instances[0])
        if node.node_type is NodeType.LLM_CALL
    )
    assert llm.model == model_name
    assert llm.cost == Decimal("0.0005289")
    assert llm.attributes["cost"] == {
        "status": "estimated",
        "source": "genai-prices",
    }


async def test_replay_records_cost_from_user_calculator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[PydanticAIUsageSummary] = []

    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[TextPart("resolved")],
            usage=RequestUsage(
                input_tokens=20,
                output_tokens=5,
                cache_read_tokens=3,
                details={"reasoning_tokens": 2},
            ),
            model_name="customer-model",
            provider_name="customer-provider",
        )

    def calculate_cost(usage: PydanticAIUsageSummary) -> Decimal:
        observed.append(usage)
        return Decimal("0.0042")

    _set_replay(monkeypatch, _replay_spec(PassthroughConfig()))
    agent = KitaruAgent(
        Agent(FunctionModel(model, model_name="customer-model")),
        agent_id=uuid.uuid4(),
        cost_calculator=calculate_cost,
    )

    await agent.run("ignored prompt")

    llm = next(
        node
        for node in _nodes(_FakeClient.instances[0])
        if node.node_type is NodeType.LLM_CALL
    )
    assert llm.cost == Decimal("0.0042")
    assert llm.attributes["cost"] == {
        "status": "estimated",
        "source": "user",
    }
    assert observed == [
        PydanticAIUsageSummary(
            model="customer-model",
            provider="customer-provider",
            input_tokens=20,
            output_tokens=5,
            cached_input_tokens=3,
            reasoning_tokens=2,
        )
    ]


async def test_cost_calculation_failure_does_not_fail_model_request() -> None:
    def fail(_: PydanticAIUsageSummary) -> Decimal:
        raise LookupError("pricing unavailable")

    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
        cost_calculator=fail,
    )

    result = await agent.run("prompt")

    assert result.output == "success (no tool calls)"
    llm = next(
        node
        for node in _nodes(_FakeClient.instances[0])
        if node.node_type is NodeType.LLM_CALL
    )
    assert llm.cost is None
    assert llm.attributes["cost"] == {
        "status": "unavailable",
        "source": "user",
        "error_type": "LookupError",
    }


async def test_automatic_cost_estimation_can_be_disabled() -> None:
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
        estimate_costs=False,
    )

    await agent.run("prompt")

    llm = next(
        node
        for node in _nodes(_FakeClient.instances[0])
        if node.node_type is NodeType.LLM_CALL
    )
    assert llm.cost is None
    assert llm.attributes["cost"] == {"status": "disabled"}


async def test_setup_error_fails_created_session() -> None:
    _FakeClient.next_ingest_error = RuntimeError("ingest failed")
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(RuntimeError, match="ingest failed"):
        await agent.run("prompt")

    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert client.sessions.updated[0][1].error
    assert client.closed


async def test_recording_failure_does_not_replace_agent_failure() -> None:
    """Keep the agent error primary when terminal session recording also fails."""

    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        raise ValueError("agent failed")

    _FakeClient.next_update_error = RuntimeError("recording failed")
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(ValueError, match="agent failed") as exc_info:
        await agent.run("prompt")

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) == "recording failed"
    assert _FakeClient.instances[0].closed


async def test_batch_failure_does_not_replace_model_failure() -> None:
    """Keep a model failure primary when recording its failed node also fails."""

    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        raise ValueError("model failed")

    _FakeClient.next_ingest_error = RuntimeError("batch failed")
    _FakeClient.next_ingest_error_after = 1
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
        batch_size=1,
    )

    with pytest.raises(ValueError, match="model failed") as exc_info:
        await agent.run("prompt")

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) == "batch failed"
    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert client.closed


async def test_terminal_batch_failure_marks_session_failed() -> None:
    """Do not leave a session running when its terminal node batch fails."""
    _FakeClient.next_ingest_error = RuntimeError("terminal batch failed")
    _FakeClient.next_ingest_error_after = 1
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(RuntimeError, match="terminal batch failed"):
        await agent.run("prompt")

    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert client.sessions.updated[0][1].error == "terminal batch failed"
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
        PassthroughConfig(),
        inputs="spec prompt",
        override=ReplayOverride(
            system_prompt="replacement system",
            model_params={"temperature": 0.2},
        ),
    )
    _set_replay(monkeypatch, spec)
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps("environment prompt"))
    agent = KitaruAgent(
        Agent(
            FunctionModel(model, model_name="original"),
            system_prompt="original system",
            model_settings={"temperature": 0.9},
        ),
        agent_id=uuid.uuid4(),
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
    assert "system_prompt" not in client.sessions.updated[0][1].model_fields_set
    llm = next(node for node in _nodes(client) if node.node_type is NodeType.LLM_CALL)
    assert llm.input_text_selector == "/0/parts/1/content"
    assert llm.output_text_selector == "/parts/0/content"
    assert llm.system_prompt_selector == "/0/parts/0/content"


async def test_records_visible_model_reasoning() -> None:
    def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        return ModelResponse(
            parts=[ThinkingPart("check the evidence"), TextPart("answer")]
        )

    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
    )

    await agent.run("question")

    client = _FakeClient.instances[0]
    llm = next(node for node in _nodes(client) if node.node_type is NodeType.LLM_CALL)
    assert llm.reasoning == "check the evidence"
    assert llm.output_text_selector == "/parts/1/content"


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

    spec = _replay_spec(PassthroughConfig(), inputs=replay_input)
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
    )

    await agent.run("caller prompt")

    assert len(seen_prompts) == 1
    assert json.loads(seen_prompts[0]) == replay_input
    client = _FakeClient.instances[0]
    assert client.sessions.created[0].inputs == replay_input
    assert _nodes(client)[0].inputs == replay_input


async def test_imported_conversation_replays_final_turn_with_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Project imported turns onto final prompt and prior message history."""
    observed: list[ModelMessage] = []

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        observed.extend(messages)
        return ModelResponse(parts=[TextPart("replayed")])

    imported_inputs = {
        "schema_version": 1,
        "turns": [
            {
                "source_trace_id": "trace-1",
                "inputs": {
                    "messages": [{"role": "user", "content": "My name is Ada."}]
                },
                "outputs": {
                    "role": "assistant",
                    "content": "Hello Ada.",
                },
            },
            {
                "source_trace_id": "trace-2",
                "inputs": {
                    "messages": [{"role": "user", "content": "What is my name?"}]
                },
                "outputs": {
                    "role": "assistant",
                    "content": "Your name is Ada.",
                },
            },
        ],
    }
    _set_replay(
        monkeypatch,
        _replay_spec(PassthroughConfig(), inputs=imported_inputs),
    )
    agent = KitaruAgent(
        Agent(FunctionModel(model), system_prompt="Answer from the conversation."),
        agent_id=uuid.uuid4(),
    )

    await agent.run("caller prompt")

    prompts = [
        part.content
        for message in observed
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]
    responses = [
        part.content
        for message in observed
        if isinstance(message, ModelResponse)
        for part in message.parts
        if isinstance(part, TextPart)
    ]
    assert prompts == ["My name is Ada.", "What is my name?"]
    assert responses == ["Hello Ada."]
    assert _FakeClient.instances[0].sessions.created[0].inputs == imported_inputs


async def test_prompt_override_replaces_imported_conversation_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply a replay prompt override after projecting imported history."""
    observed: list[str] = []

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        observed.extend(
            part.content
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, UserPromptPart) and isinstance(part.content, str)
        )
        return ModelResponse(parts=[TextPart("replayed")])

    _set_replay(
        monkeypatch,
        _replay_spec(
            PassthroughConfig(),
            inputs={
                "schema_version": 1,
                "turns": [
                    {"inputs": "first", "outputs": "answer"},
                    {"inputs": "recorded prompt", "outputs": "recorded output"},
                ],
            },
            override=ReplayOverride(prompt="replacement prompt"),
        ),
    )
    agent = KitaruAgent(Agent(FunctionModel(model)), agent_id=uuid.uuid4())

    await agent.run("caller prompt")

    assert observed == ["first", "replacement prompt"]


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


async def test_many_concurrent_runs_keep_sessions_and_nodes_isolated() -> None:
    """Stress one wrapper with many overlapping run-local capabilities."""
    run_count = 64
    agent = KitaruAgent(
        Agent(TestModel(call_tools=[])),
        agent_id=uuid.uuid4(),
        batch_size=1,
    )

    results = await asyncio.gather(
        *(agent.run(f"prompt-{index}") for index in range(run_count))
    )

    assert len(results) == run_count
    assert len(_FakeClient.instances) == run_count
    assert {client.sessions.created[0].inputs for client in _FakeClient.instances} == {
        f"prompt-{index}" for index in range(run_count)
    }
    for client in _FakeClient.instances:
        nodes = _nodes(client)
        assert [node.index for node in nodes] == [0, 1, 0]
        assert client.sessions.updated[0][1].status is SessionStatus.COMPLETED
        assert client.closed


async def test_cancelled_run_fails_session_and_closes_client() -> None:
    """Clean up Kitaru state when the caller cancels an active model request."""
    model_started = asyncio.Event()
    release_model = asyncio.Event()

    async def model(_: list[ModelMessage], __: AgentInfo) -> ModelResponse:
        model_started.set()
        await release_model.wait()
        return ModelResponse(parts=[TextPart("unused")])

    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
    )
    task = asyncio.create_task(agent.run("prompt"))
    await model_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    client = _FakeClient.instances[0]
    assert client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert client.sessions.updated[0][1].error == "CancelledError"
    assert client.closed


async def test_parallel_tool_calls_respect_batching_and_parentage() -> None:
    """Stress concurrent tool hooks across several node batch boundaries."""
    tool_count = 40
    real_calls: list[dict[str, Any]] = []

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        if any(
            isinstance(part, ToolReturnPart)
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
        ):
            return ModelResponse(parts=[TextPart("finished")])
        return ModelResponse(
            parts=[
                ToolCallPart(
                    "lookup",
                    {"value": index},
                    tool_call_id=f"lookup-{index}",
                )
                for index in range(tool_count)
            ]
        )

    original = Agent(FunctionModel(model, model_name="parallel-tools"))

    @original.tool_plain
    async def lookup(value: int) -> int:
        await asyncio.sleep(0)
        real_calls.append({"value": value})
        return value * 2

    agent = KitaruAgent(original, agent_id=uuid.uuid4(), batch_size=7)

    result = await agent.run("run tools")

    assert result.output == "finished"
    assert {call["value"] for call in real_calls} == set(range(tool_count))
    client = _FakeClient.instances[0]
    nodes = _nodes(client)
    children = [node for node in nodes if node.index != 0]
    tool_nodes = [node for node in children if node.node_type is NodeType.TOOL_CALL]
    llm_nodes = [node for node in children if node.node_type is NodeType.LLM_CALL]
    assert len(tool_nodes) == tool_count
    assert len(llm_nodes) == 2
    assert {node.index for node in children} == set(range(1, tool_count + 3))
    assert {node.parent_index for node in tool_nodes} == {llm_nodes[0].index}
    assert all(len(batch.nodes) <= 7 for _, batch in client.sessions.node_batches[1:])
    assert client.sessions.updated[0][1].status is SessionStatus.COMPLETED
    assert client.closed


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

    spec = _replay_spec(PassthroughConfig(), inputs="large spec input")
    _set_replay(monkeypatch, spec)
    monkeypatch.delenv("KITARU_TASK_INPUTS")
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
    )

    await agent.run("caller prompt")

    assert seen_prompts == ["large spec input"]


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
        PassthroughConfig(),
        override=ReplayOverride(model={"function:original": "provider:new"}),
    )
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(original, model_name="original")),
        agent_id=uuid.uuid4(),
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
            PassthroughConfig(),
            {"source": "real", "city": "Paris", "units": "metric"},
            1,
            False,
        ),
        (
            StaticConfig(
                cases=[
                    StaticCase(
                        match={"city": "London"},
                        match_mode=StaticMatchMode.EXACT,
                        result={"source": "wrong"},
                    ),
                    StaticCase(
                        match={"city": "Paris"},
                        match_mode=StaticMatchMode.SUBSET,
                        result={"source": "static"},
                    ),
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
            {"source": "static"},
            0,
            True,
        ),
        (
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
            {"source": "history"},
            0,
            True,
        ),
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
    if isinstance(policy, HistoryConfig):
        _FakeClient.next_lookup_response = ToolLookupResponse(
            match=ToolLookupMatch(
                result=expected_result,
                status=NodeStatus.COMPLETED,
                error=None,
            )
        )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("weather")

    assert result.output == "finished"
    assert returned_results == [expected_result]
    assert len(real_calls) == real_call_count
    client = _FakeClient.instances[0]
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.outputs == expected_result
    assert tool.attributes.get("mocked", False) is mocked
    if isinstance(policy, HistoryConfig):
        lookup = client.replays.lookups[0][1]
        arguments = {"city": "Paris", "units": "metric"}
        assert lookup.cache_key == compute_tool_cache_key("lookup", arguments)


def _repeating_tool_agent(
    real_calls: list[dict[str, Any]], returned_results: list[Any], call_count: int
) -> Agent[None, str]:
    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        returns = [
            part
            for message in messages
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        ]
        del returned_results[:]
        returned_results.extend(part.content for part in returns)
        if len(returns) >= call_count:
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


async def test_history_policy_consumes_baseline_occurrences_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated identical calls look up successive baseline occurrences."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL)
        ),
    )
    _FakeClient.next_lookup_responses = [
        ToolLookupResponse(
            match=ToolLookupMatch(
                result={"ticket": ticket},
                status=NodeStatus.COMPLETED,
                error=None,
            )
        )
        for ticket in ["a", "b", "c"]
    ]
    agent = KitaruAgent(
        _repeating_tool_agent(real_calls, returned_results, 3),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("tickets")

    assert result.output == "finished"
    assert returned_results == [{"ticket": "a"}, {"ticket": "b"}, {"ticket": "c"}]
    assert real_calls == []
    client = _FakeClient.instances[0]
    assert [request.occurrence for _, request in client.replays.lookups] == [0, 1, 2]


async def test_history_policy_miss_does_not_advance_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missed lookup leaves the next identical call on the same occurrence."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(
                scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.PASSTHROUGH
            )
        ),
    )
    _FakeClient.next_lookup_responses = [
        ToolLookupResponse(match=None),
        ToolLookupResponse(
            match=ToolLookupMatch(
                result={"ticket": "a"},
                status=NodeStatus.COMPLETED,
                error=None,
            )
        ),
    ]
    agent = KitaruAgent(
        _repeating_tool_agent(real_calls, returned_results, 2),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("tickets")

    assert result.output == "finished"
    assert len(real_calls) == 1
    client = _FakeClient.instances[0]
    assert [request.occurrence for _, request in client.replays.lookups] == [0, 0]


@pytest.mark.parametrize("scope", [HistoryScope.AGENT, HistoryScope.COHORT_VERSION])
async def test_history_policy_non_baseline_scope_sends_no_occurrence(
    monkeypatch: pytest.MonkeyPatch,
    scope: HistoryScope,
) -> None:
    """Agent- and cohort-scoped history lookups carry no occurrence."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(HistoryConfig(scope=scope, on_miss=ToolPolicyOnMiss.FAIL)),
    )
    _FakeClient.next_lookup_response = ToolLookupResponse(
        match=ToolLookupMatch(
            result={"source": "history"},
            status=NodeStatus.COMPLETED,
            error=None,
        )
    )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("weather")

    assert result.output == "finished"
    client = _FakeClient.instances[0]
    assert len(client.replays.lookups) == 1
    assert client.replays.lookups[0][1].occurrence is None


@pytest.mark.parametrize("recorded_result", [{"source": "history"}, None])
async def test_history_policy_replays_completed_match_without_live_execution(
    monkeypatch: pytest.MonkeyPatch,
    recorded_result: Any,
) -> None:
    """A completed match, including null, is a mocked successful tool call."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.PASSTHROUGH,
            )
        ),
    )
    _FakeClient.next_lookup_response = ToolLookupResponse(
        match=ToolLookupMatch(
            result=recorded_result,
            status=NodeStatus.COMPLETED,
            error=None,
        )
    )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("weather")

    assert result.output == "finished"
    assert returned_results == [recorded_result]
    assert real_calls == []
    client = _FakeClient.instances[0]
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.status is NodeStatus.COMPLETED
    assert tool.outputs == recorded_result
    assert tool.error is None
    assert tool.attributes == {"mocked": True, "policy": "history"}
    assert client.replays.lookups[0][1].occurrence == 0


@pytest.mark.parametrize(
    ("recorded_error", "expected_error"),
    [
        ("recorded tool failure", "recorded tool failure"),
        (None, "Recorded tool call 'lookup' failed"),
    ],
)
async def test_history_policy_raises_and_records_failed_match(
    monkeypatch: pytest.MonkeyPatch,
    recorded_error: str | None,
    expected_error: str,
) -> None:
    """A failed match raises at the tool boundary and records that failure."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    run_states: list[Any] = []
    run_state_type = capability_module._RunState

    def capture_run_state(**kwargs: Any) -> Any:
        state = run_state_type(**kwargs)
        run_states.append(state)
        return state

    monkeypatch.setattr(capability_module, "_RunState", capture_run_state)
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.PASSTHROUGH,
            )
        ),
    )
    _FakeClient.next_lookup_response = ToolLookupResponse(
        match=ToolLookupMatch(
            result=None,
            status=NodeStatus.FAILED,
            error=recorded_error,
        )
    )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(ToolPolicyError, match=expected_error):
        await agent.run("weather")

    assert real_calls == []
    assert returned_results == []
    client = _FakeClient.instances[0]
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.status is NodeStatus.FAILED
    assert tool.outputs is None
    assert tool.error == expected_error
    assert tool.attributes == {"mocked": True, "policy": "history"}
    assert client.replays.lookups[0][1].occurrence == 0
    cache_key = client.replays.lookups[0][1].cache_key
    assert run_states[0].history_occurrences[cache_key] == 1


async def test_history_policy_legacy_lookup_response_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old server response cannot be mistaken for a genuine history miss."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.PASSTHROUGH,
            )
        ),
    )
    _FakeClient.next_lookup_response = ToolLookupResponse()
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(ToolPolicyError, match="does not include 'match'"):
        await agent.run("weather")

    assert real_calls == []
    assert returned_results == []


async def test_history_policy_fails_closed_for_unexpected_match_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A nonterminal recorded match cannot execute the live tool."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.PASSTHROUGH,
            )
        ),
    )
    _FakeClient.next_lookup_response = ToolLookupResponse(
        match=ToolLookupMatch(
            result=None,
            status=NodeStatus.IN_PROGRESS,
            error=None,
        )
    )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    with pytest.raises(ToolPolicyError, match="unexpected status 'in_progress'"):
        await agent.run("weather")

    assert real_calls == []
    client = _FakeClient.instances[0]
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.status is NodeStatus.FAILED
    assert tool.attributes == {"mocked": True, "policy": "history"}
    assert client.replays.lookups[0][1].occurrence == 0


async def test_history_policy_without_cache_key_uses_miss_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip lookup when tool arguments cannot produce a cache key."""
    real_calls: list[dict[str, Any]] = []
    returned_results: list[Any] = []
    _set_replay(
        monkeypatch,
        _replay_spec(
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.PASSTHROUGH,
            )
        ),
    )
    monkeypatch.setattr(
        "kitaru_pydantic_ai.capability.compute_tool_cache_key",
        lambda *_: None,
    )
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("weather")

    assert result.output == "finished"
    assert len(real_calls) == 1
    assert _FakeClient.instances[0].replays.lookups == []


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
        StaticConfig(
            cases=[
                StaticCase(
                    match={"city": "London"},
                    match_mode=StaticMatchMode.EXACT,
                    result="unused",
                )
            ],
            on_miss=on_miss,
        )
    )
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
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
        (PassthroughConfig(), False),
        (
            StaticConfig(
                cases=[
                    StaticCase(
                        match=None,
                        match_mode=StaticMatchMode.EXACT,
                        result={"mocked": True},
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
            False,
        ),
        (
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
            False,
        ),
        (LLMConfig(model="provider:model"), True),
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
    assert native.input_text_selector == "/query"
    assert native.output_text_selector == "/answer"
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

    spec = _replay_spec(LLMConfig(model="provider:model"))
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        Agent(FunctionModel(model)),
        agent_id=uuid.uuid4(),
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

    spec = _replay_spec(
        HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.FAIL,
        )
    )
    _set_replay(monkeypatch, spec)
    _FakeClient.next_lookup_response = ToolLookupResponse(
        match=ToolLookupMatch(
            result={"source": "history"},
            status=NodeStatus.COMPLETED,
            error=None,
        )
    )
    agent = KitaruAgent(
        original,
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("lookup")

    assert result.output == "finished"
    assert returned_results == [{"source": "history"}]
    client = _FakeClient.instances[0]
    lookup = client.replays.lookups[0][1]
    json_args = {"day": "2026-07-24"}
    assert lookup.cache_key == compute_tool_cache_key("lookup_day", json_args)
    tool = next(node for node in _nodes(client) if node.node_type is NodeType.TOOL_CALL)
    assert tool.inputs == json_args


async def test_fully_consumed_stream_completes_session() -> None:
    async def stream_model(_: list[ModelMessage], __: AgentInfo) -> AsyncIterator[str]:
        yield "hello "
        yield "world"

    agent = KitaruAgent(
        Agent(FunctionModel(stream_function=stream_model)),
        agent_id=uuid.uuid4(),
    )

    async with agent.run_stream("prompt") as stream:
        output = await stream.get_output()

    assert output == "hello world"
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
        PassthroughConfig(),
        tools={"lookup": LLMConfig(model="provider:model")},
    )
    _set_replay(monkeypatch, spec)
    agent = KitaruAgent(
        _tool_agent(real_calls, returned_results),
        agent_id=uuid.uuid4(),
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
