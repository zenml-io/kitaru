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
"""Focused contract tests for the OpenAI Agents SDK runner adapter."""

import asyncio
import inspect
import uuid
from datetime import UTC, datetime
from importlib.metadata import version
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import AsyncMock

import pytest
from agents import (
    Agent,
    AgentHooks,
    FunctionTool,
    MaxTurnsExceeded,
    ModelResponse,
    RunConfig,
    RunContextWrapper,
    RunErrorDetails,
    RunErrorHandlers,
    RunHooks,
    Runner,
    RunResult,
    RunState,
    Session,
    Usage,
    UserError,
)
from agents.items import ToolCallItem, ToolCallOutputItem
from openai.types.responses import ResponseFunctionToolCall
from packaging.version import Version

import kitaru_openai_agents.recording as recording_module
import kitaru_openai_agents.runner as runner_module
from kitaru.api_models.v1.replay import (
    ReplayResponse,
    ReplayStatus,
    ToolLookupMatch,
    ToolLookupResponse,
)
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    PassthroughConfig,
    ReplayOverride,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.api_models.v1.task import AgentTaskDetails
from kitaru_openai_agents import (
    KitaruRecordingError,
    KitaruRunner,
    SessionObserver,
    ToolPolicyError,
    UnsupportedInterruptionError,
)


class _RunnerFakeSessions:
    def __init__(self) -> None:
        self.session_id = uuid.uuid4()
        self.created: list[Any] = []
        self.updated: list[Any] = []
        self.node_batches: list[Any] = []

    async def create(self, request: Any) -> Any:
        self.created.append(request)
        return SimpleNamespace(id=self.session_id)

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        self.node_batches.append(request)
        return []

    async def update(self, session_id: uuid.UUID, request: Any) -> Any:
        self.updated.append(request)
        return None


class _RunnerFakeTasks:
    async def get_spec(self, task_id: uuid.UUID) -> Any:
        return SimpleNamespace(details=AgentTaskDetails(inputs="hello"))


class _RunnerFakeReplays:
    def __init__(self, client: "_RunnerFakeClient") -> None:
        self._client = client

    async def get(self, replay_id: uuid.UUID) -> Any:
        return _RunnerFakeClient.replay

    async def tool_lookup(self, replay_id: uuid.UUID, request: Any) -> Any:
        self._client.tool_lookups.append(request)
        return _RunnerFakeClient.lookup_response


class _RunnerFakeClient:
    instances: ClassVar[list["_RunnerFakeClient"]] = []
    replay: ClassVar[ReplayResponse]
    lookup_response: ClassVar[ToolLookupResponse]

    def __init__(self) -> None:
        self.sessions = _RunnerFakeSessions()
        self.tasks = _RunnerFakeTasks()
        self.replays = _RunnerFakeReplays(self)
        self.tool_lookups: list[Any] = []
        self.closed = False
        type(self).instances.append(self)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _patch_kitaru_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _RunnerFakeClient.instances.clear()
    _RunnerFakeClient.replay = _replay()
    _RunnerFakeClient.lookup_response = ToolLookupResponse(match=None)
    monkeypatch.setattr(recording_module, "KitaruAPIClient", _RunnerFakeClient)


def _replay(
    *,
    override: ReplayOverride | None = None,
    tool_policy: ToolPolicy | None = None,
) -> ReplayResponse:
    now = datetime.now(UTC)
    return ReplayResponse(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        experiment_run_id=None,
        baseline_session_id=uuid.uuid4(),
        result_session_id=None,
        override=override,
        tool_policy=tool_policy or ToolPolicy(default=PassthroughConfig(), tools={}),
        evaluators=[],
        evaluate_baselines=False,
        status=ReplayStatus.PENDING,
        error=None,
        created=now,
        updated=now,
    )


def _native_result() -> RunResult:
    return cast(
        RunResult,
        SimpleNamespace(
            raw_responses=[], new_items=[], final_output="done", interruptions=[]
        ),
    )


def _recorded_nodes(client: _RunnerFakeClient) -> list[Any]:
    return [node for batch in client.sessions.node_batches for node in batch.nodes]


def _runner_parameters(method: Any) -> dict[str, inspect.Parameter]:
    parameters = dict(inspect.signature(method).parameters)
    parameters.pop("self", None)
    parameters.pop("cls", None)
    return {
        name: parameter.replace(annotation=inspect.Parameter.empty)
        for name, parameter in parameters.items()
    }


def test_supports_locked_openai_agents_minor_line() -> None:
    installed = Version(version("openai-agents"))

    assert installed >= Version("0.19.3")
    assert installed < Version("0.20")


def test_exports_public_recording_contract_from_canonical_package() -> None:
    assert KitaruRecordingError is recording_module.KitaruRecordingError
    assert UnsupportedInterruptionError is recording_module.UnsupportedInterruptionError
    assert SessionObserver is recording_module.SessionObserver


def test_matches_complete_public_runner_signatures() -> None:
    assert _runner_parameters(KitaruRunner.run) == _runner_parameters(Runner.run)
    assert _runner_parameters(KitaruRunner.run_sync) == _runner_parameters(
        Runner.run_sync
    )


def test_locks_supported_hook_methods() -> None:
    run_hook_methods = {
        name
        for name, member in inspect.getmembers(RunHooks, inspect.isfunction)
        if not name.startswith("_")
    }
    agent_hook_methods = {
        name
        for name, member in inspect.getmembers(AgentHooks, inspect.isfunction)
        if not name.startswith("_")
    }

    assert run_hook_methods == {
        "on_agent_end",
        "on_agent_start",
        "on_handoff",
        "on_llm_end",
        "on_llm_start",
        "on_tool_end",
        "on_tool_start",
    }
    assert agent_hook_methods == {
        "on_end",
        "on_handoff",
        "on_llm_end",
        "on_llm_start",
        "on_start",
        "on_tool_end",
        "on_tool_start",
    }


async def test_run_forwards_every_argument_and_preserves_result_identity(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_result = _native_result()
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)
    runner = KitaruRunner(agent_id=uuid.uuid4())
    context = object()
    hooks = RunHooks[Any]()
    run_config = RunConfig()
    error_handlers = RunErrorHandlers[Any]()
    session = cast(Session, object())

    result = await runner.run(
        deterministic_agent,
        "hello",
        context=context,
        max_turns=None,
        hooks=hooks,
        run_config=run_config,
        error_handlers=error_handlers,
        previous_response_id="response-1",
        auto_previous_response_id=True,
        conversation_id="conversation-1",
        session=session,
    )

    assert result is native_result
    assert native_run.await_count == 1
    awaited = native_run.await_args
    assert awaited is not None
    assert awaited.args == (deterministic_agent, "hello")
    forwarded = dict(awaited.kwargs)
    composed_hooks = forwarded.pop("hooks")
    assert composed_hooks._caller is hooks
    assert forwarded == {
        "context": context,
        "max_turns": None,
        "run_config": run_config,
        "error_handlers": error_handlers,
        "previous_response_id": "response-1",
        "auto_previous_response_id": True,
        "conversation_id": "conversation-1",
        "session": session,
    }


async def test_run_returns_a_native_result_from_public_model(
    deterministic_agent: Agent[None],
) -> None:
    result = await KitaruRunner(agent_id=uuid.uuid4()).run(
        deterministic_agent,
        "hello",
        run_config=RunConfig(tracing_disabled=True),
    )

    assert result.final_output == "deterministic result"
    assert result.last_agent is deterministic_agent


async def test_runner_wraps_history_policy_errors_as_user_errors(
    deterministic_model: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_calls: list[str] = []

    async def invoke(_context: Any, arguments: str) -> str:
        live_calls.append(arguments)
        return "live result"

    tool = FunctionTool(
        name="weather",
        description="Look up weather.",
        params_json_schema={"type": "object", "properties": {}},
        on_invoke_tool=invoke,
    )
    agent = Agent[None](name="test", model=deterministic_model, tools=[tool])
    deterministic_model.get_response = AsyncMock(
        return_value=ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    arguments='{"city":"Paris"}',
                    call_id="call-1",
                    name="weather",
                    type="function_call",
                    id="item-1",
                    status="completed",
                )
            ],
            usage=Usage(),
            response_id="response-1",
        )
    )
    _RunnerFakeClient.replay = _replay(
        tool_policy=ToolPolicy(
            default=PassthroughConfig(),
            tools={
                "weather": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.FAIL,
                )
            },
        )
    )
    _RunnerFakeClient.lookup_response = ToolLookupResponse(
        match=ToolLookupMatch(
            status=NodeStatus.FAILED,
            result=None,
            error="recorded failure",
        )
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(_RunnerFakeClient.replay.id))

    with pytest.raises(UserError, match="recorded failure") as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(
            agent,
            "hello",
            run_config=RunConfig(tracing_disabled=True),
        )

    assert isinstance(raised.value.__cause__, ToolPolicyError)
    assert live_calls == []
    assert _RunnerFakeClient.instances[0].tool_lookups[0].occurrence == 0


async def test_native_agents_failure_records_partial_model_and_tool_evidence(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_call = ResponseFunctionToolCall(
        arguments='{"city":"Paris"}',
        call_id="call-1",
        name="weather",
        type="function_call",
        id="item-1",
        status="completed",
    )
    response = ModelResponse(
        output=[raw_call],
        usage=Usage(input_tokens=2, output_tokens=3, total_tokens=5),
        response_id="response-1",
    )
    call = ToolCallItem(agent=deterministic_agent, raw_item=raw_call)
    output = ToolCallOutputItem(
        agent=deterministic_agent,
        raw_item={
            "type": "function_call_output",
            "call_id": "call-1",
            "output": "sunny",
        },
        output={"forecast": "sunny"},
    )
    native_error = MaxTurnsExceeded("maximum turns reached")
    native_error.run_data = RunErrorDetails(
        input="hello",
        new_items=[call, output],
        raw_responses=[response],
        last_agent=deterministic_agent,
        context_wrapper=cast(RunContextWrapper[Any], object()),
        input_guardrail_results=[],
        output_guardrail_results=[],
    )
    monkeypatch.setattr(Runner, "run", AsyncMock(side_effect=native_error))

    with pytest.raises(MaxTurnsExceeded) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value is native_error
    client = _RunnerFakeClient.instances[0]
    nodes = _recorded_nodes(client)
    assert [node.node_type for node in nodes] == [
        NodeType.SPAN,
        NodeType.LLM_CALL,
        NodeType.TOOL_CALL,
        NodeType.SPAN,
    ]
    assert nodes[1].external_id == "response-1"
    assert nodes[2].external_id == "call-1"
    assert nodes[2].outputs == {"forecast": "sunny"}
    assert client.sessions.updated[-1].status is SessionStatus.FAILED
    assert client.closed


async def test_native_agents_failure_stays_primary_when_cleanup_also_fails(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_error = MaxTurnsExceeded("maximum turns reached")
    native_error.run_data = cast(RunErrorDetails, SimpleNamespace())
    monkeypatch.setattr(Runner, "run", AsyncMock(side_effect=native_error))
    monkeypatch.setattr(
        runner_module.RunRecorder,
        "reconcile",
        AsyncMock(side_effect=OSError("secret reconciliation payload")),
    )
    monkeypatch.setattr(
        runner_module,
        "finalize_failure",
        AsyncMock(return_value=OSError("secret finalization payload")),
    )
    monkeypatch.setattr(
        runner_module.RunRecorder,
        "close",
        AsyncMock(side_effect=OSError("secret close payload")),
    )

    with pytest.raises(MaxTurnsExceeded) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value is native_error
    assert native_error.__notes__ == [
        "Kitaru could not reconcile partial OpenAI run data.",
        "Kitaru could not finalize the failed recording.",
        "Kitaru could not close the recording client.",
    ]
    assert "secret" not in " ".join(native_error.__notes__)


async def test_failure_cleanup_closes_after_a_second_cancellation(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_error = MaxTurnsExceeded("maximum turns reached")
    close = AsyncMock()
    monkeypatch.setattr(Runner, "run", AsyncMock(side_effect=native_error))
    monkeypatch.setattr(
        runner_module,
        "finalize_failure",
        AsyncMock(return_value=asyncio.CancelledError()),
    )
    monkeypatch.setattr(runner_module.RunRecorder, "close", close)

    with pytest.raises(MaxTurnsExceeded) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value is native_error
    assert native_error.__notes__ == ["Kitaru could not finalize the failed recording."]
    close.assert_awaited_once()


@pytest.mark.parametrize("phase", ["reconcile", "complete", "close"])
async def test_post_success_cancellation_remains_primary(
    phase: str,
    deterministic_agent: Agent[None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caller_cancellation = asyncio.CancelledError()
    monkeypatch.setattr(Runner, "run", AsyncMock(return_value=_native_result()))
    monkeypatch.setattr(
        runner_module.RunRecorder,
        phase,
        AsyncMock(side_effect=caller_cancellation),
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value is caller_cancellation


async def test_rejects_run_state_before_native_runner(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_run = AsyncMock()
    monkeypatch.setattr(Runner, "run", native_run)
    runner = KitaruRunner(agent_id=uuid.uuid4())
    state = object.__new__(RunState)

    with pytest.raises(TypeError, match="RunState"):
        await runner.run(deterministic_agent, state)

    native_run.assert_not_awaited()


async def test_standalone_run_requires_explicit_identity(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_run = AsyncMock()
    monkeypatch.setattr(Runner, "run", native_run)

    with pytest.raises(ValueError, match="agent_id or agent_version_id"):
        await KitaruRunner().run(deterministic_agent, "hello")

    native_run.assert_not_awaited()


async def test_task_bound_run_allows_server_inferred_identity(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_result = _native_result()
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))

    result = await KitaruRunner().run(deterministic_agent, "hello")

    assert result is native_result


async def test_replay_preflight_changes_the_live_run_before_session_creation(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_result = _native_result()
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)
    monkeypatch.setenv("KITARU_REPLAY_ID", str(uuid.uuid4()))
    _RunnerFakeClient.replay = _replay(
        override=ReplayOverride(
            prompt="replayed input",
            system_prompt="replayed instructions",
        )
    )
    caller_config = RunConfig(workflow_name="caller workflow")

    result = await KitaruRunner(agent_id=uuid.uuid4()).run(
        deterministic_agent,
        "original input",
        run_config=caller_config,
    )

    assert result is native_result
    awaited = native_run.await_args
    assert awaited is not None
    replayed_agent, replayed_input = awaited.args
    assert replayed_agent is not deterministic_agent
    assert replayed_agent.instructions == "replayed instructions"
    assert replayed_input == "replayed input"
    assert awaited.kwargs["run_config"] is not caller_config
    assert awaited.kwargs["run_config"].workflow_name == "caller workflow"
    assert _RunnerFakeClient.instances[0].sessions.created[0].inputs == "original input"


@pytest.mark.parametrize("batch_size", [0, -1])
def test_validates_batch_size(batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        KitaruRunner(agent_id=uuid.uuid4(), batch_size=batch_size)


def test_exposes_no_streaming_runner() -> None:
    assert not hasattr(KitaruRunner, "run_streamed")


async def test_run_sync_rejects_an_active_event_loop(
    deterministic_agent: Agent[None],
) -> None:
    runner = KitaruRunner(agent_id=uuid.uuid4())

    with pytest.raises(RuntimeError, match=r"use.*run"):
        runner.run_sync(deterministic_agent, "hello")


def test_run_sync_creates_a_missing_default_loop(
    deterministic_agent: Agent[None], deterministic_model: Any
) -> None:
    policy = asyncio.get_event_loop_policy()  # ty: ignore[deprecated]
    policy.set_event_loop(None)

    result = KitaruRunner(agent_id=uuid.uuid4()).run_sync(
        deterministic_agent,
        "hello",
        run_config=RunConfig(tracing_disabled=True),
    )
    default_loop = policy.get_event_loop()

    try:
        assert result.final_output == "deterministic result"
        assert deterministic_model.running_loops == [default_loop]
        assert not default_loop.is_closed()
    finally:
        default_loop.close()
        policy.set_event_loop(None)


def test_run_sync_replaces_a_closed_default_loop(
    deterministic_agent: Agent[None], deterministic_model: Any
) -> None:
    policy = asyncio.get_event_loop_policy()  # ty: ignore[deprecated]
    closed_loop = policy.new_event_loop()
    policy.set_event_loop(closed_loop)
    closed_loop.close()

    result = KitaruRunner(agent_id=uuid.uuid4()).run_sync(
        deterministic_agent,
        "hello",
        run_config=RunConfig(tracing_disabled=True),
    )
    default_loop = policy.get_event_loop()

    try:
        assert result.final_output == "deterministic result"
        assert default_loop is not closed_loop
        assert deterministic_model.running_loops == [default_loop]
        assert not default_loop.is_closed()
    finally:
        default_loop.close()
        policy.set_event_loop(None)


def test_run_sync_reuses_the_thread_default_loop(
    deterministic_agent: Agent[None], deterministic_model: Any
) -> None:
    policy = asyncio.get_event_loop_policy()  # ty: ignore[deprecated]
    default_loop = policy.new_event_loop()
    policy.set_event_loop(default_loop)
    runner = KitaruRunner(agent_id=uuid.uuid4())

    try:
        first = runner.run_sync(
            deterministic_agent,
            "first",
            run_config=RunConfig(tracing_disabled=True),
        )
        second = runner.run_sync(
            deterministic_agent,
            "second",
            run_config=RunConfig(tracing_disabled=True),
        )

        assert first.final_output == "deterministic result"
        assert second.final_output == "deterministic result"
        assert deterministic_model.running_loops == [default_loop, default_loop]
        assert not default_loop.is_closed()
    finally:
        default_loop.close()
        policy.set_event_loop(None)
