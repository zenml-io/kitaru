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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Generated record and replay properties for the OpenAI Agents adapter."""

import asyncio
import json
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from agents import (
    Agent,
    AgentOutputSchemaBase,
    FunctionTool,
    Handoff,
    Model,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    RunConfig,
    Tool,
    TResponseInputItem,
    Usage,
    UserError,
)
from agents.items import ToolCallOutputItem
from hypothesis import given
from hypothesis import strategies as st
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)
from openai.types.responses.response_prompt_param import ResponsePromptParam

import kitaru_openai_agents.recording as recording_module
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
    PassthroughConfig,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.client import KitaruAPIClient
from kitaru_openai_agents import KitaruRunner
from kitaru_openai_agents.replay import ToolPolicyError, prepare_replay


@dataclass(frozen=True)
class _Invocation:
    """One deterministic model-requested tool invocation."""

    tool_name: str
    arguments_json: str

    @property
    def arguments(self) -> Any:
        """Parse the public function-call arguments."""
        return json.loads(self.arguments_json)


class _SequentialToolModel(Model):
    """Emit one public function call per turn, followed by a final message."""

    def __init__(self, program: tuple[_Invocation, ...]) -> None:
        self._program = program
        self._position = 0

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: ResponsePromptParam | None,
    ) -> ModelResponse:
        """Return the next scripted call or the final assistant output."""
        response_number = self._position + 1
        if self._position < len(self._program):
            invocation = self._program[self._position]
            self._position += 1
            output: list[Any] = [
                ResponseFunctionToolCall(
                    arguments=invocation.arguments_json,
                    call_id=f"call-{response_number}",
                    name=invocation.tool_name,
                    type="function_call",
                    id=f"item-{response_number}",
                    status="completed",
                )
            ]
        else:
            self._position += 1
            output = [
                ResponseOutputMessage(
                    id=f"message-{response_number}",
                    content=[
                        ResponseOutputText(
                            annotations=[],
                            text="done",
                            type="output_text",
                            logprobs=[],
                        )
                    ],
                    role="assistant",
                    status="completed",
                    type="message",
                )
            ]
        return ModelResponse(
            output=output,
            usage=Usage(),
            response_id=f"response-{response_number}",
        )

    def stream_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: ResponsePromptParam | None,
    ) -> AsyncIterator[Any]:
        """Reject streaming because these programs are strictly sequential."""
        raise AssertionError("The sequential model must not be streamed")


@dataclass
class _RoundTripState:
    """Persist wire-round-tripped nodes and serve independent history lookups."""

    nodes: list[SessionNodeCreateRequest] = field(default_factory=list)
    replay: ReplayResponse | None = None
    lookups: list[Any] = field(default_factory=list)
    _baseline_nodes: tuple[SessionNodeCreateRequest, ...] | None = None
    _matches: dict[tuple[str, str], list[ToolLookupMatch]] = field(default_factory=dict)

    def configure_replay(
        self,
        tool_names: set[str],
        *,
        on_miss: ToolPolicyOnMiss = ToolPolicyOnMiss.FAIL,
    ) -> ReplayResponse:
        """Snapshot recorded tool nodes into a test-local lookup index."""
        if self._baseline_nodes is None:
            self._baseline_nodes = tuple(self.nodes)
        matches: dict[tuple[str, str], list[ToolLookupMatch]] = defaultdict(list)
        for node in sorted(self._baseline_nodes, key=lambda item: item.index):
            if node.node_type is not NodeType.TOOL_CALL or node.tool_name is None:
                continue
            cache_key = compute_tool_cache_key(node.tool_name, node.inputs)
            if cache_key is None:
                continue
            matches[(node.tool_name, cache_key)].append(
                ToolLookupMatch(
                    result=node.outputs,
                    status=node.status,
                    error=node.error,
                )
            )
        self._matches = dict(matches)
        self.lookups.clear()
        now = datetime.now(UTC)
        self.replay = ReplayResponse(
            id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            experiment_run_id=None,
            baseline_session_id=uuid.uuid4(),
            result_session_id=None,
            override=None,
            tool_policy=ToolPolicy(
                default=PassthroughConfig(),
                tools={
                    name: HistoryConfig(
                        scope=HistoryScope.BASELINE,
                        on_miss=on_miss,
                    )
                    for name in tool_names
                },
            ),
            evaluators=[],
            evaluate_baselines=False,
            baseline_evaluation_mode=BaselineEvaluationMode.NONE,
            status=ReplayStatus.PENDING,
            error=None,
            created=now,
            updated=now,
        )
        return self.replay


class _RoundTripSessions:
    def __init__(self, state: _RoundTripState) -> None:
        self._state = state

    async def create(self, request: Any) -> Any:
        return SimpleNamespace(id=uuid.uuid4())

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        batch = SessionNodeBatchRequest.model_validate_json(request.model_dump_json())
        self._state.nodes.extend(batch.nodes)
        return []

    async def update(self, session_id: uuid.UUID, request: Any) -> None:
        return None


class _RoundTripTasks:
    async def get_spec(self, task_id: uuid.UUID) -> Any:
        raise AssertionError("Generated runs must not resolve task inputs")


class _RoundTripReplays:
    def __init__(self, state: _RoundTripState) -> None:
        self._state = state

    async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
        replay = self._state.replay
        assert replay is not None
        assert replay_id == replay.id
        return replay

    async def tool_lookup(self, replay_id: uuid.UUID, request: Any) -> Any:
        replay = self._state.replay
        assert replay is not None
        assert replay_id == replay.id
        self._state.lookups.append(request)
        candidates = self._state._matches.get(
            (request.tool_name, request.cache_key), []
        )
        occurrence = request.occurrence
        assert occurrence is not None
        match = candidates[occurrence] if occurrence < len(candidates) else None
        return ToolLookupResponse(match=match)


class _RoundTripClient:
    def __init__(self, state: _RoundTripState) -> None:
        self.sessions = _RoundTripSessions(state)
        self.tasks = _RoundTripTasks()
        self.replays = _RoundTripReplays(state)

    async def close(self) -> None:
        return None


def _install_round_trip_client(
    monkeypatch: pytest.MonkeyPatch,
) -> _RoundTripState:
    """Install a fresh in-memory client behind the recorder's real hooks."""
    state = _RoundTripState()
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    monkeypatch.delenv("KITARU_TASK_INPUTS", raising=False)
    monkeypatch.delenv("KITARU_REPLAY_ID", raising=False)
    monkeypatch.setattr(
        recording_module,
        "KitaruAPIClient",
        lambda: _RoundTripClient(state),
    )
    return state


def _create_tools(
    tool_names: set[str],
    executions: list[_Invocation],
    result_for: Callable[[_Invocation, int], Any],
) -> list[Tool]:
    """Create ordinary enabled direct function tools with observable bodies."""
    tools: list[Tool] = []
    for tool_name in sorted(tool_names):

        async def invoke(
            _context: Any,
            arguments_json: str,
            *,
            name: str = tool_name,
        ) -> Any:
            invocation = _Invocation(name, arguments_json)
            executions.append(invocation)
            return result_for(invocation, len(executions) - 1)

        tools.append(
            FunctionTool(
                name=tool_name,
                description=f"Run deterministic {tool_name}.",
                params_json_schema={"type": "object"},
                on_invoke_tool=invoke,
                strict_json_schema=False,
            )
        )
    return tools


async def _run_program(
    program: tuple[_Invocation, ...],
    executions: list[_Invocation],
    result_for: Callable[[_Invocation, int], Any],
) -> list[Any]:
    """Run one bounded program through the public Kitaru and Agents runners."""
    tool_names = {invocation.tool_name for invocation in program}
    agent = Agent[None](
        name="generated-sequential-agent",
        model=_SequentialToolModel(program),
        tools=_create_tools(tool_names, executions, result_for),
    )
    result = await KitaruRunner(agent_id=uuid.uuid4()).run(
        agent,
        "run the scripted tool program",
        max_turns=len(program) + 1,
        run_config=RunConfig(tracing_disabled=True),
    )
    return [
        item.output for item in result.new_items if isinstance(item, ToolCallOutputItem)
    ]


def _recorded_result(invocation: _Invocation, index: int) -> Any:
    """Return an occurrence-distinguishing JSON result for a live recording."""
    return {
        "arguments": invocation.arguments,
        "recorded_index": index,
        "tool": invocation.tool_name,
    }


async def _record_then_replay(
    state: _RoundTripState,
    monkeypatch: pytest.MonkeyPatch,
    recorded_program: tuple[_Invocation, ...],
    replay_program: tuple[_Invocation, ...],
) -> tuple[list[Any], list[Any], list[_Invocation]]:
    """Record live results, then replay a fresh model without live tool calls."""
    recorded_executions: list[_Invocation] = []
    recorded_outputs = await _run_program(
        recorded_program, recorded_executions, _recorded_result
    )
    assert recorded_executions == list(recorded_program)

    replay = state.configure_replay(
        {invocation.tool_name for invocation in replay_program}
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    replay_executions: list[_Invocation] = []
    replay_outputs = await _run_program(
        replay_program,
        replay_executions,
        lambda invocation, index: {
            "unexpected_live_index": index,
            "tool": invocation.tool_name,
        },
    )
    return recorded_outputs, replay_outputs, replay_executions


def _expected_outputs(
    recorded_program: tuple[_Invocation, ...],
    recorded_outputs: list[Any],
    replay_program: tuple[_Invocation, ...],
) -> list[Any]:
    """Select results through an independent per-key occurrence model."""
    groups: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for invocation, output in zip(recorded_program, recorded_outputs, strict=True):
        groups[_get_logical_key(invocation)].append(output)

    positions: dict[tuple[str, str], int] = defaultdict(int)
    expected: list[Any] = []
    for invocation in replay_program:
        group = _get_logical_key(invocation)
        expected.append(groups[group][positions[group]])
        positions[group] += 1
    return expected


def _get_logical_key(invocation: _Invocation) -> tuple[str, str]:
    """Identify a call independently from the production cache-key helper."""
    arguments = json.dumps(
        invocation.arguments,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return invocation.tool_name, arguments


def _get_expected_occurrences(
    program: tuple[_Invocation, ...],
) -> list[int]:
    """Count per-call occurrences without using adapter state."""
    positions: dict[tuple[str, str], int] = defaultdict(int)
    expected: list[int] = []
    for invocation in program:
        logical_key = _get_logical_key(invocation)
        expected.append(positions[logical_key])
        positions[logical_key] += 1
    return expected


async def test_record_then_replay_selects_each_key_occurrence_in_recorded_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replay B,A,A from an A,B,A recording without executing live tools."""
    state = _install_round_trip_client(monkeypatch)
    a = _Invocation("lookup", '{"value":"A"}')
    b = _Invocation("lookup", '{"value":"B"}')
    recorded_program = (a, b, a)
    replay_program = (b, a, a)

    recorded_outputs, replay_outputs, replay_executions = await _record_then_replay(
        state, monkeypatch, recorded_program, replay_program
    )

    assert replay_outputs == _expected_outputs(
        recorded_program, recorded_outputs, replay_program
    )
    assert replay_executions == []
    assert [request.occurrence for request in state.lookups] == (
        _get_expected_occurrences(replay_program)
    )


async def test_history_canonicalizes_object_keys_but_preserves_array_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _install_round_trip_client(monkeypatch)
    recorded = _Invocation("lookup", '{"left":1,"right":2,"items":[1,2]}')
    reordered = _Invocation("lookup", '{"items":[1,2],"right":2,"left":1}')
    reversed_array = _Invocation("lookup", '{"right":2,"left":1,"items":[2,1]}')
    recorded_executions: list[_Invocation] = []
    recorded_outputs = await _run_program(
        (recorded,), recorded_executions, _recorded_result
    )

    replay = state.configure_replay({"lookup"})
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    replay_executions: list[_Invocation] = []
    assert (
        await _run_program((reordered,), replay_executions, _recorded_result)
        == recorded_outputs
    )
    assert replay_executions == []

    replay = state.configure_replay({"lookup"})
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    with pytest.raises(UserError, match="No history result"):
        await _run_program((reversed_array,), replay_executions, _recorded_result)
    assert replay_executions == []


async def test_completed_null_is_a_hit_distinct_from_a_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _install_round_trip_client(monkeypatch)
    invocation = _Invocation("nullable", '{"value":null}')
    recorded_executions: list[_Invocation] = []
    assert await _run_program(
        (invocation,), recorded_executions, lambda _invocation, _index: None
    ) == [None]

    replay = state.configure_replay({"nullable"})
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    replay_executions: list[_Invocation] = []
    assert await _run_program((invocation,), replay_executions, _recorded_result) == [
        None
    ]
    assert replay_executions == []

    missing = _Invocation("nullable", '{"value":"missing"}')
    replay = state.configure_replay({"nullable"})
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    with pytest.raises(UserError, match="No history result"):
        await _run_program((missing,), replay_executions, _recorded_result)
    assert replay_executions == []


async def test_misses_do_not_advance_and_passthrough_runs_once_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _install_round_trip_client(monkeypatch)
    present = _Invocation("lookup", '{"value":"present"}')
    missing = _Invocation("lookup", '{"value":"missing"}')
    await _run_program((present,), [], _recorded_result)

    replay = state.configure_replay({"lookup"}, on_miss=ToolPolicyOnMiss.PASSTHROUGH)
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    replay_executions: list[_Invocation] = []
    outputs = await _run_program(
        (present, missing, missing, present),
        replay_executions,
        lambda invocation, index: {
            "live_index": index,
            "value": invocation.arguments,
        },
    )

    assert [request.occurrence for request in state.lookups] == [0, 0, 0, 1]
    assert replay_executions == [missing, missing, present]
    assert outputs[1:] == [
        {"live_index": 0, "value": {"value": "missing"}},
        {"live_index": 1, "value": {"value": "missing"}},
        {"live_index": 2, "value": {"value": "present"}},
    ]


async def test_failed_match_is_consumed_and_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _install_round_trip_client(monkeypatch)
    invocation = _Invocation("lookup", '{"value":"same"}')
    recorded_outputs = await _run_program(
        (invocation, invocation), [], _recorded_result
    )
    tool_nodes = [node for node in state.nodes if node.node_type is NodeType.TOOL_CALL]
    tool_nodes[0].status = NodeStatus.FAILED
    tool_nodes[0].error = "recorded failure"
    replay = state.configure_replay({"lookup"})
    client = _RoundTripClient(state)
    live_executions: list[_Invocation] = []
    original_tool = _create_tools({"lookup"}, live_executions, _recorded_result)[0]
    prepared = prepare_replay(
        Agent[None](name="failed-history", tools=[original_tool]),
        "input",
        None,
        replay,
        client=cast(KitaruAPIClient, client),
    )
    replay_tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    with pytest.raises(ToolPolicyError, match="recorded failure"):
        await replay_tool.on_invoke_tool(cast(Any, None), invocation.arguments_json)
    assert (
        await replay_tool.on_invoke_tool(cast(Any, None), invocation.arguments_json)
        == recorded_outputs[1]
    )
    assert [request.occurrence for request in state.lookups] == [0, 1]
    assert live_executions == []


async def test_capture_loss_marker_fails_closed_without_live_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _install_round_trip_client(monkeypatch)
    invocation = _Invocation("lookup", '{"value":"captured"}')
    await _run_program((invocation,), [], _recorded_result)
    tool_node = next(
        node for node in state.nodes if node.node_type is NodeType.TOOL_CALL
    )
    tool_node.outputs = {"_kitaru_truncated": {"reason": "max_depth"}}

    replay = state.configure_replay({"lookup"})
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    live_executions: list[_Invocation] = []
    with pytest.raises(UserError, match="cannot be replayed safely") as raised:
        await _run_program((invocation,), live_executions, _recorded_result)
    assert isinstance(raised.value.__cause__, ToolPolicyError)
    assert live_executions == []


_GENERATED_INVOCATIONS = (
    _Invocation("alpha", '{"value":0}'),
    _Invocation("alpha", '{"value":1}'),
    _Invocation("beta", '{"value":0}'),
    _Invocation("beta", '{"value":1}'),
    _Invocation("alpha", '{"items":[0,1]}'),
    _Invocation("beta", '{"items":[1,0]}'),
)


@st.composite
def _sequential_program_pairs(
    draw: st.DrawFn,
) -> tuple[tuple[_Invocation, ...], tuple[_Invocation, ...]]:
    """Generate a bounded recording and a permutation for successful replay."""
    recorded = tuple(
        draw(
            st.lists(
                st.sampled_from(_GENERATED_INVOCATIONS),
                min_size=1,
                max_size=8,
            )
        )
    )
    order = draw(st.permutations(tuple(range(len(recorded)))))
    return recorded, tuple(recorded[index] for index in order)


@given(programs=_sequential_program_pairs())
def test_generated_sequential_programs_replay_by_key_occurrence(
    programs: tuple[tuple[_Invocation, ...], tuple[_Invocation, ...]],
) -> None:
    recorded_program, replay_program = programs
    with pytest.MonkeyPatch.context() as monkeypatch:
        state = _install_round_trip_client(monkeypatch)
        recorded_outputs, replay_outputs, replay_executions = asyncio.run(
            _record_then_replay(
                state,
                monkeypatch,
                recorded_program,
                replay_program,
            )
        )

    assert replay_outputs == _expected_outputs(
        recorded_program, recorded_outputs, replay_program
    )
    assert replay_executions == []
    assert [request.occurrence for request in state.lookups] == (
        _get_expected_occurrences(replay_program)
    )
