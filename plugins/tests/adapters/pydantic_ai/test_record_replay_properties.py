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
"""Generated record/replay properties for the PydanticAI adapter."""

import asyncio
import json
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

import kitaru_pydantic_ai.capability as capability_module
from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
    ToolLookupMatch,
    ToolLookupRequest,
    ToolLookupResponse,
)
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeCreateRequest,
)
from kitaru.cache_keys import compute_tool_cache_key
from kitaru_pydantic_ai import KitaruAgent, ToolPolicyError, ToolPolicyMissError


@dataclass(frozen=True)
class _Call:
    """One sequential framework-native tool call."""

    tool_name: str
    arguments: dict[str, JsonValue]


@dataclass(frozen=True)
class _LiveCall:
    """One execution of a live tool body."""

    call: _Call
    result: JsonValue


class _SessionsTransport:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.node_batches: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        return SimpleNamespace(id=self._client.session_id)

    async def update(self, session_id: uuid.UUID, request: Any) -> None:
        return None

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        self.node_batches.append((session_id, request))
        return []


class _TasksTransport:
    async def get_spec(self, task_id: uuid.UUID) -> Any:
        raise AssertionError(f"task lookup was not expected for {task_id}")


class _ReplaysTransport:
    def __init__(
        self,
        replay: ReplayResponse | None,
        matches: dict[str, list[ToolLookupMatch]],
        forced_misses: dict[str, int],
    ) -> None:
        self._replay = replay
        self._matches = matches
        self._forced_misses = dict(forced_misses)
        self.lookups: list[ToolLookupRequest] = []

    async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
        assert self._replay is not None
        assert replay_id == self._replay.id
        return self._replay

    async def tool_lookup(
        self, replay_id: uuid.UUID, request: ToolLookupRequest
    ) -> ToolLookupResponse:
        assert self._replay is not None
        assert replay_id == self._replay.id
        self.lookups.append(request)
        remaining_misses = self._forced_misses.get(request.cache_key, 0)
        if remaining_misses:
            self._forced_misses[request.cache_key] = remaining_misses - 1
            return ToolLookupResponse(match=None)
        assert request.occurrence is not None
        candidates = self._matches.get(request.cache_key, [])
        match = (
            candidates[request.occurrence]
            if request.occurrence < len(candidates)
            else None
        )
        return ToolLookupResponse(match=match)


class _FakeClient:
    def __init__(
        self,
        *,
        replay: ReplayResponse | None = None,
        matches: dict[str, list[ToolLookupMatch]] | None = None,
        forced_misses: dict[str, int] | None = None,
    ) -> None:
        self.session_id = uuid.uuid4()
        self.sessions = _SessionsTransport(self)
        self.tasks = _TasksTransport()
        self.replays = _ReplaysTransport(replay, matches or {}, forced_misses or {})
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _get_tool_returns(messages: list[ModelMessage]) -> list[JsonValue]:
    """Read completed tool results from model history."""
    return [
        part.content
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, ToolReturnPart)
    ]


def _make_agent(
    program: list[_Call],
    live_calls: list[_LiveCall],
    observed_results: list[JsonValue],
) -> Agent[None, str]:
    """Build a deterministic agent whose next step comes from message history."""

    def model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
        results = _get_tool_returns(messages)
        observed_results[:] = results
        if len(results) == len(program):
            return ModelResponse(parts=[TextPart("finished")])
        call = program[len(results)]
        return ModelResponse(parts=[ToolCallPart(call.tool_name, dict(call.arguments))])

    agent = Agent(FunctionModel(model, model_name="record-replay-property"))

    def execute(tool_name: str, payload: Any) -> JsonValue:
        arguments: dict[str, JsonValue] = {"payload": payload}
        if payload is None:
            result: JsonValue = None
        else:
            result = {
                "execution": len(live_calls),
                "tool": tool_name,
                "arguments": arguments,
            }
        live_calls.append(_LiveCall(_Call(tool_name, arguments), result))
        return result

    @agent.tool_plain
    def lookup(payload: Any) -> JsonValue:
        return execute("lookup", payload)

    @agent.tool_plain
    def inspect(payload: Any) -> JsonValue:
        return execute("inspect", payload)

    return agent


def _get_recorded_tool_nodes(client: _FakeClient) -> list[SessionNodeCreateRequest]:
    """Restore recorded tool requests across their actual JSON wire boundary."""
    captured = [
        node
        for _, batch in client.sessions.node_batches
        for node in batch.nodes
        if node.node_type is NodeType.TOOL_CALL
    ]
    restored = [
        SessionNodeCreateRequest.model_validate_json(node.model_dump_json())
        for node in captured
    ]
    return sorted(restored, key=lambda node: node.index)


def _build_lookup_table(
    nodes: list[SessionNodeCreateRequest],
) -> dict[str, list[ToolLookupMatch]]:
    """Index recorded matches independently by canonical tool cache key."""
    matches: dict[str, list[ToolLookupMatch]] = {}
    for node in nodes:
        assert node.tool_name is not None
        assert isinstance(node.inputs, dict)
        cache_key = compute_tool_cache_key(node.tool_name, node.inputs)
        assert cache_key is not None
        matches.setdefault(cache_key, []).append(
            ToolLookupMatch(
                result=node.outputs,
                status=node.status,
                error=node.error,
            )
        )
    return matches


def _make_replay(on_miss: ToolPolicyOnMiss) -> ReplayResponse:
    """Create a baseline-history replay response."""
    now = capability_module.datetime.now(capability_module.UTC)
    return ReplayResponse(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        experiment_run_id=None,
        baseline_session_id=uuid.uuid4(),
        result_session_id=None,
        override=None,
        tool_policy=ToolPolicy(
            default=HistoryConfig(scope=HistoryScope.BASELINE, on_miss=on_miss),
            tools={},
        ),
        evaluators=[],
        evaluate_baselines=False,
        baseline_evaluation_mode=BaselineEvaluationMode.NONE,
        status=ReplayStatus.PENDING,
        error=None,
        created=now,
        updated=now,
    )


def _install_client(monkeypatch: pytest.MonkeyPatch, client: _FakeClient) -> None:
    """Install one isolated transport client for the next agent run."""
    monkeypatch.setattr(capability_module, "KitaruAPIClient", lambda: client)


def _clear_task_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove task and replay state before a recording run."""
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


async def _record_program(
    monkeypatch: pytest.MonkeyPatch, program: list[_Call]
) -> tuple[list[SessionNodeCreateRequest], list[_LiveCall]]:
    """Record a complete program through KitaruAgent."""
    _clear_task_environment(monkeypatch)
    client = _FakeClient()
    _install_client(monkeypatch, client)
    live_calls: list[_LiveCall] = []
    observed_results: list[JsonValue] = []
    agent = KitaruAgent(
        _make_agent(program, live_calls, observed_results),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("record")

    assert result.output == "finished"
    assert observed_results == [call.result for call in live_calls]
    assert client.closed
    return _get_recorded_tool_nodes(client), live_calls


async def _replay_program(
    monkeypatch: pytest.MonkeyPatch,
    program: list[_Call],
    matches: dict[str, list[ToolLookupMatch]],
    *,
    on_miss: ToolPolicyOnMiss = ToolPolicyOnMiss.FAIL,
    forced_misses: dict[str, int] | None = None,
    live_calls: list[_LiveCall] | None = None,
) -> tuple[list[JsonValue], list[_LiveCall], list[ToolLookupRequest]]:
    """Replay a program through baseline history with an independent lookup."""
    replay = _make_replay(on_miss)
    client = _FakeClient(
        replay=replay,
        matches=matches,
        forced_misses=forced_misses,
    )
    _install_client(monkeypatch, client)
    task_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps("replay"))
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    replay_live_calls = live_calls if live_calls is not None else []
    observed_results: list[JsonValue] = []
    agent = KitaruAgent(
        _make_agent(program, replay_live_calls, observed_results),
        agent_id=uuid.uuid4(),
    )

    result = await agent.run("caller input is replaced")

    assert result.output == "finished"
    assert client.closed
    return observed_results, replay_live_calls, client.replays.lookups


def _get_logical_key(call: _Call) -> tuple[str, str]:
    """Identify a call independently from the production cache-key helper."""
    arguments = json.dumps(
        call.arguments,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return call.tool_name, arguments


def _get_expected_results(
    recorded_calls: list[_LiveCall], replay_program: list[_Call]
) -> list[JsonValue]:
    """Resolve expected results without using adapter matching logic."""
    groups: dict[tuple[str, str], list[JsonValue]] = {}
    for live_call in recorded_calls:
        groups.setdefault(_get_logical_key(live_call.call), []).append(live_call.result)

    occurrences: dict[tuple[str, str], int] = {}
    results: list[JsonValue] = []
    for call in replay_program:
        logical_key = _get_logical_key(call)
        occurrence = occurrences.get(logical_key, 0)
        results.append(groups[logical_key][occurrence])
        occurrences[logical_key] = occurrence + 1
    return results


async def test_reordered_groups_preserve_recorded_occurrence_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reordering A,B,A as B,A,A preserves each group's own order."""
    recorded = [
        _Call("lookup", {"payload": "A"}),
        _Call("lookup", {"payload": "B"}),
        _Call("lookup", {"payload": "A"}),
    ]
    replayed = [recorded[1], recorded[0], recorded[2]]
    nodes, recorded_live_calls = await _record_program(monkeypatch, recorded)
    matches = _build_lookup_table(nodes)

    results, replay_live_calls, lookups = await _replay_program(
        monkeypatch, replayed, matches
    )

    assert len(recorded_live_calls) == 3
    assert results == _get_expected_results(recorded_live_calls, replayed)
    assert replay_live_calls == []
    assert [request.occurrence for request in lookups] == [0, 0, 1]


async def test_completed_null_and_canonical_dict_arguments_replay_as_hits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Null results remain hits and object key order does not change identity."""
    recorded = [
        _Call("lookup", {"payload": None}),
        _Call("inspect", {"payload": {"a": 1, "b": 2}}),
    ]
    replayed = [
        _Call("inspect", {"payload": {"b": 2, "a": 1}}),
        recorded[0],
    ]
    nodes, recorded_live_calls = await _record_program(monkeypatch, recorded)
    matches = _build_lookup_table(nodes)

    results, live_calls, lookups = await _replay_program(monkeypatch, replayed, matches)

    assert results == _get_expected_results(recorded_live_calls, replayed)
    assert results[1] is None
    assert live_calls == []
    assert [request.occurrence for request in lookups] == [0, 0]


async def test_array_order_change_is_a_history_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Array order remains significant when computing history identity."""
    nodes, _ = await _record_program(
        monkeypatch, [_Call("lookup", {"payload": [1, 2]})]
    )
    matches = _build_lookup_table(nodes)
    live_calls: list[_LiveCall] = []

    with pytest.raises(ToolPolicyMissError, match="No history result"):
        await _replay_program(
            monkeypatch,
            [_Call("lookup", {"payload": [2, 1]})],
            matches,
            live_calls=live_calls,
        )
    assert live_calls == []


async def test_passthrough_miss_executes_once_without_advancing_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient miss runs once and leaves the recorded occurrence available."""
    call = _Call("lookup", {"payload": "A"})
    nodes, _ = await _record_program(monkeypatch, [call])
    matches = _build_lookup_table(nodes)
    cache_key = compute_tool_cache_key(call.tool_name, call.arguments)
    assert cache_key is not None

    results, live_calls, lookups = await _replay_program(
        monkeypatch,
        [call, call],
        matches,
        on_miss=ToolPolicyOnMiss.PASSTHROUGH,
        forced_misses={cache_key: 1},
    )

    assert len(live_calls) == 1
    assert results == [live_calls[0].result, matches[cache_key][0].result]
    assert [request.occurrence for request in lookups] == [0, 0]


async def test_exhausted_and_changed_arguments_do_not_advance_other_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Misses retain their occurrence and do not consume another argument group."""
    call_a = _Call("lookup", {"payload": "A"})
    call_b = _Call("lookup", {"payload": "B"})
    nodes, _ = await _record_program(monkeypatch, [call_a])
    matches = _build_lookup_table(nodes)

    _, live_calls, lookups = await _replay_program(
        monkeypatch,
        [call_b, call_a, call_a, call_a],
        matches,
        on_miss=ToolPolicyOnMiss.PASSTHROUGH,
    )

    assert len(live_calls) == 3
    assert [request.occurrence for request in lookups] == [0, 0, 1, 1]


async def test_failed_recorded_match_refuses_live_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed history match raises without calling the live tool body."""
    call = _Call("lookup", {"payload": "A"})
    nodes, _ = await _record_program(monkeypatch, [call])
    failed_node = nodes[0].model_copy(
        update={
            "status": NodeStatus.FAILED,
            "outputs": None,
            "error": "recorded failure",
        }
    )
    matches = _build_lookup_table([failed_node])
    live_calls: list[_LiveCall] = []

    with pytest.raises(ToolPolicyError, match="recorded failure"):
        await _replay_program(monkeypatch, [call], matches, live_calls=live_calls)
    assert live_calls == []


_argument_alphabet = st.sampled_from(
    [
        {"payload": None},
        {"payload": "A"},
        {"payload": "B"},
        {"payload": [1, 2]},
        {"payload": [2, 1]},
        {"payload": {"a": 1, "b": 2}},
    ]
)
_call_alphabet = st.builds(
    _Call,
    tool_name=st.sampled_from(["lookup", "inspect"]),
    arguments=_argument_alphabet,
)


@given(program=st.lists(_call_alphabet, min_size=1, max_size=8), data=st.data())
def test_generated_reordering_preserves_per_call_occurrences(
    program: list[_Call],
    data: st.DataObject,
) -> None:
    """Every bounded sequential reordering resolves the right recorded match."""
    order = data.draw(st.permutations(tuple(range(len(program)))))
    replayed = [program[index] for index in order]
    with pytest.MonkeyPatch.context() as monkeypatch:
        nodes, recorded_live_calls = asyncio.run(_record_program(monkeypatch, program))
        matches = _build_lookup_table(nodes)
        results, live_calls, lookups = asyncio.run(
            _replay_program(monkeypatch, replayed, matches)
        )

    assert results == _get_expected_results(recorded_live_calls, replayed)
    assert live_calls == []
    expected_occurrences: dict[tuple[str, str], int] = {}
    requested_occurrences: list[int] = []
    for call in replayed:
        logical_key = _get_logical_key(call)
        occurrence = expected_occurrences.get(logical_key, 0)
        requested_occurrences.append(occurrence)
        expected_occurrences[logical_key] = occurrence + 1
    assert [request.occurrence for request in lookups] == requested_occurrences
