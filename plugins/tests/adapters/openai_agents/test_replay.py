#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Focused tests for OpenAI Agents replay preflight and transformation."""

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from agents import (
    Agent,
    FunctionTool,
    ModelSettings,
    ProgrammaticToolCallingTool,
    RunConfig,
    ToolOrigin,
    ToolOriginType,
    TResponseInputItem,
    tool_namespace,
)
from agents.items import ToolCallItem
from agents.run_config import CallModelData, ModelInputData
from openai.types.responses import ResponseFunctionToolCall

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
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.client import KitaruAPIClient
from kitaru_openai_agents.recording import _capture_tool_input
from kitaru_openai_agents.replay import (
    ToolPolicyError,
    ToolPolicyMissError,
    prepare_replay,
)


def _replay(
    *,
    override: ReplayOverride | None = None,
    default: Any | None = None,
    tools: dict[str, Any] | None = None,
) -> ReplayResponse:
    now = datetime.now(UTC)
    return ReplayResponse(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        experiment_run_id=None,
        baseline_session_id=uuid.uuid4(),
        result_session_id=None,
        override=override,
        tool_policy=ToolPolicy(
            default=default or PassthroughConfig(),
            tools=tools or {},
        ),
        evaluators=[],
        evaluate_baselines=False,
        baseline_evaluation_mode=BaselineEvaluationMode.NONE,
        status=ReplayStatus.PENDING,
        error=None,
        created=now,
        updated=now,
    )


def _function_tool(
    calls: list[str],
    *,
    name: str = "lookup",
    result: Any = "real result",
    **kwargs: Any,
) -> FunctionTool:
    async def invoke(_context: Any, arguments: str) -> Any:
        calls.append(arguments)
        return result

    return FunctionTool(
        name=name,
        description="Look something up.",
        params_json_schema={"type": "object", "properties": {}},
        on_invoke_tool=invoke,
        **kwargs,
    )


def _static(
    cases: list[StaticCase],
    *,
    on_miss: ToolPolicyOnMiss = ToolPolicyOnMiss.FAIL,
) -> StaticConfig:
    return StaticConfig(cases=cases, on_miss=on_miss)


async def _invoke(tool: FunctionTool, arguments: dict[str, Any]) -> Any:
    return await tool.on_invoke_tool(cast(Any, None), json.dumps(arguments))


class _FakeReplays:
    def __init__(
        self,
        responses: list[ToolLookupResponse],
        *,
        yield_before_response: bool = False,
    ) -> None:
        self.responses = responses
        self.lookups: list[tuple[uuid.UUID, Any]] = []
        self.yield_before_response = yield_before_response

    async def tool_lookup(self, replay_id: uuid.UUID, request: Any) -> Any:
        self.lookups.append((replay_id, request))
        if self.yield_before_response:
            await asyncio.sleep(0)
        return self.responses.pop(0)


class _FakeClient:
    def __init__(
        self,
        responses: list[ToolLookupResponse],
        *,
        yield_before_response: bool = False,
    ) -> None:
        self.replays = _FakeReplays(
            responses,
            yield_before_response=yield_before_response,
        )


def _lookup_response(
    result: Any = None,
    *,
    status: NodeStatus = NodeStatus.COMPLETED,
    error: str | None = None,
) -> ToolLookupResponse:
    return ToolLookupResponse(
        match=ToolLookupMatch(result=result, status=status, error=error)
    )


def test_preserves_native_string_and_validated_item_list() -> None:
    agent = Agent[None](name="test")
    item_input = cast(
        list[TResponseInputItem],
        [{"role": "user", "content": "hello"}],
    )

    text = prepare_replay(agent, "hello", None, _replay())
    items = prepare_replay(agent, item_input, None, _replay())

    assert text.input == "hello"
    assert items.input is item_input


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ({"z": 1, "a": [True, None]}, '{"a":[true,null],"z":1}'),
        ([1, 2], "[1,2]"),
        (42, "42"),
        (None, "null"),
    ],
)
def test_converts_arbitrary_json_to_deterministic_text(
    input_value: Any, expected: str
) -> None:
    prepared = prepare_replay(Agent[None](name="test"), input_value, None, _replay())

    assert prepared.input == expected


def test_replay_prompt_wins_without_losing_original_input() -> None:
    original = {"prompt": "caller"}

    prepared = prepare_replay(
        Agent[None](name="test"),
        original,
        None,
        _replay(override=ReplayOverride(prompt="replayed")),
    )

    assert prepared.input == "replayed"


@pytest.mark.parametrize("async_filter", [False, True])
async def test_replay_instructions_run_after_caller_filter(
    async_filter: bool,
) -> None:
    events: list[str] = []

    def sync_filter(data: CallModelData[None]) -> ModelInputData:
        events.append("caller")
        return ModelInputData(input=data.model_data.input, instructions="caller")

    async def async_filter_fn(data: CallModelData[None]) -> ModelInputData:
        return sync_filter(data)

    caller_filter: Callable[
        [CallModelData[None]], ModelInputData | Awaitable[ModelInputData]
    ] = async_filter_fn if async_filter else sync_filter
    config = RunConfig(call_model_input_filter=cast(Any, caller_filter))
    original_agent = Agent[None](name="test", instructions="original")

    prepared = prepare_replay(
        original_agent,
        "input",
        config,
        _replay(override=ReplayOverride(system_prompt="replayed")),
    )
    assert prepared.run_config.call_model_input_filter is not None
    model_data = ModelInputData(input=[], instructions="materialized")
    filtered = await cast(
        Awaitable[ModelInputData],
        prepared.run_config.call_model_input_filter(
            CallModelData(
                model_data=model_data,
                agent=prepared.starting_agent,
                context=None,
            )
        ),
    )

    assert events == ["caller"]
    assert filtered.instructions == "replayed"
    assert original_agent.instructions == "original"
    assert config.call_model_input_filter is caller_filter


async def test_instruction_replacement_applies_only_to_starting_agent() -> None:
    starting_agent = Agent[None](name="starting")
    other_agent = Agent[None](name="other")
    prepared = prepare_replay(
        starting_agent,
        "input",
        None,
        _replay(override=ReplayOverride(system_prompt="replayed")),
    )
    assert prepared.run_config.call_model_input_filter is not None
    model_data = ModelInputData(input=[], instructions="other")

    filtered = await cast(
        Awaitable[ModelInputData],
        prepared.run_config.call_model_input_filter(
            CallModelData(model_data=model_data, agent=other_agent, context=None)
        ),
    )

    assert filtered is model_data


def test_replay_model_and_partial_settings_win_on_copied_run_config() -> None:
    original_settings = ModelSettings(temperature=0.2, max_tokens=100)
    original_config = RunConfig(
        model="caller-model",
        model_settings=original_settings,
        workflow_name="caller-workflow",
    )

    prepared = prepare_replay(
        Agent[None](name="test", model="agent-model"),
        "input",
        original_config,
        _replay(
            override=ReplayOverride(
                model="replay-model",
                model_params={"temperature": 0.8},
            )
        ),
    )

    assert prepared.run_config is not original_config
    assert prepared.run_config.model == "replay-model"
    assert prepared.run_config.workflow_name == "caller-workflow"
    assert prepared.run_config.model_settings == ModelSettings(
        temperature=0.8, max_tokens=100
    )
    assert original_config.model == "caller-model"
    assert original_config.model_settings is original_settings


def test_replay_model_mapping_uses_effective_caller_model() -> None:
    prepared = prepare_replay(
        Agent[None](name="test", model="agent-model"),
        "input",
        {"model": "caller-model"},
        _replay(
            override=ReplayOverride(
                model={"caller-model": "replay-model", "other": "ignored"}
            )
        ),
    )

    assert prepared.run_config.model == "replay-model"


def test_passthrough_policy_preserves_agent_and_tool() -> None:
    calls: list[str] = []
    tool = _function_tool(calls)
    agent = Agent[None](name="test", tools=[tool])

    prepared = prepare_replay(
        agent,
        "input",
        None,
        _replay(tools={"lookup": PassthroughConfig()}),
    )

    assert prepared.starting_agent is agent
    assert prepared.starting_agent.tools[0] is tool


@pytest.mark.parametrize(
    ("case", "arguments"),
    [
        (
            StaticCase(
                match={"city": "Paris", "units": "metric"},
                match_mode=StaticMatchMode.EXACT,
                result={"weather": "sunny"},
            ),
            {"city": "Paris", "units": "metric"},
        ),
        (
            StaticCase(
                match={"city": "Paris"},
                match_mode=StaticMatchMode.SUBSET,
                result={"weather": "sunny"},
            ),
            {"city": "Paris", "units": "metric"},
        ),
        (
            StaticCase(
                match=None,
                match_mode=StaticMatchMode.EXACT,
                result={"weather": "sunny"},
            ),
            {"city": "anywhere"},
        ),
    ],
)
async def test_static_match_returns_replay_result_without_running_original(
    case: StaticCase, arguments: dict[str, Any]
) -> None:
    calls: list[str] = []
    tool = _function_tool(calls)
    agent = Agent[None](name="test", tools=[tool])

    prepared = prepare_replay(
        agent,
        "input",
        None,
        _replay(tools={"lookup": _static([case])}),
    )
    prepared_tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    assert await _invoke(prepared_tool, arguments) == {"weather": "sunny"}
    assert calls == []
    assert prepared.starting_agent is not agent
    assert prepared_tool is not tool
    assert agent.tools == [tool]


@pytest.mark.parametrize(
    ("on_miss", "expected", "raises"),
    [
        (ToolPolicyOnMiss.PASSTHROUGH, "real result", None),
        (
            ToolPolicyOnMiss.ERROR_RESULT,
            {"error": "No static result for tool 'lookup'"},
            None,
        ),
        (ToolPolicyOnMiss.FAIL, None, ToolPolicyMissError),
    ],
)
async def test_static_miss_behavior(
    on_miss: ToolPolicyOnMiss,
    expected: Any,
    raises: type[BaseException] | None,
) -> None:
    calls: list[str] = []
    tool = _function_tool(calls)
    prepared = prepare_replay(
        Agent[None](name="test", tools=[tool]),
        "input",
        None,
        _replay(
            tools={
                "lookup": _static(
                    [
                        StaticCase(
                            match={"city": "Paris"},
                            match_mode=StaticMatchMode.EXACT,
                            result="matched",
                        )
                    ],
                    on_miss=on_miss,
                )
            }
        ),
    )
    prepared_tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    if raises is not None:
        with pytest.raises(raises, match="No static result"):
            await _invoke(prepared_tool, {"city": "Berlin"})
    else:
        assert await _invoke(prepared_tool, {"city": "Berlin"}) == expected
    assert len(calls) == (1 if on_miss is ToolPolicyOnMiss.PASSTHROUGH else 0)


def test_rejects_non_passthrough_default() -> None:
    with pytest.raises(ToolPolicyError, match=r"default.*passthrough"):
        prepare_replay(
            Agent[None](name="test"),
            "input",
            None,
            _replay(default=_static([])),
        )


def test_rejects_unsupported_tool_policy() -> None:
    calls: list[str] = []
    agent = Agent[None](name="test", tools=[_function_tool(calls)])

    with pytest.raises(ToolPolicyError, match="not supported"):
        prepare_replay(
            agent,
            "input",
            None,
            _replay(tools={"lookup": LLMConfig(model="model")}),
        )

    assert calls == []


async def test_history_hit_uses_canonical_arguments_and_skips_live_tool() -> None:
    calls: list[str] = []
    replay = _replay(
        tools={
            "lookup": HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.FAIL,
            )
        }
    )
    client = _FakeClient([_lookup_response({"weather": "sunny"})])
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        replay,
        client=cast(KitaruAPIClient, client),
    )

    result = await _invoke(
        cast(FunctionTool, prepared.starting_agent.tools[0]),
        {"units": "metric", "city": "Paris"},
    )

    assert result == {"weather": "sunny"}
    assert calls == []
    replay_id, request = client.replays.lookups[0]
    assert replay_id == replay.id
    assert request.tool_name == "lookup"
    assert request.occurrence == 0
    assert len(request.cache_key) == 64


async def test_baseline_history_increments_occurrence_only_after_a_hit() -> None:
    calls: list[str] = []
    client = _FakeClient(
        [
            _lookup_response("first"),
            _lookup_response("second"),
            _lookup_response("third"),
            ToolLookupResponse(match=None),
            ToolLookupResponse(match=None),
        ]
    )
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.ERROR_RESULT,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )
    tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    assert await _invoke(tool, {"city": "Paris"}) == "first"
    assert await _invoke(tool, {"city": "Paris"}) == "second"
    assert await _invoke(tool, {"city": "Paris"}) == "third"
    assert await _invoke(tool, {"city": "Paris"}) == {
        "error": "No history result for tool 'lookup'"
    }
    assert await _invoke(tool, {"city": "Paris"}) == {
        "error": "No history result for tool 'lookup'"
    }
    assert [request.occurrence for _, request in client.replays.lookups] == [
        0,
        1,
        2,
        3,
        3,
    ]


@pytest.mark.parametrize(
    ("on_miss", "expected", "raises"),
    [
        (ToolPolicyOnMiss.PASSTHROUGH, "real result", None),
        (
            ToolPolicyOnMiss.ERROR_RESULT,
            {"error": "No history result for tool 'lookup'"},
            None,
        ),
        (ToolPolicyOnMiss.FAIL, None, ToolPolicyMissError),
    ],
)
async def test_history_miss_behavior(
    on_miss: ToolPolicyOnMiss,
    expected: Any,
    raises: type[BaseException] | None,
) -> None:
    calls: list[str] = []
    client = _FakeClient([ToolLookupResponse(match=None)])
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.AGENT,
                    on_miss=on_miss,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )
    tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    if raises is not None:
        with pytest.raises(raises, match="No history result"):
            await _invoke(tool, {"city": "Paris"})
    else:
        assert await _invoke(tool, {"city": "Paris"}) == expected
    assert client.replays.lookups[0][1].occurrence is None
    assert len(calls) == (1 if on_miss is ToolPolicyOnMiss.PASSTHROUGH else 0)


async def test_history_rejects_invalid_arguments_and_lossy_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    client = _FakeClient(
        [_lookup_response({"_kitaru_truncated": {"reason": "max_depth"}})]
    )
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.FAIL,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )
    tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    with pytest.raises(ToolPolicyError, match="Invalid JSON arguments"):
        await tool.on_invoke_tool(cast(Any, None), "{")

    def raise_recursion(_: str) -> Any:
        raise RecursionError

    with monkeypatch.context() as patch:
        patch.setattr(
            "kitaru_openai_agents.replay.parse_tool_arguments",
            raise_recursion,
        )
        with pytest.raises(ToolPolicyError, match="Invalid JSON arguments"):
            await tool.on_invoke_tool(cast(Any, None), "deep JSON")
    with pytest.raises(ToolPolicyError, match="cannot be replayed safely"):
        await _invoke(tool, {"city": "Paris"})
    assert len(client.replays.lookups) == 1
    assert calls == []


@pytest.mark.parametrize(
    "result",
    [
        {"_kitaru_truncated": False, "value": 1},
        {"_kitaru_truncated": {"reason": "application_value"}},
        {"_kitaru_truncated": {"reason": "max_depth", "source": "user"}},
        {"_kitaru_unsupported_type": False},
        {"_kitaru_unsupported_type": "application_value", "value": 1},
    ],
)
async def test_history_accepts_user_fields_named_like_capture_metadata(
    result: Any,
) -> None:
    client = _FakeClient([_lookup_response(result)])
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool([])]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.FAIL,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )

    assert (
        await _invoke(
            cast(FunctionTool, prepared.starting_agent.tools[0]),
            {"city": "Paris"},
        )
        == result
    )


async def test_concurrent_identical_history_calls_use_distinct_occurrences() -> None:
    calls: list[str] = []
    client = _FakeClient(
        [
            _lookup_response("first"),
            _lookup_response("second"),
        ],
        yield_before_response=True,
    )
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.FAIL,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )
    tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    assert await asyncio.gather(
        _invoke(tool, {"city": "Paris"}),
        _invoke(tool, {"city": "Paris"}),
    ) == ["first", "second"]
    assert [request.occurrence for _, request in client.replays.lookups] == [0, 1]


async def test_recorded_tool_input_produces_the_replay_lookup_cache_key() -> None:
    raw_call = ResponseFunctionToolCall(
        arguments='{"units":"metric","city":"Paris"}',
        call_id="call-1",
        name="lookup",
        type="function_call",
        id="item-1",
        status="completed",
    )
    recorded_input, _ = _capture_tool_input(
        ToolCallItem(agent=Agent[None](name="test"), raw_item=raw_call)
    )
    client = _FakeClient([_lookup_response("recorded result")])
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool([])]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.FAIL,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )

    result = await _invoke(
        cast(FunctionTool, prepared.starting_agent.tools[0]),
        {"city": "Paris", "units": "metric"},
    )

    assert result == "recorded result"
    assert client.replays.lookups[0][1].cache_key == compute_tool_cache_key(
        "lookup", recorded_input
    )


async def test_history_replays_null_and_consumes_recorded_failures() -> None:
    calls: list[str] = []
    client = _FakeClient(
        [
            _lookup_response(None),
            _lookup_response(
                status=NodeStatus.FAILED,
                error="recorded failure",
            ),
            _lookup_response("third"),
        ]
    )
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.PASSTHROUGH,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )
    tool = cast(FunctionTool, prepared.starting_agent.tools[0])

    assert await _invoke(tool, {"city": "Paris"}) is None
    with pytest.raises(ToolPolicyError, match="recorded failure"):
        await _invoke(tool, {"city": "Paris"})
    assert await _invoke(tool, {"city": "Paris"}) == "third"
    assert [request.occurrence for _, request in client.replays.lookups] == [0, 1, 2]
    assert calls == []


async def test_history_rejects_legacy_lookup_response() -> None:
    calls: list[str] = []
    client = _FakeClient([ToolLookupResponse()])
    prepared = prepare_replay(
        Agent[None](name="test", tools=[_function_tool(calls)]),
        "input",
        None,
        _replay(
            tools={
                "lookup": HistoryConfig(
                    scope=HistoryScope.AGENT,
                    on_miss=ToolPolicyOnMiss.PASSTHROUGH,
                )
            }
        ),
        client=cast(KitaruAPIClient, client),
    )

    with pytest.raises(ToolPolicyError, match="does not include 'match'"):
        await _invoke(
            cast(FunctionTool, prepared.starting_agent.tools[0]),
            {"city": "Paris"},
        )
    assert calls == []


def test_history_requires_client_and_rejects_tool_timeout() -> None:
    policy = HistoryConfig(
        scope=HistoryScope.BASELINE,
        on_miss=ToolPolicyOnMiss.FAIL,
    )
    calls: list[str] = []
    with pytest.raises(ToolPolicyError, match="requires a Kitaru client"):
        prepare_replay(
            Agent[None](name="test", tools=[_function_tool(calls)]),
            "input",
            None,
            _replay(tools={"lookup": policy}),
        )

    with pytest.raises(ToolPolicyError, match="timeout"):
        prepare_replay(
            Agent[None](
                name="test",
                tools=[_function_tool(calls, timeout_seconds=1.0)],
            ),
            "input",
            None,
            _replay(tools={"lookup": policy}),
            client=cast(KitaruAPIClient, _FakeClient([])),
        )


def test_rejects_unknown_and_duplicate_targets() -> None:
    calls: list[str] = []
    static = _static([])

    with pytest.raises(ToolPolicyError, match="does not match"):
        prepare_replay(
            Agent[None](name="test", tools=[_function_tool(calls)]),
            "input",
            None,
            _replay(tools={"missing": static}),
        )

    duplicate_agent = Agent[None](
        name="test",
        tools=[_function_tool(calls), _function_tool(calls)],
    )
    with pytest.raises(ToolPolicyError, match="more than one"):
        prepare_replay(
            duplicate_agent,
            "input",
            None,
            _replay(tools={"lookup": static}),
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"is_enabled": False}, "disabled"),
        ({"is_enabled": lambda *_: True}, "dynamic"),
        ({"needs_approval": True}, "approval"),
        ({"needs_approval": lambda *_: True}, "approval"),
        ({"defer_loading": True}, "deferred"),
        ({"allowed_callers": ["programmatic"]}, "programmatic"),
    ],
)
def test_rejects_non_ordinary_function_tool_targets(
    kwargs: dict[str, Any], message: str
) -> None:
    calls: list[str] = []
    agent = Agent[None](name="test", tools=[_function_tool(calls, **kwargs)])

    with pytest.raises(ToolPolicyError, match=message):
        prepare_replay(
            agent,
            "input",
            None,
            _replay(tools={"lookup": _static([])}),
        )


def test_rejects_namespaced_mcp_and_agent_as_tool_targets() -> None:
    calls: list[str] = []
    ordinary = _function_tool(calls)
    namespaced = tool_namespace(
        name="weather", description="Weather tools", tools=[ordinary]
    )[0]
    mcp_tool = _function_tool(
        calls,
        _tool_origin=ToolOrigin(type=ToolOriginType.MCP, mcp_server_name="server"),
    )
    agent_tool = Agent[None](name="delegate").as_tool(
        tool_name="lookup", tool_description="Delegate lookup"
    )

    for target, tool, message in (
        ("weather.lookup", namespaced, "namespaced"),
        ("lookup", mcp_tool, "MCP"),
        ("lookup", agent_tool, "agent-as-tool"),
    ):
        with pytest.raises(ToolPolicyError, match=message):
            prepare_replay(
                Agent[None](name="test", tools=[tool]),
                "input",
                None,
                _replay(tools={target: _static([])}),
            )


def test_rejects_non_function_programmatic_and_handoff_only_targets() -> None:
    programmatic = ProgrammaticToolCallingTool()
    with pytest.raises(ToolPolicyError, match="FunctionTool"):
        prepare_replay(
            Agent[None](name="test", tools=[programmatic]),
            "input",
            None,
            _replay(tools={"programmatic_tool_calling": _static([])}),
        )

    handoff_agent = Agent[None](
        name="test",
        handoffs=[Agent[None](name="delegate")],
    )
    with pytest.raises(ToolPolicyError, match="does not match"):
        prepare_replay(
            handoff_agent,
            "input",
            None,
            _replay(tools={"transfer_to_delegate": _static([])}),
        )


async def test_concurrent_preparations_keep_static_policies_isolated() -> None:
    calls: list[str] = []
    shared_tool = _function_tool(calls)
    shared_agent = Agent[None](name="test", tools=[shared_tool])

    def prepared(result: str) -> FunctionTool:
        replay = _replay(
            tools={
                "lookup": _static(
                    [
                        StaticCase(
                            match=None,
                            match_mode=StaticMatchMode.EXACT,
                            result=result,
                        )
                    ]
                )
            }
        )
        value = prepare_replay(shared_agent, "input", None, replay)
        return cast(FunctionTool, value.starting_agent.tools[0])

    first, second = await asyncio.gather(
        _invoke(prepared("first"), {}),
        _invoke(prepared("second"), {}),
    )

    assert (first, second) == ("first", "second")
    assert calls == []
    assert shared_agent.tools == [shared_tool]
