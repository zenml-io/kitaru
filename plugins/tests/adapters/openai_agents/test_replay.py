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
from agents.run_config import CallModelData, ModelInputData

from kitaru.api_models.v1.replay import ReplayResponse, ReplayStatus
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


@pytest.mark.parametrize(
    "policy",
    [
        HistoryConfig(scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL),
        LLMConfig(model="model"),
    ],
    ids=["history", "llm"],
)
def test_rejects_unsupported_tool_policy(policy: Any) -> None:
    calls: list[str] = []
    agent = Agent[None](name="test", tools=[_function_tool(calls)])

    with pytest.raises(ToolPolicyError, match="not supported"):
        prepare_replay(
            agent,
            "input",
            None,
            _replay(tools={"lookup": policy}),
        )

    assert calls == []


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
