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
"""Pure OpenAI Agents replay preflight and per-run transformation."""

import asyncio
import copy
import inspect
from collections.abc import Awaitable
from dataclasses import dataclass, field, replace
from typing import Any, Generic, TypeVar, cast

from agents import (
    Agent,
    FunctionTool,
    ModelSettings,
    RunConfig,
    ToolOriginType,
    TResponseInputItem,
)
from agents.run_config import CallModelData, ModelInputData
from agents.tool import Tool, get_function_tool_origin

from kitaru.api_models.v1.replay import ReplayResponse, ToolLookupRequest
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    PassthroughConfig,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicyOnMiss,
)
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.client import KitaruAPIClient

from .inputs import (
    contains_capture_marker,
    normalize_openai_input,
    parse_tool_arguments,
)

TContext = TypeVar("TContext")


class ToolPolicyError(RuntimeError):
    """Raised when a replay tool policy cannot be applied safely."""


class ToolPolicyMissError(ToolPolicyError):
    """Raised when a static replay has no match and must fail."""


@dataclass(frozen=True)
class PreparedReplay(Generic[TContext]):
    """Copied OpenAI run values produced after complete replay preflight."""

    starting_agent: Agent[TContext]
    input: str | list[TResponseInputItem]
    run_config: RunConfig


@dataclass
class _HistoryState:
    """Track baseline occurrences for one prepared replay run."""

    occurrences: dict[str, int] = field(default_factory=dict)
    locks: dict[str, asyncio.Lock] = field(default_factory=dict)


def _copy_run_config(value: RunConfig | dict[str, Any] | None) -> RunConfig:
    """Normalize the caller config into an independently mutable run config."""
    if value is None:
        return RunConfig()
    if isinstance(value, RunConfig):
        return copy.copy(value)
    return RunConfig(**dict(value))


def _tool_matches_target(tool: Tool, target: str) -> bool:
    """Check a policy target against one direct starting-agent tool."""
    if isinstance(tool, FunctionTool):
        return target in (tool.name, tool.qualified_name)
    return getattr(tool, "name", None) == target


def _validate_function_tool(tool: FunctionTool, target: str) -> None:
    """Reject a target that is not an ordinary direct function tool."""
    if tool.qualified_name != tool.name:
        raise ToolPolicyError(f"Tool override '{target}' targets a namespaced tool")
    if callable(tool.is_enabled):
        raise ToolPolicyError(
            f"Tool override '{target}' has dynamic is_enabled behavior"
        )
    if tool.is_enabled is not True:
        raise ToolPolicyError(f"Tool override '{target}' targets a disabled tool")
    if tool.needs_approval is not False:
        raise ToolPolicyError(
            f"Tool override '{target}' targets an approval-bearing tool"
        )
    if tool.defer_loading:
        raise ToolPolicyError(f"Tool override '{target}' targets a deferred tool")
    if tool.allowed_callers is not None or tool.output_json_schema is not None:
        raise ToolPolicyError(f"Tool override '{target}' targets a programmatic tool")

    origin = get_function_tool_origin(tool)
    if origin is None or origin.type is not ToolOriginType.FUNCTION:
        if origin is not None and origin.type is ToolOriginType.MCP:
            kind = "MCP"
        elif origin is not None and origin.type is ToolOriginType.AGENT_AS_TOOL:
            kind = "agent-as-tool"
        else:
            kind = "non-ordinary"
        raise ToolPolicyError(f"Tool override '{target}' targets a {kind} tool")


def _preflight_tools(
    starting_agent: Agent[Any], replay: ReplayResponse
) -> list[tuple[int, FunctionTool, StaticConfig | HistoryConfig]]:
    """Resolve every named supported override before copying any tool."""
    policy = replay.tool_policy
    if not isinstance(policy.default, PassthroughConfig):
        raise ToolPolicyError("Replay tool policy default must be passthrough")

    targets: list[tuple[int, FunctionTool, StaticConfig | HistoryConfig]] = []
    for target, override in policy.tools.items():
        if not isinstance(override, PassthroughConfig | StaticConfig | HistoryConfig):
            raise ToolPolicyError(
                f"Tool policy '{override.type}' is not supported for '{target}'"
            )
        matches = [
            (index, tool)
            for index, tool in enumerate(starting_agent.tools)
            if _tool_matches_target(tool, target)
        ]
        if not matches:
            raise ToolPolicyError(
                f"Tool override '{target}' does not match a direct starting-agent tool"
            )
        if len(matches) > 1:
            raise ToolPolicyError(
                f"Tool override '{target}' matches more than one direct tool"
            )

        index, tool = matches[0]
        if not isinstance(tool, FunctionTool):
            raise ToolPolicyError(
                f"Tool override '{target}' must target a direct FunctionTool"
            )
        _validate_function_tool(tool, target)
        if isinstance(override, HistoryConfig) and tool.timeout_seconds is not None:
            raise ToolPolicyError(
                f"History override '{target}' cannot target a tool with a timeout"
            )
        if isinstance(override, StaticConfig | HistoryConfig):
            targets.append((index, tool, override))
    return targets


def _case_matches(case: StaticCase, arguments: Any) -> bool:
    """Check exact or shallow-subset matching against parsed call arguments."""
    if case.match is None:
        return True
    if case.match_mode is StaticMatchMode.EXACT:
        return arguments == case.match
    if not isinstance(case.match, dict) or not isinstance(arguments, dict):
        return False
    return all(
        name in arguments and arguments[name] == value
        for name, value in case.match.items()
    )


def _copy_static_tool(tool: FunctionTool, policy: StaticConfig) -> FunctionTool:
    """Copy one function tool and replace only its per-run invocation callback."""
    copied_tool = copy.copy(tool)
    original_invoke = copied_tool.on_invoke_tool

    async def invoke(context: Any, arguments_json: str) -> Any:
        try:
            arguments = parse_tool_arguments(arguments_json)
        except (ValueError, TypeError) as error:
            raise ToolPolicyError(
                f"Invalid JSON arguments for tool '{tool.name}'"
            ) from error

        matching = next(
            (case for case in policy.cases if _case_matches(case, arguments)),
            None,
        )
        if matching is not None:
            return matching.result
        if policy.on_miss is ToolPolicyOnMiss.PASSTHROUGH:
            return await original_invoke(context, arguments_json)

        message = f"No static result for tool '{tool.name}'"
        if policy.on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            return {"error": message}
        raise ToolPolicyMissError(message)

    copied_tool.on_invoke_tool = invoke
    return copied_tool


def _copy_history_tool(
    tool: FunctionTool,
    policy: HistoryConfig,
    replay: ReplayResponse,
    client: KitaruAPIClient,
    state: _HistoryState,
) -> FunctionTool:
    """Copy one function tool and resolve calls from Kitaru history."""
    copied_tool = copy.copy(tool)
    original_invoke = copied_tool.on_invoke_tool

    async def lookup(arguments: Any) -> tuple[bool, Any]:
        cache_key = compute_tool_cache_key(tool.name, arguments)
        if cache_key is None:
            raise ToolPolicyError(
                f"Arguments for tool '{tool.name}' cannot be replayed safely"
            )
        occurrence = None
        if policy.scope is HistoryScope.BASELINE:
            lock = state.locks.setdefault(cache_key, asyncio.Lock())
            async with lock:
                occurrence = state.occurrences.get(cache_key, 0)
                response = await client.replays.tool_lookup(
                    replay.id,
                    ToolLookupRequest(
                        tool_name=tool.name,
                        cache_key=cache_key,
                        occurrence=occurrence,
                    ),
                )
                if response.found:
                    state.occurrences[cache_key] = occurrence + 1
                return response.found, response.result
        response = await client.replays.tool_lookup(
            replay.id,
            ToolLookupRequest(
                tool_name=tool.name,
                cache_key=cache_key,
                occurrence=None,
            ),
        )
        return response.found, response.result

    async def invoke(context: Any, arguments_json: str) -> Any:
        try:
            arguments = parse_tool_arguments(arguments_json)
        except (ValueError, TypeError) as error:
            raise ToolPolicyError(
                f"Invalid JSON arguments for tool '{tool.name}'"
            ) from error

        found, result = await lookup(arguments)
        if found:
            if result is None or contains_capture_marker(result):
                raise ToolPolicyError(
                    f"History result for tool '{tool.name}' cannot be replayed safely"
                )
            return result
        if policy.on_miss is ToolPolicyOnMiss.PASSTHROUGH:
            return await original_invoke(context, arguments_json)

        message = f"No history result for tool '{tool.name}'"
        if policy.on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            return {"error": message}
        raise ToolPolicyMissError(message)

    copied_tool.on_invoke_tool = invoke
    return copied_tool


def _resolve_model(
    override: str | dict[str, str] | None,
    run_config: RunConfig,
    starting_agent: Agent[Any],
) -> str | None:
    """Resolve a direct or old-to-new replay model override."""
    if isinstance(override, str):
        return override
    if not isinstance(override, dict):
        return None
    current = run_config.model if run_config.model is not None else starting_agent.model
    if not isinstance(current, str):
        return None
    return override.get(current)


def _compose_instruction_filter(
    run_config: RunConfig,
    starting_agent: Agent[Any],
    instructions: str,
) -> None:
    """Run the caller filter first, then replace starting-agent instructions."""
    caller_filter = run_config.call_model_input_filter

    async def filter_model_input(data: CallModelData[Any]) -> ModelInputData:
        model_data = data.model_data
        if caller_filter is not None:
            filtered = caller_filter(data)
            if inspect.isawaitable(filtered):
                model_data = await cast(Awaitable[ModelInputData], filtered)
            else:
                model_data = filtered
        if data.agent is not starting_agent:
            return model_data
        return replace(model_data, instructions=instructions)

    run_config.call_model_input_filter = filter_model_input


def prepare_replay(
    starting_agent: Agent[TContext],
    input: Any,
    run_config: RunConfig | dict[str, Any] | None,
    replay: ReplayResponse,
    *,
    client: KitaruAPIClient | None = None,
) -> PreparedReplay[TContext]:
    """Preflight and apply supported replay changes without mutating caller values.

    Args:
        starting_agent: Caller-owned starting agent.
        input: Native OpenAI input or arbitrary Kitaru JSON input.
        run_config: Caller-owned OpenAI run configuration.
        replay: Kitaru replay and its resolved override policy.

    Raises:
        ToolPolicyError: If any tool policy or target is unsupported.
        TypeError: If the run configuration or model settings are invalid.

    Returns:
        Prepared per-run agent, input, and configuration.
    """
    tool_targets = _preflight_tools(starting_agent, replay)
    if (
        any(isinstance(policy, HistoryConfig) for _, _, policy in tool_targets)
        and client is None
    ):
        raise ToolPolicyError("History replay requires a Kitaru client")
    override = replay.override
    effective_input = normalize_openai_input(
        override.prompt
        if override is not None and override.prompt is not None
        else input
    )
    prepared_config = _copy_run_config(run_config)

    agent_changes: dict[str, Any] = {}
    if tool_targets:
        history_state = _HistoryState()
        tools = list(starting_agent.tools)
        for index, tool, policy in tool_targets:
            if isinstance(policy, StaticConfig):
                tools[index] = _copy_static_tool(tool, policy)
            else:
                assert client is not None
                tools[index] = _copy_history_tool(
                    tool, policy, replay, client, history_state
                )
        agent_changes["tools"] = tools
    if override is not None and override.system_prompt is not None:
        agent_changes["instructions"] = override.system_prompt
    prepared_agent = (
        starting_agent.clone(**agent_changes) if agent_changes else starting_agent
    )

    if override is not None:
        replacement_model = _resolve_model(
            override.model, prepared_config, starting_agent
        )
        if replacement_model is not None:
            prepared_config.model = replacement_model
        if override.model_params is not None:
            base_settings = prepared_config.model_settings or ModelSettings()
            prepared_config.model_settings = base_settings.resolve(
                override.model_params
            )
        if override.system_prompt is not None:
            _compose_instruction_filter(
                prepared_config, prepared_agent, override.system_prompt
            )

    return PreparedReplay(
        starting_agent=prepared_agent,
        input=effective_input,
        run_config=prepared_config,
    )
