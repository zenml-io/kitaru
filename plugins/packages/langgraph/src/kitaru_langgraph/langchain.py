#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Construction-time LangChain middleware for replay overrides."""

from collections.abc import Awaitable, Callable
from typing import Any, cast

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest, ToolCallRequest
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.types import Command

from kitaru.api_models.v1.replay import ToolLookupRequest
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    LLMConfig,
    PassthroughConfig,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolConfig,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru.cache_keys import compute_tool_cache_key

from .capability import ToolPolicyError, ToolPolicyMissError
from .capture import capture_value
from .codec import coerce_static_tool_result, decode_tool_outcome
from .recording import InvocationRecorder, get_active_invocation


def _case_matches(case: StaticCase, arguments: dict[str, Any]) -> bool:
    if case.match is None:
        return True
    if case.match_mode is StaticMatchMode.EXACT:
        return arguments == case.match
    if not isinstance(case.match, dict):
        return False
    return all(
        key in arguments and arguments[key] == value
        for key, value in case.match.items()
    )


def _tool_identity(request: ToolCallRequest) -> tuple[str, str, dict[str, Any]]:
    call = request.tool_call
    name = str(call["name"])
    call_id = str(call["id"])
    arguments = cast(dict[str, Any], call.get("args") or {})
    return name, call_id, arguments


def _tool_policy(recorder: InvocationRecorder, name: str) -> ToolConfig | None:
    if recorder.replay is None:
        return None
    policy = recorder.replay.tool_policy
    return policy.tools.get(name, policy.default)


def _replace_prompt(request: ModelRequest[Any], prompt: str) -> ModelRequest[Any]:
    messages = list(request.messages)
    for index in range(len(messages) - 1, -1, -1):
        if isinstance(messages[index], HumanMessage):
            messages[index] = messages[index].model_copy(update={"content": prompt})
            return request.override(messages=messages)
    messages.append(HumanMessage(content=prompt))
    return request.override(messages=messages)


class KitaruLangGraphMiddleware(AgentMiddleware[Any, Any]):
    """Apply Kitaru replay settings through public LangChain middleware hooks."""

    def __init__(self, *, requested_model: str | None) -> None:
        self._requested_model = requested_model

    @property
    def requested_model(self) -> str | None:
        """Return the factory's stable model identifier when one was supplied."""
        return self._requested_model

    def _model_request(self, request: ModelRequest[Any]) -> ModelRequest[Any]:
        recorder = get_active_invocation()
        if recorder is None or recorder.override is None:
            return request
        override = recorder.override
        effective = request
        replacement: str | None = None
        if isinstance(override.model, str):
            replacement = override.model
        elif isinstance(override.model, dict) and self._requested_model is not None:
            replacement = override.model.get(self._requested_model)
        if replacement is not None:
            effective = effective.override(model=init_chat_model(replacement))
        if override.prompt is not None:
            effective = _replace_prompt(effective, override.prompt)
        if override.system_prompt is not None:
            effective = effective.override(system_prompt=override.system_prompt)
        if override.model_params is not None:
            effective = effective.override(model_settings=dict(override.model_params))
        return effective

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Any],
    ) -> Any:
        """Apply overrides and call the effective live model exactly once."""
        return handler(self._model_request(request))

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[Any]],
    ) -> Any:
        """Apply overrides and await the effective live model exactly once."""
        return await handler(self._model_request(request))

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        """Apply one sync tool policy without duplicate live execution."""
        recorder = get_active_invocation()
        if recorder is None:
            return handler(request)
        bridge = recorder.sync_bridge
        if bridge is None:
            raise RuntimeError("Synchronous LangGraph recording bridge is unavailable")
        name, call_id, arguments = _tool_identity(request)
        policy = _tool_policy(recorder, name)
        if policy is None or isinstance(policy, PassthroughConfig):
            return handler(request)
        if isinstance(policy, LLMConfig):
            error = ToolPolicyError("Tool policy 'llm' is not supported")
            bridge.run(
                recorder.record_tool_substitution(
                    tool_call_id=call_id,
                    tool_name=name,
                    arguments=arguments,
                    result=None,
                    policy_name=policy.type,
                    error=error,
                )
            )
            raise error
        result: ToolMessage | Command[Any] | None = None
        if isinstance(policy, StaticConfig):
            case = next(
                (item for item in policy.cases if _case_matches(item, arguments)), None
            )
            if case is not None:
                result = coerce_static_tool_result(
                    case.result, tool_call_id=call_id, tool_name=name
                )
        elif isinstance(policy, HistoryConfig):
            try:
                result = bridge.run(
                    self._history_result(recorder, policy, name, call_id, arguments)
                )
            except ToolPolicyError as error:
                bridge.run(
                    recorder.record_tool_substitution(
                        tool_call_id=call_id,
                        tool_name=name,
                        arguments=arguments,
                        result=None,
                        policy_name=policy.type,
                        error=error,
                    )
                )
                raise
        if result is None:
            if policy.on_miss is ToolPolicyOnMiss.PASSTHROUGH:
                return handler(request)
            message = f"No {policy.type} result for tool '{name}'"
            if policy.on_miss is ToolPolicyOnMiss.ERROR_RESULT:
                result = ToolMessage(
                    content=message,
                    name=name,
                    tool_call_id=call_id,
                    status="error",
                )
                bridge.run(
                    recorder.record_tool_substitution(
                        tool_call_id=call_id,
                        tool_name=name,
                        arguments=arguments,
                        result=result,
                        policy_name=policy.type,
                    )
                )
                return result
            error = ToolPolicyMissError(message)
            bridge.run(
                recorder.record_tool_substitution(
                    tool_call_id=call_id,
                    tool_name=name,
                    arguments=arguments,
                    result=None,
                    policy_name=policy.type,
                    error=error,
                )
            )
            raise error
        bridge.run(
            recorder.record_tool_substitution(
                tool_call_id=call_id,
                tool_name=name,
                arguments=arguments,
                result=result,
                policy_name=policy.type,
            )
        )
        return result

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        """Apply one async tool policy without duplicate live execution."""
        recorder = get_active_invocation()
        if recorder is None:
            return await handler(request)
        return await self._apply_tool_async(recorder, request, handler)

    async def _apply_tool_async(
        self,
        recorder: InvocationRecorder,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        name, call_id, arguments = _tool_identity(request)
        policy = _tool_policy(recorder, name)
        if policy is None or isinstance(policy, PassthroughConfig):
            return await handler(request)
        if isinstance(policy, LLMConfig):
            error = ToolPolicyError("Tool policy 'llm' is not supported")
            await recorder.record_tool_substitution(
                tool_call_id=call_id,
                tool_name=name,
                arguments=arguments,
                result=None,
                policy_name=policy.type,
                error=error,
            )
            raise error
        result: ToolMessage | Command[Any] | None = None
        if isinstance(policy, StaticConfig):
            case = next(
                (item for item in policy.cases if _case_matches(item, arguments)), None
            )
            if case is not None:
                result = coerce_static_tool_result(
                    case.result, tool_call_id=call_id, tool_name=name
                )
        elif isinstance(policy, HistoryConfig):
            try:
                result = await self._history_result(
                    recorder, policy, name, call_id, arguments
                )
            except ToolPolicyError as error:
                await recorder.record_tool_substitution(
                    tool_call_id=call_id,
                    tool_name=name,
                    arguments=arguments,
                    result=None,
                    policy_name=policy.type,
                    error=error,
                )
                raise
        if result is None:
            return await self._handle_miss(
                recorder, request, handler, policy.type, policy.on_miss
            )
        await recorder.record_tool_substitution(
            tool_call_id=call_id,
            tool_name=name,
            arguments=arguments,
            result=result,
            policy_name=policy.type,
        )
        return result

    async def _handle_miss(
        self,
        recorder: InvocationRecorder,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
        policy_name: str,
        on_miss: ToolPolicyOnMiss,
    ) -> ToolMessage | Command[Any]:
        name, call_id, arguments = _tool_identity(request)
        if on_miss is ToolPolicyOnMiss.PASSTHROUGH:
            return await handler(request)
        message = f"No {policy_name} result for tool '{name}'"
        if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            result = ToolMessage(
                content=message,
                name=name,
                tool_call_id=call_id,
                status="error",
            )
            await recorder.record_tool_substitution(
                tool_call_id=call_id,
                tool_name=name,
                arguments=arguments,
                result=result,
                policy_name=policy_name,
            )
            return result
        error = ToolPolicyMissError(message)
        await recorder.record_tool_substitution(
            tool_call_id=call_id,
            tool_name=name,
            arguments=arguments,
            result=None,
            policy_name=policy_name,
            error=error,
        )
        raise error

    async def _history_result(
        self,
        recorder: InvocationRecorder,
        policy: HistoryConfig,
        name: str,
        call_id: str,
        arguments: dict[str, Any],
    ) -> ToolMessage | Command[Any] | None:
        """Return one valid history result without calling a live tool."""
        captured = capture_value(arguments, recorder.policy)
        if not captured.replayable:
            return None
        cache_key = compute_tool_cache_key(name, arguments)
        if cache_key is None or recorder.replay is None:
            return None
        occurrence = (
            recorder.history_occurrences.get(cache_key, 0)
            if policy.scope is HistoryScope.BASELINE
            else None
        )
        lookup = await recorder.client.replays.tool_lookup(
            recorder.replay.id,
            ToolLookupRequest(
                tool_name=name, cache_key=cache_key, occurrence=occurrence
            ),
        )
        match = lookup.match
        if match is None:
            return None
        if occurrence is not None:
            recorder.history_occurrences[cache_key] = occurrence + 1
        if match.status is NodeStatus.COMPLETED:
            return decode_tool_outcome(
                match.result,
                tool_call_id=call_id,
                tool_name=name,
            )
        if match.status is NodeStatus.FAILED:
            raise ToolPolicyError(match.error or f"Recorded tool call '{name}' failed")
        raise ToolPolicyError(
            f"History lookup for tool '{name}' returned unexpected status "
            f"'{match.status.value}'"
        )
