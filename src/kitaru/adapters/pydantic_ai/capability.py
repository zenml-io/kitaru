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
"""PydanticAI capability implementing Kitaru recording and replay."""

import asyncio
import json
import os
import uuid
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, cast

from pydantic import TypeAdapter

from kitaru.api_models.v1.replay import ReplayResponse, ToolLookupRequest
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    LLMConfig,
    PassthroughConfig,
    ReplayOverride,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolConfig,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
    TokenUsage,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.task import AgentTaskDetails
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.client import KitaruAPIClient
from pydantic_ai import UserPromptNode
from pydantic_ai.capabilities import (
    AbstractCapability,
    CapabilityOrdering,
    WrapModelRequestHandler,
    WrapRunHandler,
    WrapToolExecuteHandler,
)
from pydantic_ai.messages import (
    AudioUrl,
    BinaryContent,
    CachePoint,
    DocumentUrl,
    ImageUrl,
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelRequestPart,
    ModelResponse,
    NativeToolCallPart,
    NativeToolReturnPart,
    SystemPromptPart,
    TextContent,
    TextPart,
    ToolCallPart,
    UploadedFile,
    UserContent,
    UserPromptPart,
    VideoUrl,
)
from pydantic_ai.models import ModelRequestContext, infer_model
from pydantic_ai.run import AgentRunResult
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import RunContext, ToolDefinition

ADAPTER_VERSION = "0.1.0"
FRAMEWORK = "pydantic_ai"
_JSON_ADAPTER = TypeAdapter(Any)
_USER_CONTENT_TYPES = (
    str,
    TextContent,
    ImageUrl,
    AudioUrl,
    DocumentUrl,
    VideoUrl,
    BinaryContent,
    UploadedFile,
    CachePoint,
)


class ToolPolicyError(RuntimeError):
    """Raised when a replay tool policy cannot be applied."""


class ToolPolicyMissError(ToolPolicyError):
    """Raised when a replay tool lookup misses with fail behavior."""


def _jsonable(value: Any) -> Any:
    """Convert framework and user values to JSON-compatible data."""
    return _JSON_ADAPTER.dump_python(
        value, mode="json", warnings=False, fallback=str, serialize_as_any=True
    )


def _pydantic_prompt(value: Any) -> str | Sequence[UserContent] | None:
    """Convert arbitrary Kitaru JSON to a valid PydanticAI user prompt."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, Sequence) and all(
        isinstance(item, _USER_CONTENT_TYPES) for item in value
    ):
        return value
    return json.dumps(_jsonable(value), sort_keys=True)


def _message_content(value: Any, role: str) -> Any:
    """Extract one role's content from common provider message shapes."""
    if not isinstance(value, dict):
        return value
    messages = value.get("messages")
    if isinstance(messages, list):
        for message in reversed(messages):
            if (
                isinstance(message, dict)
                and message.get("role") == role
                and "content" in message
            ):
                return cast(dict[str, Any], message).get("content")
    if value.get("role") in (None, role) and "content" in value:
        return value["content"]
    return value


def _assistant_text(value: Any) -> str:
    """Convert a recorded assistant output into a PydanticAI text part."""
    content = _message_content(value, "assistant")
    if isinstance(content, str):
        return content
    return json.dumps(_jsonable(content), sort_keys=True)


def _project_conversation_input(
    value: Any,
) -> tuple[Any, list[ModelMessage]]:
    """Project an imported session onto final prompt plus prior history."""
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        return value, []
    turns = value.get("turns")
    if not isinstance(turns, list) or not turns:
        return value, []

    history: list[ModelMessage] = []
    for raw_turn in turns[:-1]:
        if not isinstance(raw_turn, dict):
            continue
        prior_input = _message_content(raw_turn.get("inputs"), "user")
        if prior_input is not None:
            prompt = _pydantic_prompt(prior_input)
            if prompt is not None:
                history.append(ModelRequest(parts=[UserPromptPart(prompt)]))
        prior_output = raw_turn.get("outputs")
        if prior_output is not None:
            history.append(
                ModelResponse(parts=[TextPart(_assistant_text(prior_output))])
            )
    final_turn = turns[-1]
    if not isinstance(final_turn, dict):
        return value, history
    return _message_content(final_turn.get("inputs"), "user"), history


def _prepend_history(
    messages: list[ModelMessage], history: list[ModelMessage]
) -> list[ModelMessage]:
    """Insert replay history after the original system prompt."""
    if not history:
        return messages
    result = list(messages)
    for index, message in enumerate(result):
        if not isinstance(message, ModelRequest):
            continue
        system_parts = [
            part for part in message.parts if isinstance(part, SystemPromptPart)
        ]
        if not system_parts and message.instructions is None:
            return [*history, *result]
        remaining_parts = [
            part for part in message.parts if not isinstance(part, SystemPromptPart)
        ]
        result[index] = replace(
            message,
            parts=remaining_parts,
            instructions=None,
        )
        prefix = ModelRequest(
            parts=system_parts,
            instructions=message.instructions,
        )
        return [prefix, *history, *result]
    return [*history, *result]


def _messages_json(messages: list[ModelMessage]) -> list[dict[str, Any]]:
    """Serialize PydanticAI messages with its public type adapter."""
    return ModelMessagesTypeAdapter.dump_python(messages, mode="json", fallback=str)


def _error_text(error: BaseException) -> str:
    """Return a useful message for an exception, including empty exceptions."""
    return str(error) or type(error).__name__


def _case_matches(case: StaticCase, arguments: dict[str, Any]) -> bool:
    """Check whether a static replay case matches validated tool arguments."""
    if case.match is None:
        return True
    if case.match_mode is StaticMatchMode.EXACT:
        return arguments == case.match
    return all(
        name in arguments and arguments[name] == value
        for name, value in case.match.items()
    )


def _model_identifier(request_context: ModelRequestContext) -> str:
    """Return the best public identifier for a requested model."""
    return request_context.model_id or request_context.model.model_id


def _replace_request_context(
    request_context: ModelRequestContext,
    *,
    model: Any,
    messages: list[ModelMessage],
    model_settings: ModelSettings | None,
    model_replaced: bool,
) -> ModelRequestContext:
    """Copy a request context while preserving its read-only run flags."""
    updated = replace(
        request_context,
        model=model,
        messages=messages,
        model_settings=model_settings,
    )
    updated.model_id = None if model_replaced else request_context.model_id
    updated.streaming = request_context.streaming
    return updated


def _replace_system_prompt(
    messages: list[ModelMessage], system_prompt: str
) -> list[ModelMessage]:
    """Replace all materialized system prompts and instructions with one prompt."""
    result: list[ModelMessage] = []
    first_request = True
    for message in messages:
        if not isinstance(message, ModelRequest):
            result.append(message)
            continue
        parts: list[ModelRequestPart] = [
            part for part in message.parts if not isinstance(part, SystemPromptPart)
        ]
        if first_request:
            parts.insert(0, SystemPromptPart(system_prompt))
            first_request = False
        result.append(replace(message, parts=parts, instructions=None))
    return result


def _token_usage(response: ModelResponse) -> TokenUsage:
    """Translate PydanticAI request usage into Kitaru's token fields."""
    usage = response.usage
    reasoning = usage.details.get("reasoning_tokens") if usage.details else None
    return TokenUsage(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cache_read_tokens,
        reasoning_tokens=reasoning if isinstance(reasoning, int) else None,
    )


def _unpaired_native_calls(response: ModelResponse) -> list[dict[str, Any]]:
    """Serialize native calls whose provider result is not publicly exposed."""
    returned_ids = {
        part.tool_call_id
        for part in response.parts
        if isinstance(part, NativeToolReturnPart)
    }
    return [
        {
            "external_id": part.tool_call_id,
            "tool_name": part.tool_name,
            "inputs": _jsonable(part.args_as_dict()),
            "provider": part.provider_name,
            "provider_details": _jsonable(part.provider_details),
        }
        for part in response.parts
        if isinstance(part, NativeToolCallPart)
        and part.tool_call_id not in returned_ids
    ]


@dataclass
class _RunState:
    """Mutable state isolated to one PydanticAI run."""

    client: KitaruAPIClient
    task_id: uuid.UUID | None
    replay: ReplayResponse | None
    override: ReplayOverride | None
    effective_input: Any
    prompt_input: Any
    message_history: list[ModelMessage]
    session_id: uuid.UUID | None = None
    started_at: datetime | None = None
    next_index: int = 1
    latest_llm_index: int | None = None
    buffer: list[SessionNodeCreateRequest] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    finished: bool = False
    closed: bool = False


@dataclass
class _KitaruCapability(AbstractCapability[Any]):
    """Run-local PydanticAI hooks for Kitaru recording and replay."""

    agent_id: uuid.UUID | None
    agent_version_id: uuid.UUID | None
    api_url: str
    api_key: str | None
    session_name: str | None
    batch_size: int
    _state: _RunState | None = field(default=None, repr=False)

    @classmethod
    def get_serialization_name(cls) -> None:
        """Exclude this runtime capability from PydanticAI agent specs."""
        return None

    def get_ordering(self) -> CapabilityOrdering:
        """Record outside other capabilities so their final behavior is observed."""
        return CapabilityOrdering(position="outermost")

    async def for_run(self, ctx: RunContext[Any]) -> "_KitaruCapability":
        """Create isolated client and replay state for one run."""
        client = KitaruAPIClient(base_url=self.api_url, api_key=self.api_key)
        try:
            task_value = os.environ.get("KITARU_TASK_ID")
            task_id = uuid.UUID(task_value) if task_value else None
            replay_value = os.environ.get("KITARU_REPLAY_ID")
            replay_id = uuid.UUID(replay_value) if replay_value else None
            replay = await client.replays.get(replay_id) if replay_id else None
            raw_inputs = os.environ.get("KITARU_TASK_INPUTS")
            if raw_inputs is not None:
                effective_input = json.loads(raw_inputs)
            elif task_id is not None:
                spec = await client.tasks.get_spec(task_id)
                if not isinstance(spec.details, AgentTaskDetails):
                    raise RuntimeError(f"Task {task_id} is not an agent task")
                effective_input = spec.details.inputs
            else:
                effective_input = ctx.prompt
            prompt_input, message_history = _project_conversation_input(effective_input)
            override = replay.override if replay is not None else None
        except BaseException:
            await client.close()
            raise
        return replace(
            self,
            _state=_RunState(
                client=client,
                task_id=task_id,
                replay=replay,
                override=override,
                effective_input=effective_input,
                prompt_input=prompt_input,
                message_history=message_history,
            ),
        )

    async def wrap_run(
        self, ctx: RunContext[Any], *, handler: WrapRunHandler
    ) -> AgentRunResult[Any]:
        """Create the Kitaru session before executing the agent."""
        state = self._require_state()
        started_at = datetime.now(UTC)
        identity: dict[str, Any] = {}
        if self.agent_id is not None:
            identity["agent_id"] = self.agent_id
        if self.agent_version_id is not None:
            identity["agent_version_id"] = self.agent_version_id
        try:
            session = await state.client.sessions.create(
                SessionCreateRequest(
                    **identity,
                    origin=(
                        SessionOrigin.REPLAY
                        if state.replay is not None
                        else SessionOrigin.RECORDED
                    ),
                    name=self.session_name,
                    inputs=_jsonable(state.effective_input),
                    outputs=None,
                    expected=None,
                    started_at=started_at,
                    framework=FRAMEWORK,
                    adapter_version=ADAPTER_VERSION,
                    task_id=state.task_id,
                )
            )
            state.session_id = session.id
            state.started_at = started_at
            await state.client.sessions.ingest_nodes(
                session.id,
                SessionNodeBatchRequest(
                    nodes=[
                        SessionNodeCreateRequest(
                            index=0,
                            parent_index=None,
                            node_type=NodeType.SPAN,
                            name="run",
                            status=NodeStatus.IN_PROGRESS,
                            started_at=started_at,
                            inputs=_jsonable(state.effective_input),
                            outputs=None,
                            attributes={},
                        )
                    ]
                ),
            )
        except BaseException as error:
            if state.session_id is not None:
                with suppress(BaseException):
                    await state.client.sessions.update(
                        state.session_id,
                        SessionUpdateRequest(
                            status=SessionStatus.FAILED,
                            error=_error_text(error),
                            ended_at=datetime.now(UTC),
                        ),
                    )
            with suppress(BaseException):
                await self._close()
            raise
        return await handler()

    async def after_run(
        self, ctx: RunContext[Any], *, result: AgentRunResult[Any]
    ) -> AgentRunResult[Any]:
        """Complete the Kitaru session with the final PydanticAI output."""
        try:
            await self._finish(
                node_status=NodeStatus.COMPLETED,
                session_status=SessionStatus.COMPLETED,
                outputs=result.output,
                error=None,
            )
        finally:
            await self._close()
        return result

    async def on_run_error(
        self, ctx: RunContext[Any], *, error: BaseException
    ) -> AgentRunResult[Any]:
        """Fail the Kitaru session, then propagate the original agent error."""
        try:
            await self._finish(
                node_status=NodeStatus.FAILED,
                session_status=SessionStatus.FAILED,
                outputs=None,
                error=_error_text(error),
            )
        except BaseException as recording_error:
            raise recording_error from error
        finally:
            await self._close()
        raise error

    async def before_node_run(self, ctx: RunContext[Any], *, node: Any) -> Any:
        """Replace the initial PydanticAI prompt with replay-resolved inputs."""
        state = self._require_state()
        if isinstance(node, UserPromptNode):
            return replace(node, user_prompt=_pydantic_prompt(state.prompt_input))
        return node

    async def wrap_model_request(
        self,
        ctx: RunContext[Any],
        *,
        request_context: ModelRequestContext,
        handler: WrapModelRequestHandler,
    ) -> ModelResponse:
        """Apply replay overrides and record one model request."""
        state = self._require_state()
        requested_model = _model_identifier(request_context)
        effective = _replace_request_context(
            request_context,
            model=request_context.model,
            messages=_prepend_history(request_context.messages, state.message_history),
            model_settings=request_context.model_settings,
            model_replaced=False,
        )
        override = state.override
        if override is not None:
            replacement: str | None = None
            if isinstance(override.model, str):
                replacement = override.model
            elif isinstance(override.model, dict):
                replacement = override.model.get(requested_model)
            model = infer_model(replacement) if replacement else request_context.model
            messages = effective.messages
            if override.system_prompt is not None:
                messages = _replace_system_prompt(messages, override.system_prompt)
            model_settings = effective.model_settings
            if override.model_params is not None:
                model_settings = cast(ModelSettings, dict(override.model_params))
            effective = _replace_request_context(
                effective,
                model=model,
                messages=messages,
                model_settings=model_settings,
                model_replaced=replacement is not None,
            )

        node_index = await self._allocate_node()
        started_at = datetime.now(UTC)
        try:
            response = await handler(effective)
        except BaseException as error:
            await self._buffer_node(
                SessionNodeCreateRequest(
                    index=node_index,
                    parent_index=0,
                    node_type=NodeType.LLM_CALL,
                    name="model_request",
                    status=NodeStatus.FAILED,
                    error=_error_text(error),
                    started_at=started_at,
                    ended_at=datetime.now(UTC),
                    inputs=_messages_json(effective.messages),
                    outputs=None,
                    requested_model=requested_model,
                    model=_model_identifier(effective),
                    model_params=_jsonable(effective.model_settings),
                    attributes={},
                )
            )
            raise

        unpaired_native_calls = _unpaired_native_calls(response)
        llm_node = SessionNodeCreateRequest(
            index=node_index,
            parent_index=0,
            node_type=NodeType.LLM_CALL,
            name="model_request",
            status=NodeStatus.COMPLETED,
            started_at=started_at,
            ended_at=datetime.now(UTC),
            inputs=_messages_json(effective.messages),
            outputs=_jsonable(response),
            requested_model=requested_model,
            model=response.model_name or _model_identifier(effective),
            provider=response.provider_name,
            tokens=_token_usage(response),
            cost=None,
            model_params=_jsonable(effective.model_settings),
            attributes=(
                {"provider_native_calls": unpaired_native_calls}
                if unpaired_native_calls
                else {}
            ),
        )
        await self._buffer_node(llm_node)
        state.latest_llm_index = node_index
        unsupported_native = await self._record_native_tools(response, node_index)
        if unsupported_native is not None:
            raise unsupported_native
        return response

    async def wrap_tool_execute(
        self,
        ctx: RunContext[Any],
        *,
        call: ToolCallPart,
        tool_def: ToolDefinition,
        args: dict[str, Any],
        handler: WrapToolExecuteHandler,
    ) -> Any:
        """Apply a replay policy around local function-tool execution."""
        del ctx, tool_def
        state = self._require_state()
        node_index = await self._allocate_node()
        started_at = datetime.now(UTC)
        policy = self._tool_policy(call.tool_name)
        json_args = cast(dict[str, Any], _jsonable(args))
        mocked_policy: str | None = None
        failed_result = False
        try:
            if policy is None or isinstance(policy, PassthroughConfig):
                result = await handler(args)
            elif isinstance(policy, StaticConfig):
                matching = next(
                    (case for case in policy.cases if _case_matches(case, json_args)),
                    None,
                )
                if matching is not None:
                    result = matching.result
                    mocked_policy = policy.type
                else:
                    result, mocked_policy, failed_result = await self._handle_miss(
                        policy.type, policy.on_miss, call.tool_name, args, handler
                    )
            elif isinstance(policy, HistoryConfig):
                assert state.replay is not None
                cache_key = compute_tool_cache_key(call.tool_name, json_args)
                if cache_key is None:
                    result, mocked_policy, failed_result = await self._handle_miss(
                        policy.type, policy.on_miss, call.tool_name, args, handler
                    )
                else:
                    response = await state.client.replays.tool_lookup(
                        state.replay.id,
                        ToolLookupRequest(
                            tool_name=call.tool_name,
                            cache_key=cache_key,
                        ),
                    )
                    if response.found:
                        result = response.result
                        mocked_policy = policy.type
                    else:
                        result, mocked_policy, failed_result = await self._handle_miss(
                            policy.type, policy.on_miss, call.tool_name, args, handler
                        )
            elif isinstance(policy, LLMConfig):
                raise ToolPolicyError(
                    "Tool policy 'llm' is not supported by the PydanticAI adapter"
                )
            else:  # pragma: no cover - discriminated DTO union guards this branch
                raise ToolPolicyError(f"Unsupported tool policy '{policy.type}'")
        except BaseException as error:
            await self._record_tool(
                node_index=node_index,
                parent_index=state.latest_llm_index or 0,
                tool_name=call.tool_name,
                arguments=json_args,
                result=None,
                started_at=started_at,
                status=NodeStatus.FAILED,
                error=_error_text(error),
                attributes={},
                external_id=call.tool_call_id,
            )
            raise

        attributes = {"mocked": True, "policy": mocked_policy} if mocked_policy else {}
        await self._record_tool(
            node_index=node_index,
            parent_index=state.latest_llm_index or 0,
            tool_name=call.tool_name,
            arguments=json_args,
            result=result,
            started_at=started_at,
            status=NodeStatus.FAILED if failed_result else NodeStatus.COMPLETED,
            error=(
                json.dumps(_jsonable(result), sort_keys=True) if failed_result else None
            ),
            attributes=attributes,
            external_id=call.tool_call_id,
        )
        return result

    async def _handle_miss(
        self,
        policy_type: str,
        on_miss: ToolPolicyOnMiss,
        tool_name: str,
        args: dict[str, Any],
        handler: WrapToolExecuteHandler,
    ) -> tuple[Any, str | None, bool]:
        """Apply static/history miss behavior."""
        if on_miss is ToolPolicyOnMiss.PASSTHROUGH:
            return await handler(args), None, False
        message = f"No {policy_type} result for tool '{tool_name}'"
        if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            return {"error": message}, policy_type, True
        raise ToolPolicyMissError(message)

    def _tool_policy(self, tool_name: str) -> ToolConfig | None:
        """Select a replay policy by exact tool name, then default."""
        state = self._require_state()
        if state.replay is None:
            return None
        config = state.replay.tool_policy
        return config.tools.get(tool_name, config.default)

    async def _record_native_tools(
        self, response: ModelResponse, parent_index: int
    ) -> ToolPolicyError | None:
        """Record public provider-native call/return parts truthfully."""
        calls = {
            part.tool_call_id: part
            for part in response.parts
            if isinstance(part, NativeToolCallPart)
        }
        returns = {
            part.tool_call_id: part
            for part in response.parts
            if isinstance(part, NativeToolReturnPart)
        }
        unsupported: str | None = None
        for call_id, call in calls.items():
            policy = self._tool_policy(call.tool_name)
            if isinstance(policy, LLMConfig):
                unsupported = call.tool_name
            result = returns.get(call_id)
            if result is None:
                continue
            node_index = await self._allocate_node()
            status = (
                NodeStatus.COMPLETED
                if result.outcome == "success"
                else NodeStatus.FAILED
            )
            await self._record_tool(
                node_index=node_index,
                parent_index=parent_index,
                tool_name=call.tool_name,
                arguments=call.args_as_dict(),
                result=result.content,
                started_at=response.timestamp,
                status=status,
                error=(
                    None
                    if status is NodeStatus.COMPLETED
                    else json.dumps(_jsonable(result.content), sort_keys=True)
                ),
                attributes={
                    "provider_native": True,
                    **(
                        {"provider": call.provider_name}
                        if call.provider_name is not None
                        else {}
                    ),
                    **(
                        {"provider_details": _jsonable(call.provider_details)}
                        if call.provider_details is not None
                        else {}
                    ),
                },
                external_id=call_id,
            )
        if unsupported is None:
            return None
        return ToolPolicyError(
            f"Tool policy 'llm' is not supported for provider-native tool "
            f"'{unsupported}'"
        )

    async def _record_tool(
        self,
        *,
        node_index: int,
        parent_index: int | None,
        tool_name: str,
        arguments: Any,
        result: Any,
        started_at: datetime,
        status: NodeStatus,
        error: str | None,
        attributes: dict[str, Any],
        external_id: str | None,
    ) -> None:
        """Buffer a terminal tool-call node."""
        await self._buffer_node(
            SessionNodeCreateRequest(
                index=node_index,
                parent_index=parent_index,
                external_id=external_id,
                node_type=NodeType.TOOL_CALL,
                name=tool_name,
                status=status,
                error=error,
                started_at=started_at,
                ended_at=datetime.now(UTC),
                inputs=_jsonable(arguments),
                outputs=_jsonable(result),
                tool_name=tool_name,
                attributes=attributes,
            )
        )

    async def _allocate_node(self) -> int:
        """Allocate a monotonic node index."""
        state = self._require_state()
        async with state.lock:
            index = state.next_index
            state.next_index += 1
        return index

    async def _buffer_node(self, node: SessionNodeCreateRequest) -> None:
        """Buffer one node and flush at the configured batch size."""
        state = self._require_state()
        async with state.lock:
            state.buffer.append(node)
            if len(state.buffer) >= self.batch_size:
                await self._flush_locked(state)

    async def _flush_locked(self, state: _RunState) -> None:
        """Flush buffered nodes while the caller holds the state lock."""
        if not state.buffer or state.session_id is None:
            return
        count = len(state.buffer)
        batch = SessionNodeBatchRequest(nodes=list(state.buffer))
        await state.client.sessions.ingest_nodes(state.session_id, batch)
        del state.buffer[:count]

    async def _finish(
        self,
        *,
        node_status: NodeStatus,
        session_status: SessionStatus,
        outputs: Any,
        error: str | None,
    ) -> None:
        """Write the terminal root, flush children, and finish the session."""
        state = self._require_state()
        if state.finished or state.session_id is None:
            return
        state.finished = True
        ended_at = datetime.now(UTC)
        async with state.lock:
            state.buffer.append(
                SessionNodeCreateRequest(
                    index=0,
                    parent_index=None,
                    node_type=NodeType.SPAN,
                    name="run",
                    status=node_status,
                    error=error,
                    started_at=state.started_at,
                    ended_at=ended_at,
                    inputs=_jsonable(state.effective_input),
                    outputs=_jsonable(outputs),
                    attributes={},
                )
            )
            await self._flush_locked(state)
        await state.client.sessions.update(
            state.session_id,
            SessionUpdateRequest(
                status=session_status,
                outputs=_jsonable(outputs),
                error=error,
                ended_at=ended_at,
            ),
        )

    async def _close(self) -> None:
        """Close the per-run client exactly once."""
        state = self._state
        if state is None or state.closed:
            return
        state.closed = True
        await state.client.close()

    def _require_state(self) -> _RunState:
        """Return run-local state or fail on a broken capability lifecycle."""
        if self._state is None:
            raise RuntimeError("Kitaru capability has no active run state")
        return self._state
