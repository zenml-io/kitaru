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
"""Run-scoped Kitaru lifecycle and OpenAI activity recording."""

import asyncio
import contextlib
import inspect
import json
import math
import os
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import islice
from typing import Any, Generic, TypeVar

from agents import (
    Agent,
    AgentHookContext,
    HandoffOutputItem,
    MessageOutputItem,
    ModelResponse,
    RunContextWrapper,
    RunErrorDetails,
    RunHooks,
    RunResult,
    Tool,
    ToolCallItem,
    ToolCallOutputItem,
    TResponseInputItem,
)
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputRefusal,
    ResponseOutputText,
)

from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionResponse,
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
from kitaru.client import KitaruAPIClient

from .inputs import (
    contains_capture_marker,
    normalize_openai_input,
    parse_tool_arguments,
)

TContext = TypeVar("TContext")
SessionObserver = Callable[[SessionResponse], Awaitable[None] | None]

FRAMEWORK = "openai-agents"
ADAPTER_VERSION = "0.19"
MAX_STRING_BYTES = 16 * 1024
MAX_DEPTH = 8
MAX_COLLECTION_ITEMS = 100
FINALIZATION_TIMEOUT_SECONDS = 2.0


class KitaruRecordingError(RuntimeError):
    """Report a Kitaru failure after the native OpenAI run succeeded."""

    def __init__(
        self,
        *,
        result: RunResult,
        session_id: uuid.UUID | None,
        phase: str,
    ) -> None:
        self.result = result
        self.session_id = session_id
        self.phase = phase
        self.retry_safe = False
        self.side_effects_possible = True
        super().__init__(
            f"Kitaru recording failed during {phase} after OpenAI execution; "
            "automatic retry is unsafe because model or tool side effects may "
            "already have occurred."
        )


class UnsupportedInterruptionError(RuntimeError):
    """Reject an OpenAI approval interruption that Kitaru cannot resume."""

    def __init__(self, result: RunResult) -> None:
        self.result = result
        super().__init__(
            "OpenAI approval interruptions are not supported because Kitaru v2 "
            "cannot persist and resume interrupted runs."
        )


@dataclass(frozen=True)
class ResolvedRunInput:
    """Kitaru and OpenAI inputs resolved before session creation."""

    recorded: Any
    openai: str | list[TResponseInputItem]
    replay: ReplayResponse | None
    task_bound: bool


async def resolve_run_input(
    client: KitaruAPIClient,
    caller_input: str | list[TResponseInputItem],
) -> ResolvedRunInput:
    """Resolve task and replay state before creating a Kitaru session."""
    task_value = os.environ.get("KITARU_TASK_ID")
    task_id = uuid.UUID(task_value) if task_value else None
    replay_value = os.environ.get("KITARU_REPLAY_ID")
    replay_id = uuid.UUID(replay_value) if replay_value else None
    replay = await client.replays.get(replay_id) if replay_id is not None else None

    raw_inputs = os.environ.get("KITARU_TASK_INPUTS")
    if raw_inputs is not None:
        recorded = json.loads(raw_inputs)
    elif task_id is not None:
        spec = await client.tasks.get_spec(task_id)
        if not isinstance(spec.details, AgentTaskDetails):
            raise RuntimeError(f"Task {task_id} is not an agent task")
        recorded = spec.details.inputs
    else:
        recorded = caller_input

    return ResolvedRunInput(
        recorded=recorded,
        openai=normalize_openai_input(recorded),
        replay=replay,
        task_bound=task_id is not None,
    )


@dataclass
class _ModelObservation:
    started_at: datetime
    system_prompt: str | None
    input_items: list[TResponseInputItem]
    ended_at: datetime | None = None


@dataclass
class RunRecorder:
    """Store one OpenAI invocation as one isolated Kitaru session."""

    client: KitaruAPIClient
    batch_size: int
    observer: SessionObserver | None = None
    session: SessionResponse | None = None
    started_at: datetime | None = None
    root_inputs: Any = None
    next_index: int = 1
    buffer: list[SessionNodeCreateRequest] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    model_observations: list[_ModelObservation] = field(default_factory=list)
    closed: bool = False
    _seen: set[tuple[str, str]] = field(default_factory=set, repr=False)

    async def start(
        self,
        *,
        inputs: Any,
        agent_id: uuid.UUID | None,
        agent_version_id: uuid.UUID | None,
        session_name: str | None,
        replay: bool,
    ) -> SessionResponse:
        """Create the session and root before model execution."""
        started_at = datetime.now(UTC)
        root_inputs = _capture(inputs)
        session = await self.client.sessions.create(
            SessionCreateRequest(
                agent_id=agent_id,
                agent_version_id=agent_version_id,
                origin=SessionOrigin.REPLAY if replay else SessionOrigin.RECORDED,
                status=SessionStatus.IN_PROGRESS,
                name=session_name,
                inputs=root_inputs,
                outputs=None,
                started_at=started_at,
                framework=FRAMEWORK,
                adapter_version=ADAPTER_VERSION,
            )
        )
        self.session = session
        self.started_at = started_at
        self.root_inputs = root_inputs
        await self.client.sessions.ingest_nodes(
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
                        inputs=root_inputs,
                        outputs=None,
                        attributes={},
                    )
                ]
            ),
        )
        if self.observer is not None:
            observed = self.observer(session)
            if inspect.isawaitable(observed):
                await observed
        return session

    def compose_hooks(
        self, caller: RunHooks[TContext] | None
    ) -> "RecordingRunHooks[TContext]":
        """Compose recorder bookkeeping with caller-owned run hooks."""
        return RecordingRunHooks(self, caller)

    async def reconcile(self, result: RunResult | RunErrorDetails) -> None:
        """Translate public result objects into stable buffered nodes."""
        raw_responses = result.raw_responses[:MAX_COLLECTION_ITEMS]
        new_items = result.new_items[:MAX_COLLECTION_ITEMS]
        tool_parent_indexes: dict[str, int] = {}
        for position, response in enumerate(raw_responses):
            identity = response.response_id or f"model-{position}"
            if ("model", identity) in self._seen:
                continue
            self._seen.add(("model", identity))
            observation = (
                self.model_observations[position]
                if position < len(self.model_observations)
                else None
            )
            model_index = await self._append_node(
                node_type=NodeType.LLM_CALL,
                name="model",
                parent_index=0,
                external_id=response.response_id,
                started_at=observation.started_at if observation else None,
                ended_at=observation.ended_at if observation else None,
                inputs=(
                    {
                        "system_prompt": _capture(observation.system_prompt),
                        "items": _capture(observation.input_items),
                    }
                    if observation
                    else None
                ),
                outputs=_capture_model_response(response),
                tokens=_capture_usage(response),
            )
            for output_item in response.output[:MAX_COLLECTION_ITEMS]:
                output_id = _raw_id(output_item)
                if output_id is not None:
                    tool_parent_indexes[output_id] = model_index

        calls: list[ToolCallItem] = [
            item for item in new_items if isinstance(item, ToolCallItem)
        ]
        outputs = {
            item.call_id: item
            for item in new_items
            if isinstance(item, ToolCallOutputItem) and item.call_id is not None
        }
        for position, call in enumerate(calls):
            call_id = call.call_id or f"tool-{position}"
            if ("tool", call_id) in self._seen:
                continue
            self._seen.add(("tool", call_id))
            output = outputs.get(call.call_id)
            tool_inputs, tool_attributes = _capture_tool_input(call)
            await self._append_node(
                node_type=NodeType.TOOL_CALL,
                name=call.tool_name or _raw_type(call.raw_item) or "hosted_tool",
                parent_index=tool_parent_indexes.get(call_id, 0),
                external_id=call.call_id or _raw_id(call.raw_item),
                started_at=None,
                ended_at=None,
                inputs=tool_inputs,
                outputs=_capture(output.output) if output is not None else None,
                tool_name=call.tool_name,
                attributes=tool_attributes,
            )

        for position, item in enumerate(new_items):
            if not isinstance(item, HandoffOutputItem):
                continue
            identity = _raw_id(item.raw_item) or f"handoff-{position}"
            if ("handoff", identity) in self._seen:
                continue
            self._seen.add(("handoff", identity))
            target_name = item.target_agent.name
            await self._append_node(
                node_type=NodeType.SUBAGENT_CALL,
                name="handoff",
                parent_index=0,
                external_id=_raw_id(item.raw_item),
                started_at=None,
                ended_at=None,
                inputs={"source_agent": item.source_agent.name},
                outputs={"target_agent": target_name},
                subagent_id=target_name,
            )

        known = (MessageOutputItem, ToolCallItem, ToolCallOutputItem, HandoffOutputItem)
        for position, item in enumerate(new_items):
            if isinstance(item, known):
                continue
            identity = f"{type(item).__module__}.{type(item).__qualname__}:{position}"
            if ("unknown", identity) in self._seen:
                continue
            self._seen.add(("unknown", identity))
            await self._append_node(
                node_type=NodeType.SPAN,
                name="unsupported_openai_item",
                parent_index=0,
                external_id=None,
                started_at=None,
                ended_at=None,
                inputs=None,
                outputs=None,
                attributes={"item_type": identity.rsplit(":", 1)[0]},
            )

        omitted_responses = len(result.raw_responses) - len(raw_responses)
        omitted_items = len(result.new_items) - len(new_items)
        if omitted_responses or omitted_items:
            await self._append_node(
                node_type=NodeType.SPAN,
                name="openai_capture_truncated",
                parent_index=0,
                external_id=None,
                started_at=None,
                ended_at=None,
                inputs=None,
                outputs=None,
                attributes={
                    "reason": "max_items",
                    "omitted_raw_responses": omitted_responses,
                    "omitted_new_items": omitted_items,
                },
            )

    async def complete(self, output: Any) -> None:
        """Persist buffered nodes and mark the session completed."""
        await self.flush()
        if self.session is None:
            raise RuntimeError("Recorder session was not started")
        ended_at = datetime.now(UTC)
        output_payload = _capture(output)
        await self.client.sessions.ingest_nodes(
            self.session.id,
            SessionNodeBatchRequest(
                nodes=[
                    SessionNodeCreateRequest(
                        index=0,
                        parent_index=None,
                        node_type=NodeType.SPAN,
                        name="run",
                        status=NodeStatus.COMPLETED,
                        started_at=self.started_at,
                        ended_at=ended_at,
                        inputs=self.root_inputs,
                        outputs=output_payload,
                        attributes={},
                    )
                ]
            ),
        )
        await self.client.sessions.update(
            self.session.id,
            SessionUpdateRequest(
                status=SessionStatus.COMPLETED,
                outputs=output_payload,
                ended_at=ended_at,
            ),
        )

    async def fail(self, error: BaseException) -> None:
        """Best-effort persist a failed root and failed session."""
        if self.session is None:
            return
        error_text = _capture_error(error)
        first_error: BaseException | None = None
        try:
            await self.flush()
            await self.client.sessions.ingest_nodes(
                self.session.id,
                SessionNodeBatchRequest(
                    nodes=[
                        SessionNodeCreateRequest(
                            index=0,
                            parent_index=None,
                            node_type=NodeType.SPAN,
                            name="run",
                            status=NodeStatus.FAILED,
                            error=error_text,
                            started_at=self.started_at,
                            ended_at=datetime.now(UTC),
                            inputs=self.root_inputs,
                            outputs=None,
                            attributes={},
                        )
                    ]
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as failure:
            first_error = failure
        try:
            await self.client.sessions.update(
                self.session.id,
                SessionUpdateRequest(
                    status=SessionStatus.FAILED,
                    error=error_text,
                    ended_at=datetime.now(UTC),
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as failure:
            first_error = first_error or failure
        if first_error is not None:
            raise first_error

    async def flush(self) -> None:
        """Send buffered nodes in deterministic batches."""
        if self.session is None:
            return
        async with self.lock:
            nodes = self.buffer
            self.buffer = []
        try:
            for offset in range(0, len(nodes), self.batch_size):
                await self.client.sessions.ingest_nodes(
                    self.session.id,
                    SessionNodeBatchRequest(
                        nodes=nodes[offset : offset + self.batch_size]
                    ),
                )
        except BaseException:
            async with self.lock:
                self.buffer = nodes + self.buffer
            raise

    async def close(self) -> None:
        """Close the run-scoped client exactly once."""
        if self.closed:
            return
        await self.client.close()
        self.closed = True

    async def _append_node(
        self,
        *,
        node_type: NodeType,
        name: str,
        parent_index: int,
        external_id: str | None,
        started_at: datetime | None,
        ended_at: datetime | None,
        inputs: Any,
        outputs: Any,
        tokens: TokenUsage | None = None,
        tool_name: str | None = None,
        subagent_id: str | None = None,
        attributes: Any = None,
    ) -> int:
        async with self.lock:
            index = self.next_index
            self.next_index += 1
            self.buffer.append(
                SessionNodeCreateRequest(
                    index=index,
                    parent_index=parent_index,
                    external_id=external_id,
                    node_type=node_type,
                    name=name,
                    status=NodeStatus.COMPLETED,
                    started_at=started_at,
                    ended_at=ended_at,
                    inputs=inputs,
                    outputs=outputs,
                    tokens=tokens,
                    tool_name=tool_name,
                    subagent_id=subagent_id,
                    attributes=attributes or {},
                )
            )
        return index


class RecordingRunHooks(RunHooks[TContext], Generic[TContext]):
    """Record narrow hook timing and forward caller hooks exactly once."""

    def __init__(
        self, recorder: RunRecorder, caller: RunHooks[TContext] | None
    ) -> None:
        self._recorder = recorder
        self._caller = caller

    async def on_agent_start(
        self, context: AgentHookContext[TContext], agent: Agent[TContext]
    ) -> None:
        if self._caller is not None:
            await self._caller.on_agent_start(context, agent)

    async def on_agent_end(
        self,
        context: AgentHookContext[TContext],
        agent: Agent[TContext],
        output: Any,
    ) -> None:
        if self._caller is not None:
            await self._caller.on_agent_end(context, agent, output)

    async def on_handoff(
        self,
        context: RunContextWrapper[TContext],
        from_agent: Agent[TContext],
        to_agent: Agent[TContext],
    ) -> None:
        if self._caller is not None:
            await self._caller.on_handoff(context, from_agent, to_agent)

    async def on_llm_start(
        self,
        context: RunContextWrapper[TContext],
        agent: Agent[TContext],
        system_prompt: str | None,
        input_items: list[TResponseInputItem],
    ) -> None:
        if len(self._recorder.model_observations) < MAX_COLLECTION_ITEMS:
            self._recorder.model_observations.append(
                _ModelObservation(
                    started_at=datetime.now(UTC),
                    system_prompt=system_prompt,
                    input_items=input_items,
                )
            )
        if self._caller is not None:
            await self._caller.on_llm_start(context, agent, system_prompt, input_items)

    async def on_llm_end(
        self,
        context: RunContextWrapper[TContext],
        agent: Agent[TContext],
        response: ModelResponse,
    ) -> None:
        for observation in self._recorder.model_observations:
            if observation.ended_at is None:
                observation.ended_at = datetime.now(UTC)
                break
        if self._caller is not None:
            await self._caller.on_llm_end(context, agent, response)

    async def on_tool_start(
        self,
        context: RunContextWrapper[TContext],
        agent: Agent[TContext],
        tool: Tool,
    ) -> None:
        if self._caller is not None:
            await self._caller.on_tool_start(context, agent, tool)

    async def on_tool_end(
        self,
        context: RunContextWrapper[TContext],
        agent: Agent[TContext],
        tool: Tool,
        result: object,
    ) -> None:
        if self._caller is not None:
            await self._caller.on_tool_end(context, agent, tool, result)


async def finalize_failure(
    recorder: RunRecorder, error: BaseException
) -> BaseException | None:
    """Persist a failure within a time bound and return any secondary error."""
    task = asyncio.create_task(recorder.fail(error))
    current_task = asyncio.current_task()
    cancellation_count = current_task.cancelling() if current_task is not None else 0
    try:
        await asyncio.wait_for(
            asyncio.shield(task), timeout=FINALIZATION_TIMEOUT_SECONDS
        )
    except TimeoutError as failure:
        await _cancel_and_join_failure_task(task)
        return failure
    except asyncio.CancelledError as failure:
        caller_cancelled = (
            current_task is not None and current_task.cancelling() > cancellation_count
        )
        if caller_cancelled:
            await _cancel_and_join_failure_task(task)
            return failure
        return failure
    except BaseException as failure:
        return failure
    return None


async def _cancel_and_join_failure_task(task: asyncio.Task[None]) -> None:
    """Cancel a persistence task and observe it for one bounded interval."""
    if not task.done():
        task.cancel()
    done, _ = await asyncio.wait({task}, timeout=FINALIZATION_TIMEOUT_SECONDS)
    if task in done:
        _consume_task_exception(task)
    else:
        task.add_done_callback(_consume_task_exception)


def _consume_task_exception(task: asyncio.Task[None]) -> None:
    """Retrieve a detached task result so it cannot emit a warning."""
    with contextlib.suppress(BaseException):
        task.result()


def _capture(value: Any, *, depth: int = 0) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return {"_kitaru_unsupported_type": "non_finite_float"}
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        bounded_value = value[:MAX_STRING_BYTES]
        encoded = bounded_value.encode("utf-8")
        if len(encoded) <= MAX_STRING_BYTES:
            if len(value) == len(bounded_value):
                return value
            return {
                "value": bounded_value,
                "_kitaru_truncated": {
                    "reason": "max_characters",
                    "original_characters": len(value),
                },
            }
        shortened = encoded[:MAX_STRING_BYTES].decode("utf-8", errors="ignore")
        return {
            "value": shortened,
            "_kitaru_truncated": {
                "reason": "max_bytes",
                "original_characters": len(value),
            },
        }
    if depth >= MAX_DEPTH:
        return {"_kitaru_truncated": {"reason": "max_depth"}}
    if isinstance(value, dict):
        captured: dict[str, Any] = {}
        omitted = 0
        for key, item in islice(value.items(), MAX_COLLECTION_ITEMS):
            if not isinstance(key, str):
                omitted += 1
                continue
            captured[key] = _capture(item, depth=depth + 1)
        omitted += max(0, len(value) - MAX_COLLECTION_ITEMS)
        if omitted:
            captured["_kitaru_truncated"] = {
                "reason": "max_items_or_non_string_keys",
                "omitted": omitted,
            }
        return captured
    if isinstance(value, list | tuple):
        captured = [
            _capture(item, depth=depth + 1) for item in value[:MAX_COLLECTION_ITEMS]
        ]
        if len(value) > MAX_COLLECTION_ITEMS:
            captured.append(
                {
                    "_kitaru_truncated": {
                        "reason": "max_items",
                        "omitted": len(value) - MAX_COLLECTION_ITEMS,
                    }
                }
            )
        return captured
    value_type = type(value)
    return {
        "_kitaru_unsupported_type": (
            f"{value_type.__module__}.{value_type.__qualname__}"
        )
    }


def _capture_error(error: BaseException) -> str:
    text = f"{type(error).__name__}: {error}"
    captured = _capture(text)
    if isinstance(captured, str):
        return captured
    return captured["value"]


def _capture_model_response(response: ModelResponse) -> Any:
    output: list[Any] = []
    for item in response.output[:MAX_COLLECTION_ITEMS]:
        if not isinstance(item, ResponseOutputMessage):
            output.append({"type": _raw_type(item) or type(item).__name__})
            continue
        content: list[Any] = []
        for part in item.content[:MAX_COLLECTION_ITEMS]:
            if isinstance(part, ResponseOutputText):
                content.append({"type": "output_text", "text": _capture(part.text)})
            elif isinstance(part, ResponseOutputRefusal):
                content.append({"type": "refusal", "refusal": _capture(part.refusal)})
            else:
                content.append({"type": type(part).__name__})
        output.append(
            {
                "id": item.id,
                "type": "message",
                "role": item.role,
                "status": item.status,
                "content": content,
            }
        )
        if len(item.content) > MAX_COLLECTION_ITEMS:
            content.append(
                {
                    "_kitaru_truncated": {
                        "reason": "max_items",
                        "omitted": len(item.content) - MAX_COLLECTION_ITEMS,
                    }
                }
            )
    if len(response.output) > MAX_COLLECTION_ITEMS:
        output.append(
            {
                "_kitaru_truncated": {
                    "reason": "max_items",
                    "omitted": len(response.output) - MAX_COLLECTION_ITEMS,
                }
            }
        )
    return output


def _capture_usage(response: ModelResponse) -> TokenUsage:
    usage = response.usage
    return TokenUsage(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.input_tokens_details.cached_tokens,
        reasoning_tokens=usage.output_tokens_details.reasoning_tokens,
    )


def _capture_tool_input(item: ToolCallItem) -> tuple[Any, dict[str, str]]:
    raw = item.raw_item
    if isinstance(raw, ResponseFunctionToolCall):
        try:
            arguments = parse_tool_arguments(raw.arguments)
        except (ValueError, TypeError):
            return None, {"kitaru.tool_arguments": "invalid_json"}
        try:
            captured = _capture(arguments)
        except UnicodeEncodeError:
            return None, {"kitaru.tool_arguments": "capture_loss"}
        if contains_capture_marker(captured):
            return None, {"kitaru.tool_arguments": "capture_loss"}
        return captured, {}
    if isinstance(raw, dict):
        return (
            {
                key: _capture(raw[key])
                for key in ("arguments", "action", "query")
                if key in raw
            },
            {},
        )
    arguments = getattr(raw, "arguments", None)
    return (
        ({"arguments": _capture(arguments)} if arguments is not None else None),
        {},
    )


def _raw_id(value: Any) -> str | None:
    if isinstance(value, dict):
        raw_id = value.get("call_id") or value.get("id")
    else:
        raw_id = getattr(value, "call_id", None) or getattr(value, "id", None)
    return raw_id if isinstance(raw_id, str) else None


def _raw_type(value: Any) -> str | None:
    if isinstance(value, dict):
        raw_type = value.get("type")
    else:
        raw_type = getattr(value, "type", None)
    return raw_type if isinstance(raw_type, str) else None
