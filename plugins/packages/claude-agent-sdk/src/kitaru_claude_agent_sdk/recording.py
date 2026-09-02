#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Invocation-local recording for the public Claude Agent SDK message stream."""

import asyncio
import contextlib
import itertools
import json
import math
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from importlib.metadata import version
from typing import Any, cast

from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TaskNotificationMessage,
    TaskProgressMessage,
    TaskStartedMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from kitaru.api_models.v1.replay import ReplayResponse
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

from .capability import KitaruRecordingError
from .codec import encode_tool_result

ADAPTER_VERSION = version("kitaru-claude-agent-sdk")
FRAMEWORK = "claude-agent-sdk"
MAX_STRING_BYTES = 16 * 1024
MAX_COLLECTION_ITEMS = 100
MAX_DEPTH = 8
FINALIZATION_TIMEOUT_SECONDS = 2.0
_SAFE_OPTION_FIELDS = frozenset(
    {
        "allowed_tools",
        "disallowed_tools",
        "model",
        "permission_mode",
        "system_prompt",
    }
)


@dataclass(frozen=True)
class ResolvedRunInput:
    """Kitaru and Claude inputs resolved before invocation creation."""

    recorded: Any
    claude: str
    replay: ReplayResponse | None
    task_bound: bool


async def resolve_run_input(
    client: KitaruAPIClient, caller_input: str
) -> ResolvedRunInput:
    """Resolve task and replay state without creating a Kitaru session."""
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

    claude = (
        recorded
        if isinstance(recorded, str)
        else json.dumps(
            recorded, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    )
    return ResolvedRunInput(
        recorded=recorded,
        claude=claude,
        replay=replay,
        task_bound=task_id is not None,
    )


def _error_text(error: BaseException) -> str:
    """Return the native failure without including persistence diagnostics."""
    return _captured_text(str(error) or type(error).__name__)


def _captured_text(value: str) -> str:
    """Bound a string while preserving fields that require string values."""
    captured = _capture(value)
    if isinstance(captured, str):
        return captured
    return cast(dict[str, str], captured)["value"]


def _capture(value: Any, *, depth: int = 0) -> Any:
    """Bound JSON-like public message data without stringifying opaque objects."""
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"unsupported": "non_finite_float"}
    if isinstance(value, str):
        candidate = value[:MAX_STRING_BYTES]
        encoded = candidate.encode()
        if len(value) <= MAX_STRING_BYTES and len(encoded) <= MAX_STRING_BYTES:
            return value
        return {
            "value": encoded[:MAX_STRING_BYTES].decode(errors="ignore"),
            "truncated": True,
        }
    if depth >= MAX_DEPTH:
        return {"truncated": "max_depth"}
    if isinstance(value, Mapping):
        captured: dict[str, Any] = {}
        for key, item in itertools.islice(value.items(), MAX_COLLECTION_ITEMS):
            if isinstance(key, str):
                captured[key] = _capture(item, depth=depth + 1)
        if len(value) > MAX_COLLECTION_ITEMS:
            captured["truncated_items"] = len(value) - MAX_COLLECTION_ITEMS
        return captured
    if isinstance(value, list | tuple):
        captured_items = [
            _capture(item, depth=depth + 1) for item in value[:MAX_COLLECTION_ITEMS]
        ]
        if len(value) > MAX_COLLECTION_ITEMS:
            captured_items.append(
                {"truncated_items": len(value) - MAX_COLLECTION_ITEMS}
            )
        return captured_items
    return {"unsupported_type": type(value).__name__}


def _get_token_usage(value: Mapping[str, Any] | None) -> TokenUsage | None:
    """Map Claude's public usage dictionary to Kitaru token fields."""
    if not value:
        return None

    def integer(*names: str) -> int | None:
        for name in names:
            candidate = value.get(name)
            if isinstance(candidate, int) and not isinstance(candidate, bool):
                return candidate
        return None

    return TokenUsage(
        input_tokens=integer("input_tokens", "inputTokens"),
        output_tokens=integer("output_tokens", "outputTokens"),
        cached_input_tokens=integer("cache_read_input_tokens", "cacheReadInputTokens"),
    )


def _get_terminal_metadata(message: ResultMessage) -> dict[str, Any]:
    """Select safe terminal fields without retaining arbitrary result internals."""
    terminal: dict[str, Any] = {
        "session_id": message.session_id,
        "num_turns": message.num_turns,
        "duration_ms": message.duration_ms,
        "duration_api_ms": message.duration_api_ms,
        "usage": _capture(message.usage),
        "total_cost_usd": message.total_cost_usd,
    }
    if message.stop_reason is not None:
        terminal["stop_reason"] = message.stop_reason
    if message.terminal_reason is not None:
        terminal["terminal_reason"] = message.terminal_reason
    return {"terminal": terminal}


@dataclass
class InvocationRecorder:
    """Mutable Kitaru state isolated to one Claude query invocation."""

    client: KitaruAPIClient
    session_id: uuid.UUID
    started_at: datetime
    captured_inputs: Any
    safe_options: dict[str, Any]
    replayable_tool_names: frozenset[str] = frozenset()
    next_index: int = 1
    finalized: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _message_indexes: dict[str, int] = field(default_factory=dict, repr=False)
    _tool_cache_keys: dict[str, str] = field(default_factory=dict, repr=False)
    _tool_nodes: dict[str, SessionNodeCreateRequest] = field(
        default_factory=dict, repr=False
    )
    _task_nodes: dict[str, SessionNodeCreateRequest] = field(
        default_factory=dict, repr=False
    )
    _hook_events: dict[str, list[str]] = field(default_factory=dict, repr=False)
    _tool_policy_events: dict[str, list[dict[str, Any]]] = field(
        default_factory=dict, repr=False
    )

    @classmethod
    async def start(
        cls,
        *,
        client: KitaruAPIClient,
        inputs: Any,
        agent_id: uuid.UUID | None,
        agent_version_id: uuid.UUID | None,
        session_name: str | None,
        replay: bool,
        safe_options: Mapping[str, Any] | None = None,
        replayable_tool_names: frozenset[str] = frozenset(),
    ) -> "InvocationRecorder":
        """Create the session and root before Claude execution starts."""
        started_at = datetime.now(UTC)
        captured_inputs = _capture(inputs)
        selected_options = {
            key: _capture(value)
            for key, value in (safe_options or {}).items()
            if key in _SAFE_OPTION_FIELDS
        }
        session = await client.sessions.create(
            SessionCreateRequest(
                agent_id=agent_id,
                agent_version_id=agent_version_id,
                origin=SessionOrigin.REPLAY if replay else SessionOrigin.RECORDED,
                status=SessionStatus.IN_PROGRESS,
                name=session_name,
                inputs=inputs,
                outputs=None,
                started_at=started_at,
                metadata={"options": selected_options},
                framework=FRAMEWORK,
                adapter_version=ADAPTER_VERSION,
            )
        )
        recorder = cls(
            client=client,
            session_id=session.id,
            started_at=started_at,
            captured_inputs=captured_inputs,
            safe_options=selected_options,
            replayable_tool_names=replayable_tool_names,
        )
        try:
            await recorder._persist(
                SessionNodeCreateRequest(
                    index=0,
                    parent_index=None,
                    node_type=NodeType.SPAN,
                    name="query",
                    status=NodeStatus.IN_PROGRESS,
                    started_at=started_at,
                    inputs=captured_inputs,
                    outputs=None,
                    attributes={"options": selected_options},
                )
            )
        except BaseException as error:
            # The root-ingest failure remains the caller-visible failure; the
            # runner closes this preflight client after start() re-raises.
            with contextlib.suppress(BaseException):
                await client.sessions.update(
                    session.id,
                    SessionUpdateRequest(
                        status=SessionStatus.FAILED,
                        outputs=None,
                        error=_error_text(error),
                        ended_at=datetime.now(UTC),
                    ),
                )
            raise
        return recorder

    async def record_message(self, message: Any) -> None:
        """Persist one authoritative public message without duplicating deltas."""
        async with self.lock:
            if self.finalized or isinstance(message, StreamEvent):
                return
            if isinstance(message, AssistantMessage):
                await self._record_assistant(message)
            elif isinstance(message, UserMessage):
                await self._record_user(message)
            elif isinstance(message, TaskStartedMessage):
                await self._record_task_started(message)
            elif isinstance(message, TaskProgressMessage):
                await self._record_task_progress(message)
            elif isinstance(message, TaskNotificationMessage):
                await self._record_task_notification(message)
            elif isinstance(message, SystemMessage):
                await self._record_framework_event(
                    type(message).__name__,
                    external_id=None,
                    attributes={
                        "subtype": message.subtype,
                        "data_fields": sorted(message.data)[:MAX_COLLECTION_ITEMS],
                    },
                )
            elif not isinstance(message, ResultMessage):
                await self._record_framework_event(
                    type(message).__name__,
                    external_id=None,
                    attributes={"message_type": type(message).__name__},
                )

    async def record_tool_hook(
        self, hook_input: Mapping[str, Any], *, event: str
    ) -> None:
        """Enrich one tool node from a neutral public hook observation."""
        tool_id = hook_input.get("tool_use_id")
        if not isinstance(tool_id, str) or event not in {"before", "after"}:
            return
        async with self.lock:
            events = self._hook_events.setdefault(tool_id, [])
            if event not in events:
                events.append(event)
            node = self._tool_nodes.get(tool_id)
            if node is None:
                name = hook_input.get("tool_name")
                node = self._new_node(
                    node_type=NodeType.TOOL_CALL,
                    name=name if isinstance(name, str) else "tool",
                    parent_index=0,
                    external_id=tool_id,
                    status=NodeStatus.IN_PROGRESS,
                    inputs=_capture(hook_input.get("tool_input")),
                    outputs=None,
                    tool_name=name if isinstance(name, str) else None,
                    attributes={"hook_events": list(events)},
                )
                self._tool_nodes[tool_id] = node
            else:
                node = node.model_copy(
                    update={
                        "attributes": {
                            **cast(dict[str, Any], node.attributes),
                            "hook_events": list(events),
                        }
                    }
                )
                self._tool_nodes[tool_id] = node
            await self._persist(node)

    async def record_tool_policy(
        self,
        *,
        tool_name: str,
        arguments: dict[str, Any],
        policy: str,
        live: bool,
    ) -> None:
        """Mark one wrapped call as substituted or as a live side effect."""
        cache_key = compute_tool_cache_key(tool_name, arguments)
        if cache_key is None:
            return
        event = {"policy": policy, "live": live}
        async with self.lock:
            for tool_id, node in self._tool_nodes.items():
                if (
                    node.tool_name == tool_name
                    and self._tool_cache_keys.get(tool_id) == cache_key
                    and "replay" not in node.attributes
                ):
                    updated = node.model_copy(
                        update={
                            "attributes": {
                                **cast(dict[str, Any], node.attributes),
                                "replay": event,
                            }
                        }
                    )
                    self._tool_nodes[tool_id] = updated
                    await self._persist(updated)
                    return
            self._tool_policy_events.setdefault(cache_key, []).append(event)

    async def finalize(
        self,
        *,
        terminal: ResultMessage | None = None,
        error: BaseException | None = None,
    ) -> None:
        """Finalize the root, session, and client exactly once."""
        async with self.lock:
            if self.finalized:
                return
            self.finalized = True
            ended_at = datetime.now(UTC)
            failed = error is not None or (terminal is not None and terminal.is_error)
            error_text = None
            if error is not None:
                error_text = _error_text(error)
            elif terminal is not None and terminal.is_error:
                error_text = _captured_text(
                    "; ".join(terminal.errors or []) or terminal.subtype
                )
            outputs = (
                _capture(terminal.result)
                if terminal is not None and not failed
                else None
            )
            metadata = _get_terminal_metadata(terminal) if terminal is not None else {}
            cost = (
                Decimal(str(terminal.total_cost_usd))
                if terminal is not None and terminal.total_cost_usd is not None
                else None
            )
            first_failure: BaseException | None = None
            if failed:
                failed_nodes = self._fail_unfinished_nodes(
                    ended_at=ended_at,
                    error=error_text or "Query ended before this call completed",
                )
                if failed_nodes:
                    try:
                        await self.client.sessions.ingest_nodes(
                            self.session_id,
                            SessionNodeBatchRequest(nodes=failed_nodes),
                        )
                    except Exception as persistence_error:
                        first_failure = first_failure or persistence_error
            try:
                try:
                    await self._persist(
                        SessionNodeCreateRequest(
                            index=0,
                            parent_index=None,
                            node_type=NodeType.SPAN,
                            name="query",
                            status=(
                                NodeStatus.FAILED if failed else NodeStatus.COMPLETED
                            ),
                            error=error_text,
                            started_at=self.started_at,
                            ended_at=ended_at,
                            inputs=self.captured_inputs,
                            outputs=outputs,
                            cost=cost,
                            attributes={"options": self.safe_options},
                            metadata=metadata,
                        )
                    )
                except Exception as persistence_error:
                    first_failure = persistence_error
                try:
                    await self.client.sessions.update(
                        self.session_id,
                        SessionUpdateRequest(
                            status=(
                                SessionStatus.FAILED
                                if failed
                                else SessionStatus.COMPLETED
                            ),
                            outputs=outputs,
                            error=error_text,
                            ended_at=ended_at,
                            metadata=metadata,
                        ),
                    )
                except Exception as persistence_error:
                    first_failure = first_failure or persistence_error
            finally:
                try:
                    await self.client.close()
                except Exception as persistence_error:
                    first_failure = first_failure or persistence_error
            if first_failure is not None:
                raise first_failure

    def _fail_unfinished_nodes(
        self, *, ended_at: datetime, error: str
    ) -> list[SessionNodeCreateRequest]:
        """Mark unfinished tool and subagent calls as failed."""
        failed_nodes: list[SessionNodeCreateRequest] = []
        for node_by_id in (self._tool_nodes, self._task_nodes):
            for external_id, node in node_by_id.items():
                if node.status is not NodeStatus.IN_PROGRESS:
                    continue
                failed_node = node.model_copy(
                    update={
                        "status": NodeStatus.FAILED,
                        "error": error,
                        "ended_at": ended_at,
                    }
                )
                node_by_id[external_id] = failed_node
                failed_nodes.append(failed_node)
        return failed_nodes

    async def _record_assistant(self, message: AssistantMessage) -> None:
        identity = message.message_id or message.uuid
        model_index = self._message_indexes.get(identity) if identity else None
        if model_index is None:
            provisional_indexes = [
                self._tool_nodes[block.id].index
                for block in message.content
                if isinstance(block, ToolUseBlock) and block.id in self._tool_nodes
            ]
            model_index = min(provisional_indexes, default=self.next_index)
            if model_index == self.next_index:
                self.next_index += 1
            if identity:
                self._message_indexes[identity] = model_index
        texts = [
            block.text for block in message.content if isinstance(block, TextBlock)
        ]
        thoughts = [
            block.thinking
            for block in message.content
            if isinstance(block, ThinkingBlock)
        ]
        model_node = SessionNodeCreateRequest(
            index=model_index,
            parent_index=self._parent_for(message.parent_tool_use_id),
            external_id=identity,
            node_type=NodeType.LLM_CALL,
            name="assistant",
            status=NodeStatus.FAILED if message.error else NodeStatus.COMPLETED,
            error=message.error,
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC),
            inputs=None,
            outputs={"text": texts},
            reasoning="\n".join(thoughts) or None,
            model=message.model,
            model_provider="anthropic",
            tokens=_get_token_usage(message.usage),
            attributes={
                "session_id": message.session_id,
                "stop_reason": message.stop_reason,
            },
        )
        await self._persist(model_node)
        for block in message.content:
            if isinstance(block, ToolUseBlock):
                await self._record_tool_use(block, model_index)
            elif isinstance(block, ToolResultBlock):
                await self._record_tool_result(block)

    async def _record_user(self, message: UserMessage) -> None:
        if isinstance(message.content, str):
            await self._record_framework_event(
                "user_message",
                external_id=message.uuid,
                attributes={"content": _capture(message.content)},
                parent_index=self._parent_for(message.parent_tool_use_id),
            )
            return
        non_results = 0
        for block in message.content:
            if isinstance(block, ToolResultBlock):
                await self._record_tool_result(block)
            else:
                non_results += 1
        if non_results:
            await self._record_framework_event(
                "user_message",
                external_id=message.uuid,
                attributes={"content_blocks": non_results},
                parent_index=self._parent_for(message.parent_tool_use_id),
            )

    async def _record_tool_use(self, block: ToolUseBlock, parent_index: int) -> None:
        cache_key = compute_tool_cache_key(block.name, block.input)
        if cache_key is not None:
            self._tool_cache_keys[block.id] = cache_key
        policy_events = (
            self._tool_policy_events.get(cache_key, []) if cache_key is not None else []
        )
        replay_event = policy_events.pop(0) if policy_events else None
        attributes: dict[str, Any] = {
            "hook_events": list(self._hook_events.get(block.id, []))
        }
        if replay_event is not None:
            attributes["replay"] = replay_event
        node = self._tool_nodes.get(block.id)
        if node is None or node.index == parent_index:
            node = self._new_node(
                node_type=NodeType.TOOL_CALL,
                name=block.name,
                parent_index=parent_index,
                external_id=block.id,
                status=NodeStatus.IN_PROGRESS,
                inputs=_capture(block.input),
                outputs=None,
                tool_name=block.name,
                attributes=(
                    {
                        **cast(dict[str, Any], node.attributes),
                        **attributes,
                    }
                    if node is not None
                    else attributes
                ),
            )
        else:
            node = node.model_copy(
                update={
                    "name": block.name,
                    "parent_index": parent_index,
                    "inputs": _capture(block.input),
                    "tool_name": block.name,
                    "attributes": {
                        **cast(dict[str, Any], node.attributes),
                        **attributes,
                    },
                }
            )
        self._tool_nodes[block.id] = node
        await self._persist(node)

    async def _record_tool_result(self, block: ToolResultBlock) -> None:
        node = self._tool_nodes.get(block.tool_use_id)
        tool_name = node.tool_name if node is not None else None
        if tool_name in self.replayable_tool_names:
            content = (
                [{"type": "text", "text": block.content}]
                if isinstance(block.content, str)
                else block.content
            )
            outputs = encode_tool_result(
                {"content": content, "is_error": bool(block.is_error)}
            )
        else:
            outputs = _capture(block.content)
        if node is None:
            node = self._new_node(
                node_type=NodeType.TOOL_CALL,
                name="tool",
                parent_index=0,
                external_id=block.tool_use_id,
                status=(NodeStatus.FAILED if block.is_error else NodeStatus.COMPLETED),
                inputs=None,
                outputs=outputs,
                tool_name=None,
                error="Claude tool result reported an error"
                if block.is_error
                else None,
                attributes={"orphaned": True},
            )
        else:
            node = node.model_copy(
                update={
                    "status": (
                        NodeStatus.FAILED if block.is_error else NodeStatus.COMPLETED
                    ),
                    "error": (
                        "Claude tool result reported an error"
                        if block.is_error
                        else None
                    ),
                    "ended_at": datetime.now(UTC),
                    "outputs": outputs,
                }
            )
        self._tool_nodes[block.tool_use_id] = node
        await self._persist(node)

    async def _record_task_started(self, message: TaskStartedMessage) -> None:
        if message.task_id in self._task_nodes:
            return
        node = self._new_node(
            node_type=NodeType.SUBAGENT_CALL,
            name=message.description,
            parent_index=self._parent_for(message.tool_use_id),
            external_id=message.task_id,
            status=NodeStatus.IN_PROGRESS,
            inputs={"description": _capture(message.description)},
            outputs=None,
            subagent_id=message.task_id,
            attributes={"task_type": message.task_type},
        )
        self._task_nodes[message.task_id] = node
        await self._persist(node)

    async def _record_task_progress(self, message: TaskProgressMessage) -> None:
        node = self._task_nodes.get(message.task_id)
        if node is None:
            node = self._new_task_node(message)
        node = node.model_copy(
            update={
                "attributes": {
                    **cast(dict[str, Any], node.attributes),
                    "last_tool_name": message.last_tool_name,
                    "usage": _capture(message.usage),
                }
            }
        )
        self._task_nodes[message.task_id] = node
        await self._persist(node)

    async def _record_task_notification(self, message: TaskNotificationMessage) -> None:
        node = self._task_nodes.get(message.task_id)
        if node is None:
            node = self._new_task_node(message)
        failed = message.status in {"failed", "stopped"}
        node = node.model_copy(
            update={
                "status": NodeStatus.FAILED if failed else NodeStatus.COMPLETED,
                "error": message.status if failed else None,
                "ended_at": datetime.now(UTC),
                "outputs": {"summary": _capture(message.summary)},
                "attributes": {
                    **cast(dict[str, Any], node.attributes),
                    "usage": _capture(message.usage),
                },
            }
        )
        self._task_nodes[message.task_id] = node
        await self._persist(node)

    def _new_task_node(
        self, message: TaskProgressMessage | TaskNotificationMessage
    ) -> SessionNodeCreateRequest:
        node = self._new_node(
            node_type=NodeType.SUBAGENT_CALL,
            name=message.description
            if isinstance(message, TaskProgressMessage)
            else "task",
            parent_index=self._parent_for(message.tool_use_id),
            external_id=message.task_id,
            status=NodeStatus.IN_PROGRESS,
            inputs=None,
            outputs=None,
            subagent_id=message.task_id,
            attributes={"orphaned": True},
        )
        return node

    async def _record_framework_event(
        self,
        name: str,
        *,
        external_id: str | None,
        attributes: dict[str, Any],
        parent_index: int = 0,
    ) -> None:
        await self._persist(
            self._new_node(
                node_type=NodeType.SPAN,
                name=name,
                parent_index=parent_index,
                external_id=external_id,
                status=NodeStatus.COMPLETED,
                inputs=None,
                outputs=None,
                attributes=attributes,
            )
        )

    def _new_node(self, **values: Any) -> SessionNodeCreateRequest:
        index = self.next_index
        self.next_index += 1
        now = datetime.now(UTC)
        values.setdefault("started_at", now)
        if values.get("status") is not NodeStatus.IN_PROGRESS:
            values.setdefault("ended_at", now)
        return SessionNodeCreateRequest(index=index, **values)

    def _parent_for(self, external_id: str | None) -> int:
        if external_id is None:
            return 0
        node = self._tool_nodes.get(external_id) or self._task_nodes.get(external_id)
        return node.index if node is not None else 0

    async def _persist(self, node: SessionNodeCreateRequest) -> None:
        await self.client.sessions.ingest_nodes(
            self.session_id, SessionNodeBatchRequest(nodes=[node])
        )


async def _run_finalization(
    recorder: InvocationRecorder,
    *,
    terminal: ResultMessage | None = None,
    error: BaseException | None = None,
) -> BaseException | None:
    """Bound persistence while preserving caller cancellation."""
    task = asyncio.create_task(recorder.finalize(terminal=terminal, error=error))
    try:
        await asyncio.wait_for(
            asyncio.shield(task), timeout=FINALIZATION_TIMEOUT_SECONDS
        )
    except asyncio.CancelledError:
        await _cancel_and_join_finalization_task(task)
        raise
    except TimeoutError as failure:
        await _cancel_and_join_finalization_task(task)
        return failure
    except Exception as failure:
        return failure
    return None


async def _cancel_and_join_finalization_task(task: asyncio.Task[None]) -> None:
    """Cancel persistence and observe it for one bounded interval."""
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


async def finalize_terminal(
    recorder: InvocationRecorder, terminal: ResultMessage
) -> None:
    """Finalize before yielding a successful native terminal message."""
    failure = await _run_finalization(recorder, terminal=terminal)
    if failure is not None:
        raise KitaruRecordingError(
            terminal_message=terminal,
            session_id=recorder.session_id,
            phase="finalize",
        ) from failure


async def finalize_failure(
    recorder: InvocationRecorder, error: BaseException
) -> BaseException | None:
    """Finalize a native failure and return any secondary recording failure."""
    return await _run_finalization(recorder, error=error)


__all__ = [
    "InvocationRecorder",
    "ResolvedRunInput",
    "finalize_failure",
    "finalize_terminal",
    "resolve_run_input",
]
