#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Invocation-local Kitaru recording lifecycle for LangGraph."""

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Coroutine
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
from typing import Any, Protocol, TypeVar

from langgraph.types import GraphOutput

from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.replay_config import ReplayOverride
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.task import AgentTaskDetails
from kitaru.client import KitaruAPIClient

from .capability import UnsupportedWorkerInterruptError
from .capture import CaptureBudget, CapturePolicy, capture_execution_view, capture_value

ADAPTER_VERSION = version("kitaru-langgraph")
FRAMEWORK = "langgraph"
_LOGGER = logging.getLogger(__name__)


def _error_text(error: BaseException) -> str:
    """Return a useful stored error message with a class-name fallback."""
    return str(error) or type(error).__name__


@dataclass(frozen=True)
class RecordingFailure:
    """Safe diagnostic for the first contained adapter failure."""

    stage: str
    exception_class: str


@dataclass(frozen=True)
class PendingRun:
    """Public callback data retained until a run completes."""

    index: int
    parent_index: int
    name: str
    started_at: datetime
    inputs: Any
    node_type: NodeType
    inputs_lossy: bool = False


BridgeResultT = TypeVar("BridgeResultT")


class SyncBridge(Protocol):
    """Minimal bridge contract used by synchronous middleware."""

    def run(self, coroutine: Coroutine[Any, Any, BridgeResultT]) -> BridgeResultT: ...


@dataclass
class InvocationRecorder:
    """Mutable recording state isolated to one graph invocation."""

    client: KitaruAPIClient
    policy: CapturePolicy
    batch_size: int
    task_id: uuid.UUID | None
    replay: ReplayResponse | None
    override: ReplayOverride | None
    effective_input: Any
    captured_input: Any
    config_view: Any
    session_id: uuid.UUID
    started_at: datetime
    budget: CaptureBudget
    next_index: int = 1
    history_occurrences: dict[str, int] = field(default_factory=dict)
    buffer: list[tuple[SessionNodeCreateRequest, int]] = field(default_factory=list)
    run_indexes: dict[uuid.UUID, int] = field(default_factory=dict)
    pending_runs: dict[uuid.UUID, PendingRun] = field(default_factory=dict)
    failure: RecordingFailure | None = None
    writes_disabled: bool = False
    finished: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    flush_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    sync_bridge: SyncBridge | None = None

    @classmethod
    async def setup(
        cls,
        caller_input: Any,
        config: dict[str, Any] | None,
        *,
        agent_id: uuid.UUID | None,
        agent_version_id: uuid.UUID | None,
        session_name: str | None,
        batch_size: int,
        policy: CapturePolicy,
    ) -> "InvocationRecorder":
        """Resolve execution context and persist the root before delegation."""
        client = KitaruAPIClient()
        session_id: uuid.UUID | None = None
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
                task = await client.tasks.get_spec(task_id)
                if not isinstance(task.details, AgentTaskDetails):
                    raise RuntimeError(f"Task {task_id} is not an agent task")
                effective_input = task.details.inputs
            else:
                effective_input = caller_input
            input_capture = capture_value(effective_input, policy)
            config_capture = capture_execution_view(config, policy)
            started_at = datetime.now(UTC)
            session = await client.sessions.create(
                SessionCreateRequest(
                    agent_id=agent_id,
                    agent_version_id=agent_version_id,
                    origin=(
                        SessionOrigin.REPLAY
                        if replay is not None
                        else SessionOrigin.RECORDED
                    ),
                    name=session_name,
                    inputs=input_capture.value,
                    outputs=None,
                    started_at=started_at,
                    metadata={"execution": config_capture.value},
                    framework=FRAMEWORK,
                    adapter_version=ADAPTER_VERSION,
                )
            )
            session_id = session.id
            await client.sessions.ingest_nodes(
                session.id,
                SessionNodeBatchRequest(
                    nodes=[
                        SessionNodeCreateRequest(
                            index=0,
                            parent_index=None,
                            node_type=NodeType.SPAN,
                            name="invoke",
                            status=NodeStatus.IN_PROGRESS,
                            started_at=started_at,
                            inputs=input_capture.value,
                            outputs=None,
                            attributes={"execution": config_capture.value},
                        )
                    ]
                ),
            )
        except BaseException as error:
            if session_id is not None:
                with suppress(BaseException):
                    await client.sessions.update(
                        session_id,
                        SessionUpdateRequest(
                            status=SessionStatus.FAILED,
                            error=type(error).__name__,
                            ended_at=datetime.now(UTC),
                        ),
                    )
            with suppress(BaseException):
                await client.close()
            raise
        return cls(
            client=client,
            policy=policy,
            batch_size=batch_size,
            task_id=task_id,
            replay=replay,
            override=replay.override if replay is not None else None,
            effective_input=effective_input,
            captured_input=input_capture.value,
            config_view=config_capture.value,
            session_id=session.id,
            started_at=started_at,
            budget=CaptureBudget(policy),
        )

    def latch(self, stage: str, error: BaseException) -> None:
        """Remember the first recording failure and stop ordinary writes."""
        if self.failure is None:
            self.failure = RecordingFailure(stage, type(error).__name__)
        self.writes_disabled = True

    async def allocate_index(self) -> int | None:
        """Allocate one bounded child index."""
        async with self.lock:
            if self.writes_disabled or not self.budget.reserve_node():
                return None
            index = self.next_index
            self.next_index += 1
            return index

    async def buffer_node(self, node: SessionNodeCreateRequest) -> None:
        """Buffer one node and contain any post-delegation persistence failure."""
        if self.writes_disabled:
            return
        try:
            size = len(node.model_dump_json().encode())
            should_flush = False
            async with self.lock:
                if not self.budget.reserve_bytes(size):
                    return
                self.buffer.append((node, size))
                should_flush = len(self.buffer) >= self.batch_size
            if should_flush:
                await self._flush_buffer()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self.latch("node_buffer", error)

    async def persist_node(self, node: SessionNodeCreateRequest) -> None:
        """Persist an ancestor immediately so children can safely refer to it."""
        if self.writes_disabled:
            return
        try:
            await self.client.sessions.ingest_nodes(
                self.session_id, SessionNodeBatchRequest(nodes=[node])
            )
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self.latch("ancestor_persist", error)

    async def start_chain(
        self,
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None,
        name: str,
        inputs: Any,
    ) -> None:
        """Map the outer graph to root or persist a nested public ancestor."""
        if self.writes_disabled:
            return
        if parent_run_id is None and not self.run_indexes:
            self.run_indexes[run_id] = 0
            return
        index = await self.allocate_index()
        if index is None:
            return
        parent_index = self.run_indexes.get(parent_run_id, 0)
        started_at = datetime.now(UTC)
        captured = capture_value(inputs, self.policy)
        self.run_indexes[run_id] = index
        self.pending_runs[run_id] = PendingRun(
            index=index,
            parent_index=parent_index,
            name=name,
            started_at=started_at,
            inputs=captured.value,
            node_type=NodeType.SPAN,
            inputs_lossy=captured.lossy,
        )
        await self.persist_node(
            SessionNodeCreateRequest(
                index=index,
                parent_index=parent_index,
                external_id=str(run_id),
                node_type=NodeType.SPAN,
                name=name,
                status=NodeStatus.IN_PROGRESS,
                started_at=started_at,
                inputs=captured.value,
                outputs=None,
                attributes={},
            )
        )

    async def finish_chain(
        self, *, run_id: uuid.UUID, outputs: Any, error: BaseException | None
    ) -> None:
        """Update one nested public ancestor when it completes."""
        pending = self.pending_runs.pop(run_id, None)
        if pending is None or pending.index == 0 or self.writes_disabled:
            return
        output_capture = capture_value(outputs, self.policy)
        await self.persist_node(
            SessionNodeCreateRequest(
                index=pending.index,
                parent_index=pending.parent_index,
                external_id=str(run_id),
                node_type=NodeType.SPAN,
                name=pending.name,
                status=NodeStatus.FAILED if error is not None else NodeStatus.COMPLETED,
                error=_error_text(error) if error is not None else None,
                started_at=pending.started_at,
                ended_at=datetime.now(UTC),
                inputs=pending.inputs,
                outputs=output_capture.value if error is None else None,
                attributes={},
            )
        )

    async def start_call(
        self,
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None,
        name: str,
        inputs: Any,
        node_type: NodeType,
    ) -> None:
        """Reserve one observable model or tool call."""
        if run_id in self.pending_runs:
            return
        index = await self.allocate_index()
        if index is None:
            return
        captured = capture_value(inputs, self.policy)
        self.pending_runs[run_id] = PendingRun(
            index=index,
            parent_index=self.run_indexes.get(parent_run_id, 0),
            name=name,
            started_at=datetime.now(UTC),
            inputs=captured.value,
            node_type=node_type,
            inputs_lossy=captured.lossy,
        )

    async def finish_call(
        self,
        *,
        run_id: uuid.UUID,
        outputs: Any,
        error: BaseException | None,
    ) -> None:
        """Buffer one completed observable model or tool call."""
        pending = self.pending_runs.pop(run_id, None)
        if pending is None or self.writes_disabled:
            return
        if pending.node_type is NodeType.TOOL_CALL:
            from langchain_core.messages import ToolMessage
            from langgraph.types import Command

            from .codec import encode_tool_outcome

            if isinstance(outputs, (ToolMessage, Command)):
                output_value = encode_tool_outcome(outputs, policy=self.policy)
                if pending.inputs_lossy:
                    output_value["replayable"] = False
                    reasons = list(output_value.get("loss_reasons", []))
                    if "lossy_tool_arguments" not in reasons:
                        reasons.append("lossy_tool_arguments")
                    output_value["loss_reasons"] = reasons
                output = output_value
            else:
                output = capture_value(outputs, self.policy).value
        else:
            output = capture_value(outputs, self.policy).value
        await self.buffer_node(
            SessionNodeCreateRequest(
                index=pending.index,
                parent_index=pending.parent_index,
                external_id=str(run_id),
                node_type=pending.node_type,
                name=pending.name,
                status=NodeStatus.FAILED if error is not None else NodeStatus.COMPLETED,
                error=_error_text(error) if error is not None else None,
                started_at=pending.started_at,
                ended_at=datetime.now(UTC),
                inputs=pending.inputs,
                outputs=output if error is None else None,
                tool_name=(
                    pending.name if pending.node_type is NodeType.TOOL_CALL else None
                ),
                attributes={},
            )
        )

    async def record_tool_substitution(
        self,
        *,
        tool_call_id: str,
        tool_name: str,
        arguments: Any,
        result: Any,
        policy_name: str,
        error: BaseException | None = None,
    ) -> None:
        """Record a middleware short-circuit that emits no live callback."""
        index = await self.allocate_index()
        if index is None:
            return
        input_capture = capture_value(arguments, self.policy)
        if error is None:
            from langchain_core.messages import ToolMessage
            from langgraph.types import Command

            from .codec import encode_tool_outcome

            output = (
                encode_tool_outcome(result, policy=self.policy)
                if isinstance(result, (ToolMessage, Command))
                else capture_value(result, self.policy).value
            )
        else:
            output = None
        await self.buffer_node(
            SessionNodeCreateRequest(
                index=index,
                parent_index=0,
                external_id=tool_call_id,
                node_type=NodeType.TOOL_CALL,
                name=tool_name,
                status=NodeStatus.FAILED if error is not None else NodeStatus.COMPLETED,
                error=_error_text(error) if error is not None else None,
                started_at=datetime.now(UTC),
                ended_at=datetime.now(UTC),
                inputs=input_capture.value,
                outputs=output,
                tool_name=tool_name,
                attributes={"mocked": True, "policy": policy_name},
            )
        )

    async def _flush_buffer(self) -> None:
        """Flush buffered nodes without holding the mutation lock over I/O."""
        async with self.flush_lock:
            async with self.lock:
                if not self.buffer or self.writes_disabled:
                    return
                items = self.buffer
                self.buffer = []
            await self.client.sessions.ingest_nodes(
                self.session_id,
                SessionNodeBatchRequest(nodes=[node for node, _ in items]),
            )
            async with self.lock:
                self.budget.release_bytes(sum(size for _, size in items))

    async def finalize(
        self, *, result: Any = None, error: BaseException | None = None
    ) -> None:
        """Finish the root and session without replacing the graph outcome."""
        if self.finished:
            return
        self.finished = True
        ended_at = datetime.now(UTC)
        interrupted = _has_interrupt(result)
        output_capture = capture_value(result, self.policy)
        error_text = _error_text(error) if error is not None else None
        node_status = NodeStatus.FAILED if error is not None else NodeStatus.COMPLETED
        session_status = (
            SessionStatus.FAILED if error is not None else SessionStatus.COMPLETED
        )
        if interrupted and self.task_id is not None:
            node_status = NodeStatus.FAILED
            session_status = SessionStatus.FAILED
            error_text = UnsupportedWorkerInterruptError.__name__
        metadata: dict[str, Any] = {
            "interrupted": interrupted,
            "recording_truncated": (
                output_capture.truncated
                or self.budget.dropped_nodes > 0
                or self.budget.dropped_bytes > 0
            ),
            "dropped_nodes": self.budget.dropped_nodes,
            "dropped_bytes": self.budget.dropped_bytes,
        }
        if self.failure is None:
            try:
                root = SessionNodeCreateRequest(
                    index=0,
                    parent_index=None,
                    node_type=NodeType.SPAN,
                    name="invoke",
                    status=node_status,
                    error=error_text,
                    started_at=self.started_at,
                    ended_at=ended_at,
                    inputs=self.captured_input,
                    outputs=output_capture.value if error is None else None,
                    attributes={"execution": self.config_view},
                    metadata=metadata,
                )
                await self._flush_buffer()
                await self.client.sessions.ingest_nodes(
                    self.session_id, SessionNodeBatchRequest(nodes=[root])
                )
                await self.client.sessions.update(
                    self.session_id,
                    SessionUpdateRequest(
                        status=session_status,
                        outputs=output_capture.value if error is None else None,
                        error=error_text,
                        ended_at=ended_at,
                        metadata=metadata,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as recording_error:
                self.latch("finalize", recording_error)
        if self.failure is not None:
            await self._mark_incomplete(ended_at)
        await self._close()
        if self.failure is not None:
            self.warn(graph_succeeded=error is None)

    async def _close(self) -> None:
        """Close the invocation client without absorbing task cancellation."""
        try:
            await self.client.close()
        except asyncio.CancelledError:
            raise
        except BaseException as close_error:
            self.latch("close", close_error)

    async def _mark_incomplete(self, ended_at: datetime) -> None:
        """Attempt one bounded marker after ordinary recording has stopped."""
        try:
            await self.client.sessions.update(
                self.session_id,
                SessionUpdateRequest(
                    status=SessionStatus.FAILED,
                    error="RecordingIncomplete",
                    ended_at=ended_at,
                    metadata={
                        "recording_incomplete": True,
                        "recording_truncated": True,
                    },
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException:
            return

    def warn(self, *, graph_succeeded: bool) -> None:
        """Attempt one private warning containing only safe structured fields."""
        failure = self.failure
        if failure is None:
            return
        with suppress(BaseException):
            _LOGGER.warning(
                "Kitaru LangGraph recording incomplete",
                extra={
                    "kitaru_stage": failure.stage,
                    "kitaru_exception_class": failure.exception_class,
                    "kitaru_graph_succeeded": graph_succeeded,
                    "kitaru_session_id": str(self.session_id),
                },
            )


def _has_interrupt(result: Any) -> bool:
    """Detect only locked public LangGraph interrupt return forms."""
    if isinstance(result, GraphOutput):
        return bool(result.interrupts)
    return isinstance(result, dict) and bool(result.get("__interrupt__"))


_ACTIVE_INVOCATION: ContextVar[InvocationRecorder | None] = ContextVar(
    "kitaru_langgraph_invocation", default=None
)


def get_active_invocation() -> InvocationRecorder | None:
    """Return invocation state only while the upstream graph is executing."""
    return _ACTIVE_INVOCATION.get()
