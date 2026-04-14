from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

from pydantic_ai.exceptions import ApprovalRequired, CallDeferred
from pydantic_ai.tools import AgentDepsT, RunContext
from pydantic_ai.toolsets import (
    AbstractToolset,
    FunctionToolset,
    ToolsetTool,
    WrapperToolset,
)
from pydantic_core import to_jsonable_python

import kitaru

from ._events import DeferredKind, ToolsetKind
from ._hitl import HitlConfig, hitl_config_from_tool_metadata
from ._kitaru_internal import (
    is_inside_checkpoint,
    is_inside_flow,
    suspend_checkpoint_scope,
)
from ._otel import attach_tool_correlation
from ._policy import CapturePolicy
from ._tracking import EventTracker, artifact_name, get_current_tracker


class _ToolApprovalDenied(Exception):
    """Raised when a human denies a HITL approval request."""


def _json_safe(value: Any) -> Any:
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError:  # circular reference
        return {"repr": repr(value), "python_type": value.__class__.__name__}


@dataclass(frozen=True)
class _DeferredRequest:
    kind: DeferredKind
    wait_name: str
    schema: Any
    question: str | None
    exception_metadata: dict[str, Any] | None
    run_after_wait: bool


@dataclass
class KitaruToolset(WrapperToolset[AgentDepsT]):
    """Wraps any toolset so Kitaru tracks tool calls and routes HITL waits."""

    toolset_kind: ToolsetKind = "generic"
    capture: CapturePolicy = field(default_factory=CapturePolicy)

    @property
    def id(self) -> str | None:
        return self.wrapped.id

    def visit_and_replace(
        self,
        visitor: Callable[[AbstractToolset[AgentDepsT]], AbstractToolset[AgentDepsT]],
    ) -> AbstractToolset[AgentDepsT]:
        return self

    async def for_run(self, ctx: RunContext[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
        new_wrapped = await self.wrapped.for_run(ctx)
        if new_wrapped is self.wrapped:
            return self
        return replace(self, wrapped=new_wrapped)

    async def for_run_step(
        self, ctx: RunContext[AgentDepsT]
    ) -> AbstractToolset[AgentDepsT]:
        new_wrapped = await self.wrapped.for_run_step(ctx)
        if new_wrapped is self.wrapped:
            return self
        return replace(self, wrapped=new_wrapped)

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
    ) -> Any:
        tracker = get_current_tracker()
        capture_mode = self.capture.capture_mode_for_tool(name)
        hitl_config = hitl_config_from_tool_metadata(tool.tool_def.metadata)

        # HITL interception runs whenever we're inside a flow (so kitaru.wait
        # is callable); tracking additionally requires a tracker, capture on,
        # and an enclosing checkpoint.
        intercept_hitl = is_inside_flow()
        should_track = (
            tracker is not None and capture_mode is not None and is_inside_checkpoint()
        )

        if not intercept_hitl and not should_track:
            return await super().call_tool(name, tool_args, ctx, tool)

        if not should_track:
            return await self._call_with_hitl(
                name=name,
                tool_args=tool_args,
                safe_args=_json_safe(tool_args) if hitl_config is not None else None,
                ctx=ctx,
                tool=tool,
                hitl_config=hitl_config,
                tracker=None,
            )

        assert tracker is not None and capture_mode is not None
        event_id, event_context = tracker.start_tool_event()
        if self.capture.correlate_otel_spans:
            attach_tool_correlation(event_id, event_context)

        safe_args = (
            _json_safe(tool_args)
            if capture_mode == "full" or hitl_config is not None
            else None
        )

        artifacts: dict[str, str] = {}
        if capture_mode == "full":
            args_key = artifact_name(event_id, "args")
            kitaru.save(args_key, safe_args, type="input")
            artifacts["args"] = args_key

        started_at = time.perf_counter()
        try:
            result = await self._call_with_hitl(
                name=name,
                tool_args=tool_args,
                safe_args=safe_args,
                ctx=ctx,
                tool=tool,
                hitl_config=hitl_config,
                tracker=tracker,
            )
        except BaseException as error:
            tracker.record_tool_event(
                event_id,
                event_context,
                status="failed",
                name=name,
                toolset_kind=self.toolset_kind,
                capture_mode=capture_mode,
                duration_ms=_elapsed_ms(started_at),
                hitl=hitl_config is not None,
                artifacts=artifacts,
                error=error,
            )
            raise

        duration_ms = _elapsed_ms(started_at)
        if capture_mode == "full":
            result_key = artifact_name(event_id, "result")
            kitaru.save(result_key, _json_safe(result), type="output")
            artifacts["result"] = result_key

        tracker.record_tool_event(
            event_id,
            event_context,
            status="completed",
            name=name,
            toolset_kind=self.toolset_kind,
            capture_mode=capture_mode,
            duration_ms=duration_ms,
            hitl=hitl_config is not None,
            artifacts=artifacts,
        )
        return result

    async def _call_with_hitl(
        self,
        *,
        name: str,
        tool_args: dict[str, Any],
        safe_args: Any,
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        hitl_config: HitlConfig | None,
        tracker: EventTracker | None,
    ) -> Any:
        if hitl_config is not None:
            request = _DeferredRequest(
                kind="hitl",
                wait_name=hitl_config.name or name,
                schema=hitl_config.schema,
                question=hitl_config.question,
                exception_metadata=None,
                run_after_wait=False,
            )
            return await self._handle_deferred(
                request,
                name=name,
                tool_args=tool_args,
                safe_args=safe_args,
                ctx=ctx,
                tool=tool,
                tracker=tracker,
            )

        try:
            return await super().call_tool(name, tool_args, ctx, tool)
        except ApprovalRequired as error:
            request = _DeferredRequest(
                kind="approval_required",
                wait_name=f"approve_{name}",
                schema=bool,
                question=f"Approve tool call: {name}?",
                exception_metadata=error.metadata,
                run_after_wait=True,
            )
        except CallDeferred as error:
            request = _DeferredRequest(
                kind="call_deferred",
                wait_name=f"defer_{name}",
                schema=None,
                question=f"Provide result for deferred tool: {name}",
                exception_metadata=error.metadata,
                run_after_wait=False,
            )
        return await self._handle_deferred(
            request,
            name=name,
            tool_args=tool_args,
            safe_args=safe_args,
            ctx=ctx,
            tool=tool,
            tracker=tracker,
        )

    async def _handle_deferred(
        self,
        request: _DeferredRequest,
        *,
        name: str,
        tool_args: dict[str, Any],
        safe_args: Any,
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        tracker: EventTracker | None,
    ) -> Any:
        if safe_args is None:
            safe_args = _json_safe(tool_args)
        wait_metadata = self._wait_metadata(
            name=name,
            safe_args=safe_args,
            ctx=ctx,
            exception_metadata=request.exception_metadata,
        )
        wait_value = self._invoke_wait(
            schema=request.schema,
            wait_name=request.wait_name,
            question=request.question,
            metadata=wait_metadata,
        )
        approved = bool(wait_value) if request.schema is bool else None
        if tracker is not None:
            tracker.record_deferred_event(
                tool_name=name,
                deferred_kind=request.kind,
                wait_name=request.wait_name,
                metadata={"exception_metadata": request.exception_metadata}
                if request.exception_metadata
                else None,
                approved=approved,
            )
        if approved is False:
            raise _ToolApprovalDenied(f"Tool {name!r} was not approved.")
        if not request.run_after_wait:
            return wait_value
        approved_ctx = replace(
            ctx, tool_call_approved=True, tool_call_metadata=request.exception_metadata
        )
        return await super().call_tool(name, tool_args, approved_ctx, tool)

    def _wait_metadata(
        self,
        *,
        name: str,
        safe_args: Any,
        ctx: RunContext[AgentDepsT],
        exception_metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "adapter": "pydantic_ai",
            "tool_name": name,
            "tool_call_id": ctx.tool_call_id,
            "tool_args": safe_args,
        }
        if exception_metadata:
            metadata["exception_metadata"] = _json_safe(exception_metadata)
        return metadata

    def _invoke_wait(
        self,
        *,
        schema: Any,
        wait_name: str,
        question: str | None,
        metadata: dict[str, Any],
    ) -> Any:
        with suspend_checkpoint_scope():
            return kitaru.wait(
                schema=schema, name=wait_name, question=question, metadata=metadata
            )


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 3)


def kitaruify_toolset(
    toolset: AbstractToolset[AgentDepsT],
    *,
    capture: CapturePolicy,
) -> AbstractToolset[AgentDepsT]:
    if isinstance(toolset, KitaruToolset):
        return toolset

    if isinstance(toolset, FunctionToolset):
        from ._function_toolset import KitaruFunctionToolset

        return KitaruFunctionToolset(toolset, capture=capture)

    try:
        from pydantic_ai.mcp import MCPServer
    except ImportError:  # pragma: no cover
        return KitaruToolset(toolset, toolset_kind="generic", capture=capture)

    kind: ToolsetKind = "mcp" if isinstance(toolset, MCPServer) else "generic"
    return KitaruToolset(toolset, toolset_kind=kind, capture=capture)
