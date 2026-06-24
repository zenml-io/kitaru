from __future__ import annotations

import asyncio
import inspect
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any
from typing import get_type_hints
import kitaru
from pydantic_core import to_jsonable_python

from pydantic_ai.exceptions import ApprovalRequired, CallDeferred
from pydantic_ai.tools import AgentDepsT, RunContext
from pydantic_ai.toolsets import (
    AbstractToolset,
    FunctionToolset,
    ToolsetTool,
    WrapperToolset,
)
from pydantic_ai.toolsets.function import FunctionToolsetTool

from kitaru.errors import KitaruContextError, KitaruUsageError
from kitaru.replay_context import resolve_tool_override
from kitaru.runtime import _suspend_checkpoint_scope
from kitaru.wait import _WAIT_INSIDE_CHECKPOINT_ERROR

from ._constants import (
    ADAPTER_ID,
    ADAPTER_METADATA_KEY,
    ARTIFACT_ROLE_ARGS,
    ARTIFACT_ROLE_RESULT,
    ARTIFACT_SLOT_OUTPUT,
    ARTIFACT_SLOT_TOOL_ARGS,
)
from ._events import DeferredKind, ToolsetKind
from ._hitl import HitlConfig, hitl_config_from_tool_metadata, resolve_hitl_question
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._logging import logger
from ._otel import attach_tool_correlation
from ._policy import CapturePolicy
from ._tracking import EventTracker, get_current_tracker
from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverrides,
    adapter_checkpoint_artifact_refs,
    checkpoint_cache_key,
    checkpoint_input_value,
    get_adapter_checkpoint_artifact_refs,
    get_adapter_streaming_fallback_checkpoint,
    has_explicit_tool_checkpoint_opt_out,
    resolve_tool_checkpoint_config,
    run_async_in_checkpoint,
    suspend_adapter_streaming_fallback_checkpoint,
    with_default_type,
)


class _ToolApprovalDenied(Exception):
    """Raised when a human denies a HITL approval request."""


def _json_safe(value: Any) -> Any:
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError:  # circular reference
        logger.warning(
            "Failed to JSON-serialize adapter payload; falling back to repr.",
            exc_info=True,
        )
        return {
            "repr": repr(value),
            "python_type": value.__class__.__name__,
            "serialization_error": "json_safe_failed",
        }


def _is_ordinary_sync_function_tool(tool: ToolsetTool[Any]) -> bool:
    """Return whether ``tool`` is a plain sync PydanticAI function tool."""
    return isinstance(tool, FunctionToolsetTool) and tool.is_async is False


def _raise_checkpoint_wait_not_supported(tool_name: str, kind: DeferredKind) -> None:
    raise KitaruUsageError(
        f"PydanticAI tool {tool_name!r} requested {kind!r} human input while "
        "running inside a checkpoint. Kitaru waits must be created at flow "
        "scope, not from checkpoint scope. Use `@hitl_tool` for pure wait "
        "tools. For ordinary tool bodies, disable checkpointing for this tool "
        'with `tool_checkpoint_config_by_name={"tool_name": False}` while '
        "running the agent in granular mode. If the ordinary sync tool body "
        "calls `kp.wait_for_input(...)` directly, also pass "
        "`allow_sync_tool_body_waits=True` so supported sync tool bodies run "
        "on the workflow thread for the whole agent run. Or move the wait "
        "before/after the agent call in flow code."
    )


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
    """Wraps any toolset so Kitaru tracks tool calls and routes HITL waits through the flow."""

    toolset_kind: ToolsetKind = "generic"
    capture: CapturePolicy = field(default_factory=CapturePolicy)
    # Granular-mode: when set, each ``call_tool`` opens its own @kitaru.checkpoint.
    # Per-name overrides can replace or disable (`False`) per-tool.
    tool_checkpoint_config: CheckpointConfig | None = None
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None
    _default_checkpoint_type: str = field(default="tool_call", init=False)

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

    def _should_use_tool_checkpoint(
        self,
        *,
        name: str,
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        checkpoint_config: CheckpointConfig | None,
    ) -> bool:
        """Return whether this tool call should open an adapter checkpoint."""
        del name, ctx, tool
        return (
            checkpoint_config is not None
            and is_inside_flow()
            and not is_inside_checkpoint()
        )

    def _should_suspend_streaming_fallback_checkpoint_scope(
        self,
        *,
        name: str,
        tool: ToolsetTool[AgentDepsT],
    ) -> bool:
        """Return whether an opted-out sync tool may run at flow scope.

        PydanticAI may execute several tool calls concurrently. This helper stays
        intentionally narrow: it is used only under Kitaru's private marker for a
        streamed calls-strategy fallback turn, and only for function tools whose
        sync body is forced inline by ``allow_sync_tool_body_waits=True``.
        """
        marker = get_adapter_streaming_fallback_checkpoint()
        return (
            marker is not None
            and marker.allow_sync_tool_body_waits
            and is_inside_checkpoint()
            and has_explicit_tool_checkpoint_opt_out(
                name, self.tool_checkpoint_config_by_name
            )
            and _is_ordinary_sync_function_tool(tool)
        )

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
    ) -> Any:
        hitl_config = hitl_config_from_tool_metadata(tool.tool_def.metadata)
        if hitl_config is not None:
            return await self._call_tool_tracked(
                name, tool_args, ctx, tool, hitl_config
            )

        checkpoint_config = resolve_tool_checkpoint_config(
            name,
            default=self.tool_checkpoint_config,
            by_name=self.tool_checkpoint_config_by_name,
        )
        if self._should_use_tool_checkpoint(
            name=name,
            ctx=ctx,
            tool=tool,
            checkpoint_config=checkpoint_config,
        ):
            assert checkpoint_config is not None
            capture_mode = self.capture.capture_mode_for_tool(name)
            input_artifacts: dict[str, str] = {}
            output_artifacts: dict[str, str] = {}
            checkpoint_inputs: dict[str, Any] = {}
            safe_args = _json_safe(tool_args) if capture_mode == "full" else None
            if capture_mode == "full":
                input_artifacts[ARTIFACT_ROLE_ARGS] = ARTIFACT_SLOT_TOOL_ARGS
                output_artifacts[ARTIFACT_ROLE_RESULT] = ARTIFACT_SLOT_OUTPUT
                checkpoint_inputs[ARTIFACT_SLOT_TOOL_ARGS] = checkpoint_input_value(
                    safe_args
                )

            async def _in_checkpoint() -> Any:
                with adapter_checkpoint_artifact_refs(
                    input_artifacts=input_artifacts,
                    output_artifacts=output_artifacts,
                ):
                    return await self._call_tool_tracked(
                        name,
                        tool_args,
                        ctx,
                        tool,
                        hitl_config,
                        safe_args=safe_args,
                    )

            return await run_async_in_checkpoint(
                config=with_default_type(
                    checkpoint_config, self._default_checkpoint_type
                ),
                step_name=f"{name}_tool",
                body=_in_checkpoint,
                cache_key=checkpoint_cache_key(
                    {
                        "tool_name": name,
                        ARTIFACT_SLOT_TOOL_ARGS: safe_args
                        if capture_mode == "full"
                        else tool_args,
                        "tool_call_id": ctx.tool_call_id,
                        "retry": ctx.retry,
                    }
                ),
                checkpoint_inputs=checkpoint_inputs,
            )
        return await self._call_tool_tracked(name, tool_args, ctx, tool, hitl_config)

    async def _call_tool_tracked(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        hitl_config: HitlConfig | None,
        *,
        safe_args: Any | None = None,
    ) -> Any:
        capture_mode = self.capture.capture_mode_for_tool(name)
        suspend_tool_body_checkpoint_scope = (
            hitl_config is None
            and self._should_suspend_streaming_fallback_checkpoint_scope(
                name=name,
                tool=tool,
            )
        )
        tracker = get_current_tracker()
        if tracker is None or capture_mode is None:
            return await self._call_with_hitl(
                name=name,
                tool_args=tool_args,
                safe_args=None,
                ctx=ctx,
                tool=tool,
                hitl_config=hitl_config,
                tracker=tracker,
                capture_mode=capture_mode,
                suspend_tool_body_checkpoint_scope=(
                    suspend_tool_body_checkpoint_scope
                ),
            )

        event_id, event_context = tracker.start_tool_event(
            tool_call_id=ctx.tool_call_id,
        )
        if self.capture.correlate_otel_spans:
            attach_tool_correlation(event_id, event_context)

        if capture_mode != "full":
            safe_args = None
        elif safe_args is None:
            safe_args = _json_safe(tool_args)

        artifacts: dict[str, str] = {}
        adapter_refs = get_adapter_checkpoint_artifact_refs()
        if (
            capture_mode == "full"
            and is_inside_checkpoint()
            and not suspend_tool_body_checkpoint_scope
        ):
            if (
                adapter_refs is not None
                and ARTIFACT_ROLE_ARGS in adapter_refs.input_artifacts
            ):
                artifacts[ARTIFACT_ROLE_ARGS] = adapter_refs.input_artifacts[
                    ARTIFACT_ROLE_ARGS
                ]
            else:
                args_key = tracker.artifact_name(event_id, ARTIFACT_ROLE_ARGS)
                kitaru.save(args_key, safe_args, type="input")
                artifacts[ARTIFACT_ROLE_ARGS] = args_key

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
                capture_mode=capture_mode,
                suspend_tool_body_checkpoint_scope=(
                    suspend_tool_body_checkpoint_scope
                ),
            )
        except Exception as error:
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
        if (
            capture_mode == "full"
            and is_inside_checkpoint()
            and not suspend_tool_body_checkpoint_scope
        ):
            if (
                adapter_refs is not None
                and ARTIFACT_ROLE_RESULT in adapter_refs.output_artifacts
            ):
                artifacts[ARTIFACT_ROLE_RESULT] = adapter_refs.output_artifacts[
                    ARTIFACT_ROLE_RESULT
                ]
            else:
                result_key = tracker.artifact_name(event_id, ARTIFACT_ROLE_RESULT)
                kitaru.save(result_key, _json_safe(result), type="output")
                artifacts[ARTIFACT_ROLE_RESULT] = result_key

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
        capture_mode: str | None,
        suspend_tool_body_checkpoint_scope: bool,
    ) -> Any:
        def _call_suffix() -> str:
            return _wait_call_suffix(getattr(ctx, "tool_call_id", None))

        if hitl_config is not None:
            call_suffix = _call_suffix()
            request = _DeferredRequest(
                kind="hitl",
                wait_name=f"{hitl_config.name or name}_{call_suffix}",
                schema=hitl_config.schema,
                question=resolve_hitl_question(hitl_config, tool_args),
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
                capture_mode=capture_mode,
            )

        try:
            return await self._call_wrapped_tool(
                name=name,
                tool_args=tool_args,
                ctx=ctx,
                tool=tool,
                suspend_checkpoint_scope=suspend_tool_body_checkpoint_scope,
            )
        except KitaruContextError as error:
            if is_inside_checkpoint() and str(error) == _WAIT_INSIDE_CHECKPOINT_ERROR:
                _raise_checkpoint_wait_not_supported(name, "hitl")
            raise
        except ApprovalRequired as error:
            # Scope is suspended only while the ordinary sync tool body runs.
            # Native deferred/approval handling resumes under the enclosing
            # checkpoint and remains unsupported for this focused PR.
            if is_inside_checkpoint():
                _raise_checkpoint_wait_not_supported(name, "approval_required")
            call_suffix = _call_suffix()
            request = _DeferredRequest(
                kind="approval_required",
                wait_name=f"approve_{name}_{call_suffix}",
                schema=bool,
                question=f"Approve tool call: {name}?",
                exception_metadata=error.metadata,
                run_after_wait=True,
            )
        except CallDeferred as error:
            # See the ApprovalRequired branch above: this PR supports direct
            # tool-body waits only, not native deferred/approval waits.
            if is_inside_checkpoint():
                _raise_checkpoint_wait_not_supported(name, "call_deferred")
            call_suffix = _call_suffix()
            request = _DeferredRequest(
                kind="call_deferred",
                wait_name=f"defer_{name}_{call_suffix}",
                schema=self._schema_for_deferred_tool(name, ctx),
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
            capture_mode=capture_mode,
        )

    async def _call_wrapped_tool(
        self,
        *,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        suspend_checkpoint_scope: bool,
    ) -> Any:
        override_fn = resolve_tool_override(name)
        if override_fn is not None:
            result = override_fn(**tool_args)
            if asyncio.iscoroutine(result):
                return await result
            return result

        if suspend_checkpoint_scope:
            with (
                _suspend_checkpoint_scope(),
                suspend_adapter_streaming_fallback_checkpoint(),
            ):
                return await super().call_tool(name, tool_args, ctx, tool)
        return await super().call_tool(name, tool_args, ctx, tool)

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
        capture_mode: str | None,
    ) -> Any:
        if is_inside_checkpoint():
            _raise_checkpoint_wait_not_supported(name, request.kind)
        if capture_mode == "full" and safe_args is None:
            safe_args = _json_safe(tool_args)
        wait_metadata = self._wait_metadata(
            name=name,
            safe_args=safe_args,
            ctx=ctx,
            exception_metadata=request.exception_metadata,
            capture_mode=capture_mode,
        )
        wait_value = self._invoke_wait(
            schema=request.schema,
            wait_name=request.wait_name,
            question=request.question,
            metadata=wait_metadata,
        )
        approved = bool(wait_value) if request.schema is bool else None
        deferred_metadata = (
            {"exception_metadata": _json_safe(request.exception_metadata)}
            if capture_mode == "full" and request.exception_metadata
            else None
        )
        if tracker is not None:
            tracker.record_deferred_event(
                tool_name=name,
                deferred_kind=request.kind,
                wait_name=request.wait_name,
                metadata=deferred_metadata,
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

    def _schema_for_deferred_tool(self, name: str, ctx: RunContext[AgentDepsT]) -> Any:
        if isinstance(self.wrapped, FunctionToolset):
            for candidate_name in (getattr(ctx, "tool_name", None), name):
                if candidate_name and candidate_name in self.wrapped.tools:
                    function = self.wrapped.tools[candidate_name].function
                    annotation = get_type_hints(function).get(
                        "return", inspect.Signature.empty
                    )
                    if annotation not in {inspect.Signature.empty, Any}:
                        return annotation
        raise KitaruUsageError(
            f"Cannot infer a wait schema for deferred tool {name!r}. "
            "Add an explicit return annotation or use `@hitl_tool(schema=...)`."
        )

    def _wait_metadata(
        self,
        *,
        name: str,
        safe_args: Any,
        ctx: RunContext[AgentDepsT],
        exception_metadata: dict[str, Any] | None,
        capture_mode: str | None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            ADAPTER_METADATA_KEY: ADAPTER_ID,
            "tool_name": name,
            "tool_call_id": ctx.tool_call_id,
        }
        if capture_mode == "full":
            metadata["tool_args"] = safe_args
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
        # Keep the core wait guard as the single source of truth: explicit
        # ``@hitl_tool`` calls reach this from flow scope, while native
        # deferred/approval waits inside granular tool checkpoints raise there.
        return kitaru.wait(
            schema=schema, name=wait_name, question=question, metadata=metadata
        )


_NON_WORD_PATTERN = re.compile(r"\W+")


def _wait_call_suffix(tool_call_id: str | None) -> str:
    """Build a wait-name suffix: unique per call, stable across replays.

    Uses PydanticAI's ``tool_call_id`` so two calls to the same tool in one run
    get distinct wait names, while a replayed run reuses the same ids and hits
    the cached human inputs instead of re-prompting. If PydanticAI cannot supply
    a stable, sanitizable id, fail instead of creating a random wait name that
    would drift across resume/replay.
    """
    if isinstance(tool_call_id, str) and tool_call_id:
        sanitized = _NON_WORD_PATTERN.sub("_", tool_call_id).strip("_")
        if sanitized:
            return sanitized
    raise KitaruUsageError(
        "PydanticAI did not provide a stable `tool_call_id` for this "
        "HITL/deferred tool call. Kitaru cannot create a replay-safe wait "
        "name without it. Upgrade PydanticAI or avoid durable HITL for this tool."
    )


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 3)


def kitaruify_toolset(
    toolset: AbstractToolset[AgentDepsT],
    *,
    capture: CapturePolicy,
    tool_checkpoint_config: CheckpointConfig | None = None,
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
    mcp_checkpoint_config: CheckpointConfig | None = None,
) -> AbstractToolset[AgentDepsT]:
    if isinstance(toolset, KitaruToolset):
        return toolset

    common = {
        "capture": capture,
        "tool_checkpoint_config_by_name": tool_checkpoint_config_by_name,
    }

    if isinstance(toolset, FunctionToolset):
        from ._function_toolset import KitaruFunctionToolset

        return KitaruFunctionToolset(
            toolset, tool_checkpoint_config=tool_checkpoint_config, **common
        )

    try:
        from ._mcp_compat import ensure_pydantic_ai_mcp_import_compat

        ensure_pydantic_ai_mcp_import_compat()
        from pydantic_ai.mcp import MCPServer
    except ImportError:  # pragma: no cover
        MCPServer = None

    if MCPServer is not None and isinstance(toolset, MCPServer):
        from ._mcp_server import KitaruMCPServer

        return KitaruMCPServer(
            toolset, tool_checkpoint_config=mcp_checkpoint_config, **common
        )

    return KitaruToolset(
        toolset,
        toolset_kind="generic",
        tool_checkpoint_config=tool_checkpoint_config,
        **common,
    )
