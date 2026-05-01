from __future__ import annotations

import inspect
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any
from typing import get_type_hints
from uuid import uuid4

import kitaru
from pydantic_core import to_jsonable_python

from pydantic_ai.exceptions import ApprovalRequired, CallDeferred
from pydantic_ai.tools import AgentDepsT, RunContext
from pydantic_ai.toolsets import AbstractToolset, FunctionToolset, ToolsetTool, WrapperToolset

from kitaru.errors import KitaruUsageError

from ._constants import ADAPTER_ID, ADAPTER_METADATA_KEY
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
    checkpoint_cache_key,
    resolve_tool_checkpoint_config,
    run_async_in_checkpoint,
    with_default_type,
)



class _ToolApprovalDenied(Exception):
    """Raised when a human denies a HITL approval request."""


def _json_safe(value: Any) -> Any:
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError:  # circular reference
        logger.warning('Failed to JSON-serialize adapter payload; falling back to repr.', exc_info=True)
        return {
            'repr': repr(value),
            'python_type': value.__class__.__name__,
            'serialization_error': 'json_safe_failed',
        }


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

    toolset_kind: ToolsetKind = 'generic'
    capture: CapturePolicy = field(default_factory=CapturePolicy)
    # Granular-mode: when set, each ``call_tool`` opens its own @kitaru.checkpoint.
    # Per-name overrides can replace or disable (`False`) per-tool.
    tool_checkpoint_config: CheckpointConfig | None = None
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None
    _default_checkpoint_type: str = field(default='tool_call', init=False)

    @property
    def id(self) -> str | None:
        return self.wrapped.id

    def visit_and_replace(
        self, visitor: Callable[[AbstractToolset[AgentDepsT]], AbstractToolset[AgentDepsT]]
    ) -> AbstractToolset[AgentDepsT]:
        return self

    async def for_run(self, ctx: RunContext[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
        new_wrapped = await self.wrapped.for_run(ctx)
        if new_wrapped is self.wrapped:
            return self
        return replace(self, wrapped=new_wrapped)

    async def for_run_step(self, ctx: RunContext[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
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
        hitl_config = hitl_config_from_tool_metadata(tool.tool_def.metadata)
        if hitl_config is not None:
            return await self._call_tool_tracked(name, tool_args, ctx, tool, hitl_config)

        checkpoint_config = resolve_tool_checkpoint_config(
            name,
            default=self.tool_checkpoint_config,
            by_name=self.tool_checkpoint_config_by_name,
        )
        if (
            checkpoint_config is not None
            and is_inside_flow()
            and not is_inside_checkpoint()
        ):
            async def _in_checkpoint() -> Any:
                return await self._call_tool_tracked(name, tool_args, ctx, tool, hitl_config)

            return await run_async_in_checkpoint(
                config=with_default_type(checkpoint_config, self._default_checkpoint_type),
                step_name=f'{name}_tool',
                body=_in_checkpoint,
                cache_key=checkpoint_cache_key(
                    {
                        'tool_name': name,
                        'tool_args': tool_args,
                        'tool_call_id': ctx.tool_call_id,
                        'retry': ctx.retry,
                    }
                ),
            )
        return await self._call_tool_tracked(name, tool_args, ctx, tool, hitl_config)

    async def _call_tool_tracked(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        hitl_config: HitlConfig | None,
    ) -> Any:
        capture_mode = self.capture.capture_mode_for_tool(name)
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
            )

        event_id, event_context = tracker.start_tool_event()
        if self.capture.correlate_otel_spans:
            attach_tool_correlation(event_id, event_context)

        safe_args = _json_safe(tool_args) if capture_mode == 'full' or hitl_config is not None else None

        artifacts: dict[str, str] = {}
        if capture_mode == 'full' and is_inside_checkpoint():
            args_key = tracker.artifact_name(event_id, 'args')
            kitaru.save(args_key, safe_args, type='input')
            artifacts['args'] = args_key

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
        except Exception as error:
            tracker.record_tool_event(
                event_id,
                event_context,
                status='failed',
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
        if capture_mode == 'full' and is_inside_checkpoint():
            result_key = tracker.artifact_name(event_id, 'result')
            kitaru.save(result_key, _json_safe(result), type='output')
            artifacts['result'] = result_key

        tracker.record_tool_event(
            event_id,
            event_context,
            status='completed',
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
        call_suffix = _wait_call_suffix(getattr(ctx, 'tool_call_id', None))

        if hitl_config is not None:
            request = _DeferredRequest(
                kind='hitl',
                wait_name=f'{hitl_config.name or name}_{call_suffix}',
                schema=hitl_config.schema,
                question=resolve_hitl_question(hitl_config, tool_args),
                exception_metadata=None,
                run_after_wait=False,
            )
            return await self._handle_deferred(
                request, name=name, tool_args=tool_args, safe_args=safe_args, ctx=ctx, tool=tool, tracker=tracker
            )

        try:
            return await super().call_tool(name, tool_args, ctx, tool)
        except ApprovalRequired as error:
            request = _DeferredRequest(
                kind='approval_required',
                wait_name=f'approve_{name}_{call_suffix}',
                schema=bool,
                question=f'Approve tool call: {name}?',
                exception_metadata=error.metadata,
                run_after_wait=True,
            )
        except CallDeferred as error:
            request = _DeferredRequest(
                kind='call_deferred',
                wait_name=f'defer_{name}_{call_suffix}',
                schema=self._schema_for_deferred_tool(name, ctx),
                question=f'Provide result for deferred tool: {name}',
                exception_metadata=error.metadata,
                run_after_wait=False,
            )
        return await self._handle_deferred(
            request, name=name, tool_args=tool_args, safe_args=safe_args, ctx=ctx, tool=tool, tracker=tracker
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
            name=name, safe_args=safe_args, ctx=ctx, exception_metadata=request.exception_metadata
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
                metadata={'exception_metadata': request.exception_metadata} if request.exception_metadata else None,
                approved=approved,
            )
        if approved is False:
            raise _ToolApprovalDenied(f'Tool {name!r} was not approved.')
        if not request.run_after_wait:
            return wait_value
        approved_ctx = replace(ctx, tool_call_approved=True, tool_call_metadata=request.exception_metadata)
        return await super().call_tool(name, tool_args, approved_ctx, tool)

    def _schema_for_deferred_tool(self, name: str, ctx: RunContext[AgentDepsT]) -> Any:
        if isinstance(self.wrapped, FunctionToolset):
            for candidate_name in (getattr(ctx, 'tool_name', None), name):
                if candidate_name and candidate_name in self.wrapped.tools:
                    function = self.wrapped.tools[candidate_name].function
                    annotation = get_type_hints(function).get('return', inspect.Signature.empty)
                    if annotation not in {inspect.Signature.empty, Any}:
                        return annotation
        raise KitaruUsageError(
            f'Cannot infer a wait schema for deferred tool {name!r}. '
            'Add an explicit return annotation or use `@hitl_tool(schema=...)`.'
        )

    def _wait_metadata(
        self,
        *,
        name: str,
        safe_args: Any,
        ctx: RunContext[AgentDepsT],
        exception_metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            ADAPTER_METADATA_KEY: ADAPTER_ID,
            'tool_name': name,
            'tool_call_id': ctx.tool_call_id,
            'tool_args': safe_args,
        }
        if exception_metadata:
            metadata['exception_metadata'] = _json_safe(exception_metadata)
        return metadata

    def _invoke_wait(
        self,
        *,
        schema: Any,
        wait_name: str,
        question: str | None,
        metadata: dict[str, Any],
    ) -> Any:
        return kitaru.wait(
            schema=schema, name=wait_name, question=question, metadata=metadata
        )


_NON_WORD_PATTERN = re.compile(r'\W+')


def _wait_call_suffix(tool_call_id: str | None) -> str:
    """Build a wait-name suffix: unique per call, stable across replays.

    Uses PydanticAI's ``tool_call_id`` so two calls to the same tool in one run
    get distinct wait names, while a replayed run reuses the same ids and hits
    the cached human inputs instead of re-prompting.
    """
    if isinstance(tool_call_id, str) and tool_call_id:
        sanitized = _NON_WORD_PATTERN.sub('_', tool_call_id).strip('_')
        if sanitized:
            return sanitized
    return uuid4().hex[:8]


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
        'capture': capture,
        'tool_checkpoint_config_by_name': tool_checkpoint_config_by_name,
    }

    if isinstance(toolset, FunctionToolset):
        from ._function_toolset import KitaruFunctionToolset

        return KitaruFunctionToolset(
            toolset, tool_checkpoint_config=tool_checkpoint_config, **common
        )

    try:
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
        toolset_kind='generic',
        tool_checkpoint_config=tool_checkpoint_config,
        **common,
    )
