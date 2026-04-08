"""Agent wrapper for Kitaru's PydanticAI adapter."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from threading import Lock
from typing import Any

from pydantic_ai import messages as _messages
from pydantic_ai import models
from pydantic_ai import usage as _usage
from pydantic_ai.agent import AbstractAgent, AgentRun, AgentSpec, WrapperAgent
from pydantic_ai.agent.abstract import (
    AgentBuiltinTool,
    AgentInstructions,
    AgentMetadata,
    AgentModelSettings,
    EventStreamHandler,
)
from pydantic_ai.models import Model
from pydantic_ai.output import OutputDataT, OutputSpec
from pydantic_ai.tools import AgentDepsT, DeferredToolResults
from pydantic_ai.toolsets import AbstractToolset

from kitaru.checkpoint import checkpoint
from kitaru.errors import KitaruUsageError
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint, _is_inside_flow, _next_llm_call_name

from ._hitl import attach_hitl_metadata
from ._model import KitaruModel
from ._toolset import CaptureConfig, kitaruify_toolset
from ._tracking import get_current_tracker, normalize_agent_name, tracker_scope


def _track_adapter_run(
    *,
    method: str,
    error: BaseException | None = None,
) -> None:
    """Emit PYDANTIC_AI_RUN_COMPLETED analytics."""
    from kitaru.analytics import AnalyticsEvent, track

    metadata: dict[str, Any] = {
        "method": method,
        "status": "failed" if error is not None else "completed",
    }
    if error is not None:
        metadata["error_type"] = type(error).__name__
    track(AnalyticsEvent.PYDANTIC_AI_RUN_COMPLETED, metadata)


_EVENT_STREAM_HANDLER_WRAPPED_ATTR = "_kitaru_event_stream_handler_wrapped"
_SYNTHETIC_RUN_CALLABLES: dict[str, Callable[[], Any]] = {}
_SYNTHETIC_RUN_LOCK = Lock()


@checkpoint(type="llm_call")
def _synthetic_agent_run_checkpoint(run_id: str) -> Any:
    """Synthetic checkpoint for adapter `run()` / `run_sync()` flow-scope calls."""
    with _SYNTHETIC_RUN_LOCK:
        run_callable = _SYNTHETIC_RUN_CALLABLES.get(run_id)

    if run_callable is None:
        raise KitaruUsageError(
            "Synthetic adapter checkpoint callback was not found. "
            "Wrap the agent call in an explicit @checkpoint when using "
            "distributed execution backends."
        )

    return run_callable()


def _error_payload(error: BaseException) -> dict[str, str]:
    """Build a lightweight error payload for adapter metadata."""
    return {
        "type": error.__class__.__name__,
        "message": str(error),
    }


class KitaruAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    """Wrap a PydanticAI agent so internal calls become Kitaru child events."""

    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        name: str | None = None,
        tool_capture_config: CaptureConfig | None = None,
        tool_capture_config_by_name: dict[str, CaptureConfig | None] | None = None,
    ) -> None:
        super().__init__(wrapped)

        if not isinstance(wrapped.model, Model):
            raise KitaruUsageError(
                "Wrapped PydanticAI agents must define a model at construction time."
            )

        self._name = name or wrapped.name or "agent"
        self._model = KitaruModel(wrapped.model)

        for toolset in wrapped.toolsets:
            toolset.apply(attach_hitl_metadata)

        self._toolsets = [
            toolset.visit_and_replace(
                lambda wrapped_toolset: kitaruify_toolset(
                    wrapped_toolset,
                    tool_capture_config=tool_capture_config,
                    tool_capture_config_by_name=tool_capture_config_by_name,
                )
            )
            for toolset in wrapped.toolsets
        ]

    @property
    def name(self) -> str | None:
        """Return the wrapped agent name used in event IDs."""
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        """Set adapter-visible and wrapped-agent name values."""
        self._name = value
        self.wrapped.name = value

    @property
    def model(self) -> Model:
        """Return the adapter's model wrapper."""
        return self._model

    @property
    def toolsets(self) -> Sequence[AbstractToolset[AgentDepsT]]:
        """Return adapter-wrapped toolsets."""
        return self._toolsets

    @contextmanager
    def _kitaru_overrides(self) -> Iterator[None]:
        """Force wrapped agent runs to use adapter model/toolsets."""
        with super().override(model=self._model, toolsets=self._toolsets, tools=[]):
            yield

    def _prepare_event_stream_handler(
        self,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
    ) -> EventStreamHandler[AgentDepsT] | None:
        """Wrap event stream handlers so invocations are observed inline."""
        effective_handler = event_stream_handler or self.event_stream_handler
        if effective_handler is None:
            return None

        if getattr(effective_handler, _EVENT_STREAM_HANDLER_WRAPPED_ATTR, False):
            return effective_handler

        async def _tracked_handler(run_context: Any, stream: Any) -> None:
            started_at = time.perf_counter()
            status = "completed"
            error: BaseException | None = None
            try:
                await effective_handler(run_context, stream)
            except BaseException as exc:
                status = "failed"
                error = exc
                raise
            finally:
                tracker = get_current_tracker()
                if _is_inside_checkpoint() and tracker is not None:
                    duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
                    handler_index = tracker.record_event_stream_handler(duration_ms)
                    event_id = (
                        f"{tracker.agent_name}_{tracker.run_label}"
                        f"_event_stream_handler_{handler_index}"
                    )
                    payload: dict[str, Any] = {
                        "type": "event_stream_handler",
                        "status": status,
                        "duration_ms": duration_ms,
                        "index": handler_index,
                    }
                    if error is not None:
                        payload["error"] = _error_payload(error)

                    log(pydantic_ai_event_stream_handlers={event_id: payload})

        setattr(_tracked_handler, _EVENT_STREAM_HANDLER_WRAPPED_ATTR, True)
        return _tracked_handler

    def _build_synthetic_run_id(self) -> str:
        """Build a stable synthetic checkpoint ID for one run call."""
        return _next_llm_call_name(
            prefix=f"{normalize_agent_name(self._name)}_agent_run"
        )

    def _run_sync_core(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> Any:
        """Run sync with optional synthetic checkpointing at flow scope."""
        if _is_inside_flow() and not _is_inside_checkpoint():
            run_id = self._build_synthetic_run_id()

            def _run_callable() -> Any:
                return self._run_sync_inline(
                    user_prompt=user_prompt,
                    output_type=output_type,
                    message_history=message_history,
                    deferred_tool_results=deferred_tool_results,
                    model=model,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=toolsets,
                    builtin_tools=builtin_tools,
                    event_stream_handler=event_stream_handler,
                    spec=spec,
                )

            with _SYNTHETIC_RUN_LOCK:
                _SYNTHETIC_RUN_CALLABLES[run_id] = _run_callable
            try:
                return _synthetic_agent_run_checkpoint(run_id, id=run_id)
            finally:
                with _SYNTHETIC_RUN_LOCK:
                    _SYNTHETIC_RUN_CALLABLES.pop(run_id, None)

        return self._run_sync_inline(
            user_prompt=user_prompt,
            output_type=output_type,
            message_history=message_history,
            deferred_tool_results=deferred_tool_results,
            model=model,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            infer_name=infer_name,
            toolsets=toolsets,
            builtin_tools=builtin_tools,
            event_stream_handler=event_stream_handler,
            spec=spec,
        )

    def _run_sync_inline(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> Any:
        """Run sync through the base agent implementation with wrapped handler."""
        return super().run_sync(
            user_prompt,
            output_type=output_type,
            message_history=message_history,
            deferred_tool_results=deferred_tool_results,
            model=model,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            infer_name=infer_name,
            toolsets=toolsets,
            builtin_tools=builtin_tools,
            event_stream_handler=event_stream_handler,
            spec=spec,
        )

    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> Any:
        """Run async, using synthetic checkpointing when at flow scope."""
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)

        try:
            if _is_inside_flow() and not _is_inside_checkpoint():
                result = await asyncio.to_thread(
                    self._run_sync_core,
                    user_prompt,
                    output_type=output_type,
                    message_history=message_history,
                    deferred_tool_results=deferred_tool_results,
                    model=model,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=toolsets,
                    builtin_tools=builtin_tools,
                    event_stream_handler=wrapped_handler,
                    spec=spec,
                )
            else:
                result = await super().run(
                    user_prompt,
                    output_type=output_type,
                    message_history=message_history,
                    deferred_tool_results=deferred_tool_results,
                    model=model,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=toolsets,
                    builtin_tools=builtin_tools,
                    event_stream_handler=wrapped_handler,
                    spec=spec,
                )
        except Exception as exc:
            _track_adapter_run(method="run", error=exc)
            raise
        _track_adapter_run(method="run")
        return result

    def run_sync(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> Any:
        """Run sync, using synthetic checkpointing when at flow scope."""
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        try:
            result = self._run_sync_core(
                user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                model=model,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=toolsets,
                builtin_tools=builtin_tools,
                event_stream_handler=wrapped_handler,
                spec=spec,
            )
        except Exception as exc:
            _track_adapter_run(method="run_sync", error=exc)
            raise
        _track_adapter_run(method="run_sync")
        return result

    @asynccontextmanager
    async def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AsyncIterator[AgentRun[AgentDepsT, Any]]:
        iter_status = "completed"
        iter_error: BaseException | None = None

        with self._kitaru_overrides(), tracker_scope(self._name) as tracker:
            try:
                async with self.wrapped.iter(
                    user_prompt=user_prompt,
                    output_type=output_type,
                    message_history=message_history,
                    deferred_tool_results=deferred_tool_results,
                    model=model,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=toolsets,
                    builtin_tools=builtin_tools,
                    spec=spec,
                ) as run:
                    yield run
            except Exception as error:
                iter_status = "failed"
                iter_error = error
                raise
            finally:
                if _is_inside_checkpoint():
                    log(
                        pydantic_ai_run_summaries={
                            tracker.run_label: tracker.build_run_summary(
                                status=iter_status,
                                error=_error_payload(iter_error)
                                if iter_error is not None
                                else None,
                            )
                        }
                    )
                _track_adapter_run(method="iter", error=iter_error)
