"""Duck-typed Google ADK plugin wrapper for Kitaru observability."""

from __future__ import annotations

import time
from collections.abc import Callable, Coroutine, Mapping
from typing import Any

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from . import _kitaru_internal as runtime
from ._policy import (
    ADKCallCheckpointPolicy,
    ADKCapturePolicy,
    resolve_model_checkpoint_config,
    resolve_tool_call_checkpoint_config,
)
from ._serialization import to_cache_identity, to_json_safe
from ._tracking import EventTracker, current_tracker
from ._utils import (
    checkpoint_cache_key,
    elapsed_ms,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
)

_TRUE_HOOK_ERROR = (
    "Google ADK public plugin callbacks currently provide before/after/error "
    "observation only. They do not give Kitaru a `proceed` callable that runs "
    "the model or tool operation inside a checkpoint. Use "
    "checkpoint_strategy='runner_call' for durable replay, or set "
    "ADKCallCheckpointPolicy(require_true_call_hooks=False) to collect "
    "metadata-only call events."
)


class KitaruADKPlugin:
    """Kitaru observer/wrapper object that can be passed to ADK as a plugin.

    The ``before_*``/``after_*`` methods are intentionally metadata-only because
    current ADK plugins do not wrap the underlying operation. The ``wrap_*``
    methods are for a future ADK around-call API, or for tests/fakes that can
    supply a real ``proceed`` callable.
    """

    def __init__(
        self,
        *,
        runner_name: str,
        capture: ADKCapturePolicy | None = None,
        call_policy: ADKCallCheckpointPolicy | None = None,
        tracker: EventTracker | None = None,
        observation_only: bool = False,
    ) -> None:
        self.runner_name = runner_name
        self.capture = capture or ADKCapturePolicy()
        self.call_policy = call_policy or ADKCallCheckpointPolicy()
        self.tracker = tracker
        self.observation_only = observation_only

    def assert_plugin_callbacks_can_checkpoint(self) -> None:
        """Raise when plugin-only calls mode was asked to be replay-safe."""
        if self.observation_only:
            return
        if self.call_policy.require_true_call_hooks:
            raise KitaruUsageError(_TRUE_HOOK_ERROR)

    async def before_model_callback(self, *args: Any, **kwargs: Any) -> None:
        self._record_plugin_metadata_event("model_call", args=args, kwargs=kwargs)
        self.assert_plugin_callbacks_can_checkpoint()

    async def after_model_callback(self, *args: Any, **kwargs: Any) -> None:
        self._record_plugin_metadata_event("model_call", args=args, kwargs=kwargs)

    async def on_model_error(self, *args: Any, **kwargs: Any) -> None:
        self._record_plugin_metadata_event("error", args=args, kwargs=kwargs)

    async def before_tool_callback(self, *args: Any, **kwargs: Any) -> None:
        self._record_plugin_metadata_event("tool_call", args=args, kwargs=kwargs)
        self.assert_plugin_callbacks_can_checkpoint()

    async def after_tool_callback(self, *args: Any, **kwargs: Any) -> None:
        self._record_plugin_metadata_event("tool_call", args=args, kwargs=kwargs)

    async def on_tool_error(self, *args: Any, **kwargs: Any) -> None:
        self._record_plugin_metadata_event("error", args=args, kwargs=kwargs)

    def _tracker(self) -> EventTracker | None:
        return self.tracker or current_tracker()

    def _record_plugin_metadata_event(
        self,
        kind: str,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        tracker = self._tracker()
        if tracker is None or not self.capture.emit_call_events:
            return
        metadata: dict[str, Any] = {"callback": kind}
        if self.capture.capture_mode == "full":
            metadata["args"] = to_json_safe(args, include_raw=True)
            metadata["kwargs"] = to_json_safe(kwargs, include_raw=True)
        else:
            metadata["arg_count"] = len(args)
            metadata["kwarg_keys"] = sorted(kwargs)
        if kind == "model_call":
            tracker.record_metadata_event(kind="model_call", metadata=metadata)
        elif kind == "tool_call":
            tracker.record_metadata_event(kind="tool_call", metadata=metadata)
        else:
            tracker.record_metadata_event(kind="error", metadata=metadata)

    def wrap_model_call(
        self,
        *,
        input_envelope: Mapping[str, Any],
        proceed: Callable[[], Any],
        model_name: str | None = None,
        call_id: str | None = None,
    ) -> Any:
        """Run a model call through a true around-call checkpoint if possible."""
        tracker = self._tracker()
        event_id, context = (
            tracker.start_event("model_call")
            if tracker is not None
            else (call_id or "google_adk_model_call", None)
        )
        started_at = time.perf_counter()

        def body() -> Any:
            return proceed()

        try:
            result, checkpointed = self._run_sync_call_checkpoint(
                input_envelope=input_envelope,
                body=body,
                step_name=f"google_adk_model_{model_name or event_id}",
                model_name=model_name,
            )
        except BaseException as exc:
            if tracker is not None and context is not None:
                tracker.record_event(
                    event_id,
                    context,
                    kind="model_call",
                    status="failed",
                    duration_ms=elapsed_ms(started_at),
                    model_name=model_name,
                    metadata={"call_id": call_id} if call_id else {},
                    error=exc,
                )
            raise

        if tracker is not None and context is not None:
            tracker.record_event(
                event_id,
                context,
                kind="model_call",
                status="completed" if checkpointed else "metadata_only",
                duration_ms=elapsed_ms(started_at),
                model_name=model_name,
                metadata={"call_id": call_id} if call_id else {},
            )
        track(
            AnalyticsEvent.GOOGLE_ADK_CALL_CHECKPOINTED,
            {"call_kind": "model", "checkpointed": checkpointed},
        )
        return result

    async def awrap_model_call(
        self,
        *,
        input_envelope: Mapping[str, Any],
        proceed: Callable[[], Coroutine[Any, Any, Any]],
        model_name: str | None = None,
        call_id: str | None = None,
    ) -> Any:
        """Async variant of ``wrap_model_call``."""
        tracker = self._tracker()
        event_id, context = (
            tracker.start_event("model_call")
            if tracker is not None
            else (call_id or "google_adk_model_call", None)
        )
        started_at = time.perf_counter()
        try:
            result, checkpointed = await self._run_async_call_checkpoint(
                input_envelope=input_envelope,
                body=proceed,
                step_name=f"google_adk_model_{model_name or event_id}",
                model_name=model_name,
            )
        except BaseException as exc:
            if tracker is not None and context is not None:
                tracker.record_event(
                    event_id,
                    context,
                    kind="model_call",
                    status="failed",
                    duration_ms=elapsed_ms(started_at),
                    model_name=model_name,
                    metadata={"call_id": call_id} if call_id else {},
                    error=exc,
                )
            raise
        if tracker is not None and context is not None:
            tracker.record_event(
                event_id,
                context,
                kind="model_call",
                status="completed" if checkpointed else "metadata_only",
                duration_ms=elapsed_ms(started_at),
                model_name=model_name,
                metadata={"call_id": call_id} if call_id else {},
            )
        track(
            AnalyticsEvent.GOOGLE_ADK_CALL_CHECKPOINTED,
            {"call_kind": "model", "checkpointed": checkpointed},
        )
        return result

    def wrap_tool_call(
        self,
        *,
        input_envelope: Mapping[str, Any],
        proceed: Callable[[], Any],
        tool_name: str,
        tool_call_id: str | None = None,
    ) -> Any:
        """Run a tool call through a true around-call checkpoint if possible."""
        tracker = self._tracker()
        event_id, context = (
            tracker.start_event("tool_call")
            if tracker is not None
            else (tool_call_id or f"google_adk_tool_{tool_name}", None)
        )
        started_at = time.perf_counter()
        try:
            result, checkpointed = self._run_sync_tool_checkpoint(
                input_envelope=input_envelope,
                body=proceed,
                step_name=f"google_adk_tool_{tool_name}",
                tool_name=tool_name,
            )
        except BaseException as exc:
            if tracker is not None and context is not None:
                tracker.record_event(
                    event_id,
                    context,
                    kind="tool_call",
                    status="failed",
                    duration_ms=elapsed_ms(started_at),
                    tool_name=tool_name,
                    tool_call_id=tool_call_id,
                    error=exc,
                )
            raise
        if tracker is not None and context is not None:
            tracker.record_event(
                event_id,
                context,
                kind="tool_call",
                status="completed" if checkpointed else "metadata_only",
                duration_ms=elapsed_ms(started_at),
                tool_name=tool_name,
                tool_call_id=tool_call_id,
            )
        track(
            AnalyticsEvent.GOOGLE_ADK_CALL_CHECKPOINTED,
            {"call_kind": "tool", "checkpointed": checkpointed},
        )
        return result

    async def awrap_tool_call(
        self,
        *,
        input_envelope: Mapping[str, Any],
        proceed: Callable[[], Coroutine[Any, Any, Any]],
        tool_name: str,
        tool_call_id: str | None = None,
    ) -> Any:
        """Async variant of ``wrap_tool_call``."""
        tracker = self._tracker()
        event_id, context = (
            tracker.start_event("tool_call")
            if tracker is not None
            else (tool_call_id or f"google_adk_tool_{tool_name}", None)
        )
        started_at = time.perf_counter()
        try:
            result, checkpointed = await self._run_async_tool_checkpoint(
                input_envelope=input_envelope,
                body=proceed,
                step_name=f"google_adk_tool_{tool_name}",
                tool_name=tool_name,
            )
        except BaseException as exc:
            if tracker is not None and context is not None:
                tracker.record_event(
                    event_id,
                    context,
                    kind="tool_call",
                    status="failed",
                    duration_ms=elapsed_ms(started_at),
                    tool_name=tool_name,
                    tool_call_id=tool_call_id,
                    error=exc,
                )
            raise
        if tracker is not None and context is not None:
            tracker.record_event(
                event_id,
                context,
                kind="tool_call",
                status="completed" if checkpointed else "metadata_only",
                duration_ms=elapsed_ms(started_at),
                tool_name=tool_name,
                tool_call_id=tool_call_id,
            )
        track(
            AnalyticsEvent.GOOGLE_ADK_CALL_CHECKPOINTED,
            {"call_kind": "tool", "checkpointed": checkpointed},
        )
        return result

    def _ensure_checkpoint_allowed_or_metadata_only(self) -> bool:
        if not runtime.is_inside_flow():
            return False
        if not runtime.is_inside_checkpoint():
            return True
        if self.call_policy.nested_checkpoint_policy == "metadata_only":
            return False
        raise KitaruUsageError(
            "Google ADK calls-mode checkpointing cannot open a model/tool "
            "checkpoint while already inside a Kitaru checkpoint. Set "
            "ADKCallCheckpointPolicy(nested_checkpoint_policy='metadata_only') "
            "to record metadata only in nested contexts."
        )

    def _run_sync_call_checkpoint(
        self,
        *,
        input_envelope: Mapping[str, Any],
        body: Callable[[], Any],
        step_name: str,
        model_name: str | None,
    ) -> tuple[Any, bool]:
        config = resolve_model_checkpoint_config(self.call_policy)
        if config is None or not self._ensure_checkpoint_allowed_or_metadata_only():
            return body(), False
        checkpoint_input = to_json_safe(input_envelope)
        return run_sync_in_checkpoint(
            config=config,
            step_name=step_name,
            body=body,
            cache_key=checkpoint_cache_key(to_cache_identity(input_envelope)),
            checkpoint_inputs={"model_input": checkpoint_input},
        ), True

    async def _run_async_call_checkpoint(
        self,
        *,
        input_envelope: Mapping[str, Any],
        body: Callable[[], Coroutine[Any, Any, Any]],
        step_name: str,
        model_name: str | None,
    ) -> tuple[Any, bool]:
        config = resolve_model_checkpoint_config(self.call_policy)
        if config is None or not self._ensure_checkpoint_allowed_or_metadata_only():
            return await body(), False
        checkpoint_input = to_json_safe(input_envelope)
        return await run_async_in_checkpoint(
            config=config,
            step_name=step_name,
            body=body,
            cache_key=checkpoint_cache_key(to_cache_identity(input_envelope)),
            checkpoint_inputs={"model_input": checkpoint_input},
        ), True

    def _run_sync_tool_checkpoint(
        self,
        *,
        input_envelope: Mapping[str, Any],
        body: Callable[[], Any],
        step_name: str,
        tool_name: str,
    ) -> tuple[Any, bool]:
        config = resolve_tool_call_checkpoint_config(
            self.call_policy, tool_name=tool_name
        )
        if config is None or not self._ensure_checkpoint_allowed_or_metadata_only():
            return body(), False
        checkpoint_input = to_json_safe(input_envelope)
        return run_sync_in_checkpoint(
            config=config,
            step_name=step_name,
            body=body,
            cache_key=checkpoint_cache_key(to_cache_identity(input_envelope)),
            checkpoint_inputs={"tool_args": checkpoint_input},
        ), True

    async def _run_async_tool_checkpoint(
        self,
        *,
        input_envelope: Mapping[str, Any],
        body: Callable[[], Coroutine[Any, Any, Any]],
        step_name: str,
        tool_name: str,
    ) -> tuple[Any, bool]:
        config = resolve_tool_call_checkpoint_config(
            self.call_policy, tool_name=tool_name
        )
        if config is None or not self._ensure_checkpoint_allowed_or_metadata_only():
            return await body(), False
        checkpoint_input = to_json_safe(input_envelope)
        return await run_async_in_checkpoint(
            config=config,
            step_name=step_name,
            body=body,
            cache_key=checkpoint_cache_key(to_cache_identity(input_envelope)),
            checkpoint_inputs={"tool_args": checkpoint_input},
        ), True
