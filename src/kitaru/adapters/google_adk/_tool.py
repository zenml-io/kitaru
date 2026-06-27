"""Google ADK tool wrapper for true tool-call checkpoints."""

from __future__ import annotations

import importlib
import inspect
import time
from collections.abc import Mapping
from typing import Any

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from . import _kitaru_internal as runtime
from ._policy import (
    ADKCallCheckpointPolicy,
    ADKCapturePolicy,
    resolve_tool_call_checkpoint_config,
)
from ._serialization import object_metadata, to_json_safe
from ._tracking import EventTracker, current_tracker
from ._utils import checkpoint_cache_key, elapsed_ms, run_async_in_checkpoint

_BaseTool: type[Any] = importlib.import_module("google.adk.tools.base_tool").BaseTool


def _base_tool_init_kwargs(values: dict[str, Any]) -> dict[str, Any]:
    fields = getattr(_BaseTool, "model_fields", None)
    if isinstance(fields, Mapping):
        return {key: value for key, value in values.items() if key in fields}
    return values


class KitaruADKTool(_BaseTool):  # type: ignore[misc, valid-type]
    """Wrap an ADK tool-like object at ``run_async``.

    Public ADK docs expose ``BaseTool.run_async(*, args, tool_context)`` for
    local tool execution. This wrapper places that actual method call inside a
    Kitaru checkpoint when running from a flow body.
    """

    def __init__(
        self,
        tool: Any,
        *,
        name: str | None = None,
        capture: ADKCapturePolicy | None = None,
        call_policy: ADKCallCheckpointPolicy | None = None,
        tracker: EventTracker | None = None,
    ) -> None:
        resolved_name = name or str(getattr(tool, "name", None) or type(tool).__name__)
        metadata = getattr(tool, "custom_metadata", None)
        base_values = {
            "name": resolved_name,
            "description": str(getattr(tool, "description", "")),
            "is_long_running": bool(getattr(tool, "is_long_running", False)),
            "custom_metadata": metadata if isinstance(metadata, dict) else None,
        }
        try:
            super().__init__(**_base_tool_init_kwargs(base_values))
        except TypeError:
            super().__init__()
            for key, value in base_values.items():
                object.__setattr__(self, key, value)

        object.__setattr__(self, "_tool", tool)
        object.__setattr__(self, "_name", resolved_name)
        object.__setattr__(self, "_capture", capture or ADKCapturePolicy())
        object.__setattr__(
            self,
            "_call_policy",
            call_policy or ADKCallCheckpointPolicy(),
        )
        object.__setattr__(self, "_tracker", tracker)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._tool, name)

    async def process_llm_request(self, *, tool_context: Any, llm_request: Any) -> Any:
        process = getattr(self._tool, "process_llm_request", None)
        if not callable(process):
            return None
        result = process(tool_context=tool_context, llm_request=llm_request)
        if inspect.isawaitable(result):
            return await result
        return result

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        """Run the wrapped ADK tool inside a checkpoint if possible."""
        tracker = self._tracker or current_tracker()
        event_id, context = (
            tracker.start_event("tool_call")
            if tracker is not None
            else (f"google_adk_tool_{self.name}", None)
        )
        started_at = time.perf_counter()

        async def body() -> Any:
            run = getattr(self._tool, "run_async", None)
            if not callable(run):
                raise KitaruUsageError(
                    "Wrapped Google ADK tool does not expose `run_async(...)`."
                )
            return await run(args=args, tool_context=tool_context)

        try:
            result, checkpointed = await self._checkpoint_or_run(
                args=args,
                tool_context=tool_context,
                body=body,
            )
        except BaseException as exc:
            if tracker is not None and context is not None:
                tracker.record_event(
                    event_id,
                    context,
                    kind="tool_call",
                    status="failed",
                    duration_ms=elapsed_ms(started_at),
                    tool_name=self.name,
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
                tool_name=self.name,
            )
        track(
            AnalyticsEvent.GOOGLE_ADK_CALL_CHECKPOINTED,
            {"call_kind": "tool", "checkpointed": checkpointed},
        )
        return result

    async def _checkpoint_or_run(
        self, *, args: Any, tool_context: Any, body: Any
    ) -> tuple[Any, bool]:
        config = resolve_tool_call_checkpoint_config(
            self._call_policy,
            tool_name=self.name,
        )
        if config is None or not self._can_checkpoint():
            return await body(), False
        tool_input = to_json_safe(
            {
                "tool_name": self.name,
                "args": args,
                "tool_context": object_metadata(tool_context),
                "wrapped_tool": object_metadata(self._tool),
            },
            include_raw=self._capture.capture_mode == "full",
        )
        return await run_async_in_checkpoint(
            config=config,
            step_name=f"google_adk_tool_{self.name}",
            body=body,
            cache_key=checkpoint_cache_key(tool_input),
            checkpoint_inputs={"tool_args": tool_input},
        ), True

    def _can_checkpoint(self) -> bool:
        if not runtime.is_inside_flow():
            return False
        if not runtime.is_inside_checkpoint():
            return True
        if self._call_policy.nested_checkpoint_policy == "metadata_only":
            return False
        raise KitaruUsageError(
            "KitaruADKTool cannot open a tool-call checkpoint while already "
            "inside a Kitaru checkpoint. Set "
            "ADKCallCheckpointPolicy(nested_checkpoint_policy='metadata_only') "
            "to execute the wrapped tool directly and record metadata only."
        )


def wrap_tool(
    tool: Any,
    *,
    name: str | None = None,
    capture: ADKCapturePolicy | None = None,
    call_policy: ADKCallCheckpointPolicy | None = None,
    tracker: EventTracker | None = None,
) -> KitaruADKTool:
    """Return ``tool`` wrapped for Kitaru ADK calls-mode checkpoints."""
    return KitaruADKTool(
        tool,
        name=name,
        capture=capture,
        call_policy=call_policy,
        tracker=tracker,
    )
