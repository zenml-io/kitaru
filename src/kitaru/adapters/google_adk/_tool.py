"""Google ADK tool wrapper for true tool-call checkpoints."""

from __future__ import annotations

import importlib
import inspect
import time
from collections.abc import Mapping
from typing import Any, cast

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
_NO_TOOL_CONTEXT_STATE = object()
_TOOL_RESULT_ENVELOPE_KEY = "__kitaru_adk_tool_checkpoint_result__"
_TOOL_RESULT_ENVELOPE_VERSION = 1


def _base_tool_init_kwargs(values: dict[str, Any]) -> dict[str, Any]:
    fields = getattr(_BaseTool, "model_fields", None)
    if isinstance(fields, Mapping):
        return {key: value for key, value in values.items() if key in fields}
    return values


def _copy_json_state_value(value: Any, *, path: str) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        copied: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise KitaruUsageError(
                    "Checkpointed Google ADK tool calls can only replay "
                    "`tool_context.state` mappings with string keys. Disable "
                    "checkpointing for this tool with "
                    "`tool_checkpoint_config_by_name={tool_name: False}` if "
                    "the tool needs non-JSON state."
                )
            copied[key] = _copy_json_state_value(item, path=f"{path}.{key}")
        return copied
    if isinstance(value, list):
        return [
            _copy_json_state_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise KitaruUsageError(
        "Checkpointed Google ADK tool calls need replay-safe "
        "`tool_context.state` values. Kitaru found an unsupported value at "
        f"{path}: {type(value).__module__}.{type(value).__name__}. Disable "
        "checkpointing for this tool with "
        "`tool_checkpoint_config_by_name={tool_name: False}` if the tool "
        "needs non-JSON state."
    )


def _read_tool_context_state(tool_context: Any) -> Any:
    try:
        state = tool_context.state
    except AttributeError:
        return _NO_TOOL_CONTEXT_STATE
    if state is None:
        return _NO_TOOL_CONTEXT_STATE
    items = getattr(state, "items", None)
    if not callable(items) or not hasattr(state, "__setitem__"):
        raise KitaruUsageError(
            "Checkpointed Google ADK tool calls need `tool_context.state` to "
            "behave like a mutable mapping so Kitaru can replay state "
            "mutations when a checkpoint is reused. Disable checkpointing for "
            "this tool with "
            "`tool_checkpoint_config_by_name={tool_name: False}` if the tool "
            "uses a custom state object."
        )
    return state


def _snapshot_tool_context_state(tool_context: Any) -> dict[str, Any] | object:
    state = _read_tool_context_state(tool_context)
    if state is _NO_TOOL_CONTEXT_STATE:
        return _NO_TOOL_CONTEXT_STATE
    return _copy_json_state_value(dict(state.items()), path="tool_context.state")


def _build_state_delta(
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any]:
    return {
        "before": before,
        "after": after,
        "set": {
            key: value
            for key, value in after.items()
            if key not in before or before[key] != value
        },
        "deleted": [key for key in before if key not in after],
    }


def _apply_state_delta(
    tool_context: Any,
    delta: Mapping[str, Any] | None,
) -> None:
    if delta is None:
        return
    state = _read_tool_context_state(tool_context)
    if state is _NO_TOOL_CONTEXT_STATE:
        raise KitaruUsageError(
            "A cached Google ADK tool checkpoint includes state mutations, "
            "but this `tool_context` has no replayable `.state` mapping."
        )
    current = _snapshot_tool_context_state(tool_context)
    before = delta.get("before")
    after = delta.get("after")
    if current == after:
        return
    if current != before:
        raise KitaruUsageError(
            "Refusing to replay a cached Google ADK tool state mutation onto "
            "a different starting `tool_context.state`. The tool call should "
            "run again with a checkpoint input that includes the current state."
        )
    deleted = delta.get("deleted", [])
    if deleted and not hasattr(state, "__delitem__"):
        raise KitaruUsageError(
            "A cached Google ADK tool checkpoint needs to delete keys from "
            "`tool_context.state`, but this state object does not support "
            "deletion. Disable checkpointing for this tool with "
            "`tool_checkpoint_config_by_name={tool_name: False}`."
        )
    for key in deleted:
        if key in state:
            del state[key]
    for key, value in delta.get("set", {}).items():
        state[key] = _copy_json_state_value(value, path=f"tool_context.state.{key}")


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
        before_state = _snapshot_tool_context_state(tool_context)
        state_input = (
            {"available": False}
            if before_state is _NO_TOOL_CONTEXT_STATE
            else {
                "available": True,
                "value": before_state,
                "cache_identity": checkpoint_cache_key(before_state),
            }
        )
        tool_input = to_json_safe(
            {
                "tool_name": self.name,
                "args": args,
                "tool_context": object_metadata(tool_context),
                "tool_context_state_before": state_input,
                "wrapped_tool": object_metadata(self._tool),
            },
            include_raw=self._capture.capture_mode == "full",
        )

        async def checkpoint_body() -> dict[str, Any]:
            result = await body()
            if before_state is _NO_TOOL_CONTEXT_STATE:
                state_delta = None
            else:
                after_state = _snapshot_tool_context_state(tool_context)
                if after_state is _NO_TOOL_CONTEXT_STATE:
                    raise KitaruUsageError(
                        "Google ADK tool checkpointing cannot replay a tool "
                        "that removes `tool_context.state` during execution."
                    )
                state_delta = _build_state_delta(
                    cast(dict[str, Any], before_state),
                    cast(dict[str, Any], after_state),
                )
            return {
                _TOOL_RESULT_ENVELOPE_KEY: _TOOL_RESULT_ENVELOPE_VERSION,
                "result": result,
                "tool_context_state_delta": state_delta,
            }

        checkpointed_result = await run_async_in_checkpoint(
            config=config,
            step_name=f"google_adk_tool_{self.name}",
            body=checkpoint_body,
            cache_key=checkpoint_cache_key(tool_input),
            checkpoint_inputs={"tool_args": tool_input},
        )
        if (
            isinstance(checkpointed_result, Mapping)
            and checkpointed_result.get(_TOOL_RESULT_ENVELOPE_KEY)
            == _TOOL_RESULT_ENVELOPE_VERSION
        ):
            _apply_state_delta(
                tool_context,
                checkpointed_result.get("tool_context_state_delta"),
            )
            return checkpointed_result.get("result"), True
        return checkpointed_result, True

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
