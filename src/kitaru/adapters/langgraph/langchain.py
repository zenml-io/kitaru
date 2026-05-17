"""LangChain middleware for LangGraph calls-mode checkpointing."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, cast

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import langchain  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - import guard only
    if exc.name != "langchain":
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.langgraph.langchain requires LangChain. Install with "
        "`uv sync --extra langgraph` or `pip install 'kitaru[langgraph]'`."
    ) from exc

try:
    from langchain.agents.middleware import AgentMiddleware
except ImportError as exc:  # pragma: no cover - exercised via import-guard tests
    raise ImportError(
        "LangChain is installed, but Kitaru could not import "
        "langchain.agents.middleware.AgentMiddleware. The installed LangChain "
        "version may be incompatible with Kitaru's LangGraph calls-mode "
        f"middleware. Original error: {exc}"
    ) from exc

from ._kitaru_internal import (
    get_current_checkpoint_id,
    get_current_checkpoint_name,
    is_inside_checkpoint,
    is_inside_flow,
)
from ._policy import (
    LangGraphCallCheckpointPolicy,
    LangGraphCapturePolicy,
    resolve_model_checkpoint_config,
    resolve_tool_call_checkpoint_config,
)
from ._serialization import redact_config, to_cache_identity, to_json_safe
from ._tracking import (
    EventContext,
    EventTracker,
    resolve_active_call_checkpoint_policy,
    resolve_active_capture_policy,
)
from ._tracking import get_current_tracker as _get_current_tracker
from ._utils import (
    CheckpointConfig,
    adapter_checkpoint_artifact_refs,
    checkpoint_cache_key,
    get_adapter_checkpoint_artifact_refs,
    run_sync_in_checkpoint,
    safe_step_name,
)

CheckpointMode = Literal["true", "metadata_only"]


class KitaruLangGraphMiddleware(AgentMiddleware):
    """LangChain agent middleware that creates Kitaru call checkpoints.

    The middleware relies on ``KitaruGraphRunner(checkpoint_strategy="calls")``
    to install the active tracker and policy ContextVars. Without that active
    runner context, calls are allowed through normally and no durable Kitaru
    checkpoints are opened.
    """

    def __init__(
        self,
        *,
        graph_name: str | None = None,
        checkpoint_policy: LangGraphCallCheckpointPolicy | None = None,
        capture: LangGraphCapturePolicy | None = None,
    ) -> None:
        super().__init__()
        self._graph_name = graph_name
        self._checkpoint_policy = checkpoint_policy
        self._capture = capture

    @property
    def graph_name(self) -> str | None:
        """Fallback graph name supplied to the middleware constructor."""
        return self._graph_name

    def wrap_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        """Wrap a synchronous LangChain model call."""
        policy = resolve_active_call_checkpoint_policy(self._checkpoint_policy)
        capture = resolve_active_capture_policy(self._capture)
        tracker, event_id, event_context = _start_model_event(capture)
        checkpoint_config = resolve_model_checkpoint_config(policy)
        model_label = _model_label(request)
        checkpoint_name = _model_checkpoint_name(
            tracker=tracker,
            event_context=event_context,
            model_label=model_label,
            graph_name=self._graph_name,
        )

        if _can_open_true_checkpoint(tracker, checkpoint_config):
            effective_config = _checkpoint_config_with_defaults(
                cast(CheckpointConfig, checkpoint_config)
            )
            model_input = (
                _model_input_envelope(request) if capture.save_model_input else None
            )
            checkpoint_inputs = (
                {"model_input": model_input} if model_input is not None else None
            )
            input_artifacts = (
                {"model_input": "model_input"} if capture.save_model_input else {}
            )
            output_artifacts = (
                {"output": "output"} if capture.save_model_response else {}
            )

            def _in_checkpoint() -> Any:
                with adapter_checkpoint_artifact_refs(
                    input_artifacts=input_artifacts,
                    output_artifacts=output_artifacts,
                ):
                    return self._tracked_model_call(
                        request,
                        handler,
                        capture=capture,
                        tracker=tracker,
                        event_id=event_id,
                        event_context=event_context,
                        checkpoint_mode="true",
                        checkpoint_name=checkpoint_name,
                        model_input=model_input,
                    )

            return run_sync_in_checkpoint(
                config=effective_config,
                step_name=checkpoint_name,
                body=_in_checkpoint,
                cache_key=_model_cache_key(
                    request,
                    enabled=effective_config.get("cache", False),
                ),
                checkpoint_inputs=checkpoint_inputs,
            )

        return self._tracked_model_call(
            request,
            handler,
            capture=capture,
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
            checkpoint_mode="metadata_only",
            checkpoint_name=None,
            model_input=None,
        )

    async def awrap_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Wrap an async LangChain model call without opening a checkpoint."""
        capture = resolve_active_capture_policy(self._capture)
        tracker, event_id, event_context = _start_model_event(capture)
        return await self._atracked_model_call(
            request,
            handler,
            capture=capture,
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
        )

    def wrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        """Wrap a synchronous LangChain tool call."""
        policy = resolve_active_call_checkpoint_policy(self._checkpoint_policy)
        capture = resolve_active_capture_policy(self._capture)
        tool_name = _tool_name(request)
        tool_label = _tool_label(request)
        tool_call_id = _tool_call_id(request)
        tracker, event_id, event_context = _start_tool_event(
            capture,
            tool_call_id=tool_call_id,
        )
        checkpoint_config = resolve_tool_call_checkpoint_config(
            policy,
            tool_name=tool_name,
        )
        checkpoint_name = _tool_checkpoint_name(
            tracker=tracker,
            event_context=event_context,
            tool_name=tool_label,
            tool_call_id=tool_call_id,
            graph_name=self._graph_name,
        )

        if _can_open_true_checkpoint(tracker, checkpoint_config):
            effective_config = _checkpoint_config_with_defaults(
                cast(CheckpointConfig, checkpoint_config)
            )
            tool_args = _tool_args_envelope(request) if capture.save_tool_args else None
            checkpoint_inputs = (
                {"tool_args": tool_args} if tool_args is not None else None
            )
            input_artifacts = (
                {"tool_args": "tool_args"} if capture.save_tool_args else {}
            )
            output_artifacts = {"output": "output"} if capture.save_tool_result else {}

            def _in_checkpoint() -> Any:
                with adapter_checkpoint_artifact_refs(
                    input_artifacts=input_artifacts,
                    output_artifacts=output_artifacts,
                ):
                    return self._tracked_tool_call(
                        request,
                        handler,
                        capture=capture,
                        tracker=tracker,
                        event_id=event_id,
                        event_context=event_context,
                        checkpoint_mode="true",
                        checkpoint_name=checkpoint_name,
                        tool_name=tool_name,
                        tool_call_id=tool_call_id,
                    )

            return run_sync_in_checkpoint(
                config=effective_config,
                step_name=checkpoint_name,
                body=_in_checkpoint,
                cache_key=_tool_cache_key(
                    request,
                    tool_name=tool_name,
                    tool_call_id=tool_call_id,
                    enabled=effective_config.get("cache", False),
                ),
                checkpoint_inputs=checkpoint_inputs,
            )

        return self._tracked_tool_call(
            request,
            handler,
            capture=capture,
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
            checkpoint_mode="metadata_only",
            checkpoint_name=None,
            tool_name=tool_name,
            tool_call_id=tool_call_id,
        )

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Wrap an async LangChain tool call without opening a checkpoint."""
        capture = resolve_active_capture_policy(self._capture)
        tool_name = _tool_name(request)
        tool_call_id = _tool_call_id(request)
        tracker, event_id, event_context = _start_tool_event(
            capture,
            tool_call_id=tool_call_id,
        )
        return await self._atracked_tool_call(
            request,
            handler,
            capture=capture,
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
            tool_name=tool_name,
            tool_call_id=tool_call_id,
        )

    def _tracked_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
        *,
        capture: LangGraphCapturePolicy,
        tracker: EventTracker | None,
        event_id: str | None,
        event_context: EventContext | None,
        checkpoint_mode: CheckpointMode,
        checkpoint_name: str | None,
        model_input: Mapping[str, Any] | None,
    ) -> Any:
        if tracker is None or event_id is None or event_context is None:
            return handler(request)

        artifacts = _model_artifacts(capture)
        started_at = time.perf_counter()
        try:
            response = handler(request)
        except Exception as error:
            tracker.record_model_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=_elapsed_ms(started_at),
                artifacts=artifacts,
                metadata=_model_metadata(
                    request,
                    capture=capture,
                    model_input=model_input,
                    response=None,
                ),
                error=error,
                checkpoint_id=_current_checkpoint_id(checkpoint_mode),
                checkpoint_name=_current_checkpoint_name(
                    checkpoint_mode, checkpoint_name
                ),
                model_name=_model_name(request),
                node_name=_node_name(request),
                source="langchain",
                checkpoint_mode=checkpoint_mode,
            )
            raise

        response_messages = _model_response_messages(response)
        tracker.reserve_tool_call_order(
            parent_model_event_id=event_id,
            tool_call_ids=_tool_call_ids_from_messages(response_messages),
        )
        tracker.record_model_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=_elapsed_ms(started_at),
            artifacts=artifacts,
            metadata=_model_metadata(
                request,
                capture=capture,
                model_input=model_input,
                response=response,
                response_messages=response_messages,
            ),
            checkpoint_id=_current_checkpoint_id(checkpoint_mode),
            checkpoint_name=_current_checkpoint_name(checkpoint_mode, checkpoint_name),
            model_name=_model_name(request),
            node_name=_node_name(request),
            source="langchain",
            checkpoint_mode=checkpoint_mode,
        )
        return response

    async def _atracked_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
        *,
        capture: LangGraphCapturePolicy,
        tracker: EventTracker | None,
        event_id: str | None,
        event_context: EventContext | None,
    ) -> Any:
        if tracker is None or event_id is None or event_context is None:
            return await handler(request)

        started_at = time.perf_counter()
        try:
            response = await handler(request)
        except Exception as error:
            tracker.record_model_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=_elapsed_ms(started_at),
                metadata=_model_metadata(
                    request,
                    capture=capture,
                    model_input=None,
                    response=None,
                ),
                error=error,
                model_name=_model_name(request),
                node_name=_node_name(request),
                source="langchain",
                checkpoint_mode="metadata_only",
            )
            raise

        response_messages = _model_response_messages(response)
        tracker.reserve_tool_call_order(
            parent_model_event_id=event_id,
            tool_call_ids=_tool_call_ids_from_messages(response_messages),
        )
        tracker.record_model_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=_elapsed_ms(started_at),
            metadata=_model_metadata(
                request,
                capture=capture,
                model_input=None,
                response=response,
                response_messages=response_messages,
            ),
            model_name=_model_name(request),
            node_name=_node_name(request),
            source="langchain",
            checkpoint_mode="metadata_only",
        )
        return response

    def _tracked_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
        *,
        capture: LangGraphCapturePolicy,
        tracker: EventTracker | None,
        event_id: str | None,
        event_context: EventContext | None,
        checkpoint_mode: CheckpointMode,
        checkpoint_name: str | None,
        tool_name: str,
        tool_call_id: str | None,
    ) -> Any:
        if tracker is None or event_id is None or event_context is None:
            return handler(request)

        artifacts = _tool_artifacts(capture)
        started_at = time.perf_counter()
        try:
            result = handler(request)
        except Exception as error:
            tracker.record_tool_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=_elapsed_ms(started_at),
                artifacts=artifacts,
                metadata=_tool_metadata(request, capture=capture, result=None),
                error=error,
                checkpoint_id=_current_checkpoint_id(checkpoint_mode),
                checkpoint_name=_current_checkpoint_name(
                    checkpoint_mode, checkpoint_name
                ),
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                node_name=_node_name(request),
                source="langchain",
                checkpoint_mode=checkpoint_mode,
            )
            raise

        tracker.record_tool_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=_elapsed_ms(started_at),
            artifacts=artifacts,
            metadata=_tool_metadata(request, capture=capture, result=result),
            checkpoint_id=_current_checkpoint_id(checkpoint_mode),
            checkpoint_name=_current_checkpoint_name(checkpoint_mode, checkpoint_name),
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            node_name=_node_name(request),
            source="langchain",
            checkpoint_mode=checkpoint_mode,
        )
        return result

    async def _atracked_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
        *,
        capture: LangGraphCapturePolicy,
        tracker: EventTracker | None,
        event_id: str | None,
        event_context: EventContext | None,
        tool_name: str,
        tool_call_id: str | None,
    ) -> Any:
        if tracker is None or event_id is None or event_context is None:
            return await handler(request)

        started_at = time.perf_counter()
        try:
            result = await handler(request)
        except Exception as error:
            tracker.record_tool_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=_elapsed_ms(started_at),
                metadata=_tool_metadata(request, capture=capture, result=None),
                error=error,
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                node_name=_node_name(request),
                source="langchain",
                checkpoint_mode="metadata_only",
            )
            raise

        tracker.record_tool_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=_elapsed_ms(started_at),
            metadata=_tool_metadata(request, capture=capture, result=result),
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            node_name=_node_name(request),
            source="langchain",
            checkpoint_mode="metadata_only",
        )
        return result


def _start_model_event(
    capture: LangGraphCapturePolicy,
) -> tuple[EventTracker | None, str | None, EventContext | None]:
    tracker = _get_current_tracker()
    if tracker is None or not capture.emit_call_events:
        return tracker, None, None
    event_id, context = tracker.start_model_event()
    return tracker, event_id, context


def _start_tool_event(
    capture: LangGraphCapturePolicy,
    *,
    tool_call_id: str | None,
) -> tuple[EventTracker | None, str | None, EventContext | None]:
    tracker = _get_current_tracker()
    if tracker is None or not capture.emit_call_events:
        return tracker, None, None
    event_id, context = tracker.start_tool_event(tool_call_id=tool_call_id)
    return tracker, event_id, context


def _can_open_true_checkpoint(
    tracker: EventTracker | None,
    checkpoint_config: CheckpointConfig | None,
) -> bool:
    return (
        tracker is not None
        and checkpoint_config is not None
        and is_inside_flow()
        and not is_inside_checkpoint()
    )


def _checkpoint_config_with_defaults(config: CheckpointConfig) -> CheckpointConfig:
    return {
        **config,
        "retries": config.get("retries", 0),
        "cache": config.get("cache", False),
        "runtime": config.get("runtime", "inline"),
    }


def _model_cache_key(request: Any, *, enabled: bool) -> str | None:
    if not enabled:
        return None
    return checkpoint_cache_key(
        {
            "adapter": "langgraph",
            "integration": "langchain",
            "kind": "model_call",
            "model": _model_identity(request),
            "input": to_cache_identity(_model_input_envelope(request)),
        }
    )


def _tool_cache_key(
    request: Any,
    *,
    tool_name: str,
    tool_call_id: str | None,
    enabled: bool,
) -> str | None:
    if not enabled:
        return None
    return checkpoint_cache_key(
        {
            "adapter": "langgraph",
            "integration": "langchain",
            "kind": "tool_call",
            "tool_name": tool_name,
            "tool_call_id": tool_call_id,
            "tool_args": to_cache_identity(_tool_args_envelope(request)),
        }
    )


def _current_checkpoint_id(checkpoint_mode: CheckpointMode) -> str | None:
    if checkpoint_mode != "true":
        return None
    try:
        return get_current_checkpoint_id() if is_inside_checkpoint() else None
    except Exception:
        return None


def _current_checkpoint_name(
    checkpoint_mode: CheckpointMode,
    fallback: str | None,
) -> str | None:
    if checkpoint_mode != "true":
        return fallback
    try:
        current_name = get_current_checkpoint_name() if is_inside_checkpoint() else None
    except Exception:
        current_name = None
    return current_name or fallback


def _model_checkpoint_name(
    *,
    tracker: EventTracker | None,
    event_context: EventContext | None,
    model_label: str,
    graph_name: str | None,
) -> str:
    graph = (
        tracker.graph_name
        if tracker is not None
        else safe_step_name(graph_name or "graph")
    )
    run_label = tracker.run_label if tracker is not None else "standalone"
    sequence = event_context.sequence_index if event_context is not None else 0
    return safe_step_name(f"model_call__{model_label}_{sequence}__{graph}_{run_label}")


def _tool_checkpoint_name(
    *,
    tracker: EventTracker | None,
    event_context: EventContext | None,
    tool_name: str,
    tool_call_id: str | None,
    graph_name: str | None,
) -> str:
    graph = (
        tracker.graph_name
        if tracker is not None
        else safe_step_name(graph_name or "graph")
    )
    run_label = tracker.run_label if tracker is not None else "standalone"
    sequence = tool_call_id or (
        str(event_context.sequence_index) if event_context is not None else "0"
    )
    return safe_step_name(f"tool_call__{tool_name}_{sequence}__{graph}_{run_label}")


def _model_artifacts(capture: LangGraphCapturePolicy) -> dict[str, str]:
    refs = get_adapter_checkpoint_artifact_refs()
    artifacts: dict[str, str] = {}
    if (
        capture.save_model_input
        and refs is not None
        and "model_input" in refs.input_artifacts
    ):
        artifacts["model_input"] = refs.input_artifacts["model_input"]
    if (
        capture.save_model_response
        and refs is not None
        and "output" in refs.output_artifacts
    ):
        artifacts["output"] = refs.output_artifacts["output"]
    return artifacts


def _tool_artifacts(capture: LangGraphCapturePolicy) -> dict[str, str]:
    refs = get_adapter_checkpoint_artifact_refs()
    artifacts: dict[str, str] = {}
    if (
        capture.save_tool_args
        and refs is not None
        and "tool_args" in refs.input_artifacts
    ):
        artifacts["tool_args"] = refs.input_artifacts["tool_args"]
    if (
        capture.save_tool_result
        and refs is not None
        and "output" in refs.output_artifacts
    ):
        artifacts["output"] = refs.output_artifacts["output"]
    return artifacts


def _model_metadata(
    request: Any,
    *,
    capture: LangGraphCapturePolicy,
    model_input: Mapping[str, Any] | None,
    response: Any | None,
    response_messages: list[Any] | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "model_name": _model_name(request),
        "model_type": _type_label(getattr(request, "model", None)),
        "tool_count": len(_request_tools(request)),
    }
    if capture.save_model_input:
        metadata["input_available"] = model_input is not None
    if response is not None:
        metadata["response_type"] = _type_label(response)
        messages = response_messages or _model_response_messages(response)
        metadata["response_message_count"] = len(messages)
    if response is not None and capture.save_model_usage:
        usage = _usage_from_response(response)
        if usage is not None:
            metadata["usage"] = usage
    return metadata


def _tool_metadata(
    request: Any,
    *,
    capture: LangGraphCapturePolicy,
    result: Any | None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "tool_name": _tool_name(request),
        "tool_call_id": _tool_call_id(request),
        "tool_type": _type_label(getattr(request, "tool", None)),
    }
    if capture.save_tool_args:
        metadata["args_available"] = _tool_args(request) is not None
    if result is not None:
        metadata["result_type"] = _type_label(result)
        metadata["result_status"] = _result_status(result)
    return metadata


def _model_input_envelope(request: Any) -> dict[str, Any]:
    return {
        "model": _model_identity(request),
        "messages": to_json_safe(getattr(request, "messages", None)),
        "system_message": to_json_safe(getattr(request, "system_message", None)),
        "tool_choice": to_json_safe(getattr(request, "tool_choice", None)),
        "tools": [_tool_identity(tool) for tool in _request_tools(request)],
        "response_format": to_json_safe(getattr(request, "response_format", None)),
        "model_settings": redact_config(getattr(request, "model_settings", None)),
    }


def _tool_args_envelope(request: Any) -> dict[str, Any]:
    return {
        "tool_name": _tool_name(request),
        "tool_call_id": _tool_call_id(request),
        "args": to_json_safe(_tool_args(request)),
        "tool_call": to_json_safe(getattr(request, "tool_call", None)),
    }


def _model_identity(request: Any) -> dict[str, Any]:
    model = getattr(request, "model", None)
    return {
        "model_name": _model_name(request),
        "python_type": _type_label(model),
    }


def _model_name(request: Any) -> str | None:
    model = getattr(request, "model", None)
    for attr in ("model_name", "model", "name"):
        value = getattr(model, attr, None)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _model_label(request: Any) -> str:
    return safe_step_name(_model_name(request) or _node_name(request) or "model")


def _request_tools(request: Any) -> list[Any]:
    tools = getattr(request, "tools", None)
    if isinstance(tools, list | tuple):
        return list(tools)
    return []


def _tool_identity(tool: Any) -> dict[str, Any]:
    if isinstance(tool, Mapping):
        return {
            "name": _mapping_string(tool, "name"),
            "python_type": "dict",
        }
    return {
        "name": _object_string(tool, "name"),
        "python_type": _type_label(tool),
    }


def _tool_name(request: Any) -> str:
    tool_call = getattr(request, "tool_call", None)
    if isinstance(tool_call, Mapping):
        name = tool_call.get("name")
        if isinstance(name, str) and name.strip():
            return name
    tool = getattr(request, "tool", None)
    name = getattr(tool, "name", None)
    if isinstance(name, str) and name.strip():
        return name
    return "tool"


def _tool_label(request: Any) -> str:
    return safe_step_name(_tool_name(request))


def _tool_call_id(request: Any) -> str | None:
    tool_call = getattr(request, "tool_call", None)
    value = _call_value(tool_call, "id") or _call_value(tool_call, "tool_call_id")
    if value is None:
        value = _call_value(tool_call, "call_id")
    return str(value) if value is not None else None


def _tool_args(request: Any) -> Any:
    tool_call = getattr(request, "tool_call", None)
    if isinstance(tool_call, Mapping):
        return tool_call.get("args")
    return getattr(tool_call, "args", None)


def _tool_call_ids_from_messages(messages: list[Any]) -> list[str]:
    ids: list[str] = []
    for message in messages:
        tool_calls = _message_tool_calls(message)
        for tool_call in tool_calls:
            tool_call_id = _call_value(tool_call, "id") or _call_value(
                tool_call, "tool_call_id"
            )
            if tool_call_id is None:
                tool_call_id = _call_value(tool_call, "call_id")
            if tool_call_id is not None:
                ids.append(str(tool_call_id))
    return ids


def _model_response_messages(response: Any) -> list[Any]:
    inner = getattr(response, "model_response", response)
    result = getattr(inner, "result", None)
    if isinstance(result, list | tuple):
        return list(result)
    if result is not None:
        return [result]
    if isinstance(inner, list | tuple):
        return list(inner)
    return [inner]


def _message_tool_calls(message: Any) -> list[Any]:
    if isinstance(message, Mapping):
        value = message.get("tool_calls") or message.get("tool_call_chunks")
    else:
        value = getattr(message, "tool_calls", None) or getattr(
            message, "tool_call_chunks", None
        )
    if isinstance(value, list | tuple):
        return list(value)
    return []


def _usage_from_response(response: Any) -> Any | None:
    for value in (response, getattr(response, "model_response", None)):
        if value is None:
            continue
        usage = getattr(value, "usage", None) or getattr(value, "usage_metadata", None)
        if usage is not None:
            return to_json_safe(usage)
        if isinstance(value, Mapping):
            usage = value.get("usage") or value.get("usage_metadata")
            if usage is not None:
                return to_json_safe(usage)
    for message in _model_response_messages(response):
        usage = getattr(message, "usage_metadata", None) or getattr(
            message, "usage", None
        )
        if usage is not None:
            return to_json_safe(usage)
    return None


def _node_name(request: Any) -> str | None:
    runtime = getattr(request, "runtime", None)
    for value in (
        runtime,
        getattr(runtime, "context", None),
        getattr(runtime, "config", None),
    ):
        node_name = _object_string(value, "node_name") or _object_string(value, "name")
        if node_name:
            return node_name
        if isinstance(value, Mapping):
            node_name = _mapping_string(value, "node_name") or _mapping_string(
                value, "name"
            )
            if node_name:
                return node_name
    return None


def _result_status(result: Any) -> str | None:
    status = getattr(result, "status", None)
    if isinstance(status, str):
        return status
    if isinstance(result, Mapping):
        mapping_status = result.get("status")
        if isinstance(mapping_status, str):
            return mapping_status
    return None


def _call_value(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _mapping_string(value: Mapping[str, Any], key: str) -> str | None:
    nested = value.get(key)
    return nested if isinstance(nested, str) and nested.strip() else None


def _object_string(value: Any, attr: str) -> str | None:
    nested = getattr(value, attr, None)
    return nested if isinstance(nested, str) and nested.strip() else None


def _type_label(value: Any) -> str:
    if value is None:
        return "NoneType"
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 3)


__all__ = ["KitaruLangGraphMiddleware"]
