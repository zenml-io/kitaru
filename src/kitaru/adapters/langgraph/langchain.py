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
    from langchain_core.messages import (
        BaseMessage,
        messages_from_dict,
        messages_to_dict,
    )
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
from ._sandbox_tool import (
    _SANDBOX_COMMAND_TOOL_CACHE_IDENTITY_ATTR,
    SandboxCommandToolArgs,
    create_sandbox_command_tool,
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

_MODEL_RESPONSE_CHECKPOINT_ENVELOPE_SCHEMA = (
    "kitaru.langgraph.langchain.model_response_checkpoint.v1"
)
_MODEL_RESPONSE_CHECKPOINT_SCHEMA_KEY = "schema"
_MODEL_RESPONSE_CHECKPOINT_RESPONSE_KEY = "response"
_MODEL_RESPONSE_CHECKPOINT_MESSAGES_KEY = "messages"
_MODEL_RESPONSE_CHECKPOINT_TARGET_KEY = "target"
_MODEL_RESPONSE_CHECKPOINT_RESULT_IS_SEQUENCE_KEY = "result_is_sequence"
_MODEL_RESPONSE_CHECKPOINT_RESULT_TARGET = "result"
_MODEL_RESPONSE_CHECKPOINT_RESPONSE_TARGET = "response"
ModelResponseCheckpointTarget = Literal["result", "response"]


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
                _persisted_model_input_envelope(request)
                if capture.save_model_input
                else None
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
                    response = self._tracked_model_call(
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
                    return _model_response_checkpoint_envelope(response)

            checkpoint_output = run_sync_in_checkpoint(
                config=effective_config,
                step_name=checkpoint_name,
                body=_in_checkpoint,
                cache_key=_model_cache_key(
                    request,
                    enabled=effective_config.get("cache", False),
                ),
                checkpoint_inputs=checkpoint_inputs,
            )
            return _restore_model_response_checkpoint_output(checkpoint_output)

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
            persisted_tool_args = (
                _persisted_tool_args_envelope(request)
                if capture.save_tool_args
                else None
            )
            checkpoint_inputs = (
                {"tool_args": persisted_tool_args}
                if persisted_tool_args is not None
                else None
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

        input_artifacts = _model_artifacts(capture, include_output=False)
        output_artifacts = _model_artifacts(capture, include_output=True)
        started_at = time.perf_counter()
        try:
            response = handler(request)
        except Exception as error:
            tracker.record_model_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=_elapsed_ms(started_at),
                artifacts=input_artifacts,
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
            artifacts=output_artifacts,
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

        input_artifacts = _tool_artifacts(capture, include_output=False)
        output_artifacts = _tool_artifacts(capture, include_output=True)
        started_at = time.perf_counter()
        try:
            result = handler(request)
        except Exception as error:
            tracker.record_tool_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=_elapsed_ms(started_at),
                artifacts=input_artifacts,
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
            artifacts=output_artifacts,
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
            "model_response_checkpoint_schema": (
                _MODEL_RESPONSE_CHECKPOINT_ENVELOPE_SCHEMA
            ),
            "model": _model_identity(request),
            "input": _model_cache_identity_envelope(request),
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
            "tool_args": _tool_cache_identity_envelope(request),
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
    sequence = (
        event_context.sequence_index
        if event_context is not None
        else tracker.next_checkpoint_sequence()
        if tracker is not None
        else 0
    )
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
    event_sequence = str(
        event_context.sequence_index
        if event_context is not None
        else tracker.next_checkpoint_sequence()
        if tracker is not None
        else 0
    )
    call_disambiguator = (
        f"{tool_call_id}_{event_sequence}" if tool_call_id else event_sequence
    )
    return safe_step_name(
        f"tool_call__{tool_name}_{call_disambiguator}__{graph}_{run_label}"
    )


def _model_artifacts(
    capture: LangGraphCapturePolicy, *, include_output: bool
) -> dict[str, str]:
    refs = get_adapter_checkpoint_artifact_refs()
    artifacts: dict[str, str] = {}
    if (
        capture.save_model_input
        and refs is not None
        and "model_input" in refs.input_artifacts
    ):
        artifacts["model_input"] = refs.input_artifacts["model_input"]
    if (
        include_output
        and capture.save_model_response
        and refs is not None
        and "output" in refs.output_artifacts
    ):
        artifacts["output"] = refs.output_artifacts["output"]
    return artifacts


def _tool_artifacts(
    capture: LangGraphCapturePolicy, *, include_output: bool
) -> dict[str, str]:
    refs = get_adapter_checkpoint_artifact_refs()
    artifacts: dict[str, str] = {}
    if (
        capture.save_tool_args
        and refs is not None
        and "tool_args" in refs.input_artifacts
    ):
        artifacts["tool_args"] = refs.input_artifacts["tool_args"]
    if (
        include_output
        and capture.save_tool_result
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


def _persisted_model_input_envelope(request: Any) -> dict[str, Any]:
    """Return model-call input metadata safe to persist as checkpoint input.

    Prompts and system messages are free text, so they can contain secrets even
    when their field names do not look secret-like. Persist only structural
    details here. The raw-enough values used for cache identity live in
    ``_model_cache_identity_envelope`` and are hashed immediately instead of
    being stored in Kitaru artifacts or event metadata.
    """
    messages = _request_messages(request)
    return {
        "model": _model_identity(request),
        "message_count": len(messages),
        "messages": [_message_summary(message) for message in messages],
        "system_message": _system_message_summary(
            getattr(request, "system_message", None)
        ),
        "tool_choice": redact_config(getattr(request, "tool_choice", None)),
        "tools": [_tool_identity(tool) for tool in _request_tools(request)],
        "response_format": redact_config(getattr(request, "response_format", None)),
        "model_settings": redact_config(getattr(request, "model_settings", None)),
    }


def _model_cache_identity_envelope(request: Any) -> dict[str, Any]:
    """Return raw-enough model-call identity used only for cache hashing."""
    return {
        "model": _model_identity(request),
        "messages": to_cache_identity(getattr(request, "messages", None)),
        "system_message": to_cache_identity(getattr(request, "system_message", None)),
        "tool_choice": to_cache_identity(getattr(request, "tool_choice", None)),
        "tools": to_cache_identity(getattr(request, "tools", None)),
        "response_format": to_cache_identity(getattr(request, "response_format", None)),
        "model_settings": to_cache_identity(getattr(request, "model_settings", None)),
    }


def _request_messages(request: Any) -> list[Any]:
    messages = getattr(request, "messages", None)
    if isinstance(messages, list | tuple):
        return list(messages)
    return []


def _message_summary(message: Any) -> dict[str, Any]:
    tool_calls = _message_tool_calls(message)
    return {
        "type": _short_type_label(message),
        "role": _string_or_none(
            _call_value(message, "role") or _call_value(message, "type")
        ),
        "name": _string_or_none(_call_value(message, "name")),
        "content": (
            "[OMITTED]" if _call_value(message, "content") is not None else None
        ),
        "tool_call_count": len(tool_calls),
        "tool_call_ids": _tool_call_ids_from_call_payloads(tool_calls),
    }


def _system_message_summary(system_message: Any) -> dict[str, Any]:
    if system_message is None:
        return {"present": False, "content": None}
    return {
        "present": True,
        "type": _short_type_label(system_message),
        "content": "[OMITTED]",
    }


def _tool_call_ids_from_call_payloads(tool_calls: list[Any]) -> list[str]:
    ids: list[str] = []
    for tool_call in tool_calls:
        tool_call_id = _call_value(tool_call, "id") or _call_value(
            tool_call, "tool_call_id"
        )
        if tool_call_id is None:
            tool_call_id = _call_value(tool_call, "call_id")
        if tool_call_id is not None:
            ids.append(str(tool_call_id))
    return ids


def _persisted_tool_args_envelope(request: Any) -> dict[str, Any]:
    """Return the redacted tool payload safe to persist as checkpoint input."""
    args = redact_config(_tool_args(request))
    tool_call = redact_config(getattr(request, "tool_call", None))
    if _is_sandbox_command_tool_request(request):
        args = _redact_sandbox_command_text(args)
        tool_call = _redact_sandbox_command_text(tool_call)
    return _tool_payload_envelope(
        request,
        args=args,
        tool_call=tool_call,
    )


def _is_sandbox_command_tool_request(request: Any) -> bool:
    tool = getattr(request, "tool", None)
    return getattr(tool, "args_schema", None) is SandboxCommandToolArgs


def _redact_sandbox_command_text(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, nested in value.items():
            key_text = _safe_mapping_key_text(key)
            redacted[key_text] = (
                "[REDACTED]"
                if key_text == "command"
                else _redact_sandbox_command_text(nested)
            )
        return redacted
    if isinstance(value, list):
        return [_redact_sandbox_command_text(item) for item in value]
    return value


def _safe_mapping_key_text(key: Any) -> str:
    try:
        return str(key)
    except Exception:
        return f"<unprintable key {_type_label(key)}>"


def _tool_cache_identity_envelope(request: Any) -> dict[str, Any]:
    """Return the raw-enough tool payload used only for immediate cache hashing."""
    payload = _tool_payload_envelope(
        request,
        args=to_cache_identity(_tool_args(request)),
        tool_call=to_cache_identity(getattr(request, "tool_call", None)),
    )
    private_identity = _tool_private_cache_identity(request)
    if private_identity is not None:
        payload["tool_private_identity"] = private_identity
    return payload


def _tool_private_cache_identity(request: Any) -> Any | None:
    tool = getattr(request, "tool", None)
    identity = getattr(tool, _SANDBOX_COMMAND_TOOL_CACHE_IDENTITY_ATTR, None)
    if identity is None:
        return None
    return to_cache_identity(identity)


def _tool_payload_envelope(
    request: Any,
    *,
    args: Any,
    tool_call: Any,
) -> dict[str, Any]:
    return {
        "tool_name": _tool_name(request),
        "tool_call_id": _tool_call_id(request),
        "args": args,
        "tool_call": tool_call,
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


def _model_response_checkpoint_envelope(response: Any) -> dict[str, Any]:
    """Return a checkpoint-safe wrapper for LangChain model responses."""
    payload: dict[str, Any] = {
        _MODEL_RESPONSE_CHECKPOINT_SCHEMA_KEY: (
            _MODEL_RESPONSE_CHECKPOINT_ENVELOPE_SCHEMA
        ),
        _MODEL_RESPONSE_CHECKPOINT_RESPONSE_KEY: response,
    }
    message_payload = _model_response_message_payload(response)
    if message_payload is not None:
        payload[_MODEL_RESPONSE_CHECKPOINT_MESSAGES_KEY] = message_payload
    return payload


def _model_response_message_payload(response: Any) -> dict[str, Any] | None:
    result_ref = _model_response_result_ref(response)
    if result_ref is not None:
        _owner, result = result_ref
        if isinstance(result, list | tuple) and _all_langchain_messages(result):
            return _model_response_messages_payload(
                target=_MODEL_RESPONSE_CHECKPOINT_RESULT_TARGET,
                result_is_sequence=True,
                messages=list(result),
            )
        if isinstance(result, BaseMessage):
            return _model_response_messages_payload(
                target=_MODEL_RESPONSE_CHECKPOINT_RESULT_TARGET,
                result_is_sequence=False,
                messages=[result],
            )
    if isinstance(response, list | tuple) and _all_langchain_messages(response):
        return _model_response_messages_payload(
            target=_MODEL_RESPONSE_CHECKPOINT_RESPONSE_TARGET,
            result_is_sequence=True,
            messages=list(response),
        )
    return None


def _model_response_messages_payload(
    *,
    target: ModelResponseCheckpointTarget,
    result_is_sequence: bool,
    messages: list[BaseMessage],
) -> dict[str, Any]:
    return {
        _MODEL_RESPONSE_CHECKPOINT_TARGET_KEY: target,
        _MODEL_RESPONSE_CHECKPOINT_RESULT_IS_SEQUENCE_KEY: result_is_sequence,
        _MODEL_RESPONSE_CHECKPOINT_MESSAGES_KEY: messages_to_dict(messages),
    }


def _restore_model_response_checkpoint_output(output: Any) -> Any:
    if not _is_model_response_checkpoint_envelope(output):
        return output
    response = output.get(_MODEL_RESPONSE_CHECKPOINT_RESPONSE_KEY)
    message_payload = output.get(_MODEL_RESPONSE_CHECKPOINT_MESSAGES_KEY)
    if message_payload is None:
        return response
    if not isinstance(message_payload, Mapping):
        raise RuntimeError("Invalid LangGraph model checkpoint envelope: messages.")
    message_dicts = message_payload.get(_MODEL_RESPONSE_CHECKPOINT_MESSAGES_KEY)
    if not isinstance(message_dicts, list):
        raise RuntimeError("Invalid LangGraph model checkpoint envelope: message list.")
    restored_messages = messages_from_dict(message_dicts)
    target = message_payload.get(_MODEL_RESPONSE_CHECKPOINT_TARGET_KEY)
    result_is_sequence = (
        message_payload.get(_MODEL_RESPONSE_CHECKPOINT_RESULT_IS_SEQUENCE_KEY)
        is not False
    )
    if target == _MODEL_RESPONSE_CHECKPOINT_RESPONSE_TARGET:
        return (
            restored_messages
            if result_is_sequence
            else _single_restored_message(restored_messages)
        )
    if target == _MODEL_RESPONSE_CHECKPOINT_RESULT_TARGET:
        return _restore_model_response_result(
            response,
            restored_messages,
            result_is_sequence=result_is_sequence,
        )
    raise RuntimeError("Invalid LangGraph model checkpoint envelope: target.")


def _is_model_response_checkpoint_envelope(output: Any) -> bool:
    return (
        isinstance(output, Mapping)
        and output.get(_MODEL_RESPONSE_CHECKPOINT_SCHEMA_KEY)
        == _MODEL_RESPONSE_CHECKPOINT_ENVELOPE_SCHEMA
    )


def _restore_model_response_result(
    response: Any,
    restored_messages: list[BaseMessage],
    *,
    result_is_sequence: bool,
) -> Any:
    result_ref = _model_response_result_ref(response)
    if result_ref is None:
        raise RuntimeError(
            "LangGraph model checkpoint response no longer has a result field."
        )
    owner, _original_result = result_ref
    result_value: Any = (
        restored_messages
        if result_is_sequence
        else _single_restored_message(restored_messages)
    )
    try:
        owner.result = result_value
        return response
    except Exception as assign_error:
        copied_owner, copy_error = _copy_with_result(owner, result_value)
        if copied_owner is None:
            raise RuntimeError(
                "LangGraph model checkpoint response could not restore "
                "LangChain message subclasses."
            ) from (copy_error or assign_error)
        if owner is response:
            return copied_owner
        try:
            response.model_response = copied_owner
            return response
        except Exception as nested_assign_error:
            copied_response, nested_copy_error = _copy_with_model_response(
                response, copied_owner
            )
            if copied_response is not None:
                return copied_response
            raise RuntimeError(
                "LangGraph model checkpoint response could not restore nested "
                "LangChain message subclasses."
            ) from (nested_copy_error or nested_assign_error)


def _single_restored_message(
    restored_messages: list[BaseMessage],
) -> BaseMessage | None:
    return restored_messages[0] if restored_messages else None


def _copy_with_result(
    owner: Any,
    result: Any,
) -> tuple[Any | None, Exception | None]:
    return _copy_with_update(owner, {"result": result})


def _copy_with_model_response(
    response: Any,
    model_response: Any,
) -> tuple[Any | None, Exception | None]:
    return _copy_with_update(response, {"model_response": model_response})


def _copy_with_update(
    value: Any,
    update: dict[str, Any],
) -> tuple[Any | None, Exception | None]:
    last_error: Exception | None = None
    for method_name in ("model_copy", "copy"):
        copy_method = getattr(value, method_name, None)
        if not callable(copy_method):
            continue
        try:
            return copy_method(update=update), None
        except Exception as error:
            last_error = error
    return None, last_error


def _model_response_result_ref(response: Any) -> tuple[Any, Any] | None:
    inner = getattr(response, "model_response", response)
    return (inner, getattr(inner, "result", None)) if hasattr(inner, "result") else None


def _all_langchain_messages(messages: list[Any] | tuple[Any, ...]) -> bool:
    return all(isinstance(message, BaseMessage) for message in messages)


def _model_response_messages(response: Any) -> list[Any]:
    result_ref = _model_response_result_ref(response)
    if result_ref is not None:
        _owner, result = result_ref
        if isinstance(result, list | tuple):
            return list(result)
        if result is not None:
            return [result]
    inner = getattr(response, "model_response", response)
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


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _short_type_label(value: Any) -> str:
    if value is None:
        return "NoneType"
    return type(value).__qualname__


def _type_label(value: Any) -> str:
    if value is None:
        return "NoneType"
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 3)


__all__ = [
    "KitaruLangGraphMiddleware",
    "SandboxCommandToolArgs",
    "create_sandbox_command_tool",
]
