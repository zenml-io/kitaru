"""PydanticAI Model wrapper that records each request as a Kitaru event.

We do not route through ``kitaru.llm`` — it is a text-in/text-out facade over
OpenAI and Anthropic only and would discard streaming, tool calls, structured
output, and every other provider PydanticAI supports. Granular mode reuses the
``type='llm_call'`` checkpoint convention so dashboards group native and adapter
LLM calls uniformly.
"""

import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

import kitaru
from pydantic import TypeAdapter
from pydantic_core import PydanticSerializationError

from pydantic_ai._run_context import RunContext
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelResponse,
    ModelResponseStreamEvent,
)
from pydantic_ai.models import ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import RequestUsage

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._logging import logger
from ._otel import attach_model_correlation
from ._policy import CapturePolicy
from ._tracking import EventTracker, ModelEventContext, get_current_tracker
from ._utils import (
    CheckpointConfig,
    checkpoint_cache_key,
    run_async_in_checkpoint,
    with_default_type,
)

_MODEL_RESPONSE_ADAPTER = TypeAdapter(ModelResponse)
_MODEL_STREAM_EVENT_ADAPTER = TypeAdapter(ModelResponseStreamEvent)
_VOLATILE_MESSAGE_ENVELOPE_KEYS = frozenset({"timestamp", "run_id"})


def _serialize_messages(messages: list[ModelMessage]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        ModelMessagesTypeAdapter.dump_python(messages, mode="json"),
    )


def _without_volatile_message_envelope_keys(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in item.items()
        if key not in _VOLATILE_MESSAGE_ENVELOPE_KEYS
    }


def _strip_message_envelope_for_cache(message: dict[str, Any]) -> dict[str, Any]:
    """Return a cache-key copy without PydanticAI per-run envelope fields."""
    stable_message = _without_volatile_message_envelope_keys(message)
    parts = stable_message.get("parts")
    if isinstance(parts, list):
        # Strip at parts level only. Nested dicts, such as tool args/results, are
        # user payload and must stay intact even when they use these key names.
        stable_message["parts"] = [
            _without_volatile_message_envelope_keys(part)
            if isinstance(part, dict)
            else part
            for part in parts
        ]
    return stable_message


def _serialize_messages_for_cache(messages: list[ModelMessage]) -> list[dict[str, Any]]:
    """Serialize messages for cache keys while preserving user payload content."""
    return [
        _strip_message_envelope_for_cache(message)
        for message in _serialize_messages(messages)
    ]


def _serialize_model_response(response: ModelResponse) -> dict[str, Any]:
    return cast(
        dict[str, Any], _MODEL_RESPONSE_ADAPTER.dump_python(response, mode="json")
    )


def _trackable_tool_call_ids(
    response: ModelResponse,
    model_request_parameters: ModelRequestParameters,
    capture: CapturePolicy,
) -> list[str]:
    """Return executable response tool-call ids that will produce Kitaru events."""
    function_tool_names = {
        tool.name for tool in model_request_parameters.function_tools
    }
    tool_call_ids: list[str] = []
    for tool_call in response.tool_calls:
        tool_call_id = tool_call.tool_call_id
        if (
            tool_call.tool_name in function_tool_names
            and isinstance(tool_call_id, str)
            and tool_call_id
            and capture.capture_mode_for_tool(tool_call.tool_name) is not None
        ):
            tool_call_ids.append(tool_call_id)
    return tool_call_ids


def _serialize_stream_event(event: Any) -> dict[str, Any]:
    try:
        return cast(
            dict[str, Any], _MODEL_STREAM_EVENT_ADAPTER.dump_python(event, mode="json")
        )
    except (TypeError, ValueError, PydanticSerializationError):
        logger.warning(
            "Failed to serialize PydanticAI stream event; falling back to repr.",
            exc_info=True,
        )
        return {
            "event_type": type(event).__name__,
            "repr": repr(event),
            "serialization_error": "stream_event_serialization_failed",
        }


class KitaruStreamedResponse(StreamedResponse):
    """`StreamedResponse` proxy that invokes `on_event` for each streamed event."""

    def __init__(
        self, wrapped: StreamedResponse, *, on_event: Callable[[Any], None]
    ) -> None:
        super().__init__(wrapped.model_request_parameters)
        self._wrapped = wrapped
        self._on_event = on_event

    async def _get_event_iterator(self) -> AsyncIterator[ModelResponseStreamEvent]:
        try:
            async for event in self._wrapped:
                self._on_event(event)
                yield event
        finally:
            self.final_result_event = self._wrapped.final_result_event

    def get(self) -> ModelResponse:
        return self._wrapped.get()

    def usage(self) -> RequestUsage:
        return self._wrapped.usage()

    @property
    def model_name(self) -> str:
        return self._wrapped.model_name

    @property
    def provider_name(self) -> str:
        return self._wrapped.provider_name or ""

    @property
    def provider_url(self) -> str | None:
        return self._wrapped.provider_url

    @property
    def timestamp(self) -> datetime:
        return self._wrapped.timestamp


class KitaruModel(WrapperModel):
    """Wrapper model that records each request as a `ModelEvent` when inside a checkpoint."""

    def __init__(
        self,
        wrapped: Any,
        *,
        capture: CapturePolicy,
        agent_name: str,
        checkpoint_config: CheckpointConfig | None = None,
    ) -> None:
        super().__init__(wrapped)
        self._capture = capture
        self._agent_name = agent_name
        self._checkpoint_config = checkpoint_config

    def _should_track(self) -> bool:
        return self._capture.emit_child_events and is_inside_checkpoint()

    def _reserve_tool_call_order(
        self,
        *,
        tracker: EventTracker,
        event_id: str,
        event_context: ModelEventContext,
        response: ModelResponse,
        model_request_parameters: ModelRequestParameters,
    ) -> None:
        tracker.reserve_tool_call_order(
            parent_model_event_id=event_id,
            turn_index=event_context.turn_index,
            tool_call_ids=_trackable_tool_call_ids(
                response,
                model_request_parameters,
                self._capture,
            ),
        )

    def _record_cached_model_checkpoint_event(
        self,
        *,
        response: ModelResponse,
        model_request_parameters: ModelRequestParameters,
        duration_ms: float,
    ) -> None:
        tracker = get_current_tracker()
        if tracker is None or not self._capture.emit_child_events:
            return

        event_id, event_context = tracker.start_model_event()
        if self._capture.correlate_otel_spans:
            attach_model_correlation(event_id, event_context)
        tracker.record_model_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=duration_ms,
            artifacts={},
            model_name=response.model_name,
            usage=response.usage,
        )
        self._reserve_tool_call_order(
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
            response=response,
            model_request_parameters=model_request_parameters,
        )

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        if (
            self._checkpoint_config is not None
            and is_inside_flow()
            and not is_inside_checkpoint()
        ):
            body_executed = False

            # `_in_checkpoint` closes over live `messages` / `self`; safe only for
            # inline runtime. `run_async_in_checkpoint` rejects `runtime='isolated'`
            # to stop these references from hitting a pickling boundary.
            async def _in_checkpoint() -> ModelResponse:
                nonlocal body_executed
                body_executed = True
                return await self._tracked_request(
                    messages, model_settings, model_request_parameters
                )

            cache_key = None
            if self._checkpoint_config.get("cache") is not False:
                cache_key = checkpoint_cache_key(
                    {
                        "messages": _serialize_messages_for_cache(messages),
                        "model_settings": model_settings,
                        "model_request_parameters": model_request_parameters,
                    }
                )
            started_at = time.perf_counter()
            response = await run_async_in_checkpoint(
                config=with_default_type(self._checkpoint_config, "llm_call"),
                step_name=f"{self._agent_name}_model_request",
                body=_in_checkpoint,
                cache_key=cache_key,
            )
            if not body_executed:
                self._record_cached_model_checkpoint_event(
                    response=response,
                    model_request_parameters=model_request_parameters,
                    duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                )
            return response
        return await self._tracked_request(
            messages, model_settings, model_request_parameters
        )

    async def _tracked_request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        tracker = get_current_tracker()
        if tracker is None or not self._should_track():
            return await super().request(
                messages, model_settings, model_request_parameters
            )

        event_id, event_context = tracker.start_model_event()
        if self._capture.correlate_otel_spans:
            attach_model_correlation(event_id, event_context)

        artifacts: dict[str, str] = {}
        if self._capture.save_prompts:
            prompt_key = tracker.artifact_name(event_id, "prompt")
            kitaru.save(prompt_key, _serialize_messages(messages), type="prompt")
            artifacts["prompt"] = prompt_key

        started_at = time.perf_counter()
        try:
            response = await super().request(
                messages, model_settings, model_request_parameters
            )
        except Exception as error:
            tracker.record_model_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                artifacts=artifacts,
                error=error,
            )
            raise

        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        if self._capture.save_responses:
            response_key = tracker.artifact_name(event_id, "response")
            kitaru.save(
                response_key, _serialize_model_response(response), type="response"
            )
            artifacts["response"] = response_key

        tracker.record_model_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=duration_ms,
            artifacts=artifacts,
            model_name=response.model_name,
            usage=response.usage,
        )
        self._reserve_tool_call_order(
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
            response=response,
            model_request_parameters=model_request_parameters,
        )
        return response

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        tracker = get_current_tracker()
        if tracker is None or not self._should_track():
            async with super().request_stream(
                messages, model_settings, model_request_parameters, run_context
            ) as streamed_response:
                yield streamed_response
            return

        event_id, event_context = tracker.start_model_event()
        if self._capture.correlate_otel_spans:
            attach_model_correlation(event_id, event_context)

        artifacts: dict[str, str] = {}
        if self._capture.save_prompts:
            prompt_key = tracker.artifact_name(event_id, "prompt")
            kitaru.save(prompt_key, _serialize_messages(messages), type="prompt")
            artifacts["prompt"] = prompt_key

        save_transcripts = self._capture.save_stream_transcripts
        save_responses = self._capture.save_responses
        stream_events: list[dict[str, Any]] = []
        stream_event_count = 0

        def _on_stream_event(event: Any) -> None:
            nonlocal stream_event_count
            stream_event_count += 1
            if save_transcripts:
                stream_events.append(_serialize_stream_event(event))

        started_at = time.perf_counter()
        try:
            async with super().request_stream(
                messages, model_settings, model_request_parameters, run_context
            ) as streamed_response:
                tracked_stream = KitaruStreamedResponse(
                    streamed_response, on_event=_on_stream_event
                )
                yield tracked_stream
            response = tracked_stream.get()
        except Exception as error:
            tracker.record_model_event(
                event_id,
                event_context,
                status="failed",
                duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                artifacts=artifacts,
                error=error,
                stream_event_count=stream_event_count,
            )
            raise

        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        serialized_response: dict[str, Any] | None = None
        if save_responses or save_transcripts:
            serialized_response = _serialize_model_response(response)
        if save_responses:
            response_key = tracker.artifact_name(event_id, "response")
            kitaru.save(response_key, serialized_response, type="response")
            artifacts["response"] = response_key
        if save_transcripts:
            transcript_key = tracker.artifact_name(event_id, "stream_transcript")
            kitaru.save(
                transcript_key,
                {
                    "event_count": stream_event_count,
                    "duration_ms": duration_ms,
                    "events": stream_events,
                    "final_response": serialized_response,
                },
                type="context",
            )
            artifacts["stream_transcript"] = transcript_key

        tracker.record_model_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=duration_ms,
            artifacts=artifacts,
            model_name=response.model_name,
            usage=response.usage,
            stream_event_count=stream_event_count,
        )
        # Pydantic AI only builds the CallToolsNode after this stream context
        # closes and the final ModelResponse is returned, so this reservation is
        # still made before any corresponding KitaruToolset calls can start.
        self._reserve_tool_call_order(
            tracker=tracker,
            event_id=event_id,
            event_context=event_context,
            response=response,
            model_request_parameters=model_request_parameters,
        )
