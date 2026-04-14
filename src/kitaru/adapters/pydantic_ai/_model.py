import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

from pydantic import TypeAdapter
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
from pydantic_core import PydanticSerializationError

import kitaru

from ._kitaru_internal import is_inside_checkpoint
from ._otel import attach_model_correlation
from ._policy import CapturePolicy
from ._tracking import artifact_name, get_current_tracker

_MODEL_RESPONSE_ADAPTER = TypeAdapter(ModelResponse)
_MODEL_STREAM_EVENT_ADAPTER = TypeAdapter(ModelResponseStreamEvent)


def _serialize_messages(messages: list[ModelMessage]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        ModelMessagesTypeAdapter.dump_python(messages, mode="json"),
    )


def _serialize_model_response(response: ModelResponse) -> dict[str, Any]:
    return cast(
        dict[str, Any], _MODEL_RESPONSE_ADAPTER.dump_python(response, mode="json")
    )


def _serialize_stream_event(event: Any) -> dict[str, Any]:
    try:
        return cast(
            dict[str, Any], _MODEL_STREAM_EVENT_ADAPTER.dump_python(event, mode="json")
        )
    except (TypeError, ValueError, PydanticSerializationError):
        return {"event_type": type(event).__name__, "repr": repr(event)}


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
    """Records each request as a ``ModelEvent`` when inside a checkpoint."""

    def __init__(
        self, wrapped: Any, *, capture: CapturePolicy, agent_name: str
    ) -> None:
        super().__init__(wrapped)
        self._capture = capture
        self._agent_name = agent_name

    def _should_track(self) -> bool:
        return self._capture.emit_child_events and is_inside_checkpoint()

    async def request(
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
            prompt_key = artifact_name(event_id, "prompt")
            kitaru.save(prompt_key, _serialize_messages(messages), type="prompt")
            artifacts["prompt"] = prompt_key

        started_at = time.perf_counter()
        try:
            response = await super().request(
                messages, model_settings, model_request_parameters
            )
        except BaseException as error:
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
            response_key = artifact_name(event_id, "response")
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
            prompt_key = artifact_name(event_id, "prompt")
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
        except BaseException as error:
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
            response_key = artifact_name(event_id, "response")
            kitaru.save(response_key, serialized_response, type="response")
            artifacts["response"] = response_key
        if save_transcripts:
            transcript_key = artifact_name(event_id, "stream_transcript")
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
