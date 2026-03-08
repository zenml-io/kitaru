"""Model interception for Kitaru's PydanticAI adapter."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

from pydantic import TypeAdapter
from pydantic_ai import ModelMessage, ModelResponse, ModelResponseStreamEvent
from pydantic_ai.models import ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import RunContext
from pydantic_ai.usage import RequestUsage

from kitaru.artifacts import save
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint

from ._tracking import ModelEventContext, get_current_tracker

_MODEL_RESPONSE_ADAPTER = TypeAdapter(ModelResponse)
_MODEL_MESSAGES_ADAPTER = TypeAdapter(list[ModelMessage])
_MODEL_STREAM_EVENT_ADAPTER = TypeAdapter(ModelResponseStreamEvent)


def _serialize_messages(messages: list[ModelMessage]) -> list[dict[str, Any]]:
    """Serialize model messages to JSON-compatible data."""
    return cast(
        list[dict[str, Any]],
        _MODEL_MESSAGES_ADAPTER.dump_python(messages, mode="json"),
    )


def _serialize_model_response(response: ModelResponse) -> dict[str, Any]:
    """Serialize a model response to JSON-compatible data."""
    return cast(
        dict[str, Any],
        _MODEL_RESPONSE_ADAPTER.dump_python(response, mode="json"),
    )


def _serialize_stream_event(event: Any) -> dict[str, Any]:
    """Serialize one stream event with a resilient fallback shape."""
    try:
        return cast(
            dict[str, Any],
            _MODEL_STREAM_EVENT_ADAPTER.dump_python(event, mode="json"),
        )
    except Exception:
        return {
            "event_type": event.__class__.__name__,
            "repr": repr(event),
        }


def _usage_payload(response: ModelResponse) -> dict[str, int | None]:
    """Extract token usage metrics from a model response."""
    usage = response.usage
    return {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens,
    }


def _error_payload(error: Exception) -> dict[str, str]:
    """Build a lightweight error payload for child-event metadata."""
    return {
        "type": error.__class__.__name__,
        "message": str(error),
    }


def _save_with_fallback(name: str, value: Any, *, artifact_type: str) -> str:
    """Save an artifact and fall back to a blob repr if serialization fails."""
    try:
        save(name, value, type=artifact_type)
        return artifact_type
    except Exception:
        fallback_value = {
            "repr": repr(value),
            "python_type": value.__class__.__name__,
        }
        save(name, fallback_value, type="blob")
        return "blob"


class KitaruStreamedResponse(StreamedResponse):
    """Proxy stream response that tees events for transcript recording."""

    def __init__(
        self,
        wrapped: StreamedResponse,
        *,
        on_event: Callable[[Any], None],
    ) -> None:
        super().__init__(wrapped.model_request_parameters)
        self._wrapped = wrapped
        self._on_event = on_event

    def __aiter__(self) -> AsyncIterator[Any]:
        async def _iter() -> AsyncIterator[Any]:
            async for event in self._wrapped:
                self.final_result_event = self._wrapped.final_result_event
                self._on_event(event)
                yield event
            self.final_result_event = self._wrapped.final_result_event

        return _iter()

    async def _get_event_iterator(self) -> AsyncIterator[ModelResponseStreamEvent]:
        async for event in self._wrapped:
            self.final_result_event = self._wrapped.final_result_event
            self._on_event(event)
            yield event
        self.final_result_event = self._wrapped.final_result_event

    def get(self) -> ModelResponse:
        return self._wrapped.get()

    def usage(self) -> RequestUsage:
        return self._wrapped.usage()

    @property
    def model_name(self) -> str:
        return self._wrapped.model_name

    @property
    def provider_name(self) -> str | None:
        return self._wrapped.provider_name

    @property
    def provider_url(self) -> str | None:
        return self._wrapped.provider_url

    @property
    def timestamp(self) -> datetime:
        return self._wrapped.timestamp


class KitaruModel(WrapperModel):
    """Model wrapper that records model requests as checkpoint child events."""

    def _build_model_event_payload(
        self,
        *,
        event_context: ModelEventContext,
        status: str,
        duration_ms: float,
        prompt_artifact: str,
        response_artifact: str | None,
        transcript_artifact: str | None,
        model_response: ModelResponse | None,
        error: Exception | None = None,
        stream_event_count: int | None = None,
    ) -> dict[str, Any]:
        """Build metadata payload for one tracked model call."""
        artifacts: dict[str, str] = {"prompt": prompt_artifact}
        if response_artifact is not None:
            artifacts["response"] = response_artifact
        if transcript_artifact is not None:
            artifacts["stream_transcript"] = transcript_artifact

        payload: dict[str, Any] = {
            "type": "llm_call",
            "status": status,
            "sequence_index": event_context.sequence_index,
            "turn_index": event_context.turn_index,
            "parent_event_ids": event_context.parent_event_ids,
            "fan_in_from": event_context.fan_in_from,
            "duration_ms": duration_ms,
            "artifacts": artifacts,
        }

        if model_response is not None:
            payload["model_name"] = model_response.model_name
            payload["usage"] = _usage_payload(model_response)

        if stream_event_count is not None:
            payload["stream_event_count"] = stream_event_count

        if error is not None:
            payload["error"] = _error_payload(error)

        return payload

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        tracker = get_current_tracker()
        if not _is_inside_checkpoint() or tracker is None:
            return await super().request(
                messages, model_settings, model_request_parameters
            )

        event_id, event_context = tracker.start_model_event()
        prompt_artifact = f"{event_id}_prompt"
        response_artifact = f"{event_id}_response"

        _save_with_fallback(
            prompt_artifact,
            _serialize_messages(messages),
            artifact_type="prompt",
        )

        started_at = time.perf_counter()
        try:
            response = await super().request(
                messages,
                model_settings,
                model_request_parameters,
            )
        except Exception as error:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
            tracker.fail_model_event(event_id)
            log(
                pydantic_ai_events={
                    event_id: self._build_model_event_payload(
                        event_context=event_context,
                        status="failed",
                        duration_ms=duration_ms,
                        prompt_artifact=prompt_artifact,
                        response_artifact=None,
                        transcript_artifact=None,
                        model_response=None,
                        error=error,
                    )
                }
            )
            raise

        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)

        _save_with_fallback(
            response_artifact,
            _serialize_model_response(response),
            artifact_type="response",
        )

        log(
            pydantic_ai_events={
                event_id: self._build_model_event_payload(
                    event_context=event_context,
                    status="completed",
                    duration_ms=duration_ms,
                    prompt_artifact=prompt_artifact,
                    response_artifact=response_artifact,
                    transcript_artifact=None,
                    model_response=response,
                )
            }
        )
        tracker.complete_model_event(event_id)
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
        if not _is_inside_checkpoint() or tracker is None:
            async with super().request_stream(
                messages,
                model_settings,
                model_request_parameters,
                run_context,
            ) as streamed_response:
                yield streamed_response
            return

        event_id, event_context = tracker.start_model_event()
        prompt_artifact = f"{event_id}_prompt"
        response_artifact = f"{event_id}_response"
        transcript_artifact = f"{event_id}_stream_transcript"

        _save_with_fallback(
            prompt_artifact,
            _serialize_messages(messages),
            artifact_type="prompt",
        )

        stream_events: list[dict[str, Any]] = []
        started_at = time.perf_counter()
        try:
            async with super().request_stream(
                messages,
                model_settings,
                model_request_parameters,
                run_context,
            ) as streamed_response:
                tracked_stream = KitaruStreamedResponse(
                    streamed_response,
                    on_event=lambda event: stream_events.append(
                        _serialize_stream_event(event)
                    ),
                )
                yield tracked_stream

            response = tracked_stream.get()
        except Exception as error:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
            tracker.fail_model_event(event_id)
            log(
                pydantic_ai_events={
                    event_id: self._build_model_event_payload(
                        event_context=event_context,
                        status="failed",
                        duration_ms=duration_ms,
                        prompt_artifact=prompt_artifact,
                        response_artifact=None,
                        transcript_artifact=None,
                        model_response=None,
                        error=error,
                        stream_event_count=len(stream_events),
                    )
                }
            )
            raise

        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)

        _save_with_fallback(
            response_artifact,
            _serialize_model_response(response),
            artifact_type="response",
        )

        _save_with_fallback(
            transcript_artifact,
            {
                "event_count": len(stream_events),
                "duration_ms": duration_ms,
                "events": stream_events,
                "final_response": _serialize_model_response(response),
            },
            artifact_type="context",
        )

        log(
            pydantic_ai_events={
                event_id: self._build_model_event_payload(
                    event_context=event_context,
                    status="completed",
                    duration_ms=duration_ms,
                    prompt_artifact=prompt_artifact,
                    response_artifact=response_artifact,
                    transcript_artifact=transcript_artifact,
                    model_response=response,
                    stream_event_count=len(stream_events),
                )
            }
        )
        tracker.complete_model_event(event_id)
