"""Model interception for Kitaru's PydanticAI adapter."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

from pydantic import TypeAdapter
from pydantic_ai import ModelMessage, ModelResponse
from pydantic_ai.models import ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import RunContext

from kitaru.artifacts import save
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint

from ._tracking import get_current_tracker

_MODEL_RESPONSE_ADAPTER = TypeAdapter(ModelResponse)
_MODEL_MESSAGES_ADAPTER = TypeAdapter(list[ModelMessage])


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


class KitaruModel(WrapperModel):
    """Model wrapper that records model requests as checkpoint child events."""

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

        event_id = tracker.next_event_id("llm_call")
        parent_event_ids = tracker.parent_ids_for_model()
        prompt_artifact = f"{event_id}_prompt"
        response_artifact = f"{event_id}_response"

        _save_with_fallback(
            prompt_artifact,
            _serialize_messages(messages),
            artifact_type="prompt",
        )

        try:
            response = await super().request(
                messages,
                model_settings,
                model_request_parameters,
            )
        except Exception as error:
            log(
                pydantic_ai_events={
                    event_id: {
                        "type": "llm_call",
                        "status": "failed",
                        "parent_event_ids": parent_event_ids,
                        "artifacts": {"prompt": prompt_artifact},
                        "error": _error_payload(error),
                    }
                }
            )
            raise

        _save_with_fallback(
            response_artifact,
            _serialize_model_response(response),
            artifact_type="response",
        )

        log(
            pydantic_ai_events={
                event_id: {
                    "type": "llm_call",
                    "status": "completed",
                    "model_name": response.model_name,
                    "parent_event_ids": parent_event_ids,
                    "usage": _usage_payload(response),
                    "artifacts": {
                        "prompt": prompt_artifact,
                        "response": response_artifact,
                    },
                }
            }
        )
        tracker.mark_model_complete(event_id)
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

        event_id = tracker.next_event_id("llm_call")
        parent_event_ids = tracker.parent_ids_for_model()
        prompt_artifact = f"{event_id}_prompt"
        response_artifact = f"{event_id}_response"

        _save_with_fallback(
            prompt_artifact,
            _serialize_messages(messages),
            artifact_type="prompt",
        )

        try:
            async with super().request_stream(
                messages,
                model_settings,
                model_request_parameters,
                run_context,
            ) as streamed_response:
                yield streamed_response

            response = streamed_response.get()
        except Exception as error:
            log(
                pydantic_ai_events={
                    event_id: {
                        "type": "llm_call",
                        "status": "failed",
                        "parent_event_ids": parent_event_ids,
                        "artifacts": {"prompt": prompt_artifact},
                        "error": _error_payload(error),
                    }
                }
            )
            raise

        _save_with_fallback(
            response_artifact,
            _serialize_model_response(response),
            artifact_type="response",
        )
        log(
            pydantic_ai_events={
                event_id: {
                    "type": "llm_call",
                    "status": "completed",
                    "model_name": response.model_name,
                    "parent_event_ids": parent_event_ids,
                    "usage": _usage_payload(response),
                    "artifacts": {
                        "prompt": prompt_artifact,
                        "response": response_artifact,
                    },
                }
            }
        )
        tracker.mark_model_complete(event_id)
