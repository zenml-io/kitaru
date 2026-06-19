"""Best-effort live events for Gemini Interactions streaming."""

from typing import Any

import kitaru.events as kitaru_events
from kitaru.adapters._streaming_utils import BaseStreamPublisher, clip_stream_text

from ._constants import ADAPTER_ID
from ._stream_shapes import (
    ARGUMENT_DELTA_TYPES,
    CONTENT_START_EVENT_TYPE,
    CONTENT_STOP_EVENT_TYPE,
    DELTA_EVENT_TYPES,
    DONE_EVENT_TYPE,
    ERROR_EVENT_TYPE,
    INTERACTION_COMPLETED_EVENT_TYPES,
    MEDIA_DELTA_TYPES,
    STEP_START_EVENT_TYPE,
    STEP_STOP_EVENT_TYPE,
    TEXT_DELTA_TYPES,
    THOUGHT_DELTA_TYPES,
    delta_from_event,
    delta_text,
    delta_type_value,
    event_type,
    function_name_from_step,
    interaction_from_event,
    is_safe_stream_text_delta_source,
    normalized_delta_type,
    role_from_step,
    safe_event_identity,
    step_from_event,
    step_index,
    step_type_from_step,
    string_from,
    usage_from,
)

GEMINI_STREAM_STARTED = "gemini_interactions.stream.started"
GEMINI_STREAM_EVENT = "gemini_interactions.stream.event"
GEMINI_STREAM_COMPLETED = "gemini_interactions.stream.completed"
GEMINI_STREAM_FAILED = "gemini_interactions.stream.failed"
GEMINI_STREAM_EVENT_KINDS = (
    GEMINI_STREAM_STARTED,
    GEMINI_STREAM_EVENT,
    GEMINI_STREAM_COMPLETED,
    GEMINI_STREAM_FAILED,
)
GEMINI_STREAM_TERMINAL_EVENT_KINDS = (
    GEMINI_STREAM_COMPLETED,
    GEMINI_STREAM_FAILED,
)

_MAX_DISPLAY_CHARS = 240
_MAX_TEXT_DELTA_CHARS = 240
_MAX_ERROR_CHARS = 500
_LIFECYCLE_EVENT_METADATA = {
    STEP_START_EVENT_TYPE: ("step_start", "Gemini step started"),
    CONTENT_START_EVENT_TYPE: ("content_start", "Gemini content started"),
    STEP_STOP_EVENT_TYPE: ("step_stop", "Gemini step stopped"),
    CONTENT_STOP_EVENT_TYPE: ("content_stop", "Gemini content stopped"),
}


class GeminiStreamPublisher(BaseStreamPublisher):
    """Normalize Gemini stream events and publish Kitaru live events.

    Live events are observability only. Every publishing operation is best
    effort so a missing event backend or malformed SDK event never changes the
    Gemini interaction result.
    """

    _started_kind = GEMINI_STREAM_STARTED
    _completed_kind = GEMINI_STREAM_COMPLETED
    _failed_kind = GEMINI_STREAM_FAILED
    _normalization_failed_kind = GEMINI_STREAM_EVENT
    _started_display = "Gemini Interactions stream started"
    _completed_display_prefix = "Gemini Interactions stream completed"
    _failed_display_prefix = "Gemini Interactions stream failed"
    _normalization_failed_display = "Gemini Interactions stream event"
    _error_message_limit = _MAX_ERROR_CHARS

    def __init__(
        self,
        *,
        runner_name: str,
        surface: str,
        include_text_deltas: bool = False,
    ) -> None:
        super().__init__(
            publish=lambda *args, **kwargs: kitaru_events.publish(*args, **kwargs)
        )
        self._runner_name = runner_name
        self._surface = surface
        self._include_text_deltas = include_text_deltas
        self._steps_by_index: dict[int, dict[str, str]] = {}
        self._steps_by_id: dict[str, dict[str, str]] = {}

    def event(self, event: Any) -> None:
        self._publish_stream_item(event)

    def completed(self, *, status: str, interaction_id: str | None = None) -> None:
        fields: dict[str, Any] = {}
        if interaction_id is not None:
            fields["interaction_id"] = interaction_id
        self._publish_completed(status=status, **fields)

    def failed(self, error: BaseException) -> None:
        self._publish_failed(error)

    def _normalize_publishable(self, item: Any) -> tuple[str, dict[str, Any]]:
        return GEMINI_STREAM_EVENT, self.normalize_event(item)

    def normalize_event(self, event: Any) -> dict[str, Any]:
        kind = event_type(event)
        if kind == "interaction.created":
            return self._normalize_interaction_event(
                event,
                category="interaction_created",
                display="Gemini interaction created",
                event_type=kind,
            )
        if kind == "interaction.status_update":
            return self._normalize_interaction_event(
                event,
                category="interaction_status_update",
                display="Gemini interaction status updated",
                event_type=kind,
            )
        if kind in INTERACTION_COMPLETED_EVENT_TYPES:
            return self._normalize_interaction_event(
                event,
                category="interaction_completed",
                display="Gemini interaction completed",
                event_type=kind,
            )
        lifecycle_metadata = _LIFECYCLE_EVENT_METADATA.get(kind)
        if lifecycle_metadata is not None:
            category, display = lifecycle_metadata
            return self._normalize_step_event(
                event,
                category=category,
                display=display,
                event_type=kind,
            )
        if kind in DELTA_EVENT_TYPES:
            return self._normalize_step_delta(event, event_type=kind)
        if kind == ERROR_EVENT_TYPE:
            return self._base_payload(
                category="error",
                display="Gemini stream error",
                event_type=kind,
                **safe_event_identity(event),
            )
        if kind == DONE_EVENT_TYPE:
            return self._base_payload(
                category="done",
                display="Gemini stream done",
                event_type=kind,
                **safe_event_identity(event),
            )
        return self._base_payload(
            category="unknown_event",
            display=kind,
            event_type=kind,
            **safe_event_identity(event),
        )

    def _normalize_interaction_event(
        self,
        event: Any,
        *,
        category: str,
        display: str,
        event_type: str,
    ) -> dict[str, Any]:
        interaction = interaction_from_event(event)
        status = string_from(event, "status") or string_from(interaction, "status")
        interaction_id = string_from(event, "interaction_id") or string_from(
            interaction, "id"
        )
        payload = self._base_payload(
            category=category,
            display=f"{display}: {status}" if status else display,
            event_type=event_type,
            **safe_event_identity(event),
        )
        if status is not None:
            payload["status"] = status
        if interaction_id is not None:
            payload["interaction_id"] = interaction_id
        if usage_from(interaction) is not None or usage_from(event) is not None:
            payload["has_usage"] = True
        return payload

    def _normalize_step_event(
        self,
        event: Any,
        *,
        category: str,
        display: str,
        event_type: str,
    ) -> dict[str, Any]:
        step = step_from_event(event)
        self._remember_step(event, step)
        step_type = string_from(step, "type") or string_from(event, "step_type")
        step_id = string_from(step, "id") or string_from(event, "step_id")
        call_id = string_from(step, "call_id")
        tool_name = function_name_from_step(step)
        payload = self._base_payload(
            category=category,
            display=f"{display}: {step_type}" if step_type else display,
            event_type=event_type,
            **safe_event_identity(event),
        )
        index = step_index(event, step)
        if index is not None:
            payload["step_index"] = index
        if step_type is not None:
            payload["step_type"] = step_type
        if step_id is not None:
            payload["step_id"] = step_id
        if call_id is not None:
            payload["call_id"] = call_id
        if tool_name is not None:
            payload["tool_name"] = tool_name
        return payload

    def _normalize_step_delta(self, event: Any, *, event_type: str) -> dict[str, Any]:
        step = step_from_event(event)
        step_snapshot = self._remember_step(event, step)
        delta = delta_from_event(event)
        display_delta_type = delta_type_value(event, delta)
        normalized_type = normalized_delta_type(event, delta)
        base_fields = safe_event_identity(event)
        index = step_index(event, step)
        if index is not None:
            base_fields["step_index"] = index
        step_id = string_from(step, "id") or string_from(event, "step_id")
        if step_id is not None:
            base_fields["step_id"] = step_id
        step_type = string_from(step, "type") or string_from(event, "step_type")
        if step_type is not None:
            base_fields["step_type"] = step_type

        if normalized_type in TEXT_DELTA_TYPES:
            text_delta = delta_text(delta)
            include_text = (
                self._include_text_deltas
                and text_delta is not None
                and is_safe_stream_text_delta_source(
                    event_type_value=event_type,
                    role=step_snapshot.get("role"),
                    step_type=step_snapshot.get("type"),
                )
            )
            payload = self._base_payload(
                category="text_delta",
                display=(
                    clip_stream_text(text_delta, _MAX_DISPLAY_CHARS)
                    if include_text
                    else "Gemini text delta"
                ),
                event_type=event_type,
                delta_type=display_delta_type,
                **base_fields,
            )
            if include_text:
                payload["text_delta"] = clip_stream_text(
                    text_delta, _MAX_TEXT_DELTA_CHARS
                )
            return payload

        if normalized_type in ARGUMENT_DELTA_TYPES:
            tool_name = function_name_from_step(step) or step_snapshot.get("tool_name")
            call_id = string_from(step, "call_id") or string_from(step, "id")
            payload = self._base_payload(
                category="tool_arguments_delta",
                display=(
                    f"Gemini tool arguments updated: {tool_name}"
                    if tool_name
                    else "Gemini tool arguments updated"
                ),
                event_type=event_type,
                delta_type=display_delta_type,
                **base_fields,
            )
            if tool_name is not None:
                payload["tool_name"] = tool_name
            if call_id is not None:
                payload["call_id"] = call_id
            return payload

        if normalized_type in THOUGHT_DELTA_TYPES:
            return self._base_payload(
                category="thought_delta",
                display="Gemini thought metadata updated",
                event_type=event_type,
                delta_type=display_delta_type,
                **base_fields,
            )

        if normalized_type in MEDIA_DELTA_TYPES:
            return self._base_payload(
                category="media_delta",
                display=f"Gemini media delta: {display_delta_type}",
                event_type=event_type,
                delta_type=display_delta_type,
                **base_fields,
            )

        return self._base_payload(
            category="step_delta",
            display=f"Gemini step delta: {display_delta_type}",
            event_type=event_type,
            delta_type=display_delta_type,
            **base_fields,
        )

    def _remember_step(self, event: Any, step: Any) -> dict[str, str]:
        snapshot = self._step_snapshot(event, step)
        index = step_index(event, step)
        step_id = string_from(step, "id") or string_from(event, "step_id")
        existing: dict[str, str] = {}
        if index is not None:
            existing.update(self._steps_by_index.get(index, {}))
        if step_id is not None:
            existing.update(self._steps_by_id.get(step_id, {}))
        existing.update(snapshot)
        if index is not None:
            self._steps_by_index[index] = existing
        if step_id is not None:
            self._steps_by_id[step_id] = existing
        return existing

    @staticmethod
    def _step_snapshot(event: Any, step: Any) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        role = role_from_step(step) or role_from_step(event)
        step_type = step_type_from_step(step) or step_type_from_step(event)
        if role is not None:
            snapshot["role"] = role
        if step_type is not None:
            snapshot["type"] = step_type
        tool_name = function_name_from_step(step) or function_name_from_step(event)
        if tool_name is not None:
            snapshot["tool_name"] = tool_name
        return snapshot

    def _base_payload(
        self, *, category: str, display: str, **fields: Any
    ) -> dict[str, Any]:
        return {
            "adapter": ADAPTER_ID,
            "runner_name": self._runner_name,
            "surface": self._surface,
            "scope": "interaction",
            "category": category,
            "display": display,
            **fields,
        }
