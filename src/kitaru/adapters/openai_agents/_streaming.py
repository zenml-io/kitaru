"""Best-effort live events for OpenAI Agents SDK streaming."""

from typing import Any

import kitaru.events as kitaru_events

OPENAI_STREAM_STARTED = "openai_agents.stream.started"
OPENAI_STREAM_EVENT = "openai_agents.stream.event"
OPENAI_STREAM_COMPLETED = "openai_agents.stream.completed"
OPENAI_STREAM_FAILED = "openai_agents.stream.failed"
OPENAI_STREAM_EVENT_KINDS = (
    OPENAI_STREAM_STARTED,
    OPENAI_STREAM_EVENT,
    OPENAI_STREAM_COMPLETED,
    OPENAI_STREAM_FAILED,
)
OPENAI_STREAM_TERMINAL_EVENT_KINDS = (
    OPENAI_STREAM_COMPLETED,
    OPENAI_STREAM_FAILED,
)

_MAX_DISPLAY_CHARS = 240
_MAX_TEXT_DELTA_CHARS = 240
_MAX_ERROR_CHARS = 500

_RUN_ITEM_LABELS = {
    "message_output_created": "Message output created",
    "tool_called": "Tool called",
    "tool_output": "Tool output received",
    "handoff_requested": "Handoff requested",
    "handoff_occured": "Handoff occurred",
    "handoff_occurred": "Handoff occurred",
    "reasoning_item_created": "Reasoning item created",
    "mcp_approval_requested": "MCP approval requested",
    "mcp_approval_response": "MCP approval response received",
}


class OpenAIStreamPublisher:
    """Normalize OpenAI stream events and publish Kitaru live events.

    Live events are observability only. Every publishing operation is best
    effort so a missing checkpoint scope or streaming backend never changes the
    OpenAI run result.
    """

    def __init__(self, *, agent_name: str) -> None:
        self._agent_name = agent_name

    def started(self) -> None:
        self._publish(
            OPENAI_STREAM_STARTED,
            {
                "adapter": "openai_agents",
                "agent_name": self._agent_name,
                "category": "lifecycle",
                "display": "OpenAI Agents stream started",
            },
        )

    def event(self, event: Any) -> None:
        try:
            payload = self.normalize_event(event)
        except Exception:
            payload = self._base_payload(
                category="stream_event_normalization_failed",
                display="OpenAI stream event",
                event_type=type(event).__name__,
            )
        self._publish(OPENAI_STREAM_EVENT, payload)

    def completed(self, *, status: str) -> None:
        self._publish(
            OPENAI_STREAM_COMPLETED,
            {
                "adapter": "openai_agents",
                "agent_name": self._agent_name,
                "category": "lifecycle",
                "display": f"OpenAI Agents stream completed: {status}",
                "status": status,
            },
            flush=True,
        )

    def failed(self, error: BaseException) -> None:
        message = _safe_exception_message(error)
        self._publish(
            OPENAI_STREAM_FAILED,
            {
                "adapter": "openai_agents",
                "agent_name": self._agent_name,
                "category": "lifecycle",
                "display": f"OpenAI Agents stream failed: {message}",
                "error_type": type(error).__name__,
                "message": message,
            },
            flush=True,
        )

    def normalize_event(self, event: Any) -> dict[str, Any]:
        category = _event_category(event)
        if category == "raw_response_event":
            return self._normalize_raw_response_event(event)
        if category == "run_item_stream_event":
            return self._normalize_run_item_stream_event(event)
        if category == "agent_updated_stream_event":
            return self._normalize_agent_updated_stream_event(event)
        event_type = _safe_string(getattr(event, "type", None)) or type(event).__name__
        return self._base_payload(
            category=category,
            display=event_type,
            event_type=event_type,
        )

    def _normalize_raw_response_event(self, event: Any) -> dict[str, Any]:
        data = getattr(event, "data", None)
        event_type = _safe_string(getattr(data, "type", None)) or type(data).__name__
        text_delta = _safe_string(getattr(data, "delta", None))
        payload = self._base_payload(
            category="raw_response_event",
            display=_clip(text_delta, _MAX_DISPLAY_CHARS) if text_delta else event_type,
            event_type=event_type,
        )
        if text_delta is not None:
            payload["text_delta"] = _clip(text_delta, _MAX_TEXT_DELTA_CHARS)
        return payload

    def _normalize_run_item_stream_event(self, event: Any) -> dict[str, Any]:
        name = _safe_string(getattr(event, "name", None)) or "run_item"
        item = getattr(event, "item", None)
        item_type = _safe_string(getattr(item, "type", None)) or type(item).__name__
        item_name = _first_string(
            getattr(item, "name", None),
            getattr(item, "tool_name", None),
            getattr(getattr(item, "raw_item", None), "name", None),
        )
        call_id = _first_string(
            getattr(item, "call_id", None),
            getattr(getattr(item, "raw_item", None), "call_id", None),
        )
        payload = self._base_payload(
            category="run_item_stream_event",
            display=_run_item_display(name, item_name, call_id),
            name=name,
            item_type=item_type,
        )
        if item_name is not None:
            payload["item_name"] = item_name
        if call_id is not None:
            payload["call_id"] = call_id
        return payload

    def _normalize_agent_updated_stream_event(self, event: Any) -> dict[str, Any]:
        new_agent = getattr(event, "new_agent", None)
        new_agent_name = _safe_string(getattr(new_agent, "name", None))
        display = (
            f"Agent updated: {new_agent_name}" if new_agent_name else "Agent updated"
        )
        payload = self._base_payload(
            category="agent_updated_stream_event",
            display=display,
        )
        if new_agent_name is not None:
            payload["new_agent_name"] = new_agent_name
        return payload

    def _base_payload(
        self, *, category: str, display: str, **fields: Any
    ) -> dict[str, Any]:
        return {
            "adapter": "openai_agents",
            "agent_name": self._agent_name,
            "category": category,
            "display": display,
            **fields,
        }

    @staticmethod
    def _publish(kind: str, payload: dict[str, Any], *, flush: bool = False) -> None:
        try:
            kitaru_events.publish(kind, payload, flush=flush)
        except Exception:
            return


def _event_category(event: Any) -> str:
    value = _safe_string(getattr(event, "type", None))
    if value:
        return value
    value = _safe_string(getattr(event, "name", None))
    if value in {
        "raw_response_event",
        "run_item_stream_event",
        "agent_updated_stream_event",
    }:
        return value
    return type(event).__name__


def _run_item_display(name: str, item_name: str | None, call_id: str | None) -> str:
    label = _RUN_ITEM_LABELS.get(name, name.replace("_", " ").strip().capitalize())
    detail = item_name or call_id
    return f"{label}: {detail}" if detail else label


def _first_string(*values: Any) -> str | None:
    for value in values:
        text = _safe_string(value)
        if text is not None:
            return text
    return None


def _safe_string(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _safe_exception_message(error: BaseException) -> str:
    try:
        message = str(error)
    except Exception:
        message = ""
    return _clip(message, _MAX_ERROR_CHARS) or type(error).__name__


def _clip(value: str, limit: int) -> str:
    collapsed = " ".join(value.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3].rstrip() + "..."
