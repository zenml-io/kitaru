"""Best-effort live events for Claude Agent SDK streaming."""

from dataclasses import dataclass
from typing import Any

import kitaru.events as kitaru_events
from kitaru.adapters._streaming_utils import (
    BaseStreamPublisher,
    clip_stream_text,
    float_or_none,
    int_or_none,
    safe_string,
)

from ._runner import (
    ClaudeResultMessageError,
    _api_error_status_or_none,
    _assistant_error_or_none,
    _diagnostic_subtype_or_none,
)

CLAUDE_STREAM_STARTED = "claude_agent_sdk.stream.started"
CLAUDE_STREAM_EVENT = "claude_agent_sdk.stream.event"
CLAUDE_STREAM_COMPLETED = "claude_agent_sdk.stream.completed"
CLAUDE_STREAM_FAILED = "claude_agent_sdk.stream.failed"
CLAUDE_STREAM_EVENT_KINDS = (
    CLAUDE_STREAM_STARTED,
    CLAUDE_STREAM_EVENT,
    CLAUDE_STREAM_COMPLETED,
    CLAUDE_STREAM_FAILED,
)
CLAUDE_STREAM_TERMINAL_EVENT_KINDS = (
    CLAUDE_STREAM_COMPLETED,
    CLAUDE_STREAM_FAILED,
)

_MAX_DISPLAY_CHARS = 240
_MAX_TEXT_DELTA_CHARS = 240
_MAX_ERROR_CHARS = 500

_CLAUDE_STREAM_EVENT_TYPE = "StreamEvent"
_CLAUDE_ASSISTANT_MESSAGE_TYPE = "AssistantMessage"
_CLAUDE_USER_MESSAGE_TYPE = "UserMessage"
_CLAUDE_SYSTEM_MESSAGE_TYPE = "SystemMessage"
_CLAUDE_RESULT_MESSAGE_TYPE = "ResultMessage"
_CLAUDE_RATE_LIMIT_EVENT_TYPE = "RateLimitEvent"
_CONTENT_BLOCK_TOOL_USE_CLASS = "ToolUseBlock"
_CONTENT_BLOCK_SERVER_TOOL_USE_CLASS = "ServerToolUseBlock"
_CONTENT_BLOCK_TEXT_CLASS = "TextBlock"
_CONTENT_BLOCK_THINKING_CLASS = "ThinkingBlock"


@dataclass(frozen=True)
class _ContentBlockToolState:
    tool_name: str | None = None
    tool_id: str | None = None


class ClaudeStreamPublisher(BaseStreamPublisher):
    """Normalize Claude SDK messages and publish Kitaru live events.

    Live events are observability only. Every publishing operation is best
    effort so a missing checkpoint scope or streaming backend never changes the
    Claude run result.
    """

    _started_kind = CLAUDE_STREAM_STARTED
    _completed_kind = CLAUDE_STREAM_COMPLETED
    _failed_kind = CLAUDE_STREAM_FAILED
    _normalization_failed_kind = CLAUDE_STREAM_EVENT
    _started_display = "Claude Agent SDK stream started"
    _completed_display_prefix = "Claude Agent SDK stream completed"
    _failed_display_prefix = "Claude Agent SDK stream failed"
    _normalization_failed_display = "Claude Agent SDK stream event"
    _error_message_limit = _MAX_ERROR_CHARS

    def __init__(self, *, runner_name: str, include_text_deltas: bool = False) -> None:
        super().__init__(
            publish=lambda *args, **kwargs: kitaru_events.publish(*args, **kwargs)
        )
        self._runner_name = runner_name
        self._include_text_deltas = include_text_deltas
        self._tool_state_by_content_block_index: dict[int, _ContentBlockToolState] = {}

    def event(self, message: Any) -> None:
        self._publish_stream_item(message)

    def completed(self, *, status: str) -> None:
        self._clear_tool_state()
        self._publish_completed(status=status)

    def failed(self, error: BaseException) -> None:
        self._clear_tool_state()
        self._publish_failed(error)

    def _failed_fields(self, error: BaseException) -> dict[str, Any]:
        if isinstance(error, ClaudeResultMessageError):
            return {
                "claude_result_diagnostic": error.diagnostic.as_metadata(),
            }
        return {}

    def _normalize_publishable(self, item: Any) -> tuple[str, dict[str, Any]]:
        return CLAUDE_STREAM_EVENT, self.normalize_message(item)

    def normalize_message(self, message: Any) -> dict[str, Any]:
        message_type = type(message).__name__
        if message_type == _CLAUDE_STREAM_EVENT_TYPE:
            return self._normalize_stream_event(message)
        if message_type == _CLAUDE_ASSISTANT_MESSAGE_TYPE:
            return self._normalize_assistant_message(message)
        if message_type == _CLAUDE_USER_MESSAGE_TYPE:
            return self._normalize_user_message(message)
        if message_type == _CLAUDE_SYSTEM_MESSAGE_TYPE:
            return self._normalize_system_message(message)
        if message_type == _CLAUDE_RESULT_MESSAGE_TYPE:
            return self._normalize_result_message(message)
        if message_type == _CLAUDE_RATE_LIMIT_EVENT_TYPE:
            return self._normalize_rate_limit_event(message)
        return self._base_payload(
            category="unknown_message",
            display=f"Claude message: {message_type}",
            message_type=message_type,
        )

    def _normalize_stream_event(self, message: Any) -> dict[str, Any]:
        event = getattr(message, "event", None)
        if not isinstance(event, dict):
            return self._base_payload(
                category="stream_event",
                display="Claude stream event",
                event_type=type(event).__name__,
            )

        event_type = safe_string(event.get("type")) or "stream_event"
        if event_type == "content_block_start":
            return self._normalize_content_block_start(event)
        if event_type == "content_block_delta":
            return self._normalize_content_block_delta(event)
        if event_type == "message_start":
            self._clear_tool_state()
            return self._base_payload(
                category="message_start",
                display="Claude assistant message started",
                event_type=event_type,
            )
        if event_type == "message_delta":
            return self._base_payload(
                category="message_delta",
                display="Claude assistant message updated",
                event_type=event_type,
            )
        if event_type == "message_stop":
            self._clear_tool_state()
            return self._base_payload(
                category="message_stop",
                display="Claude assistant message stopped",
                event_type=event_type,
            )
        if event_type == "content_block_stop":
            index = int_or_none(event.get("index"))
            if index is not None:
                self._tool_state_by_content_block_index.pop(index, None)
            payload = self._base_payload(
                category="content_block_stop",
                display="Claude content block stopped",
                event_type=event_type,
            )
            if index is not None:
                payload["content_block_index"] = index
            return payload
        return self._base_payload(
            category="stream_event",
            display=event_type,
            event_type=event_type,
        )

    def _normalize_content_block_start(self, event: dict[str, Any]) -> dict[str, Any]:
        index = int_or_none(event.get("index"))
        content_block = event.get("content_block")
        block_type = _content_block_type(content_block)
        tool_name = _content_block_string(content_block, "name")
        tool_id = _content_block_string(content_block, "id")
        if index is not None:
            self._tool_state_by_content_block_index.pop(index, None)
            if tool_name is not None or tool_id is not None:
                self._tool_state_by_content_block_index[index] = _ContentBlockToolState(
                    tool_name=tool_name, tool_id=tool_id
                )

        if block_type in {"tool_use", "server_tool_use"} or tool_name is not None:
            payload = self._base_payload(
                category="tool_call_started",
                display=(
                    f"Claude tool call started: {tool_name}"
                    if tool_name
                    else "Claude tool call started"
                ),
                event_type="content_block_start",
                content_block_type=block_type,
            )
            if index is not None:
                payload["content_block_index"] = index
            if tool_name is not None:
                payload["tool_name"] = tool_name
            if tool_id is not None:
                payload["tool_id"] = tool_id
            return payload

        payload = self._base_payload(
            category="content_block_start",
            display=(
                f"Claude content block started: {block_type}"
                if block_type
                else "Claude content block started"
            ),
            event_type="content_block_start",
        )
        if block_type is not None:
            payload["content_block_type"] = block_type
        if index is not None:
            payload["content_block_index"] = index
        return payload

    def _normalize_content_block_delta(self, event: dict[str, Any]) -> dict[str, Any]:
        index = int_or_none(event.get("index"))
        delta = event.get("delta")
        if not isinstance(delta, dict):
            return self._base_payload(
                category="content_block_delta",
                display="Claude content block delta",
                event_type="content_block_delta",
            )

        delta_type = safe_string(delta.get("type")) or "delta"
        if delta_type == "text_delta":
            text_delta = safe_string(delta.get("text")) or ""
            payload = self._base_payload(
                category="text_delta",
                display=(
                    clip_stream_text(text_delta, _MAX_DISPLAY_CHARS)
                    if self._include_text_deltas and text_delta
                    else "Claude text delta"
                ),
                event_type="content_block_delta",
                delta_type=delta_type,
            )
            if self._include_text_deltas:
                payload["text_delta"] = clip_stream_text(
                    text_delta, _MAX_TEXT_DELTA_CHARS
                )
            if index is not None:
                payload["content_block_index"] = index
            return payload

        if delta_type == "input_json_delta":
            tool_state = (
                self._tool_state_by_content_block_index.get(index)
                if index is not None
                else None
            )
            tool_name = tool_state.tool_name if tool_state is not None else None
            tool_id = tool_state.tool_id if tool_state is not None else None
            payload = self._base_payload(
                category="tool_input_delta",
                display=(
                    f"Claude tool input updated: {tool_name}"
                    if tool_name
                    else "Claude tool input updated"
                ),
                event_type="content_block_delta",
                delta_type=delta_type,
            )
            if index is not None:
                payload["content_block_index"] = index
            if tool_name is not None:
                payload["tool_name"] = tool_name
            if tool_id is not None:
                payload["tool_id"] = tool_id
            return payload

        payload = self._base_payload(
            category="content_block_delta",
            display=f"Claude content delta: {delta_type}",
            event_type="content_block_delta",
            delta_type=delta_type,
        )
        if index is not None:
            payload["content_block_index"] = index
        return payload

    def _normalize_assistant_message(self, message: Any) -> dict[str, Any]:
        content = getattr(message, "content", None)
        content_count = len(content) if isinstance(content, list) else None
        model = safe_string(getattr(message, "model", None))
        payload = self._base_payload(
            category="assistant_message",
            display="Claude assistant message completed",
            message_type=_CLAUDE_ASSISTANT_MESSAGE_TYPE,
        )
        if model is not None:
            payload["model"] = model
        if content_count is not None:
            payload["content_block_count"] = content_count
        if getattr(message, "usage", None) is not None:
            payload["has_usage"] = True
        stop_reason = safe_string(getattr(message, "stop_reason", None))
        if stop_reason is not None:
            payload["stop_reason"] = stop_reason
        assistant_error = _assistant_error_or_none(getattr(message, "error", None))
        if assistant_error is not None:
            payload["assistant_error"] = assistant_error
        return payload

    def _normalize_user_message(self, message: Any) -> dict[str, Any]:
        has_tool_use_result = getattr(message, "tool_use_result", None) is not None
        content = getattr(message, "content", None)
        payload = self._base_payload(
            category="user_message",
            display="Claude tool result received"
            if has_tool_use_result
            else "Claude user message received",
            message_type=_CLAUDE_USER_MESSAGE_TYPE,
            has_tool_use_result=has_tool_use_result,
        )
        if isinstance(content, list):
            payload["content_block_count"] = len(content)
        elif isinstance(content, str):
            payload["content_kind"] = "text"
        return payload

    def _normalize_system_message(self, message: Any) -> dict[str, Any]:
        subtype = safe_string(getattr(message, "subtype", None))
        payload = self._base_payload(
            category="system_message",
            display=f"Claude system message: {subtype}"
            if subtype
            else "Claude system message",
            message_type=_CLAUDE_SYSTEM_MESSAGE_TYPE,
        )
        if subtype is not None:
            payload["subtype"] = subtype
        return payload

    def _normalize_result_message(self, message: Any) -> dict[str, Any]:
        subtype = _diagnostic_subtype_or_none(getattr(message, "subtype", None))
        is_error = bool(getattr(message, "is_error", False))
        payload = self._base_payload(
            category="result_message",
            display="Claude final result received",
            message_type=_CLAUDE_RESULT_MESSAGE_TYPE,
            status="failed" if is_error else "completed",
            is_error=is_error,
            has_usage=getattr(message, "usage", None) is not None,
            has_model_usage=getattr(message, "model_usage", None) is not None,
            has_result=getattr(message, "result", None) is not None,
            has_structured_output=getattr(message, "structured_output", None)
            is not None,
        )
        session_id = safe_string(getattr(message, "session_id", None))
        if session_id is not None:
            payload["session_id"] = session_id
        if subtype is not None:
            payload["subtype"] = subtype
        api_error_status = _api_error_status_or_none(
            getattr(message, "api_error_status", None)
        )
        if api_error_status is not None:
            payload["api_error_status"] = api_error_status
        stop_reason = safe_string(getattr(message, "stop_reason", None))
        if stop_reason is not None:
            payload["stop_reason"] = stop_reason
        num_turns = int_or_none(getattr(message, "num_turns", None))
        if num_turns is not None:
            payload["num_turns"] = num_turns
        cost = float_or_none(getattr(message, "total_cost_usd", None))
        if cost is not None:
            payload["has_cost"] = True
        return payload

    def _normalize_rate_limit_event(self, message: Any) -> dict[str, Any]:
        info = getattr(message, "rate_limit_info", None)
        status = safe_string(getattr(info, "status", None))
        rate_limit_type = safe_string(getattr(info, "rate_limit_type", None))
        payload = self._base_payload(
            category="rate_limit",
            display=f"Claude rate limit: {status}" if status else "Claude rate limit",
            message_type=_CLAUDE_RATE_LIMIT_EVENT_TYPE,
        )
        if status is not None:
            payload["status"] = status
        if rate_limit_type is not None:
            payload["rate_limit_type"] = rate_limit_type
        utilization = float_or_none(getattr(info, "utilization", None))
        if utilization is not None:
            payload["utilization"] = utilization
        return payload

    def _base_payload(
        self, *, category: str, display: str, **fields: Any
    ) -> dict[str, Any]:
        return {
            "adapter": "claude_agent_sdk",
            "runner_name": self._runner_name,
            "scope": "invocation",
            "category": category,
            "display": display,
            **fields,
        }

    def _clear_tool_state(self) -> None:
        self._tool_state_by_content_block_index.clear()


def _content_block_type(value: Any) -> str | None:
    if isinstance(value, dict):
        return safe_string(value.get("type"))
    type_value = safe_string(getattr(value, "type", None))
    if type_value is not None:
        return type_value
    name = type(value).__name__
    if name == _CONTENT_BLOCK_TOOL_USE_CLASS:
        return "tool_use"
    if name == _CONTENT_BLOCK_SERVER_TOOL_USE_CLASS:
        return "server_tool_use"
    if name == _CONTENT_BLOCK_TEXT_CLASS:
        return "text"
    if name == _CONTENT_BLOCK_THINKING_CLASS:
        return "thinking"
    return None


def _content_block_string(value: Any, key: str) -> str | None:
    if isinstance(value, dict):
        return safe_string(value.get(key))
    return safe_string(getattr(value, key, None))
