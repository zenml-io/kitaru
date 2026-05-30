"""Best-effort live events for Claude Agent SDK streaming."""

from dataclasses import dataclass
from typing import Any

import kitaru.events as kitaru_events

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


@dataclass(frozen=True)
class _ContentBlockToolState:
    tool_name: str | None = None
    tool_id: str | None = None


class ClaudeStreamPublisher:
    """Normalize Claude SDK messages and publish Kitaru live events.

    Live events are observability only. Every publishing operation is best
    effort so a missing checkpoint scope or streaming backend never changes the
    Claude run result.
    """

    def __init__(self, *, runner_name: str) -> None:
        self._runner_name = runner_name
        self._tool_state_by_content_block_index: dict[int, _ContentBlockToolState] = {}

    def started(self) -> None:
        self._publish(
            CLAUDE_STREAM_STARTED,
            self._base_payload(
                category="lifecycle",
                display="Claude Agent SDK stream started",
            ),
        )

    def event(self, message: Any) -> None:
        try:
            payload = self.normalize_message(message)
        except Exception:
            payload = self._base_payload(
                category="stream_event_normalization_failed",
                display="Claude Agent SDK stream event",
                event_type=type(message).__name__,
            )
        self._publish(CLAUDE_STREAM_EVENT, payload)

    def completed(self, *, status: str) -> None:
        self._clear_tool_state()
        self._publish(
            CLAUDE_STREAM_COMPLETED,
            self._base_payload(
                category="lifecycle",
                display=f"Claude Agent SDK stream completed: {status}",
                status=status,
            ),
            flush=True,
        )

    def failed(self, error: BaseException) -> None:
        self._clear_tool_state()
        message = _safe_exception_message(error)
        self._publish(
            CLAUDE_STREAM_FAILED,
            self._base_payload(
                category="lifecycle",
                display=f"Claude Agent SDK stream failed: {message}",
                error_type=type(error).__name__,
                message=message,
            ),
            flush=True,
        )

    def normalize_message(self, message: Any) -> dict[str, Any]:
        message_type = type(message).__name__
        if message_type == "StreamEvent":
            return self._normalize_stream_event(message)
        if message_type == "AssistantMessage":
            return self._normalize_assistant_message(message)
        if message_type == "UserMessage":
            return self._normalize_user_message(message)
        if message_type == "SystemMessage":
            return self._normalize_system_message(message)
        if message_type == "ResultMessage":
            return self._normalize_result_message(message)
        if message_type == "RateLimitEvent":
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

        event_type = _safe_string(event.get("type")) or "stream_event"
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
            index = _int_or_none(event.get("index"))
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
        index = _int_or_none(event.get("index"))
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
        index = _int_or_none(event.get("index"))
        delta = event.get("delta")
        if not isinstance(delta, dict):
            return self._base_payload(
                category="content_block_delta",
                display="Claude content block delta",
                event_type="content_block_delta",
            )

        delta_type = _safe_string(delta.get("type")) or "delta"
        if delta_type == "text_delta":
            text_delta = _safe_string(delta.get("text")) or ""
            payload = self._base_payload(
                category="text_delta",
                display=_clip(text_delta, _MAX_DISPLAY_CHARS)
                if text_delta
                else "Claude text delta",
                event_type="content_block_delta",
                delta_type=delta_type,
                text_delta=_clip(text_delta, _MAX_TEXT_DELTA_CHARS),
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
        model = _safe_string(getattr(message, "model", None))
        payload = self._base_payload(
            category="assistant_message",
            display="Claude assistant message completed",
            message_type="AssistantMessage",
        )
        if model is not None:
            payload["model"] = model
        if content_count is not None:
            payload["content_block_count"] = content_count
        if getattr(message, "usage", None) is not None:
            payload["has_usage"] = True
        stop_reason = _safe_string(getattr(message, "stop_reason", None))
        if stop_reason is not None:
            payload["stop_reason"] = stop_reason
        return payload

    def _normalize_user_message(self, message: Any) -> dict[str, Any]:
        has_tool_use_result = getattr(message, "tool_use_result", None) is not None
        content = getattr(message, "content", None)
        payload = self._base_payload(
            category="user_message",
            display="Claude tool result received"
            if has_tool_use_result
            else "Claude user message received",
            message_type="UserMessage",
            has_tool_use_result=has_tool_use_result,
        )
        if isinstance(content, list):
            payload["content_block_count"] = len(content)
        elif isinstance(content, str):
            payload["content_kind"] = "text"
        return payload

    def _normalize_system_message(self, message: Any) -> dict[str, Any]:
        subtype = _safe_string(getattr(message, "subtype", None))
        payload = self._base_payload(
            category="system_message",
            display=f"Claude system message: {subtype}"
            if subtype
            else "Claude system message",
            message_type="SystemMessage",
        )
        if subtype is not None:
            payload["subtype"] = subtype
        return payload

    def _normalize_result_message(self, message: Any) -> dict[str, Any]:
        subtype = _safe_string(getattr(message, "subtype", None))
        is_error = bool(getattr(message, "is_error", False))
        payload = self._base_payload(
            category="result_message",
            display="Claude final result received",
            message_type="ResultMessage",
            status="failed" if is_error else "completed",
            is_error=is_error,
            has_usage=getattr(message, "usage", None) is not None,
            has_model_usage=getattr(message, "model_usage", None) is not None,
            has_result=getattr(message, "result", None) is not None,
            has_structured_output=getattr(message, "structured_output", None)
            is not None,
        )
        session_id = _safe_string(getattr(message, "session_id", None))
        if session_id is not None:
            payload["session_id"] = session_id
        if subtype is not None:
            payload["subtype"] = subtype
        stop_reason = _safe_string(getattr(message, "stop_reason", None))
        if stop_reason is not None:
            payload["stop_reason"] = stop_reason
        num_turns = _int_or_none(getattr(message, "num_turns", None))
        if num_turns is not None:
            payload["num_turns"] = num_turns
        cost = _float_or_none(getattr(message, "total_cost_usd", None))
        if cost is not None:
            payload["has_cost"] = True
        return payload

    def _normalize_rate_limit_event(self, message: Any) -> dict[str, Any]:
        info = getattr(message, "rate_limit_info", None)
        status = _safe_string(getattr(info, "status", None))
        rate_limit_type = _safe_string(getattr(info, "rate_limit_type", None))
        payload = self._base_payload(
            category="rate_limit",
            display=f"Claude rate limit: {status}" if status else "Claude rate limit",
            message_type="RateLimitEvent",
        )
        if status is not None:
            payload["status"] = status
        if rate_limit_type is not None:
            payload["rate_limit_type"] = rate_limit_type
        utilization = _float_or_none(getattr(info, "utilization", None))
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

    @staticmethod
    def _publish(kind: str, payload: dict[str, Any], *, flush: bool = False) -> None:
        try:
            kitaru_events.publish(kind, payload, flush=flush)
        except Exception:
            return


def _content_block_type(value: Any) -> str | None:
    if isinstance(value, dict):
        return _safe_string(value.get("type"))
    type_value = _safe_string(getattr(value, "type", None))
    if type_value is not None:
        return type_value
    name = type(value).__name__
    if name == "ToolUseBlock":
        return "tool_use"
    if name == "ServerToolUseBlock":
        return "server_tool_use"
    if name == "TextBlock":
        return "text"
    if name == "ThinkingBlock":
        return "thinking"
    return None


def _content_block_string(value: Any, key: str) -> str | None:
    if isinstance(value, dict):
        return _safe_string(value.get(key))
    return _safe_string(getattr(value, key, None))


def _safe_string(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _safe_exception_message(error: BaseException) -> str:
    safe_live_message = getattr(error, "safe_live_message", None)
    if isinstance(safe_live_message, str) and safe_live_message.strip():
        return _clip(safe_live_message, _MAX_ERROR_CHARS)

    try:
        message = str(error)
    except Exception:
        message = ""
    return _clip(message, _MAX_ERROR_CHARS) or type(error).__name__


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clip(value: str, limit: int) -> str:
    collapsed = " ".join(value.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3].rstrip() + "..."
