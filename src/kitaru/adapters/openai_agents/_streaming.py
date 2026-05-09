"""Best-effort stream event publishing for the OpenAI Agents adapter."""

import importlib
import logging
import uuid
from typing import Any

from ._serialization import to_json_safe
from ._tracking import normalize_agent_name

logger = logging.getLogger(__name__)

STREAM_START_KIND = "openai_agents.stream.start"
STREAM_EVENT_KIND = "openai_agents.stream.event"
STREAM_END_KIND = "openai_agents.stream.end"
STREAM_ERROR_KIND = "openai_agents.stream.error"

_KNOWN_EVENT_CATEGORIES = frozenset(
    {
        "raw_response_event",
        "run_item_stream_event",
        "agent_updated_stream_event",
    }
)


def new_stream_id(agent_name: str) -> str:
    """Return a compact stream ID for one OpenAI Agents streamed run."""
    return f"openai_agents:{normalize_agent_name(agent_name)}:{uuid.uuid4().hex[:8]}"


class OpenAIStreamPublisher:
    """Publish normalized OpenAI stream events through ZenML when available.

    Publishing is deliberately best effort. If the installed ZenML version does
    not expose ``zenml.streams`` yet, or if the active store/publisher rejects an
    event, the OpenAI run should continue unaffected.
    """

    def __init__(
        self,
        *,
        agent_name: str,
        include_raw: bool,
        stream_id: str | None = None,
    ) -> None:
        self.agent_name = normalize_agent_name(agent_name)
        self.include_raw = include_raw
        self.stream_id = stream_id or new_stream_id(agent_name)
        self._index = 0

    def publish_start(self) -> None:
        """Publish the terminally ordered stream-start marker."""
        self._publish(
            STREAM_START_KIND,
            {
                "adapter": "openai_agents",
                "agent_name": self.agent_name,
                "status": "started",
            },
        )

    def publish_sdk_event(self, event: Any) -> None:
        """Normalize and publish one SDK stream event."""
        self._publish(
            STREAM_EVENT_KIND, normalize_stream_event(event, self.include_raw)
        )

    def publish_end(self, *, status: str) -> None:
        """Publish and flush the normal terminal stream marker."""
        self._publish(
            STREAM_END_KIND,
            {
                "adapter": "openai_agents",
                "agent_name": self.agent_name,
                "status": status,
            },
            flush=True,
        )

    def publish_error(self, error: BaseException) -> None:
        """Publish and flush the error terminal stream marker."""
        self._publish(
            STREAM_ERROR_KIND,
            {
                "adapter": "openai_agents",
                "agent_name": self.agent_name,
                "status": "failed",
                "error_type": type(error).__name__,
                "message": str(error),
            },
            flush=True,
        )

    def _publish(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        flush: bool = False,
    ) -> None:
        index = self._index
        self._index += 1
        enriched_payload = {
            "adapter": "openai_agents",
            "agent_name": self.agent_name,
            "stream_id": self.stream_id,
            **payload,
        }
        try:
            streams = _load_zenml_streams()
            if streams is None:
                return
            streams.publish(
                enriched_payload,
                kind=kind,
                stream_id=self.stream_id,
                index=index,
            )
            if flush and hasattr(streams, "flush"):
                streams.flush()
        except Exception as exc:  # pragma: no cover - logging-only fallback
            logger.warning(
                "Failed to publish OpenAI Agents stream event %s: %s",
                kind,
                exc,
            )


def normalize_stream_event(event: Any, include_raw: bool) -> dict[str, Any]:
    """Return Kitaru's minimal, serializable stream-event payload."""
    category = _event_category(event)
    payload: dict[str, Any] = {
        "adapter": "openai_agents",
        "category": category,
    }
    text_delta = _text_delta(event)
    if text_delta:
        payload["text_delta"] = text_delta
        payload["display"] = text_delta
    else:
        display = _display_text(event, category)
        if display:
            payload["display"] = display
    if include_raw:
        payload["raw"] = to_json_safe(event)
    return payload


def _load_zenml_streams() -> Any | None:
    try:
        return importlib.import_module("zenml.streams")
    except (ImportError, ModuleNotFoundError):
        logger.debug("ZenML stream publishing is unavailable; dropping stream event.")
        return None


def _event_category(event: Any) -> str:
    event_type = _get_value(event, "type")
    if isinstance(event_type, str) and event_type in _KNOWN_EVENT_CATEGORIES:
        return event_type
    if isinstance(event_type, str) and event_type:
        return "unknown"
    return "unknown"


def _display_text(event: Any, category: str) -> str | None:
    if category == "agent_updated_stream_event":
        new_agent = _get_value(event, "new_agent")
        agent_name = _get_value(new_agent, "name")
        if isinstance(agent_name, str) and agent_name:
            return f"agent updated: {agent_name}"
    if category == "run_item_stream_event":
        item = _get_value(event, "item")
        item_type = _get_value(item, "type") or _get_value(event, "name")
        if isinstance(item_type, str) and item_type:
            return f"run item: {item_type}"
    return category if category != "unknown" else None


def _text_delta(event: Any) -> str | None:
    for candidate in (
        _get_value(event, "delta"),
        _get_value(_get_value(event, "data"), "delta"),
        _get_value(event, "text_delta"),
        _get_value(_get_value(event, "data"), "text_delta"),
    ):
        if isinstance(candidate, str) and candidate:
            return candidate
    return None


def _get_value(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)
