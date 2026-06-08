"""Small shared helpers for adapter live stream publishers."""

from collections.abc import Callable
from typing import Any, ClassVar


class BaseStreamPublisher:
    """Narrow shared lifecycle and safety contract for live stream publishers.

    Adapters still own SDK-specific event normalization. This base owns the
    repeated envelope flow: lifecycle events, normalization fallback events,
    enabled-gated best-effort publishing, and safe failure messages.
    """

    _started_kind: ClassVar[str]
    _completed_kind: ClassVar[str]
    _failed_kind: ClassVar[str]
    _normalization_failed_kind: ClassVar[str]
    _started_display: ClassVar[str]
    _completed_display_prefix: ClassVar[str]
    _failed_display_prefix: ClassVar[str]
    _normalization_failed_category: ClassVar[str] = "stream_event_normalization_failed"
    _normalization_failed_display: ClassVar[str]
    _error_message_limit: ClassVar[int] = 500

    def __init__(
        self,
        *,
        publish: Callable[..., object],
        enabled: bool = True,
    ) -> None:
        self._live_event_publish = publish
        self._live_events_enabled = enabled

    def started(self) -> None:
        """Publish the adapter's stream-started lifecycle event."""
        self._publish(
            self._started_kind,
            self._base_payload(
                category="lifecycle",
                display=self._started_display,
                **self._started_fields(),
            ),
        )

    def _publish_completed(self, *, status: str, **fields: Any) -> None:
        """Publish the adapter's terminal completed lifecycle event."""
        self._publish(
            self._completed_kind,
            self._base_payload(
                category="lifecycle",
                display=f"{self._completed_display_prefix}: {status}",
                status=status,
                **fields,
            ),
            flush=True,
        )

    def _publish_failed(self, error: BaseException, **fields: Any) -> None:
        """Publish the adapter's terminal failed lifecycle event."""
        message = safe_live_exception_message(error, limit=self._error_message_limit)
        self._publish(
            self._failed_kind,
            self._base_payload(
                category="lifecycle",
                display=f"{self._failed_display_prefix}: {message}",
                error_type=type(error).__name__,
                message=message,
                **self._failed_fields(error),
                **fields,
            ),
            flush=True,
        )

    def _publish_stream_item(self, item: Any) -> None:
        """Normalize and publish one stream item with fallback safety."""
        try:
            kind, payload = self._normalize_publishable(item)
        except Exception:
            kind = self._normalization_failed_kind
            payload = self._normalization_failed_payload(item)
        self._publish(kind, payload)

    def _normalize_publishable(self, item: Any) -> tuple[str, dict[str, Any]]:
        """Return the event kind and payload for one adapter-specific item."""
        raise NotImplementedError

    def _normalization_failed_payload(self, item: Any) -> dict[str, Any]:
        return self._base_payload(
            category=self._normalization_failed_category,
            display=self._normalization_failed_display,
            event_type=type(item).__name__,
        )

    def _base_payload(
        self, *, category: str, display: str, **fields: Any
    ) -> dict[str, Any]:
        """Build the adapter-specific live-event payload envelope."""
        raise NotImplementedError

    def _started_fields(self) -> dict[str, Any]:
        return {}

    def _failed_fields(self, error: BaseException) -> dict[str, Any]:
        _ = error
        return {}

    def _publish(
        self, kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        if not self._live_events_enabled:
            return
        publish_live_event_best_effort(
            self._live_event_publish,
            kind,
            payload,
            flush=flush,
        )


def clip_stream_text(value: str, limit: int) -> str:
    """Collapse whitespace and clip text for live event display fields."""
    collapsed = " ".join(value.split())
    if len(collapsed) <= limit:
        return collapsed
    if limit <= 3:
        return collapsed[:limit]
    return collapsed[: limit - 3].rstrip() + "..."


def safe_string(value: Any) -> str | None:
    """Return ``value`` only when it is already a string."""
    if isinstance(value, str):
        return value
    return None


def first_string(*values: Any) -> str | None:
    """Return the first argument that is already a string."""
    for value in values:
        text = safe_string(value)
        if text is not None:
            return text
    return None


def safe_int(value: Any) -> int | None:
    """Return ``value`` only when it is an int, excluding bool."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def int_or_none(value: Any) -> int | None:
    """Coerce a value to int when safe enough for SDK metadata fields."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def float_or_none(value: Any) -> float | None:
    """Coerce a value to float when safe enough for SDK metadata fields."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_live_exception_message(error: BaseException, *, limit: int) -> str:
    """Return the safe message an adapter may expose in live event payloads."""
    safe_live_message = getattr(error, "safe_live_message", None)
    if isinstance(safe_live_message, str) and safe_live_message.strip():
        return clip_stream_text(safe_live_message, limit)
    return type(error).__name__


def publish_live_event_best_effort(
    publish: Callable[..., object],
    kind: str,
    payload: dict[str, Any],
    *,
    flush: bool = False,
) -> None:
    """Publish one live event without letting observability affect execution."""
    try:
        publish(kind, payload, flush=flush)
    except Exception:
        return
