"""Best-effort live event publishing from inside Kitaru checkpoints."""

from __future__ import annotations

import importlib
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Protocol, cast

from kitaru._serialization import to_json_safe
from kitaru.errors import KitaruContextError, KitaruUsageError
from kitaru.runtime import (
    _CheckpointScope,
    _get_current_checkpoint,
    _get_current_checkpoint_event_correlation_id,
    _next_checkpoint_event_index,
)

logger = logging.getLogger(__name__)

CHECKPOINT_STARTED_KIND = "kitaru.checkpoint.started"
CHECKPOINT_PROGRESS_KIND = "kitaru.checkpoint.progress"
CHECKPOINT_RETURNED_KIND = "kitaru.checkpoint.returned"
CHECKPOINT_FAILED_KIND = "kitaru.checkpoint.failed"

_LIFECYCLE_FLUSH_TIMEOUT = 0.1
_RESERVED_CONTROL_KINDS = frozenset({"cursor", "end", "error", "gap", "system"})
_PUBLISH_OUTSIDE_CHECKPOINT_ERROR = (
    "kitaru.{api_name}() can only be called inside a @checkpoint."
)


class _ZenMLStreaming(Protocol):
    """Small typed surface Kitaru needs from ``zenml.streaming``."""

    def publish(
        self,
        payload: dict[str, Any],
        *,
        kind: str = "event",
        correlation_id: str | None = None,
        index: int | None = None,
    ) -> None: ...

    def flush(self, timeout: float = 2.0) -> bool: ...


@dataclass(frozen=True)
class _CheckpointLifecycleEvent:
    """Kind/status pair for an automatic checkpoint lifecycle event."""

    kind: str
    status: str


_CHECKPOINT_STARTED_EVENT = _CheckpointLifecycleEvent(
    kind=CHECKPOINT_STARTED_KIND,
    status="started",
)
_CHECKPOINT_RETURNED_EVENT = _CheckpointLifecycleEvent(
    kind=CHECKPOINT_RETURNED_KIND,
    status="returned",
)
_CHECKPOINT_FAILED_EVENT = _CheckpointLifecycleEvent(
    kind=CHECKPOINT_FAILED_KIND,
    status="failed",
)


def _require_checkpoint_scope(api_name: str) -> _CheckpointScope:
    """Return the active checkpoint scope or raise the standard context error."""
    scope = _get_current_checkpoint()
    if scope is None:
        raise KitaruContextError(
            _PUBLISH_OUTSIDE_CHECKPOINT_ERROR.format(api_name=api_name)
        )
    return scope


def _normalize_kind(kind: str) -> str:
    """Validate and normalize a public event kind."""
    if not isinstance(kind, str):
        raise KitaruUsageError("Event kind must be a non-empty string.")
    if "\n" in kind or "\r" in kind:
        raise KitaruUsageError("Event kind cannot contain newline characters.")
    normalized = kind.strip()
    if not normalized:
        raise KitaruUsageError("Event kind must be a non-empty string.")
    if normalized in _RESERVED_CONTROL_KINDS:
        raise KitaruUsageError(
            f"Event kind {normalized!r} is reserved for server control events."
        )
    return normalized


def _normalize_correlation_id(correlation_id: str | None) -> str:
    """Resolve and validate the correlation ID for this event."""
    if correlation_id is not None:
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise KitaruUsageError(
                "Checkpoint event correlation ID must be a non-empty string."
            )
        return correlation_id.strip()

    resolved = _get_current_checkpoint_event_correlation_id()
    if not isinstance(resolved, str) or not resolved.strip():
        raise KitaruUsageError(
            "Checkpoint event correlation ID must be a non-empty string."
        )
    return resolved.strip()


def _normalize_percent(percent: float | None) -> float | None:
    """Validate an optional checkpoint-progress percentage."""
    if percent is None:
        return None
    if isinstance(percent, bool) or not isinstance(percent, int | float):
        raise KitaruUsageError("Progress percent must be a number between 0 and 1.")
    normalized = float(percent)
    if not math.isfinite(normalized) or normalized < 0 or normalized > 1:
        raise KitaruUsageError("Progress percent must be a number between 0 and 1.")
    return normalized


def _checkpoint_metadata(
    scope: _CheckpointScope,
    *,
    correlation_id: str,
) -> dict[str, str]:
    """Build the Kitaru-owned payload metadata for the active checkpoint."""
    metadata = {
        "source": "kitaru",
        "checkpoint_name": scope.name,
        "correlation_id": correlation_id,
    }
    if scope.execution_id is not None:
        metadata["execution_id"] = scope.execution_id
    if scope.checkpoint_id is not None:
        metadata["checkpoint_id"] = scope.checkpoint_id
    if scope.type is not None:
        metadata["checkpoint_type"] = scope.type
    return metadata


def _coerce_zenml_streaming(module: Any) -> _ZenMLStreaming | None:
    """Return a typed streaming producer when the imported module has one."""
    if callable(getattr(module, "publish", None)) and callable(
        getattr(module, "flush", None)
    ):
        return cast(_ZenMLStreaming, module)
    return None


@lru_cache(maxsize=1)
def _load_zenml_streaming() -> _ZenMLStreaming | None:
    """Load ZenML's released streaming producer module."""
    try:
        module = importlib.import_module("zenml.streaming")
    except (ImportError, ModuleNotFoundError):
        logger.debug("ZenML streaming is unavailable; dropping Kitaru event.")
        return None
    return _coerce_zenml_streaming(module)


def flush(timeout: float = 2.0) -> bool:
    """Flush pending live events when ZenML streaming is available.

    Flushing does not require checkpoint scope because it only drains the
    process-local ZenML publisher queue. It returns ``False`` if flushing itself
    fails or times out.
    """
    try:
        streaming = _load_zenml_streaming()
        if streaming is None:
            return True
        return bool(streaming.flush(timeout=timeout))
    except Exception as exc:  # pragma: no cover - logging-only fallback
        logger.warning("Failed to flush Kitaru checkpoint events: %s", exc)
        return False


def _build_event_envelope(
    scope: _CheckpointScope,
    *,
    correlation_id: str,
    fields: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build the payload envelope sent to ZenML streaming."""
    envelope: dict[str, Any] = {
        "kitaru": _checkpoint_metadata(scope, correlation_id=correlation_id)
    }
    if fields:
        envelope.update(
            {key: value for key, value in dict(fields).items() if key != "kitaru"}
        )
    return envelope


def _serialize_live_event_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Serialize a live event without letting user fields erase metadata."""
    serialized: dict[str, Any] = {}
    if "kitaru" in envelope:
        serialized["kitaru"] = to_json_safe(envelope["kitaru"], include_repr=False)

    for key, value in envelope.items():
        if key == "kitaru":
            continue
        serialized[key] = to_json_safe(value, include_repr=False)
    return serialized


def _publish_to_zenml_streaming(
    *,
    kind: str,
    envelope: dict[str, Any],
    correlation_id: str,
    index: int,
) -> None:
    """Publish one envelope to ZenML streaming when the producer is available."""
    streaming = _load_zenml_streaming()
    if streaming is None:
        return

    streaming.publish(
        _serialize_live_event_envelope(envelope),
        kind=kind,
        correlation_id=correlation_id,
        index=index,
    )


def _publish_envelope(
    kind: str,
    envelope_fields: Mapping[str, Any] | None = None,
    *,
    api_name: str,
    correlation_id: str | None = None,
    index: int | None = None,
    flush_after_publish: bool = False,
    flush_timeout: float = 2.0,
) -> None:
    """Publish one already-shaped event envelope, best effort after validation."""
    normalized_kind = _normalize_kind(kind)
    scope = _require_checkpoint_scope(api_name)
    resolved_correlation_id = _normalize_correlation_id(correlation_id)
    # Reserve indexes before transport publish. Even when streaming is disabled
    # or publish fails, the checkpoint-local event sequence must keep moving so
    # later events cannot reuse an already-attempted explicit index.
    event_index = _next_checkpoint_event_index(index)
    envelope = _build_event_envelope(
        scope,
        correlation_id=resolved_correlation_id,
        fields=envelope_fields,
    )

    try:
        _publish_to_zenml_streaming(
            kind=normalized_kind,
            envelope=envelope,
            correlation_id=resolved_correlation_id,
            index=event_index,
        )
        if flush_after_publish:
            flush(timeout=flush_timeout)
    except Exception as exc:  # pragma: no cover - logging-only fallback
        logger.warning("Failed to publish Kitaru checkpoint event %s: %s", kind, exc)


def publish(
    kind: str,
    payload: Mapping[str, Any] | None = None,
    *,
    message: str | None = None,
    correlation_id: str | None = None,
    index: int | None = None,
    flush: bool = False,
) -> None:
    """Publish a custom live event from inside the current checkpoint.

    Publishing is best effort: transport failures are logged and dropped, but
    misuse such as calling this outside ``@checkpoint`` raises immediately.
    """
    if payload is not None and not isinstance(payload, Mapping):
        raise KitaruUsageError("Event payload must be a mapping when provided.")
    if message is not None and not isinstance(message, str):
        raise KitaruUsageError("Event message must be a string when provided.")

    fields: dict[str, Any] = {}
    if message is not None:
        fields["message"] = message
    if payload:
        fields["data"] = dict(payload)

    _publish_envelope(
        kind,
        fields,
        api_name="events.publish",
        correlation_id=correlation_id,
        index=index,
        flush_after_publish=flush,
    )


def progress(
    message: str,
    *,
    percent: float | None = None,
    correlation_id: str | None = None,
    flush: bool = False,
    **fields: Any,
) -> None:
    """Publish a standard checkpoint progress event."""
    if not isinstance(message, str) or not message.strip():
        raise KitaruUsageError("Progress message must be a non-empty string.")

    payload = dict(fields)
    normalized_percent = _normalize_percent(percent)
    if normalized_percent is not None:
        payload["percent"] = normalized_percent

    event_fields: dict[str, Any] = {"message": message}
    if payload:
        event_fields["data"] = payload

    _publish_envelope(
        CHECKPOINT_PROGRESS_KIND,
        event_fields,
        api_name="progress",
        correlation_id=correlation_id,
        flush_after_publish=flush,
    )


def _safe_exception_message(error: BaseException) -> str:
    """Return an exception message without trusting ``error.__str__``."""
    try:
        message = str(error)
    except BaseException:
        return f"<{type(error).__name__} message unavailable>"
    return message or type(error).__name__


def _publish_checkpoint_lifecycle(
    event: _CheckpointLifecycleEvent,
    *,
    error: BaseException | None = None,
    flush_after_publish: bool = False,
) -> None:
    """Publish one automatic checkpoint lifecycle event."""
    fields: dict[str, Any] = {"status": event.status}
    if error is not None:
        fields["error_type"] = type(error).__name__
        fields["message"] = _safe_exception_message(error)

    _publish_envelope(
        event.kind,
        fields,
        api_name="events.publish",
        flush_after_publish=flush_after_publish,
        flush_timeout=_LIFECYCLE_FLUSH_TIMEOUT,
    )


def _publish_checkpoint_started() -> None:
    """Publish the automatic checkpoint-started lifecycle event."""
    _publish_checkpoint_lifecycle(_CHECKPOINT_STARTED_EVENT)


def _publish_checkpoint_returned() -> None:
    """Publish the automatic checkpoint-body-returned lifecycle event.

    This runs before ZenML/Kitaru output materialization. The event means the
    Python checkpoint function returned, not that the checkpoint output is
    already durably persisted.
    """
    _publish_checkpoint_lifecycle(
        _CHECKPOINT_RETURNED_EVENT,
        flush_after_publish=True,
    )


def _publish_checkpoint_failed(error: BaseException) -> None:
    """Publish the automatic checkpoint-failed lifecycle event."""
    # WHY: This path renders a user-controlled exception object. Failure-event
    # publishing must never replace the original checkpoint exception.
    try:
        _publish_checkpoint_lifecycle(
            _CHECKPOINT_FAILED_EVENT,
            error=error,
            flush_after_publish=True,
        )
    except BaseException:  # pragma: no cover - defensive best-effort boundary
        logger.warning(
            "Failed to publish Kitaru checkpoint failure lifecycle event.",
            exc_info=True,
        )


__all__ = [
    "CHECKPOINT_FAILED_KIND",
    "CHECKPOINT_PROGRESS_KIND",
    "CHECKPOINT_RETURNED_KIND",
    "CHECKPOINT_STARTED_KIND",
    "flush",
    "progress",
    "publish",
]
