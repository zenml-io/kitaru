"""Best-effort live event publishing from inside Kitaru checkpoints."""

from __future__ import annotations

import importlib
import logging
from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from kitaru._serialization import to_json_safe
from kitaru.errors import KitaruContextError, KitaruUsageError
from kitaru.runtime import (
    _get_current_checkpoint,
    _get_current_checkpoint_event_stream_id,
    _next_checkpoint_event_index,
)

logger = logging.getLogger(__name__)

CHECKPOINT_STARTED_KIND = "kitaru.checkpoint.started"
CHECKPOINT_PROGRESS_KIND = "kitaru.checkpoint.progress"
CHECKPOINT_COMPLETED_KIND = "kitaru.checkpoint.completed"
CHECKPOINT_FAILED_KIND = "kitaru.checkpoint.failed"

_PUBLISH_OUTSIDE_CHECKPOINT_ERROR = (
    "kitaru.{api_name}() can only be called inside a @checkpoint."
)


def _require_checkpoint_scope(api_name: str) -> Any:
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
    normalized = kind.strip()
    if not normalized:
        raise KitaruUsageError("Event kind must be a non-empty string.")
    return normalized


def _normalize_index(index: int | None) -> int | None:
    """Validate an optional caller-provided stream index."""
    if index is None:
        return None
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise KitaruUsageError("Event index must be a non-negative integer.")
    return index


def _checkpoint_metadata(scope: Any) -> dict[str, str | None]:
    """Build the Kitaru-owned payload metadata for the active checkpoint."""
    return {
        "source": "kitaru",
        "execution_id": scope.execution_id,
        "checkpoint_id": scope.checkpoint_id,
        "checkpoint_name": scope.name,
        "checkpoint_type": scope.type,
    }


@lru_cache(maxsize=1)
def _load_zenml_streams() -> Any | None:
    """Load ZenML's stream producer module if this ZenML build exposes it."""
    try:
        return importlib.import_module("zenml.streams")
    except (ImportError, ModuleNotFoundError):
        logger.debug("ZenML stream publishing is unavailable; dropping event.")
        return None


def _flush_streams(streams: Any, *, timeout: float) -> None:
    """Flush the ZenML stream publisher if the active build exposes flushing."""
    flush_fn = getattr(streams, "flush", None)
    if flush_fn is None:
        return
    flush_fn(timeout=timeout)


def _publish_envelope(
    kind: str,
    envelope_fields: Mapping[str, Any] | None = None,
    *,
    api_name: str,
    stream_id: str | None = None,
    index: int | None = None,
    flush: bool = False,
    flush_timeout: float = 2.0,
) -> None:
    """Publish one already-shaped event envelope, best effort after validation."""
    normalized_kind = _normalize_kind(kind)
    normalized_index = _normalize_index(index)
    scope = _require_checkpoint_scope(api_name)
    event_stream_id = stream_id or _get_current_checkpoint_event_stream_id()
    event_index = _next_checkpoint_event_index(normalized_index)
    try:
        streams = _load_zenml_streams()
        publish_fn = getattr(streams, "publish", None) if streams is not None else None
        if publish_fn is None:
            return

        envelope: dict[str, Any] = {"kitaru": _checkpoint_metadata(scope)}
        if envelope_fields:
            envelope.update(dict(envelope_fields))

        publish_fn(
            to_json_safe(envelope),
            kind=normalized_kind,
            stream_id=event_stream_id,
            index=event_index,
        )
        if flush:
            _flush_streams(streams, timeout=flush_timeout)
    except Exception as exc:  # pragma: no cover - logging-only fallback
        logger.warning("Failed to publish Kitaru checkpoint event %s: %s", kind, exc)


def publish(
    kind: str,
    payload: Mapping[str, Any] | None = None,
    *,
    message: str | None = None,
    stream_id: str | None = None,
    index: int | None = None,
    flush: bool = False,
) -> None:
    """Publish a custom live event from inside the current checkpoint.

    Publishing is best effort: transport failures are logged and dropped, but
    misuse such as calling this outside ``@checkpoint`` raises immediately.
    """
    if payload is not None and not isinstance(payload, Mapping):
        raise KitaruUsageError("Event payload must be a mapping when provided.")

    fields: dict[str, Any] = {}
    if message is not None:
        fields["message"] = message
    if payload:
        fields["data"] = dict(payload)

    _publish_envelope(
        kind,
        fields,
        api_name="events.publish",
        stream_id=stream_id,
        index=index,
        flush=flush,
    )


def progress(
    message: str,
    *,
    percent: float | None = None,
    stream_id: str | None = None,
    flush: bool = False,
    **fields: Any,
) -> None:
    """Publish a standard checkpoint progress event."""
    if not isinstance(message, str) or not message.strip():
        raise KitaruUsageError("Progress message must be a non-empty string.")

    payload = dict(fields)
    if percent is not None:
        payload["percent"] = percent

    event_fields: dict[str, Any] = {"message": message}
    if payload:
        event_fields["data"] = payload

    _publish_envelope(
        CHECKPOINT_PROGRESS_KIND,
        event_fields,
        api_name="progress",
        stream_id=stream_id,
        flush=flush,
    )


def flush(timeout: float = 2.0) -> None:
    """Flush pending checkpoint live events when ZenML streaming is available."""
    _require_checkpoint_scope("events.flush")
    try:
        streams = _load_zenml_streams()
        if streams is None:
            return
        _flush_streams(streams, timeout=timeout)
    except Exception as exc:  # pragma: no cover - logging-only fallback
        logger.warning("Failed to flush Kitaru checkpoint events: %s", exc)


def _publish_checkpoint_lifecycle(
    *,
    kind: str,
    status: str,
    error: BaseException | None = None,
    flush: bool = False,
) -> None:
    """Publish one automatic checkpoint lifecycle event."""
    fields: dict[str, Any] = {"status": status}
    if error is not None:
        fields["error_type"] = type(error).__name__
        fields["message"] = str(error)

    _publish_envelope(
        kind,
        fields,
        api_name="events.publish",
        flush=flush,
    )


def _publish_checkpoint_started() -> None:
    """Publish the automatic checkpoint-started lifecycle event."""
    _publish_checkpoint_lifecycle(
        kind=CHECKPOINT_STARTED_KIND,
        status="started",
    )


def _publish_checkpoint_completed() -> None:
    """Publish the automatic checkpoint-completed lifecycle event."""
    _publish_checkpoint_lifecycle(
        kind=CHECKPOINT_COMPLETED_KIND,
        status="completed",
        flush=True,
    )


def _publish_checkpoint_failed(error: BaseException) -> None:
    """Publish the automatic checkpoint-failed lifecycle event."""
    _publish_checkpoint_lifecycle(
        kind=CHECKPOINT_FAILED_KIND,
        status="failed",
        error=error,
        flush=True,
    )


__all__ = [
    "CHECKPOINT_COMPLETED_KIND",
    "CHECKPOINT_FAILED_KIND",
    "CHECKPOINT_PROGRESS_KIND",
    "CHECKPOINT_STARTED_KIND",
    "flush",
    "progress",
    "publish",
]
