"""Best-effort live event publishing surface.

This branch does not yet carry the full live-event transport layer. The public
surface still exists so adapter stream publishers can route through
``kitaru.events.publish`` and tests can monkeypatch that seam. Publishing is a
no-op unless a future transport implementation is installed.
"""

from collections.abc import Mapping
from typing import Any

from kitaru.errors import KitaruUsageError


def publish(
    kind: str,
    payload: Mapping[str, Any] | None = None,
    *,
    message: str | None = None,
    correlation_id: str | None = None,
    index: int | None = None,
    flush: bool = False,
) -> None:
    """Publish a custom live event best-effort.

    The current implementation validates the public call shape and then drops
    the event. Adapter publishers catch publish failures, so this no-op keeps
    streaming observability from changing provider results on branches without
    a live-event backend.
    """
    _ = correlation_id, index, flush
    _normalize_kind(kind)
    if payload is not None and not isinstance(payload, Mapping):
        raise KitaruUsageError("Event payload must be a mapping when provided.")
    if message is not None and not isinstance(message, str):
        raise KitaruUsageError("Event message must be a string when provided.")


def progress(
    message: str,
    *,
    percent: float | None = None,
    correlation_id: str | None = None,
    flush: bool = False,
    **fields: Any,
) -> None:
    """Publish a checkpoint progress event best-effort."""
    _ = percent, correlation_id, flush, fields
    if not isinstance(message, str) or not message.strip():
        raise KitaruUsageError("Progress message must be a non-empty string.")


def flush(timeout: float = 2.0) -> bool:
    """Flush pending live events when a backend is available."""
    _ = timeout
    return True


def _normalize_kind(kind: str) -> str:
    if not isinstance(kind, str):
        raise KitaruUsageError("Event kind must be a non-empty string.")
    normalized = kind.strip()
    if not normalized:
        raise KitaruUsageError("Event kind must be a non-empty string.")
    if "\n" in normalized or "\r" in normalized:
        raise KitaruUsageError("Event kind cannot contain newline characters.")
    return normalized


__all__ = ["flush", "progress", "publish"]
