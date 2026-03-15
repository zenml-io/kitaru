"""Internal helpers for runtime log normalization and sorting."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from zenml.models import StepRunResponse

from kitaru._client._models import LogEntry
from kitaru.errors import KitaruUsageError


def _normalize_log_source(source: str) -> str:
    """Normalize a runtime log source selector."""
    normalized = source.strip().lower()
    if not normalized:
        raise KitaruUsageError("`source` must be a non-empty string.")
    return normalized


def _parse_log_timestamp(value: str | None) -> datetime | None:
    """Parse an optional log timestamp for sorting."""
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _log_sort_key(entry: LogEntry, fallback_index: int) -> tuple[int, float, int]:
    """Build a stable sort key for runtime log entries."""
    parsed_timestamp = _parse_log_timestamp(entry.timestamp)
    if parsed_timestamp is None:
        return (1, float("inf"), fallback_index)
    return (0, parsed_timestamp.timestamp(), fallback_index)


def _sort_log_entries(entries: list[LogEntry]) -> list[LogEntry]:
    """Sort runtime log entries chronologically with stable fallback order."""
    indexed = list(enumerate(entries))
    indexed.sort(key=lambda item: _log_sort_key(item[1], item[0]))
    return [entry for _, entry in indexed]


def _step_log_fetch_order_key(step: StepRunResponse) -> tuple[float, str, str]:
    """Order step runs deterministically for sequential log retrieval."""
    start_time = step.start_time
    if start_time is None:
        start_key = float("inf")
    else:
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
        start_key = start_time.timestamp()

    from kitaru._client._mappers import _normalize_checkpoint_name

    return (start_key, _normalize_checkpoint_name(step.name), str(step.id))


def _coerce_log_level(value: Any) -> str | None:
    """Coerce a log level value from API payloads to a string."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, Mapping):
        nested = value.get("value")
        if isinstance(nested, str):
            normalized_nested = nested.strip()
            return normalized_nested or None
    return str(value)


def _coerce_log_text(value: Any) -> str | None:
    """Coerce optional log text fields to stripped strings."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return str(value)


def _coerce_log_lineno(value: Any) -> int | None:
    """Coerce an optional log line number value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _map_runtime_log_entry(
    raw_entry: Mapping[str, Any],
    *,
    source: str,
    checkpoint_name: str | None,
) -> LogEntry:
    """Map one raw REST log payload entry into a public `LogEntry`."""
    message_value = raw_entry.get("message")
    if isinstance(message_value, str):
        message = message_value
    elif message_value is None:
        message = ""
    else:
        message = str(message_value)

    timestamp_value = raw_entry.get("timestamp")
    timestamp: str | None
    if isinstance(timestamp_value, datetime):
        timestamp = timestamp_value.isoformat()
    elif isinstance(timestamp_value, str):
        stripped_timestamp = timestamp_value.strip()
        timestamp = stripped_timestamp or None
    else:
        timestamp = None

    return LogEntry(
        message=message,
        level=_coerce_log_level(raw_entry.get("level")),
        timestamp=timestamp,
        source=source,
        checkpoint_name=checkpoint_name,
        module=_coerce_log_text(raw_entry.get("module")),
        filename=_coerce_log_text(raw_entry.get("filename")),
        lineno=_coerce_log_lineno(raw_entry.get("lineno")),
    )


def _is_empty_log_result_error(message: str) -> bool:
    """Return whether an error message indicates an empty log collection."""
    lowered = message.lower()
    return "no logs found" in lowered


def _is_otel_log_retrieval_error(message: str) -> bool:
    """Return whether an error message points to OTEL export-only retrieval."""
    lowered = message.lower()
    if "notimplementederror" in lowered:
        return True
    return "otel" in lowered and "not implemented" in lowered


__all__ = [
    "_coerce_log_level",
    "_coerce_log_lineno",
    "_coerce_log_text",
    "_is_empty_log_result_error",
    "_is_otel_log_retrieval_error",
    "_log_sort_key",
    "_map_runtime_log_entry",
    "_normalize_log_source",
    "_parse_log_timestamp",
    "_sort_log_entries",
    "_step_log_fetch_order_key",
]
