"""Shared memory helpers for CLI and MCP transport layers."""

from __future__ import annotations

from typing import Any

import kitaru.inspection as inspection
from kitaru.client import KitaruClient
from kitaru.memory import (
    _MemoryScopeType,
    _validate_memory_identifier,
    _validate_memory_scope_type,
    _validate_memory_version,
)


def normalize_memory_scope(scope: str) -> str:
    """Validate and normalize a transport-level memory scope."""
    return _validate_memory_identifier(scope, kind="scope", error_type=ValueError)


def normalize_memory_key(key: str) -> str:
    """Validate and normalize a transport-level memory key."""
    return _validate_memory_identifier(key, kind="key", error_type=ValueError)


def normalize_memory_version(version: int | None) -> int | None:
    """Validate and normalize an optional transport-level memory version."""
    return _validate_memory_version(version, error_type=ValueError)


def normalize_memory_prefix(prefix: str | None) -> str | None:
    """Validate and normalize an optional transport-level memory prefix."""
    if prefix is None:
        return None
    return _validate_memory_identifier(prefix, kind="prefix", error_type=ValueError)


def normalize_memory_scope_type(
    scope_type: str | None,
    *,
    default: str = "namespace",
) -> _MemoryScopeType:
    """Validate and normalize a transport-level memory scope type."""
    candidate = default if scope_type is None else scope_type
    return _validate_memory_scope_type(candidate, error_type=ValueError)


def scopes_memory_payload(
    client: KitaruClient,
) -> list[dict[str, Any]]:
    """Build serialized payloads for all discovered memory scopes."""
    scopes = client.memories.scopes()
    return [inspection.serialize_memory_scope_info(info) for info in scopes]


def get_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    scope: str,
    version: int | None = None,
) -> dict[str, Any] | None:
    """Build a serialized payload for reading one memory value."""
    entry = client.memories.get(
        normalize_memory_key(key),
        scope=normalize_memory_scope(scope),
        version=normalize_memory_version(version),
    )
    if entry is None:
        return None

    value_payload = inspection.serialize_memory_value(
        client.artifacts.get(entry.artifact_id).load()
    )
    return {
        **inspection.serialize_memory_entry(entry),
        "value": value_payload["value"],
        "value_format": value_payload["value_format"],
    }


def list_memory_payload(
    client: KitaruClient,
    *,
    scope: str,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Build serialized payloads for listing memories in one scope."""
    entries = client.memories.list(
        scope=normalize_memory_scope(scope),
        prefix=normalize_memory_prefix(prefix),
    )
    return [inspection.serialize_memory_entry(entry) for entry in entries]


def history_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    scope: str,
) -> list[dict[str, Any]]:
    """Build serialized payloads for one memory key's full history."""
    return inspection.serialize_memory_history(
        client.memories.history(
            normalize_memory_key(key),
            scope=normalize_memory_scope(scope),
        )
    )


def set_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    value: Any,
    scope: str,
    scope_type: str | None = None,
) -> dict[str, Any]:
    """Build a serialized payload for writing one memory value."""
    entry = client.memories.set(
        normalize_memory_key(key),
        value,
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
    )
    return inspection.serialize_memory_entry(entry)


def delete_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    scope: str,
) -> dict[str, Any] | None:
    """Build a serialized payload for deleting one memory key."""
    entry = client.memories.delete(
        normalize_memory_key(key),
        scope=normalize_memory_scope(scope),
    )
    if entry is None:
        return None
    return inspection.serialize_memory_entry(entry)
