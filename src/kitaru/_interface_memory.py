"""Shared memory helpers for CLI and MCP transport layers."""

from __future__ import annotations

from typing import Any

import kitaru.inspection as inspection
from kitaru.client import KitaruClient
from kitaru.memory import (
    _MemoryCompactionSourceMode,
    _MemoryScopeType,
    _validate_memory_compaction_source_mode,
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
    scope_type: str,
) -> _MemoryScopeType:
    """Validate and normalize a transport-level memory scope type."""
    if scope_type is None:
        raise ValueError("`scope_type` is required.")
    return _validate_memory_scope_type(scope_type, error_type=ValueError)


def normalize_memory_compaction_source_mode(
    source_mode: str | None,
    *,
    default: str = "current",
) -> _MemoryCompactionSourceMode:
    """Validate and normalize a transport-level compaction source mode."""
    candidate = default if source_mode is None else source_mode
    return _validate_memory_compaction_source_mode(candidate, error_type=ValueError)


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
    scope_type: str,
    version: int | None = None,
) -> dict[str, Any] | None:
    """Build a serialized payload for reading one memory value."""
    entry = client.memories.get(
        normalize_memory_key(key),
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
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
    scope_type: str,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Build serialized payloads for listing memories in one scope."""
    entries = client.memories.list(
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
        prefix=normalize_memory_prefix(prefix),
    )
    return [inspection.serialize_memory_entry(entry) for entry in entries]


def history_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    scope: str,
    scope_type: str,
) -> list[dict[str, Any]]:
    """Build serialized payloads for one memory key's full history."""
    return inspection.serialize_memory_history(
        client.memories.history(
            normalize_memory_key(key),
            scope=normalize_memory_scope(scope),
            scope_type=normalize_memory_scope_type(scope_type),
        )
    )


def set_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    value: Any,
    scope: str,
    scope_type: str,
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
    scope_type: str,
) -> dict[str, Any] | None:
    """Build a serialized payload for deleting one memory key."""
    entry = client.memories.delete(
        normalize_memory_key(key),
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
    )
    if entry is None:
        return None
    return inspection.serialize_memory_entry(entry)


def normalize_memory_keep(keep: int | None) -> int | None:
    """Validate and normalize an optional transport-level keep parameter."""
    if keep is None:
        return None
    if isinstance(keep, bool) or not isinstance(keep, int) or keep < 0:
        raise ValueError("`keep` must be a non-negative integer or None.")
    return keep


def purge_memory_payload(
    client: KitaruClient,
    *,
    key: str,
    scope: str,
    scope_type: str,
    keep: int | None = None,
) -> dict[str, Any]:
    """Build a serialized payload for purging one memory key."""
    result = client.memories.purge(
        normalize_memory_key(key),
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
        keep=normalize_memory_keep(keep),
    )
    return inspection.serialize_purge_result(result)


def purge_scope_memory_payload(
    client: KitaruClient,
    *,
    scope: str,
    scope_type: str,
    keep: int | None = None,
    include_deleted: bool = False,
) -> dict[str, Any]:
    """Build a serialized payload for purging all keys in a scope."""
    result = client.memories.purge_scope(
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
        keep=normalize_memory_keep(keep),
        include_deleted=include_deleted,
    )
    return inspection.serialize_purge_result(result)


def compact_memory_payload(
    client: KitaruClient,
    *,
    scope: str,
    scope_type: str,
    key: str | None = None,
    keys: list[str] | None = None,
    source_mode: str | None = None,
    target_key: str | None = None,
    instruction: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """Build a serialized payload for compacting memory."""
    validated_key = normalize_memory_key(key) if key is not None else None
    validated_keys = (
        [normalize_memory_key(k) for k in keys] if keys is not None else None
    )
    validated_target = (
        normalize_memory_key(target_key) if target_key is not None else None
    )
    result = client.memories.compact(
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
        key=validated_key,
        keys=validated_keys,
        source_mode=normalize_memory_compaction_source_mode(source_mode),
        target_key=validated_target,
        instruction=instruction,
        model=model,
        max_tokens=max_tokens,
    )
    return inspection.serialize_compact_result(result)


def compaction_log_memory_payload(
    client: KitaruClient,
    *,
    scope: str,
    scope_type: str,
) -> list[dict[str, Any]]:
    """Build serialized payloads for compaction audit log entries."""
    records = client.memories.compaction_log(
        scope=normalize_memory_scope(scope),
        scope_type=normalize_memory_scope_type(scope_type),
    )
    return [inspection.serialize_compaction_record(record) for record in records]


def reindex_memory_payload(
    client: KitaruClient,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    """Build a serialized payload for memory reindex/backfill."""
    result = client.memories.reindex(apply=apply)
    return inspection.serialize_memory_reindex_result(result)
