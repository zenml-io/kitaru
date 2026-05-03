"""Public runtime API wrappers for Kitaru memory."""

from typing import Any

from kitaru import runtime
from kitaru.memory import _operations as operations
from kitaru.memory import _scope as scope_mod
from kitaru.memory import _steps as steps
from kitaru.memory._constants import _list
from kitaru.memory._models import MemoryEntry


def set(key: str, value: Any) -> None:
    """Persist a new version of a memory key in the active scope.

    Inside a flow, this dispatches through a synthetic non-cacheable ZenML step so
    the write happens at runtime. Outside a flow, it writes directly to the
    artifact store using the configured process-local scope.
    """
    scope = scope_mod._resolve_memory_scope_for_operation("set")
    normalized_key = scope_mod._validate_memory_identifier(key, kind="key")
    if runtime._is_inside_flow():
        steps._memory_set_step(scope.scope, scope.scope_type, normalized_key, value)
    else:
        operations._set_impl(scope, normalized_key, value)
    return None


def get(key: str, *, version: int | None = None, strict: bool = False) -> Any | None:
    """Return the current value for a memory key in the active scope.

    Inside a flow, reads run through a synthetic non-cacheable ZenML step so
    the lookup happens at runtime. Outside a flow, reads query the artifact
    store directly using the configured process-local scope.

    If the memory entry exists but its backing artifact cannot be loaded from
    the current runtime, lenient mode (``strict=False``, the default) emits a
    ``RuntimeWarning`` and returns ``None``; strict mode raises
    ``KitaruMemoryArtifactUnavailableError``. Missing or tombstoned entries
    always return ``None`` regardless of mode.
    """
    scope = scope_mod._resolve_memory_scope_for_operation("get")
    normalized_key = scope_mod._validate_memory_identifier(key, kind="key")
    normalized_version = scope_mod._validate_memory_version(version)
    if runtime._is_inside_flow():
        return steps._memory_get_step(
            scope.scope,
            scope.scope_type,
            normalized_key,
            normalized_version,
            strict,
        )
    return operations._get_impl(
        scope, normalized_key, normalized_version, strict=strict
    )


def list() -> _list[MemoryEntry]:
    """List the latest active memory entries for the active scope."""
    scope = scope_mod._resolve_memory_scope_for_operation("list")
    if runtime._is_inside_flow():
        return steps._memory_list_step(scope.scope, scope.scope_type)
    return operations._list_impl(scope)


def history(key: str) -> _list[MemoryEntry]:
    """Return all versions of a memory key, including tombstones."""
    scope = scope_mod._resolve_memory_scope_for_operation("history")
    normalized_key = scope_mod._validate_memory_identifier(key, kind="key")
    if runtime._is_inside_flow():
        return steps._memory_history_step(scope.scope, scope.scope_type, normalized_key)
    return operations._history_impl(scope, normalized_key)


def delete(key: str) -> MemoryEntry | None:
    """Soft-delete a memory key by writing a tombstone version."""
    scope = scope_mod._resolve_memory_scope_for_operation("delete")
    normalized_key = scope_mod._validate_memory_identifier(key, kind="key")
    if runtime._is_inside_flow():
        return steps._memory_delete_step(scope.scope, scope.scope_type, normalized_key)
    return operations._delete_impl(scope, normalized_key)
