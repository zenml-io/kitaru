"""Scope resolution and validation for Kitaru memory."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Literal, cast

from kitaru.errors import (
    KitaruContextError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.memory._constants import (
    _COMPACTION_LOG_PREFIX,
    _MEMORY_IDENTIFIER_PATTERN,
)
from kitaru.memory._models import (
    _MemoryCompactionSourceMode,
    _MemoryScope,
    _MemoryScopeType,
)
from kitaru.runtime import (
    _get_current_execution_id,
    _get_current_flow,
    _get_current_flow_id,
    _is_inside_checkpoint,
    _is_inside_flow,
)

_RUNTIME_MEMORY_SCOPE_DEFAULT: _MemoryScope | None = None
_CURRENT_MEMORY_SCOPE: ContextVar[_MemoryScope | None] = ContextVar(
    "kitaru_current_memory_scope",
    default=None,
)


def _validate_memory_identifier(
    value: str,
    *,
    kind: Literal["key", "scope", "prefix"],
    error_type: type[Exception] = KitaruUsageError,
    _allow_compaction_prefix: bool = False,
) -> str:
    """Validate and normalize a memory key or scope identifier."""
    normalized = value.strip()
    if not normalized:
        raise error_type(
            f"Memory {kind} must be non-empty and may not be whitespace-only."
        )
    if not _MEMORY_IDENTIFIER_PATTERN.fullmatch(normalized):
        raise error_type(
            f"Memory {kind} {normalized!r} may only contain letters, "
            "numbers, '.', '_', '-', and '/'. Colons are not allowed."
        )
    if (
        not _allow_compaction_prefix
        and kind == "key"
        and normalized.startswith(_COMPACTION_LOG_PREFIX)
    ):
        raise error_type(
            f"Memory key prefix '{_COMPACTION_LOG_PREFIX}' is reserved "
            "for compaction audit logs."
        )
    return normalized


def _validate_memory_scope_type(
    scope_type: str,
    *,
    error_type: type[Exception] = KitaruUsageError,
) -> _MemoryScopeType:
    """Validate and normalize a memory scope type."""
    normalized = str(scope_type).strip().lower()
    if normalized not in {"namespace", "flow", "execution"}:
        raise error_type(
            "Memory scope_type must be one of 'namespace', 'flow', or 'execution'."
        )
    return cast(_MemoryScopeType, normalized)


def _validate_memory_version(
    version: int | None,
    *,
    error_type: type[Exception] = KitaruUsageError,
) -> int | None:
    """Validate and normalize an optional memory version number."""
    if version is None:
        return None
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise error_type("Memory version must be an integer >= 1.")
    return version


def _validate_memory_compaction_source_mode(
    value: str,
    *,
    error_type: type[Exception] = KitaruUsageError,
) -> _MemoryCompactionSourceMode:
    """Validate and normalize a compaction source mode."""
    normalized = str(value).strip().lower()
    if normalized not in {"current", "history"}:
        raise error_type(
            "Memory compaction source_mode must be 'current' or 'history'."
        )
    return cast(_MemoryCompactionSourceMode, normalized)


def _require_memory_boundary(api_name: str) -> None:
    """Enforce shared public memory context restrictions."""
    qualified_name = f"kitaru.memory.{api_name}()"

    if _is_inside_checkpoint():
        raise KitaruContextError(
            f"{qualified_name} cannot be called inside a @checkpoint. "
            "Move memory operations to the flow body."
        )


def _implicit_flow_memory_scope(api_name: str) -> _MemoryScope:
    """Resolve the implicit flow-ID-backed memory scope."""
    qualified_name = f"kitaru.memory.{api_name}()"

    flow_scope = _get_current_flow()
    resolved_flow_id = _get_current_flow_id()
    if flow_scope is None:
        raise KitaruStateError(
            f"{qualified_name} requires an active flow scope inside @flow."
        )
    if resolved_flow_id is None:
        raise KitaruStateError(
            f"{qualified_name} could not resolve a durable flow ID from the "
            f"ZenML runtime. This typically means the pipeline has not been "
            f"registered yet or the runtime context is not fully initialized."
        )

    return _MemoryScope(
        scope=_validate_memory_identifier(resolved_flow_id, kind="scope"),
        scope_type="flow",
    )


def _resolve_configured_scope(
    scope: str | None,
    *,
    scope_type: _MemoryScopeType | None,
) -> _MemoryScope:
    """Resolve validated configuration input into a configured scope."""
    normalized_scope = (
        _validate_memory_identifier(scope, kind="scope") if scope is not None else None
    )
    normalized_scope_type = (
        _validate_memory_scope_type(scope_type) if scope_type is not None else None
    )

    if normalized_scope is not None:
        return _MemoryScope(
            scope=normalized_scope,
            scope_type=normalized_scope_type or "namespace",
        )

    if normalized_scope_type is None:
        raise KitaruUsageError(
            "kitaru.memory.configure() requires `scope=` or `scope_type=`."
        )

    if normalized_scope_type == "namespace":
        raise KitaruUsageError(
            "kitaru.memory.configure(scope_type='namespace') requires "
            "an explicit `scope=` value."
        )

    if not _is_inside_flow():
        raise KitaruContextError(
            "kitaru.memory.configure() can only infer flow or execution scopes "
            "inside a @flow. Provide an explicit `scope=` outside flows."
        )

    if normalized_scope_type == "flow":
        return _MemoryScope(
            scope=_implicit_flow_memory_scope("configure").scope,
            scope_type="flow",
        )

    execution_id = _get_current_execution_id()
    if execution_id is None:
        raise KitaruStateError(
            "kitaru.memory.configure(scope_type='execution') requires an "
            "active execution ID inside @flow."
        )
    return _MemoryScope(
        scope=_validate_memory_identifier(execution_id, kind="scope"),
        scope_type="execution",
    )


def _resolve_memory_scope_for_operation(api_name: str) -> _MemoryScope:
    """Resolve the effective memory scope for a public API call."""
    _require_memory_boundary(api_name)

    if _is_inside_flow():
        configured_scope = _CURRENT_MEMORY_SCOPE.get()
        if configured_scope is not None:
            return configured_scope
        return _implicit_flow_memory_scope(api_name)

    if _RUNTIME_MEMORY_SCOPE_DEFAULT is not None:
        return _RUNTIME_MEMORY_SCOPE_DEFAULT

    raise KitaruStateError(
        f"kitaru.memory.{api_name}() outside a @flow requires an explicit scope. "
        "Call kitaru.memory.configure(scope=...) first."
    )


@contextmanager
def _memory_scope_session() -> Iterator[None]:
    """Snapshot the current process-local memory default for one flow run."""
    token = _CURRENT_MEMORY_SCOPE.set(_RUNTIME_MEMORY_SCOPE_DEFAULT)
    try:
        yield
    finally:
        _CURRENT_MEMORY_SCOPE.reset(token)


def _coerce_memory_scope(scope: str, scope_type: str) -> _MemoryScope:
    """Reconstruct a validated memory scope inside synthetic memory steps."""
    normalized_scope = _validate_memory_identifier(
        scope,
        kind="scope",
        error_type=KitaruRuntimeError,
    )
    normalized_scope_type = _validate_memory_scope_type(
        scope_type,
        error_type=KitaruRuntimeError,
    )
    return _MemoryScope(
        scope=normalized_scope,
        scope_type=normalized_scope_type,
    )


def configure(
    scope: str | None = None,
    *,
    scope_type: _MemoryScopeType | None = None,
) -> None:
    """Configure the active memory scope for subsequent memory operations.

    Inside a flow this updates the flow-local scope for later ``memory.*`` calls.
    Outside a flow this stores a process-local default that is used immediately by
    outside-flow memory operations and also seeds later flow runs.
    """
    global _RUNTIME_MEMORY_SCOPE_DEFAULT

    _require_memory_boundary("configure")
    configured_scope = _resolve_configured_scope(scope, scope_type=scope_type)

    if _is_inside_flow():
        _CURRENT_MEMORY_SCOPE.set(configured_scope)
    else:
        _RUNTIME_MEMORY_SCOPE_DEFAULT = configured_scope

    return None
