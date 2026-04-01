"""Configurable memory primitives backed by ZenML artifact versions.

The public API is exposed as a module namespace:

    from kitaru import memory

    @flow
    def my_agent() -> None:
        memory.set("preferences", {"theme": "dark"})
        prefs = memory.get("preferences")

Current status:
- allowed in the flow body
- forbidden inside ``@checkpoint``
- configurable scope defaults via ``memory.configure(...)``
- outside-flow reads/writes deferred to a later phase
"""

from __future__ import annotations

import builtins
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict
from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactType, StepType
from zenml.models.v2.base.page import Page
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.steps.step_decorator import step

from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.runtime import (
    _get_current_execution_id,
    _get_current_flow,
    _is_inside_checkpoint,
    _is_inside_flow,
)

# The public API defines ``list()`` which shadows ``builtins.list``.
# Alias the builtin so type annotations resolve correctly under ty.
_list = builtins.list

_MEMORY_ARTIFACT_PREFIX = "kitaru_mem"
_MEMORY_TAG_MARKER = "kitaru:memory"
_MEMORY_TAG_SCOPE_PREFIX = "kitaru:memory:scope:"
_MEMORY_TAG_KEY_PREFIX = "kitaru:memory:key:"
_MEMORY_SCOPE_TYPE_METADATA_KEY = "kitaru_memory_scope_type"
_MEMORY_DELETED_METADATA_KEY = "kitaru_memory_deleted"
_MEMORY_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._\-/]+$")
_MEMORY_PAGE_SIZE = 100
_MEMORY_STEP_EXTRA_PREFIX = {"kitaru": {"boundary": "memory"}}
_MemoryScopeType = Literal["namespace", "flow", "execution"]


class MemoryEntry(BaseModel):
    """A single persisted memory version."""

    key: str
    value_type: str
    version: int
    scope: str
    scope_type: str
    created_at: datetime
    is_deleted: bool
    artifact_id: str
    execution_id: str | None

    model_config = ConfigDict(frozen=True)


@dataclass(frozen=True)
class _MemoryScope:
    """Resolved runtime memory scope for the current call."""

    scope: str
    scope_type: _MemoryScopeType


@dataclass(frozen=True)
class _ConfiguredMemoryScope:
    """User-configured memory scope default."""

    scope: str
    scope_type: _MemoryScopeType


_RUNTIME_MEMORY_SCOPE_DEFAULT: _ConfiguredMemoryScope | None = None
_CURRENT_MEMORY_SCOPE: ContextVar[_ConfiguredMemoryScope | None] = ContextVar(
    "kitaru_current_memory_scope",
    default=None,
)


def _validate_memory_identifier(
    value: str,
    *,
    kind: Literal["key", "scope"],
    error_type: type[KitaruError] = KitaruUsageError,
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
    return normalized


def _validate_memory_scope_type(
    scope_type: str,
    *,
    error_type: type[KitaruError] = KitaruUsageError,
) -> _MemoryScopeType:
    """Validate and normalize a memory scope type."""
    normalized = str(scope_type).strip().lower()
    if normalized not in {"namespace", "flow", "execution"}:
        raise error_type(
            "Memory scope_type must be one of 'namespace', 'flow', or 'execution'."
        )
    return cast(_MemoryScopeType, normalized)


def _require_memory_boundary(api_name: str) -> None:
    """Enforce shared public memory context restrictions."""
    qualified_name = f"kitaru.memory.{api_name}()"

    if _is_inside_checkpoint():
        raise KitaruContextError(
            f"{qualified_name} cannot be called inside a @checkpoint. "
            "Move memory operations to the flow body."
        )


def _implicit_flow_memory_scope(api_name: str) -> _MemoryScope:
    """Resolve the implicit flow-name-backed memory scope."""
    qualified_name = f"kitaru.memory.{api_name}()"

    flow_scope = _get_current_flow()
    if flow_scope is None or flow_scope.name is None:
        raise KitaruStateError(
            f"{qualified_name} requires an active flow name inside @flow."
        )

    return _MemoryScope(
        scope=_validate_memory_identifier(flow_scope.name, kind="scope"),
        scope_type="flow",
    )


def _resolve_configured_scope(
    scope: str | None,
    *,
    scope_type: _MemoryScopeType | None,
) -> _ConfiguredMemoryScope:
    """Resolve validated configuration input into a configured scope."""
    normalized_scope = (
        _validate_memory_identifier(scope, kind="scope") if scope is not None else None
    )
    normalized_scope_type = (
        _validate_memory_scope_type(scope_type) if scope_type is not None else None
    )

    if normalized_scope is not None:
        return _ConfiguredMemoryScope(
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
        return _ConfiguredMemoryScope(
            scope=_implicit_flow_memory_scope("configure").scope,
            scope_type="flow",
        )

    execution_id = _get_current_execution_id()
    if execution_id is None:
        raise KitaruStateError(
            "kitaru.memory.configure(scope_type='execution') requires an "
            "active execution ID inside @flow."
        )
    return _ConfiguredMemoryScope(
        scope=_validate_memory_identifier(execution_id, kind="scope"),
        scope_type="execution",
    )


def _resolve_memory_scope_for_operation(api_name: str) -> _MemoryScope:
    """Resolve the effective memory scope for a public API call."""
    qualified_name = f"kitaru.memory.{api_name}()"
    _require_memory_boundary(api_name)

    if not _is_inside_flow():
        raise KitaruContextError(
            f"{qualified_name} can only be called inside a @flow. "
            "Outside-flow memory support is not available yet."
        )

    configured_scope = _CURRENT_MEMORY_SCOPE.get()
    if configured_scope is not None:
        return _MemoryScope(
            scope=configured_scope.scope,
            scope_type=configured_scope.scope_type,
        )

    return _implicit_flow_memory_scope(api_name)


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


def _memory_artifact_name(scope: str, key: str) -> str:
    """Build the canonical artifact name for a memory key."""
    return f"{_MEMORY_ARTIFACT_PREFIX}:{scope}:{key}"


def _memory_scope_tag(scope: str) -> str:
    """Build the scope tag used for memory queries."""
    return f"{_MEMORY_TAG_SCOPE_PREFIX}{scope}"


def _memory_key_tag(key: str) -> str:
    """Build the key tag used for memory queries."""
    return f"{_MEMORY_TAG_KEY_PREFIX}{key}"


def _memory_tags(scope: str, key: str) -> _list[str]:
    """Build the storage tags for a memory artifact version."""
    return [
        _MEMORY_TAG_MARKER,
        _memory_scope_tag(scope),
        _memory_key_tag(key),
    ]


def _memory_metadata(*, scope_type: str, deleted: bool) -> dict[str, Any]:
    """Build metadata attached to each memory artifact version."""
    return {
        _MEMORY_SCOPE_TYPE_METADATA_KEY: scope_type,
        _MEMORY_DELETED_METADATA_KEY: deleted,
    }


def _parse_memory_artifact_identity(artifact_name: str) -> tuple[str, str]:
    """Parse ``kitaru_mem:<scope>:<key>`` into its scope/key parts."""
    prefix = f"{_MEMORY_ARTIFACT_PREFIX}:"
    if not artifact_name.startswith(prefix):
        raise KitaruRuntimeError(
            f"Memory artifact name {artifact_name!r} does not start with {prefix!r}."
        )

    remainder = artifact_name.removeprefix(prefix)
    try:
        scope, key = remainder.split(":", maxsplit=1)
    except ValueError as exc:
        raise KitaruRuntimeError(
            f"Memory artifact name {artifact_name!r} is not in "
            f"'{_MEMORY_ARTIFACT_PREFIX}:<scope>:<key>' format."
        ) from exc

    return (
        _validate_memory_identifier(
            scope,
            kind="scope",
            error_type=KitaruRuntimeError,
        ),
        _validate_memory_identifier(
            key,
            kind="key",
            error_type=KitaruRuntimeError,
        ),
    )


def _parse_memory_version(raw_version: str) -> int:
    """Convert a ZenML artifact version string into an integer version."""
    try:
        return int(raw_version)
    except (TypeError, ValueError) as exc:
        raise KitaruRuntimeError(
            f"Memory artifact version {raw_version!r} is not a valid integer version."
        ) from exc


def _is_deleted_artifact(artifact: ArtifactVersionResponse) -> bool:
    """Check whether a memory artifact version is a tombstone."""
    raw_deleted = artifact.run_metadata.get(_MEMORY_DELETED_METADATA_KEY, False)
    if isinstance(raw_deleted, str):
        return raw_deleted.strip().lower() == "true"
    return bool(raw_deleted)


def _resolve_scope_type(artifact: ArtifactVersionResponse) -> str:
    """Read the required scope-type metadata from a memory artifact version."""
    raw_scope_type = artifact.run_metadata.get(_MEMORY_SCOPE_TYPE_METADATA_KEY)
    if raw_scope_type is None:
        raise KitaruRuntimeError(
            f"Memory artifact {artifact.id} is missing required metadata "
            f"{_MEMORY_SCOPE_TYPE_METADATA_KEY!r}."
        )
    return str(raw_scope_type)


def _infer_value_type(artifact: ArtifactVersionResponse) -> str:
    """Infer a stable human-readable type label for a memory value."""
    import_path = getattr(artifact.data_type, "import_path", None)
    if isinstance(import_path, str) and import_path.strip():
        return import_path.rsplit(".", maxsplit=1)[-1]
    return "unknown"


def _artifact_to_memory_entry(artifact: ArtifactVersionResponse) -> MemoryEntry:
    """Convert a ZenML artifact version into a `MemoryEntry`."""
    scope, key = _parse_memory_artifact_identity(artifact.name)
    return MemoryEntry(
        key=key,
        value_type=_infer_value_type(artifact),
        version=_parse_memory_version(artifact.version),
        scope=scope,
        scope_type=_resolve_scope_type(artifact),
        created_at=artifact.created,
        is_deleted=_is_deleted_artifact(artifact),
        artifact_id=str(artifact.id),
        execution_id=(
            str(artifact.producer_pipeline_run_id)
            if artifact.producer_pipeline_run_id is not None
            else None
        ),
    )


def _sort_memory_artifacts(
    artifacts: _list[ArtifactVersionResponse],
) -> _list[ArtifactVersionResponse]:
    """Sort artifact versions newest-first with deterministic tie-breakers."""
    return sorted(
        artifacts,
        key=lambda artifact: (
            _parse_memory_version(artifact.version),
            artifact.created,
            str(artifact.id),
        ),
        reverse=True,
    )


def _paginate_artifact_versions(
    client: Client,
    **kwargs: Any,
) -> _list[ArtifactVersionResponse]:
    """Collect all artifact-version pages for a query."""
    page: Page[ArtifactVersionResponse] = client.list_artifact_versions(
        page=1,
        size=_MEMORY_PAGE_SIZE,
        hydrate=True,
        sort_by="version_number:desc",
        **kwargs,
    )
    items = [*page.items]
    while page.index < page.total_pages:
        page = client.list_artifact_versions(
            page=page.index + 1,
            size=_MEMORY_PAGE_SIZE,
            hydrate=True,
            sort_by="version_number:desc",
            **kwargs,
        )
        items.extend(page.items)
    return items


def _set_impl(scope: _MemoryScope, key: str, value: Any) -> None:
    """Persist a new version of a memory key inside a synthetic memory step."""
    try:
        save_artifact(
            data=value,
            name=_memory_artifact_name(scope.scope, key),
            artifact_type=ArtifactType.DATA,
            tags=_memory_tags(scope.scope, key),
            user_metadata=_memory_metadata(scope_type=scope.scope_type, deleted=False),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to set memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _get_impl(
    scope: _MemoryScope,
    key: str,
    version: int | None = None,
) -> Any | None:
    """Read a memory key inside a synthetic memory step."""
    try:
        client = Client()
        artifact_name = _memory_artifact_name(scope.scope, key)
        # Only the newest version is needed — fetch a single page of size 1
        # instead of paginating all versions.
        page: Page[ArtifactVersionResponse] = client.list_artifact_versions(
            artifact=artifact_name,
            version=version,
            page=1,
            size=1,
            hydrate=True,
            sort_by="version_number:desc",
        )
        artifacts = page.items
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to get memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    if not artifacts:
        return None

    selected = artifacts[0]
    if _is_deleted_artifact(selected):
        return None

    try:
        return selected.load()
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _list_impl(scope: _MemoryScope) -> _list[MemoryEntry]:
    """List the latest active memory entries inside a synthetic memory step."""
    try:
        client = Client()
        artifacts = _paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER, _memory_scope_tag(scope.scope)],
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to list memories in scope {scope.scope!r}: {exc}"
        ) from exc

    latest_by_artifact: dict[str, ArtifactVersionResponse] = {}
    for artifact in _sort_memory_artifacts(artifacts):
        latest_by_artifact.setdefault(artifact.name, artifact)

    entries = [
        _artifact_to_memory_entry(artifact)
        for artifact in latest_by_artifact.values()
        if not _is_deleted_artifact(artifact)
    ]
    return sorted(entries, key=lambda entry: entry.key)


def _history_impl(scope: _MemoryScope, key: str) -> _list[MemoryEntry]:
    """Return all versions of a memory key inside a synthetic memory step."""
    try:
        client = Client()
        artifacts = _paginate_artifact_versions(
            client,
            artifact=_memory_artifact_name(scope.scope, key),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            "Failed to fetch memory history for key "
            f"{key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    return [
        _artifact_to_memory_entry(artifact)
        for artifact in _sort_memory_artifacts(artifacts)
    ]


def _delete_impl(scope: _MemoryScope, key: str) -> MemoryEntry | None:
    """Soft-delete a memory key inside a synthetic memory step."""
    artifact_name = _memory_artifact_name(scope.scope, key)

    try:
        client = Client()
        # Check if the key exists and whether it's already deleted.
        page: Page[ArtifactVersionResponse] = client.list_artifact_versions(
            artifact=artifact_name,
            page=1,
            size=1,
            hydrate=True,
            sort_by="version_number:desc",
        )
        if not page.items:
            return None

        latest_current = page.items[0]
        if _is_deleted_artifact(latest_current):
            return _artifact_to_memory_entry(latest_current)

        save_artifact(
            data=None,
            name=artifact_name,
            artifact_type=ArtifactType.DATA,
            tags=_memory_tags(scope.scope, key),
            user_metadata=_memory_metadata(scope_type=scope.scope_type, deleted=True),
        )

        # Re-fetch the tombstone so the response includes producer linkage
        # that ZenML writes *after* create_artifact_version returns.
        tombstone_page: Page[ArtifactVersionResponse] = client.list_artifact_versions(
            artifact=artifact_name,
            page=1,
            size=1,
            hydrate=True,
            sort_by="version_number:desc",
        )
        if not tombstone_page.items:
            raise KitaruRuntimeError(
                f"Memory delete for key {key!r} in scope {scope.scope!r} "
                "did not produce a readable tombstone version."
            )
        return _artifact_to_memory_entry(tombstone_page.items[0])
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to delete memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _memory_step(*, name: str, operation: str):
    """Build a private synthetic ZenML step for one memory operation."""
    extra = {
        "kitaru": {
            **_MEMORY_STEP_EXTRA_PREFIX["kitaru"],
            "operation": operation,
        },
    }
    return step(
        name=name,
        enable_cache=False,
        step_type=StepType.TOOL_CALL,
        extra=extra,
    )


@_memory_step(name="kitaru_memory_set", operation="set")
def _memory_set_step(scope: str, scope_type: str, key: str, value: Any) -> None:
    """Synthetic non-cacheable step for `memory.set()`."""
    _set_impl(_coerce_memory_scope(scope, scope_type), key, value)


@_memory_step(name="kitaru_memory_get", operation="get")
def _memory_get_step(
    scope: str,
    scope_type: str,
    key: str,
    version: int | None = None,
) -> Any | None:
    """Synthetic non-cacheable step for `memory.get()`."""
    return _get_impl(_coerce_memory_scope(scope, scope_type), key, version)


@_memory_step(name="kitaru_memory_list", operation="list")
def _memory_list_step(scope: str, scope_type: str) -> _list[MemoryEntry]:
    """Synthetic non-cacheable step for `memory.list()`."""
    return _list_impl(_coerce_memory_scope(scope, scope_type))


@_memory_step(name="kitaru_memory_history", operation="history")
def _memory_history_step(scope: str, scope_type: str, key: str) -> _list[MemoryEntry]:
    """Synthetic non-cacheable step for `memory.history()`."""
    return _history_impl(_coerce_memory_scope(scope, scope_type), key)


@_memory_step(name="kitaru_memory_delete", operation="delete")
def _memory_delete_step(
    scope: str,
    scope_type: str,
    key: str,
) -> MemoryEntry | None:
    """Synthetic non-cacheable step for `memory.delete()`."""
    return _delete_impl(_coerce_memory_scope(scope, scope_type), key)


def configure(
    scope: str | None = None,
    *,
    scope_type: _MemoryScopeType | None = None,
) -> None:
    """Configure the active memory scope for subsequent flow-body operations."""
    global _RUNTIME_MEMORY_SCOPE_DEFAULT

    _require_memory_boundary("configure")
    configured_scope = _resolve_configured_scope(scope, scope_type=scope_type)

    if _is_inside_flow():
        _CURRENT_MEMORY_SCOPE.set(configured_scope)
    else:
        _RUNTIME_MEMORY_SCOPE_DEFAULT = configured_scope

    return None


def set(key: str, value: Any) -> None:
    """Persist a new version of a memory key in the active scope."""
    scope = _resolve_memory_scope_for_operation("set")
    normalized_key = _validate_memory_identifier(key, kind="key")
    _memory_set_step(scope.scope, scope.scope_type, normalized_key, value)
    return None


def get(key: str, *, version: int | None = None) -> Any | None:
    """Return the current value for a memory key in the active scope."""
    scope = _resolve_memory_scope_for_operation("get")
    normalized_key = _validate_memory_identifier(key, kind="key")
    if version is not None and version < 1:
        raise KitaruUsageError("Memory version must be >= 1.")
    return _memory_get_step(scope.scope, scope.scope_type, normalized_key, version)


def list() -> _list[MemoryEntry]:
    """List the latest active memory entries for the active scope."""
    scope = _resolve_memory_scope_for_operation("list")
    return _memory_list_step(scope.scope, scope.scope_type)


def history(key: str) -> _list[MemoryEntry]:
    """Return all versions of a memory key, including tombstones."""
    scope = _resolve_memory_scope_for_operation("history")
    normalized_key = _validate_memory_identifier(key, kind="key")
    return _memory_history_step(scope.scope, scope.scope_type, normalized_key)


def delete(key: str) -> MemoryEntry | None:
    """Soft-delete a memory key by writing a tombstone version."""
    scope = _resolve_memory_scope_for_operation("delete")
    normalized_key = _validate_memory_identifier(key, kind="key")
    return _memory_delete_step(scope.scope, scope.scope_type, normalized_key)


__all__ = ["MemoryEntry", "configure", "delete", "get", "history", "list", "set"]
