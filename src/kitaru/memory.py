"""Flow-scoped memory primitives backed by ZenML artifact versions.

The public API is exposed as a module namespace:

    from kitaru import memory

    @flow
    def my_agent() -> None:
        memory.set("preferences", {"theme": "dark"})
        prefs = memory.get("preferences")

Phase BE-1 intentionally keeps memory narrow:
- allowed in the flow body
- forbidden inside ``@checkpoint``
- outside-flow support deferred to a later phase
"""

from __future__ import annotations

import re
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
from kitaru.runtime import _get_current_flow, _is_inside_checkpoint, _is_inside_flow

_MEMORY_ARTIFACT_PREFIX = "kitaru_mem"
_MEMORY_TAG_MARKER = "kitaru:memory"
_MEMORY_TAG_SCOPE_PREFIX = "kitaru:memory:scope:"
_MEMORY_TAG_KEY_PREFIX = "kitaru:memory:key:"
_MEMORY_SCOPE_TYPE_METADATA_KEY = "kitaru_memory_scope_type"
_MEMORY_DELETED_METADATA_KEY = "kitaru_memory_deleted"
_MEMORY_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._\-/]+$")
_MEMORY_PAGE_SIZE = 100
_MEMORY_STEP_EXTRA_PREFIX = {"kitaru": {"boundary": "memory"}}


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
    scope_type: Literal["flow"]


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


def _require_memory_flow_context(api_name: str) -> _MemoryScope:
    """Resolve the active flow-derived memory scope for a public API call."""
    qualified_name = f"kitaru.memory.{api_name}()"

    if _is_inside_checkpoint():
        raise KitaruContextError(
            f"{qualified_name} cannot be called inside a @checkpoint. "
            "Move memory operations to the flow body."
        )

    if not _is_inside_flow():
        raise KitaruContextError(
            f"{qualified_name} can only be called inside a @flow. "
            "Outside-flow memory support is not available yet."
        )

    flow_scope = _get_current_flow()
    if flow_scope is None or flow_scope.name is None:
        raise KitaruStateError(
            f"{qualified_name} requires an active flow name inside @flow."
        )

    return _MemoryScope(
        scope=_validate_memory_identifier(flow_scope.name, kind="scope"),
        scope_type="flow",
    )


def _coerce_memory_scope(scope: str, scope_type: str) -> _MemoryScope:
    """Reconstruct a validated memory scope inside synthetic memory steps."""
    normalized_scope = _validate_memory_identifier(
        scope,
        kind="scope",
        error_type=KitaruRuntimeError,
    )
    normalized_scope_type = str(scope_type).strip().lower()
    if normalized_scope_type != "flow":
        raise KitaruRuntimeError(
            "BE-1 synthetic memory steps only support flow-scoped memory, "
            f"got scope_type={scope_type!r}."
        )
    return _MemoryScope(
        scope=normalized_scope,
        scope_type=cast(Literal["flow"], normalized_scope_type),
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


def _memory_tags(scope: str, key: str) -> list[str]:
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

    try:
        return type(artifact.load()).__name__
    except Exception:
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
    artifacts: list[ArtifactVersionResponse],
) -> list[ArtifactVersionResponse]:
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
) -> list[ArtifactVersionResponse]:
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
            "Failed to set memory key "
            f"{key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _get_impl(
    scope: _MemoryScope,
    key: str,
    version: int | None = None,
) -> Any | None:
    """Read a memory key inside a synthetic memory step."""
    try:
        client = Client()
        artifacts = _paginate_artifact_versions(
            client,
            artifact=_memory_artifact_name(scope.scope, key),
            version=version,
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            "Failed to get memory key "
            f"{key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    if not artifacts:
        return None

    selected = _sort_memory_artifacts(artifacts)[0]
    if _is_deleted_artifact(selected):
        return None

    try:
        return selected.load()
    except Exception as exc:
        raise KitaruBackendError(
            "Failed to load memory key "
            f"{key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _list_impl(scope: _MemoryScope) -> list[MemoryEntry]:
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


def _history_impl(scope: _MemoryScope, key: str) -> list[MemoryEntry]:
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
        current_versions = _paginate_artifact_versions(client, artifact=artifact_name)
        if not current_versions:
            return None

        latest_current = _sort_memory_artifacts(current_versions)[0]
        if _is_deleted_artifact(latest_current):
            return _artifact_to_memory_entry(latest_current)

        save_artifact(
            data=None,
            name=artifact_name,
            artifact_type=ArtifactType.DATA,
            tags=_memory_tags(scope.scope, key),
            user_metadata=_memory_metadata(scope_type=scope.scope_type, deleted=True),
        )

        latest_versions = _paginate_artifact_versions(client, artifact=artifact_name)
        if not latest_versions:
            raise KitaruRuntimeError(
                f"Memory delete for key {key!r} in scope {scope.scope!r} "
                "did not produce a readable tombstone version."
            )
        return _artifact_to_memory_entry(_sort_memory_artifacts(latest_versions)[0])
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            "Failed to delete memory key "
            f"{key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _memory_step(*, name: str, operation: str):
    """Build a private synthetic ZenML step for one memory operation."""
    extra = {
        **_MEMORY_STEP_EXTRA_PREFIX,
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
def _memory_list_step(scope: str, scope_type: str) -> list[MemoryEntry]:
    """Synthetic non-cacheable step for `memory.list()`."""
    return _list_impl(_coerce_memory_scope(scope, scope_type))


@_memory_step(name="kitaru_memory_history", operation="history")
def _memory_history_step(scope: str, scope_type: str, key: str) -> list[MemoryEntry]:
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


def set(key: str, value: Any) -> None:
    """Persist a new version of a flow-scoped memory key."""
    scope = _require_memory_flow_context("set")
    normalized_key = _validate_memory_identifier(key, kind="key")
    _memory_set_step(scope.scope, scope.scope_type, normalized_key, value)
    return None


def get(key: str, *, version: int | None = None) -> Any | None:
    """Return the current value for a flow-scoped memory key, if present."""
    scope = _require_memory_flow_context("get")
    normalized_key = _validate_memory_identifier(key, kind="key")
    if version is not None and version < 1:
        raise KitaruUsageError("Memory version must be >= 1.")
    return _memory_get_step(scope.scope, scope.scope_type, normalized_key, version)


def list() -> list[MemoryEntry]:
    """List the latest active memory entries for the current flow scope."""
    scope = _require_memory_flow_context("list")
    return _memory_list_step(scope.scope, scope.scope_type)


def history(key: str) -> list[MemoryEntry]:
    """Return all versions of a memory key, including tombstones."""
    scope = _require_memory_flow_context("history")
    normalized_key = _validate_memory_identifier(key, kind="key")
    return _memory_history_step(scope.scope, scope.scope_type, normalized_key)


def delete(key: str) -> MemoryEntry | None:
    """Soft-delete a flow-scoped memory key by writing a tombstone version."""
    scope = _require_memory_flow_context("delete")
    normalized_key = _validate_memory_identifier(key, kind="key")
    return _memory_delete_step(scope.scope, scope.scope_type, normalized_key)


__all__ = ["MemoryEntry", "delete", "get", "history", "list", "set"]
