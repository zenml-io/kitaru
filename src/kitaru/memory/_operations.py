"""CRUD operations for Kitaru memory."""

import warnings
from collections.abc import Callable
from typing import Any

from zenml.client import Client
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import (
    KitaruBackendError,
    KitaruError,
    KitaruMemoryArtifactUnavailableError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.memory import _artifacts as artifact_store
from kitaru.memory._constants import (
    _MEMORY_SCOPE_TYPE_SORT_ORDER,
    _MEMORY_TAG_MARKER,
    _list,
)
from kitaru.memory._models import (
    MemoryEntry,
    MemoryScopeInfo,
    MemoryScopeType,
    _ExecutionFlowContext,
    _MemoryScope,
)
from kitaru.memory._scope import _validate_memory_scope_type
from kitaru.runtime import _is_inside_flow


def _track_memory_event(
    event_name: AnalyticsEvent,
    *,
    scope: _MemoryScope,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Emit one semantic memory analytics event with shared low-risk metadata."""
    base_metadata: dict[str, Any] = {
        "inside_flow": _is_inside_flow(),
        "scope_type": scope.scope_type,
    }
    if metadata is not None:
        base_metadata.update(
            {key: value for key, value in metadata.items() if value is not None}
        )
    track(event_name, base_metadata)


def _get_entry_impl(
    scope: _MemoryScope,
    key: str,
    version: int | None = None,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> MemoryEntry | None:
    """Return the selected memory entry metadata for a scope/key/version."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        selected = artifact_store._fetch_memory_artifact(
            client,
            scope,
            key,
            version,
            project=project,
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to get memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    if selected is None or artifact_store._is_deleted_artifact(selected):
        return None
    return artifact_store._artifact_to_memory_entry(selected)


def _set_entry_impl(
    scope: _MemoryScope,
    key: str,
    value: Any,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> MemoryEntry:
    """Persist a new version of a memory key and return its metadata entry."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        latest_current = artifact_store._fetch_memory_artifact(
            client,
            scope,
            key,
            project=project,
        )
        resolved_scope_type = scope.scope_type
        if latest_current is not None:
            existing_scope_type = _validate_memory_scope_type(
                artifact_store._resolve_scope_type(latest_current),
                error_type=KitaruRuntimeError,
            )
            if existing_scope_type != scope.scope_type:
                raise KitaruUsageError(
                    "Memory scope_type mismatch for existing key "
                    f"{key!r} in scope {scope.scope!r}: existing history uses "
                    f"{existing_scope_type!r}, but this write requested "
                    f"{scope.scope_type!r}."
                )
            resolved_scope_type = existing_scope_type

        flow_context: _ExecutionFlowContext | None = None
        if resolved_scope_type == "execution":
            flow_context = artifact_store._resolve_execution_flow_context(
                client,
                scope=scope,
                project=project,
            )
        elif resolved_scope_type == "flow":
            flow_context = artifact_store._resolve_active_flow_scope_context(scope)

        created = artifact_store._save_memory_artifact(
            client=client,
            scope=scope,
            key=key,
            value=value,
            deleted=False,
            scope_type=resolved_scope_type,
            project=project,
            flow_context=flow_context,
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to set memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    return artifact_store._artifact_to_memory_entry(created)


def _set_impl(scope: _MemoryScope, key: str, value: Any) -> None:
    """Persist a new version of a memory key for the resolved scope."""
    entry = _set_entry_impl(scope, key, value)
    _track_memory_event(
        AnalyticsEvent.MEMORY_WRITTEN,
        scope=scope,
        metadata={
            "value_type": entry.value_type,
            "execution_flow_indexed": (
                entry.flow_id is not None if scope.scope_type == "execution" else False
            ),
        },
    )


def _memory_artifact_unavailable_message(
    *,
    key: str,
    scope_name: str,
    scope_type: str,
    artifact_id: str,
    cause: Exception,
) -> str:
    """Render the one-true message for an unreachable memory artifact value.

    Shared by the SDK read path and the interface helper so strict-mode
    error text, lenient-mode warnings, and CLI/MCP payload diagnostics
    all agree verbatim.
    """
    return (
        f"Memory key {key!r} in scope {scope_name!r} ({scope_type}) "
        f"points to artifact {artifact_id!r}, but the artifact value could "
        f"not be loaded from this environment: {type(cause).__name__}: {cause}"
    )


def _get_impl(
    scope: _MemoryScope,
    key: str,
    version: int | None = None,
    *,
    strict: bool = False,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> Any | None:
    """Read a memory key for the resolved scope. See ``memory.get`` for semantics."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        selected = artifact_store._fetch_memory_artifact(
            client,
            scope,
            key,
            version,
            project=project,
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to get memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    if selected is None or artifact_store._is_deleted_artifact(selected):
        return None

    try:
        return selected.load()
    except Exception as exc:
        message = _memory_artifact_unavailable_message(
            key=key,
            scope_name=scope.scope,
            scope_type=scope.scope_type,
            artifact_id=str(selected.id),
            cause=exc,
        )
        if strict:
            raise KitaruMemoryArtifactUnavailableError(message) from exc
        # stacklevel=3 surfaces the warning at the public memory.get(...)
        # caller: _get_impl -> get() wrapper -> user code.
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        return None


def _list_impl(
    scope: _MemoryScope,
    *,
    prefix: str | None = None,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> _list[MemoryEntry]:
    """List the latest active memory entries for the resolved scope."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        artifacts = artifact_store._paginate_artifact_versions(
            client,
            tags=[
                _MEMORY_TAG_MARKER,
                artifact_store._memory_scope_tag(scope.scope),
                artifact_store._memory_scope_type_tag(scope.scope_type),
            ],
            **artifact_store._memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to list memories in scope {scope.scope!r}: {exc}"
        ) from exc

    latest_by_artifact: dict[str, MemoryEntry] = {}
    for _artifact, entry in artifact_store._iter_matching_memory_artifacts(
        artifact_store._sort_memory_artifacts(artifacts),
        scope=scope,
    ):
        latest_by_artifact.setdefault(entry.key, entry)

    entries = [entry for entry in latest_by_artifact.values() if not entry.is_deleted]
    if prefix is not None:
        entries = [entry for entry in entries if entry.key.startswith(prefix)]
    return sorted(entries, key=lambda entry: entry.key)


def _list_scopes_impl(
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> _list[MemoryScopeInfo]:
    """Discover all memory scopes with entry counts."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        artifacts = artifact_store._paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER],
            **artifact_store._memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(f"Failed to discover memory scopes: {exc}") from exc

    # Group the latest version per artifact name, then aggregate by scope.
    latest_by_artifact: dict[str, ArtifactVersionResponse] = {}
    for artifact in artifact_store._sort_memory_artifacts(artifacts):
        latest_by_artifact.setdefault(artifact.name, artifact)

    scope_stats: dict[tuple[str, MemoryScopeType], int] = {}
    for _artifact, entry in artifact_store._iter_matching_memory_artifacts(
        [*latest_by_artifact.values()],
    ):
        if entry.is_deleted:
            continue
        identity = (entry.scope, entry.scope_type)
        scope_stats[identity] = scope_stats.get(identity, 0) + 1

    return sorted(
        [
            MemoryScopeInfo(scope=scope, scope_type=scope_type, entry_count=count)
            for (scope, scope_type), count in scope_stats.items()
        ],
        key=lambda info: (
            info.scope,
            _MEMORY_SCOPE_TYPE_SORT_ORDER[info.scope_type],
        ),
    )


def _history_impl(
    scope: _MemoryScope,
    key: str,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> _list[MemoryEntry]:
    """Return all versions of a memory key for the resolved scope."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        artifacts = artifact_store._paginate_artifact_versions(
            client,
            artifact=artifact_store._memory_artifact_name(scope, key),
            **artifact_store._memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            "Failed to fetch memory history for key "
            f"{key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    return [
        entry
        for _artifact, entry in artifact_store._iter_matching_memory_artifacts(
            artifact_store._sort_memory_artifacts(artifacts),
            scope=scope,
        )
    ]


def _delete_impl(
    scope: _MemoryScope,
    key: str,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> MemoryEntry | None:
    """Soft-delete a memory key for the resolved scope."""
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        latest_current = artifact_store._fetch_memory_artifact(
            client,
            scope,
            key,
            project=project,
        )
        if latest_current is None:
            return None

        if artifact_store._is_deleted_artifact(latest_current):
            entry = artifact_store._artifact_to_memory_entry(latest_current)
            _track_memory_event(
                AnalyticsEvent.MEMORY_DELETED,
                scope=scope,
                metadata={"already_deleted": True},
            )
            return entry

        resolved_scope_type = _validate_memory_scope_type(
            artifact_store._resolve_scope_type(latest_current),
            error_type=KitaruRuntimeError,
        )
        flow_context: _ExecutionFlowContext | None = None
        if resolved_scope_type == "execution":
            flow_context = artifact_store._resolve_execution_flow_context(
                client,
                scope=scope,
                project=project,
            )
        elif resolved_scope_type == "flow":
            flow_context = artifact_store._resolve_active_flow_scope_context(scope)

        tombstone = artifact_store._save_memory_artifact(
            client=client,
            scope=scope,
            key=key,
            value=None,
            deleted=True,
            scope_type=resolved_scope_type,
            project=project,
            flow_context=flow_context,
        )
        entry = artifact_store._artifact_to_memory_entry(tombstone)
        _track_memory_event(
            AnalyticsEvent.MEMORY_DELETED,
            scope=scope,
            metadata={"already_deleted": False},
        )
        return entry
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to delete memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc
