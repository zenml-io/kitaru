"""Artifact storage helpers for Kitaru memory."""

import builtins
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactType
from zenml.models.v2.base.page import Page
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.steps.step_context import StepContext

from kitaru._source_aliases import normalize_flow_name
from kitaru.errors import KitaruError, KitaruRuntimeError
from kitaru.memory._constants import (
    _MEMORY_ARTIFACT_PREFIX,
    _MEMORY_DELETED_METADATA_KEY,
    _MEMORY_FLOW_ID_METADATA_KEY,
    _MEMORY_FLOW_NAME_METADATA_KEY,
    _MEMORY_PAGE_SIZE,
    _MEMORY_SCOPE_TYPE_METADATA_KEY,
    _MEMORY_TAG_FLOW_ID_PREFIX,
    _MEMORY_TAG_KEY_PREFIX,
    _MEMORY_TAG_MARKER,
    _MEMORY_TAG_SCOPE_PREFIX,
    _MEMORY_TAG_SCOPE_TYPE_PREFIX,
    _MEMORY_VERSION_SORT,
    _list,
)
from kitaru.memory._models import (
    MemoryEntry,
    _ExecutionFlowContext,
    _MemoryScope,
    _MemoryScopeType,
)
from kitaru.memory._scope import (
    _validate_memory_identifier,
    _validate_memory_scope_type,
)
from kitaru.runtime import (
    _get_current_execution_id,
    _get_current_flow,
    _get_current_flow_id,
)

logger = logging.getLogger(__name__)


@contextmanager
def _temporary_active_project(
    client: Client,
    project: str | None,
) -> Iterator[None]:
    """Temporarily activate a project while performing a direct memory write."""
    if not project:
        yield
        return

    active_project = client.active_project
    active_project_id = str(active_project.id)
    if project in {active_project_id, active_project.name}:
        yield
        return

    client.set_active_project(project)
    try:
        yield
    finally:
        client.set_active_project(active_project_id)


def _memory_artifact_name(scope: _MemoryScope, key: str) -> str:
    """Build the canonical artifact name for a memory key."""
    return f"{_MEMORY_ARTIFACT_PREFIX}:{scope.scope_type}:{scope.scope}:{key}"


def _memory_scope_tag(scope: str) -> str:
    """Build the scope tag used for memory queries."""
    return f"{_MEMORY_TAG_SCOPE_PREFIX}{scope}"


def _memory_key_tag(key: str) -> str:
    """Build the key tag used for memory queries."""
    return f"{_MEMORY_TAG_KEY_PREFIX}{key}"


def _memory_scope_type_tag(scope_type: str) -> str:
    """Build the scope-type tag used for memory queries."""
    return f"{_MEMORY_TAG_SCOPE_TYPE_PREFIX}{scope_type}"


def _memory_flow_id_tag(flow_id: str) -> str:
    """Build the flow-id tag used for execution-scope memory queries."""
    return f"{_MEMORY_TAG_FLOW_ID_PREFIX}{flow_id}"


def _optional_metadata_string(value: object | None) -> str | None:
    """Coerce optional metadata into a stripped string."""
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _extract_flow_context_from_pipeline(
    pipeline: object | None,
) -> _ExecutionFlowContext | None:
    """Extract flow id/name from a ZenML pipeline object, if present."""
    flow_id = _optional_metadata_string(getattr(pipeline, "id", None))
    if flow_id is None:
        return None
    return _ExecutionFlowContext(
        flow_id=flow_id,
        flow_name=normalize_flow_name(getattr(pipeline, "name", None)),
    )


def _warn_flow_context_unresolved(
    scope: _MemoryScope,
    project: str | None,
    reason: str,
    active_context_reason: str | None = None,
) -> None:
    """Log a warning when flow-context resolution fails."""
    if active_context_reason is not None:
        reason = f"{active_context_reason}; {reason}"
    logger.warning(
        "Unable to resolve flow context for execution-scoped memory write "
        "in scope %r (project=%r): %s",
        scope.scope,
        project,
        reason,
    )


def _resolve_execution_flow_context(
    client: Client,
    *,
    scope: _MemoryScope,
    project: str | None = None,
) -> _ExecutionFlowContext | None:
    """Resolve logical flow membership for an execution-scoped write."""
    active_context_reason: str | None = None
    current_execution_id = _get_current_execution_id()

    if current_execution_id == scope.scope:
        if step_context := StepContext.get():
            pipeline = getattr(
                getattr(step_context, "pipeline_run", None), "pipeline", None
            )
            if ctx := _extract_flow_context_from_pipeline(pipeline):
                return ctx
            active_context_reason = "active step context did not expose a pipeline id"
        else:
            active_context_reason = (
                "active execution matched target scope but no StepContext was available"
            )

    try:
        run = client.get_pipeline_run(
            name_id_or_prefix=scope.scope,
            allow_name_prefix_match=False,
            hydrate=True,
            project=project,
        )
    except KitaruError:
        raise
    except Exception as exc:
        _warn_flow_context_unresolved(
            scope,
            project,
            f"failed to resolve execution run {scope.scope!r}: {exc}",
            active_context_reason,
        )
        return None

    if ctx := _extract_flow_context_from_pipeline(getattr(run, "pipeline", None)):
        return ctx

    _warn_flow_context_unresolved(
        scope,
        project,
        "resolved execution run did not expose a pipeline id",
        active_context_reason,
    )
    return None


def _resolve_active_flow_scope_context(
    scope: _MemoryScope,
) -> _ExecutionFlowContext | None:
    """Resolve current runtime flow metadata for a flow-scoped write."""
    if scope.scope_type != "flow":
        return None

    flow_scope = _get_current_flow()
    resolved_flow_id = _get_current_flow_id()
    if flow_scope is None or resolved_flow_id is None:
        return None
    if resolved_flow_id != scope.scope:
        return None

    return _ExecutionFlowContext(
        flow_id=resolved_flow_id,
        flow_name=normalize_flow_name(flow_scope.name),
    )


def _memory_tags(
    scope: str,
    key: str,
    *,
    scope_type: _MemoryScopeType,
    flow_context: _ExecutionFlowContext | None = None,
) -> _list[str]:
    """Build the storage tags for a memory artifact version."""
    tags = [
        _MEMORY_TAG_MARKER,
        _memory_scope_tag(scope),
        _memory_key_tag(key),
        _memory_scope_type_tag(scope_type),
    ]
    # Flow-ID tag is only added for execution-scoped entries as a cross-reference
    # back to the parent flow.  For flow-scoped entries the scope itself *is*
    # the flow ID (encoded in the kitaru:memory:scope:<id> tag), so a separate
    # flow_id tag would be redundant.  Metadata still records flow_id/flow_name
    # unconditionally for auditability.
    if scope_type == "execution" and flow_context is not None:
        tags.append(_memory_flow_id_tag(flow_context.flow_id))
    return tags


def _memory_metadata(
    *,
    scope_type: _MemoryScopeType,
    deleted: bool,
    flow_context: _ExecutionFlowContext | None = None,
) -> dict[str, Any]:
    """Build metadata attached to each memory artifact version."""
    metadata: dict[str, Any] = {
        _MEMORY_SCOPE_TYPE_METADATA_KEY: scope_type,
        _MEMORY_DELETED_METADATA_KEY: deleted,
    }
    if flow_context is not None:
        metadata[_MEMORY_FLOW_ID_METADATA_KEY] = flow_context.flow_id
        if flow_context.flow_name is not None:
            metadata[_MEMORY_FLOW_NAME_METADATA_KEY] = flow_context.flow_name
    return metadata


def _parse_memory_artifact_identity(artifact_name: str) -> tuple[_MemoryScope, str]:
    """Parse ``kitaru_mem:<scope_type>:<scope>:<key>`` into its parts."""
    prefix = f"{_MEMORY_ARTIFACT_PREFIX}:"
    if not artifact_name.startswith(prefix):
        raise KitaruRuntimeError(
            f"Memory artifact name {artifact_name!r} does not start with {prefix!r}."
        )

    remainder = artifact_name.removeprefix(prefix)
    try:
        scope_type, scope, key = remainder.split(":", maxsplit=2)
    except ValueError as exc:
        raise KitaruRuntimeError(
            f"Memory artifact name {artifact_name!r} is not in "
            f"'{_MEMORY_ARTIFACT_PREFIX}:<scope_type>:<scope>:<key>' format."
        ) from exc

    return (
        _MemoryScope(
            scope=_validate_memory_identifier(
                scope,
                kind="scope",
                error_type=KitaruRuntimeError,
            ),
            scope_type=_validate_memory_scope_type(
                scope_type,
                error_type=KitaruRuntimeError,
            ),
        ),
        _validate_memory_identifier(
            key,
            kind="key",
            error_type=KitaruRuntimeError,
            _allow_compaction_prefix=True,
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
    parsed_scope, key = _parse_memory_artifact_identity(artifact.name)
    metadata_scope_type = _validate_memory_scope_type(
        _resolve_scope_type(artifact),
        error_type=KitaruRuntimeError,
    )
    if parsed_scope.scope_type != metadata_scope_type:
        raise KitaruRuntimeError(
            "Memory artifact identity mismatch for "
            f"{artifact.name!r}: artifact name encodes scope_type "
            f"{parsed_scope.scope_type!r}, but metadata encodes "
            f"{metadata_scope_type!r}."
        )
    flow_id = _optional_metadata_string(
        artifact.run_metadata.get(_MEMORY_FLOW_ID_METADATA_KEY)
    )
    flow_name = normalize_flow_name(
        artifact.run_metadata.get(_MEMORY_FLOW_NAME_METADATA_KEY)
    )
    return MemoryEntry(
        key=key,
        value_type=_infer_value_type(artifact),
        version=_parse_memory_version(artifact.version),
        scope=parsed_scope.scope,
        scope_type=metadata_scope_type,
        created_at=artifact.created,
        is_deleted=_is_deleted_artifact(artifact),
        artifact_id=str(artifact.id),
        execution_id=(
            str(artifact.producer_pipeline_run_id)
            if artifact.producer_pipeline_run_id is not None
            else None
        ),
        flow_id=flow_id,
        flow_name=flow_name,
    )


def _artifact_tag_names(artifact: ArtifactVersionResponse) -> set[str]:
    """Normalize artifact-version tags into a comparable name set."""
    tag_names: builtins.set[str] = builtins.set()
    for raw_tag in getattr(artifact, "tags", []) or []:
        if isinstance(raw_tag, str):
            normalized = raw_tag.strip()
        else:
            normalized = _optional_metadata_string(getattr(raw_tag, "name", None))
        if normalized:
            tag_names.add(normalized)
    return tag_names


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
    *,
    hydrate: bool = True,
    **kwargs: Any,
) -> _list[ArtifactVersionResponse]:
    """Collect all artifact-version pages for a query."""
    page: Page[ArtifactVersionResponse] = client.list_artifact_versions(
        page=1,
        size=_MEMORY_PAGE_SIZE,
        hydrate=hydrate,
        sort_by=_MEMORY_VERSION_SORT,
        **kwargs,
    )
    items = [*page.items]
    while page.index < page.total_pages:
        page = client.list_artifact_versions(
            page=page.index + 1,
            size=_MEMORY_PAGE_SIZE,
            hydrate=hydrate,
            sort_by=_MEMORY_VERSION_SORT,
            **kwargs,
        )
        items.extend(page.items)
    return items


def _memory_query_kwargs(
    *,
    project: str | None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Attach optional project scoping to a ZenML memory query."""
    if project is not None:
        kwargs["project"] = project
    return kwargs


def _iter_matching_memory_artifacts(
    artifacts: _list[ArtifactVersionResponse],
    *,
    scope: _MemoryScope | None = None,
) -> Iterator[tuple[ArtifactVersionResponse, MemoryEntry]]:
    """Yield well-formed memory artifacts with their parsed entries."""
    for artifact in artifacts:
        try:
            entry = _artifact_to_memory_entry(artifact)
        except KitaruRuntimeError:
            logger.debug(
                "Skipping unparsable memory artifact %s: %s",
                artifact.name,
                artifact.id,
            )
            continue
        parsed_scope = _MemoryScope(
            scope=entry.scope,
            scope_type=entry.scope_type,
        )
        if scope is not None and parsed_scope != scope:
            continue
        yield artifact, entry


def _fetch_memory_artifact(
    client: Client,
    scope: _MemoryScope,
    key: str,
    version: int | None = None,
    *,
    project: str | None = None,
) -> ArtifactVersionResponse | None:
    """Fetch one memory artifact version for a scope/key/version query."""
    page: Page[ArtifactVersionResponse] = client.list_artifact_versions(
        **_memory_query_kwargs(
            project=project,
            artifact=_memory_artifact_name(scope, key),
            version=version,
            page=1,
            size=1,
            hydrate=True,
            sort_by=_MEMORY_VERSION_SORT,
        )
    )
    if not page.items:
        return None
    return page.items[0]


def _fetch_exact_artifact_version(
    client: Client,
    artifact_id: str,
    *,
    project: str | None = None,
) -> ArtifactVersionResponse:
    """Re-fetch one artifact version by exact ID after a write."""
    try:
        return client.get_artifact_version(
            name_id_or_prefix=artifact_id,
            hydrate=True,
            **_memory_query_kwargs(project=project),
        )
    except Exception as exc:
        raise KitaruRuntimeError(
            "Memory write succeeded but the created artifact version could not "
            f"be reloaded by exact ID {artifact_id!r}: {exc}"
        ) from exc


def _save_memory_artifact(
    *,
    client: Client,
    scope: _MemoryScope,
    key: str,
    value: Any,
    deleted: bool,
    scope_type: _MemoryScopeType,
    project: str | None = None,
    flow_context: _ExecutionFlowContext | None = None,
) -> ArtifactVersionResponse:
    """Persist a memory artifact version and reload the exact created version."""

    with _temporary_active_project(client, project):
        created = save_artifact(
            data=value,
            name=_memory_artifact_name(scope, key),
            artifact_type=ArtifactType.DATA,
            tags=_memory_tags(
                scope.scope,
                key,
                scope_type=scope_type,
                flow_context=flow_context,
            ),
            user_metadata=_memory_metadata(
                scope_type=scope_type,
                deleted=deleted,
                flow_context=flow_context,
            ),
        )
    return _fetch_exact_artifact_version(
        client,
        str(created.id),
        project=project,
    )


def _resolve_memory_client_factory(
    client_factory: Callable[[], Client] | None,
) -> Callable[[], Client]:
    """Resolve an optional client factory lazily for test patchability."""
    return Client if client_factory is None else client_factory
