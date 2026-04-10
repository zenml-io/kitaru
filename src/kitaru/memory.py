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
- outside-flow reads/writes supported after ``memory.configure(scope=...)``
"""

import builtins
import logging
import re
from collections.abc import Callable, Iterator
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
from zenml.steps.step_context import StepContext
from zenml.steps.step_decorator import step

from kitaru._source_aliases import normalize_flow_name
from kitaru.analytics import AnalyticsEvent, track
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
_MEMORY_TAG_SCOPE_TYPE_PREFIX = "kitaru:memory:scope_type:"
_MEMORY_TAG_FLOW_ID_PREFIX = "kitaru:memory:flow_id:"
_MEMORY_SCOPE_TYPE_METADATA_KEY = "kitaru_memory_scope_type"
_MEMORY_DELETED_METADATA_KEY = "kitaru_memory_deleted"
_MEMORY_FLOW_ID_METADATA_KEY = "kitaru_memory_flow_id"
_MEMORY_FLOW_NAME_METADATA_KEY = "kitaru_memory_flow_name"
_MEMORY_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._\-/]+$")
_MEMORY_PAGE_SIZE = 100
_MEMORY_VERSION_SORT = "desc:version_number"
_MEMORY_STEP_EXTRA_PREFIX = {"kitaru": {"boundary": "memory"}}
_MemoryScopeType = Literal["namespace", "flow", "execution"]
_MemoryCompactionSourceMode = Literal["current", "history"]


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
    flow_id: str | None = None
    flow_name: str | None = None

    model_config = ConfigDict(frozen=True)


class MemoryScopeInfo(BaseModel):
    """Summary of one discovered memory scope."""

    scope: str
    scope_type: str
    entry_count: int

    model_config = ConfigDict(frozen=True)


class PurgeResult(BaseModel):
    """Result of a memory purge operation."""

    versions_deleted: int
    keys_affected: int
    scope: str

    model_config = ConfigDict(frozen=True)


class CompactionRecord(BaseModel):
    """Audit log entry for one compaction or purge operation."""

    operation: Literal["compact", "purge"]
    scope: str
    timestamp: datetime
    source_keys: _list[str]
    source_versions: _list[int]
    target_key: str | None
    target_version: int | None
    instruction: str | None
    model: str | None
    source_mode: _MemoryCompactionSourceMode | None = None
    keys_affected: int
    versions_deleted: int
    keep: int | None

    model_config = ConfigDict(frozen=True)


class CompactResult(BaseModel):
    """Result of an LLM-powered memory compaction."""

    entry: MemoryEntry
    sources_read: int
    scope: str
    compaction_record: CompactionRecord

    model_config = ConfigDict(frozen=True)


class MemoryReindexIssue(BaseModel):
    """One non-fatal issue encountered while reindexing memory versions."""

    artifact_id: str
    artifact_name: str
    scope: str | None
    key: str | None
    reason: str

    model_config = ConfigDict(frozen=True)


class MemoryReindexResult(BaseModel):
    """Summary of one memory reindex/backfill operation."""

    dry_run: bool
    versions_scanned: int
    execution_scope_versions_scanned: int
    already_indexed: int
    versions_needing_updates: int
    versions_updated: int
    scope_type_tags_identified: int
    flow_tags_identified: int
    scope_type_tags_added: int
    flow_tags_added: int
    issues_count: int
    issue_samples: _list[MemoryReindexIssue]

    model_config = ConfigDict(frozen=True)


_COMPACTION_LOG_PREFIX = "_compaction/"
_MEMORY_REINDEX_ISSUE_SAMPLE_LIMIT = 10


@dataclass
class _ReindexCounters:
    """Mutable accumulator for reindex statistics."""

    versions_scanned: int = 0
    execution_scope_versions_scanned: int = 0
    already_indexed: int = 0
    versions_needing_updates: int = 0
    versions_updated: int = 0
    scope_type_tags_identified: int = 0
    flow_tags_identified: int = 0
    scope_type_tags_added: int = 0
    flow_tags_added: int = 0
    issues_count: int = 0


@dataclass(frozen=True)
class _MemoryScope:
    """Resolved or configured memory scope."""

    scope: str
    scope_type: _MemoryScopeType


@dataclass(frozen=True)
class _ExecutionFlowContext:
    """Resolved logical flow context for an execution-scoped memory write."""

    flow_id: str
    flow_name: str | None = None


_RUNTIME_MEMORY_SCOPE_DEFAULT: _MemoryScope | None = None
_CURRENT_MEMORY_SCOPE: ContextVar[_MemoryScope | None] = ContextVar(
    "kitaru_current_memory_scope",
    default=None,
)

logger = logging.getLogger(__name__)


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
    qualified_name = f"kitaru.memory.{api_name}()"
    _require_memory_boundary(api_name)

    if _is_inside_flow():
        configured_scope = _CURRENT_MEMORY_SCOPE.get()
        if configured_scope is not None:
            return configured_scope
        return _implicit_flow_memory_scope(api_name)

    if _RUNTIME_MEMORY_SCOPE_DEFAULT is not None:
        return _RUNTIME_MEMORY_SCOPE_DEFAULT

    raise KitaruStateError(
        f"{qualified_name} outside a @flow requires an explicit scope. "
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
    if flow_context is not None:
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
    scope, key = _parse_memory_artifact_identity(artifact.name)
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
            artifact=_memory_artifact_name(scope.scope, key),
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
            name=_memory_artifact_name(scope.scope, key),
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


def _record_reindex_issue(
    issue_samples: _list[MemoryReindexIssue],
    *,
    artifact_id: str,
    artifact_name: str,
    scope: str | None,
    key: str | None,
    reason: str,
) -> None:
    """Append one sampled reindex issue if the sample budget allows."""
    if len(issue_samples) >= _MEMORY_REINDEX_ISSUE_SAMPLE_LIMIT:
        return
    issue_samples.append(
        MemoryReindexIssue(
            artifact_id=artifact_id,
            artifact_name=artifact_name,
            scope=scope,
            key=key,
            reason=reason,
        )
    )


def _lookup_reindex_flow_context(
    client: Client,
    *,
    run_identifier: str,
    project: str | None = None,
) -> tuple[_ExecutionFlowContext | None, str | None]:
    """Resolve flow context for one run identifier used during reindexing."""
    try:
        run = client.get_pipeline_run(
            name_id_or_prefix=run_identifier,
            allow_name_prefix_match=False,
            hydrate=True,
            project=project,
        )
    except KitaruError:
        raise
    except Exception as exc:
        return None, f"lookup failed: {exc}"

    flow_context = _extract_flow_context_from_pipeline(getattr(run, "pipeline", None))
    if flow_context is None:
        return None, "resolved run did not expose a pipeline id"
    return flow_context, None


def _resolve_reindex_flow_context(
    client: Client,
    *,
    producer_run_id: str | None,
    scope: str,
    project: str | None,
    producer_run_cache: dict[str, tuple[_ExecutionFlowContext | None, str | None]],
    execution_scope_cache: dict[str, tuple[_ExecutionFlowContext | None, str | None]],
) -> tuple[_ExecutionFlowContext | None, str]:
    """Resolve flow context for historical execution-scope memory."""
    reasons: _list[str] = []

    if producer_run_id is not None:
        cached = producer_run_cache.get(producer_run_id)
        if cached is None:
            cached = _lookup_reindex_flow_context(
                client,
                run_identifier=producer_run_id,
                project=project,
            )
            producer_run_cache[producer_run_id] = cached
        flow_context, reason = cached
        if flow_context is not None:
            return flow_context, ""
        if reason is not None:
            reasons.append(f"producer run {producer_run_id!r}: {reason}")
        # Skip redundant scope lookup when both identifiers are the same run.
        if producer_run_id == scope:
            return None, reasons[0] if reasons else "producer run matches scope"

    cached_scope = execution_scope_cache.get(scope)
    if cached_scope is None:
        cached_scope = _lookup_reindex_flow_context(
            client,
            run_identifier=scope,
            project=project,
        )
        execution_scope_cache[scope] = cached_scope
    flow_context, reason = cached_scope
    if flow_context is not None:
        return flow_context, ""
    if reason is not None:
        reasons.append(f"execution scope {scope!r}: {reason}")

    if not reasons:
        reasons.append(
            "could not resolve flow context from producer run or execution scope"
        )
    return None, "; ".join(reasons)


def _reindex_impl(
    *,
    dry_run: bool = True,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> MemoryReindexResult:
    """Backfill missing memory indexing tags on historical artifact versions."""
    try:
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER],
            **_memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to list memory artifacts for reindexing: {exc}"
        ) from exc

    producer_run_cache: dict[str, tuple[_ExecutionFlowContext | None, str | None]] = {}
    execution_scope_cache: dict[
        str, tuple[_ExecutionFlowContext | None, str | None]
    ] = {}
    issue_samples: _list[MemoryReindexIssue] = []
    counts = _ReindexCounters()

    for artifact in artifacts:
        counts.versions_scanned += 1
        artifact_id = str(artifact.id)
        artifact_name = artifact.name
        scope: str | None = None
        key: str | None = None
        issue_recorded = False

        try:
            scope, key = _parse_memory_artifact_identity(artifact_name)
            scope_type = _validate_memory_scope_type(
                _resolve_scope_type(artifact),
                error_type=KitaruRuntimeError,
            )
        except Exception as exc:
            counts.issues_count += 1
            issue_recorded = True
            _record_reindex_issue(
                issue_samples,
                artifact_id=artifact_id,
                artifact_name=artifact_name,
                scope=scope,
                key=key,
                reason=str(exc),
            )
            continue

        tag_names = _artifact_tag_names(artifact)
        add_tags: _list[str] = []
        added_scope_type_tag = False
        added_flow_tag = False

        scope_type_tag = _memory_scope_type_tag(scope_type)
        if scope_type_tag not in tag_names:
            add_tags.append(scope_type_tag)
            added_scope_type_tag = True
            counts.scope_type_tags_identified += 1

        if scope_type == "execution":
            counts.execution_scope_versions_scanned += 1
            has_flow_tag = any(
                tag_name.startswith(_MEMORY_TAG_FLOW_ID_PREFIX)
                for tag_name in tag_names
            )
            if not has_flow_tag:
                producer_run_id = _optional_metadata_string(
                    artifact.producer_pipeline_run_id
                )
                flow_context, reason = _resolve_reindex_flow_context(
                    client,
                    producer_run_id=producer_run_id,
                    scope=scope,
                    project=project,
                    producer_run_cache=producer_run_cache,
                    execution_scope_cache=execution_scope_cache,
                )
                if flow_context is not None:
                    add_tags.append(_memory_flow_id_tag(flow_context.flow_id))
                    added_flow_tag = True
                    counts.flow_tags_identified += 1
                else:
                    counts.issues_count += 1
                    issue_recorded = True
                    _record_reindex_issue(
                        issue_samples,
                        artifact_id=artifact_id,
                        artifact_name=artifact_name,
                        scope=scope,
                        key=key,
                        reason=reason,
                    )

        if add_tags:
            counts.versions_needing_updates += 1
        elif not issue_recorded:
            counts.already_indexed += 1

        if not add_tags or dry_run:
            continue

        try:
            client.update_artifact_version(
                name_id_or_prefix=artifact_id,
                add_tags=add_tags,
                **_memory_query_kwargs(project=project),
            )
        except KitaruError:
            raise
        except Exception as exc:
            counts.issues_count += 1
            _record_reindex_issue(
                issue_samples,
                artifact_id=artifact_id,
                artifact_name=artifact_name,
                scope=scope,
                key=key,
                reason=f"failed to add tags {add_tags!r}: {exc}",
            )
            continue

        counts.versions_updated += 1
        if added_scope_type_tag:
            counts.scope_type_tags_added += 1
        if added_flow_tag:
            counts.flow_tags_added += 1

    result = MemoryReindexResult(
        dry_run=dry_run,
        versions_scanned=counts.versions_scanned,
        execution_scope_versions_scanned=counts.execution_scope_versions_scanned,
        already_indexed=counts.already_indexed,
        versions_needing_updates=counts.versions_needing_updates,
        versions_updated=counts.versions_updated,
        scope_type_tags_identified=counts.scope_type_tags_identified,
        flow_tags_identified=counts.flow_tags_identified,
        scope_type_tags_added=counts.scope_type_tags_added,
        flow_tags_added=counts.flow_tags_added,
        issues_count=counts.issues_count,
        issue_samples=issue_samples,
    )
    # Reindex is a global operation without a per-scope context, so it
    # calls track() directly instead of _track_memory_event().
    track(
        AnalyticsEvent.MEMORY_REINDEX_RUN,
        {
            "inside_flow": _is_inside_flow(),
            "dry_run": result.dry_run,
            "versions_scanned": result.versions_scanned,
            "versions_updated": result.versions_updated,
            "issues_count": result.issues_count,
        },
    )
    return result


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
        client = _resolve_memory_client_factory(client_factory)()
        selected = _fetch_memory_artifact(
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

    if selected is None or _is_deleted_artifact(selected):
        return None
    return _artifact_to_memory_entry(selected)


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
        client = _resolve_memory_client_factory(client_factory)()
        latest_current = _fetch_memory_artifact(
            client,
            scope,
            key,
            project=project,
        )
        resolved_scope_type = scope.scope_type
        if latest_current is not None:
            existing_scope_type = _validate_memory_scope_type(
                _resolve_scope_type(latest_current),
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
            flow_context = _resolve_execution_flow_context(
                client,
                scope=scope,
                project=project,
            )

        created = _save_memory_artifact(
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

    return _artifact_to_memory_entry(created)


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


def _get_impl(
    scope: _MemoryScope,
    key: str,
    version: int | None = None,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> Any | None:
    """Read a memory key for the resolved scope."""
    try:
        client = _resolve_memory_client_factory(client_factory)()
        selected = _fetch_memory_artifact(
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

    if selected is None or _is_deleted_artifact(selected):
        return None

    try:
        return selected.load()
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc


def _list_impl(
    scope: _MemoryScope,
    *,
    prefix: str | None = None,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> _list[MemoryEntry]:
    """List the latest active memory entries for the resolved scope."""
    try:
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER, _memory_scope_tag(scope.scope)],
            **_memory_query_kwargs(project=project),
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
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER],
            **_memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(f"Failed to discover memory scopes: {exc}") from exc

    # Group the latest version per artifact name, then aggregate by scope.
    latest_by_artifact: dict[str, ArtifactVersionResponse] = {}
    for artifact in _sort_memory_artifacts(artifacts):
        latest_by_artifact.setdefault(artifact.name, artifact)

    scope_stats: dict[str, tuple[str, int]] = {}
    for artifact in latest_by_artifact.values():
        if _is_deleted_artifact(artifact):
            continue
        scope, _key = _parse_memory_artifact_identity(artifact.name)
        scope_type = _resolve_scope_type(artifact)
        prev_type, prev_count = scope_stats.get(scope, (scope_type, 0))
        scope_stats[scope] = (prev_type, prev_count + 1)

    return sorted(
        [
            MemoryScopeInfo(scope=scope, scope_type=scope_type, entry_count=count)
            for scope, (scope_type, count) in scope_stats.items()
        ],
        key=lambda info: info.scope,
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
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            artifact=_memory_artifact_name(scope.scope, key),
            **_memory_query_kwargs(project=project),
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


def _delete_impl(
    scope: _MemoryScope,
    key: str,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> MemoryEntry | None:
    """Soft-delete a memory key for the resolved scope."""
    try:
        client = _resolve_memory_client_factory(client_factory)()
        latest_current = _fetch_memory_artifact(
            client,
            scope,
            key,
            project=project,
        )
        if latest_current is None:
            return None

        if _is_deleted_artifact(latest_current):
            entry = _artifact_to_memory_entry(latest_current)
            _track_memory_event(
                AnalyticsEvent.MEMORY_DELETED,
                scope=scope,
                metadata={"already_deleted": True},
            )
            return entry

        resolved_scope_type = _validate_memory_scope_type(
            _resolve_scope_type(latest_current),
            error_type=KitaruRuntimeError,
        )
        flow_context: _ExecutionFlowContext | None = None
        if resolved_scope_type == "execution":
            flow_context = _resolve_execution_flow_context(
                client,
                scope=scope,
                project=project,
            )

        tombstone = _save_memory_artifact(
            client=client,
            scope=scope,
            key=key,
            value=None,
            deleted=True,
            scope_type=resolved_scope_type,
            project=project,
            flow_context=flow_context,
        )
        entry = _artifact_to_memory_entry(tombstone)
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


def _write_compaction_record(
    scope: _MemoryScope,
    record: CompactionRecord,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> None:
    """Persist a compaction audit record under the reserved prefix."""
    log_key = f"{_COMPACTION_LOG_PREFIX}{scope.scope}"
    try:
        client = _resolve_memory_client_factory(client_factory)()
        flow_context: _ExecutionFlowContext | None = None
        if scope.scope_type == "execution":
            flow_context = _resolve_execution_flow_context(
                client,
                scope=scope,
                project=project,
            )

        _save_memory_artifact(
            client=client,
            scope=scope,
            key=log_key,
            value=record.model_dump(mode="json"),
            deleted=False,
            scope_type=scope.scope_type,
            project=project,
            flow_context=flow_context,
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to write compaction record for scope {scope.scope!r}: {exc}"
        ) from exc


def _compaction_log_impl(
    scope: _MemoryScope,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> _list[CompactionRecord]:
    """Read all compaction audit records for a scope."""
    log_key = f"{_COMPACTION_LOG_PREFIX}{scope.scope}"
    try:
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            artifact=_memory_artifact_name(scope.scope, log_key),
            **_memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to read compaction log for scope {scope.scope!r}: {exc}"
        ) from exc

    records: _list[CompactionRecord] = []
    for artifact in _sort_memory_artifacts(artifacts):
        if _is_deleted_artifact(artifact):
            continue
        try:
            raw = artifact.load()
            records.append(CompactionRecord.model_validate(raw))
        except Exception:
            continue
    return records


def _collect_single_key_current_entries(
    client: Client,
    scope: _MemoryScope,
    key: str,
    *,
    project: str | None = None,
) -> _list[tuple[str, int, Any]]:
    """Collect the current non-deleted value of one key for compaction."""
    artifact = _fetch_memory_artifact(
        client,
        scope,
        key,
        project=project,
    )
    if artifact is None:
        raise KitaruUsageError(
            "compact() found no current value for key "
            f"{key!r} in scope {scope.scope!r}."
        )
    if _is_deleted_artifact(artifact):
        raise KitaruUsageError(
            f"compact() cannot summarize key {key!r} in scope {scope.scope!r} "
            "because its current value is deleted."
        )

    try:
        value = artifact.load()
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load memory key {key!r} in scope {scope.scope!r}: {exc}"
        ) from exc

    return [(key, _parse_memory_version(artifact.version), value)]


def _collect_single_key_history_entries(
    client: Client,
    scope: _MemoryScope,
    key: str,
    *,
    project: str | None = None,
) -> _list[tuple[str, int, Any]]:
    """Collect all non-deleted historical versions of one key for compaction."""
    source_entries: _list[tuple[str, int, Any]] = []
    artifacts = _paginate_artifact_versions(
        client,
        artifact=_memory_artifact_name(scope.scope, key),
        **_memory_query_kwargs(project=project),
    )
    for artifact in _sort_memory_artifacts(artifacts):
        if _is_deleted_artifact(artifact):
            continue
        try:
            value = artifact.load()
            version = _parse_memory_version(artifact.version)
            source_entries.append((key, version, value))
        except Exception:
            continue
    return source_entries


def _collect_multi_key_current_entries(
    client: Client,
    scope: _MemoryScope,
    keys: _list[str],
    *,
    project: str | None = None,
) -> _list[tuple[str, int, Any]]:
    """Collect current non-deleted values for many keys for compaction."""
    source_entries: _list[tuple[str, int, Any]] = []
    for key in keys:
        artifact = _fetch_memory_artifact(
            client,
            scope,
            key,
            project=project,
        )
        if artifact is None or _is_deleted_artifact(artifact):
            continue
        try:
            value = artifact.load()
            version = _parse_memory_version(artifact.version)
            source_entries.append((key, version, value))
        except Exception:
            continue
    return source_entries


def _list_unused_memory_artifact_versions(
    client: Client,
    *,
    artifact_name: str,
    project: str | None = None,
) -> _list[ArtifactVersionResponse]:
    """List unused versions for one exact memory artifact."""
    return _paginate_artifact_versions(
        client,
        hydrate=False,
        artifact=artifact_name,
        only_unused=True,
        **_memory_query_kwargs(project=project),
    )


def _delete_preflighted_memory_versions(
    client: Client,
    *,
    scope: _MemoryScope,
    key: str,
    to_delete: _list[ArtifactVersionResponse],
    project: str | None = None,
) -> int:
    """Preflight deletability for one key, then delete versions directly."""
    if not to_delete:
        return 0

    artifact_name = _memory_artifact_name(scope.scope, key)
    try:
        unused_versions = _list_unused_memory_artifact_versions(
            client,
            artifact_name=artifact_name,
            project=project,
        )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to preflight purge eligibility for key {key!r} "
            f"in scope {scope.scope!r}: {exc}"
        ) from exc

    deletable_ids = {artifact.id for artifact in unused_versions}
    blocked_versions = [
        _parse_memory_version(artifact.version)
        for artifact in to_delete
        if artifact.id not in deletable_ids
    ]
    if blocked_versions:
        raise KitaruBackendError(
            f"Cannot purge versions {blocked_versions!r} for key {key!r} "
            f"in scope {scope.scope!r} because they are not unused."
        )

    # Bypass client.delete_artifact_version(): the high-level wrapper
    # re-scans all unused artifacts per call, causing O(N*total) queries.
    # Direct zen_store access after our own preflight keeps it O(1) per version.
    deleted_count = 0
    for artifact in to_delete:
        try:
            client.zen_store.delete_artifact_version(artifact.id)
            deleted_count += 1
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete artifact version {artifact.id} during "
                f"purge of key {key!r} in scope {scope.scope!r}: {exc}"
            ) from exc
    return deleted_count


def _purge_impl(
    scope: _MemoryScope,
    key: str,
    *,
    keep: int | None = None,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> PurgeResult:
    """Physically delete old versions of a memory key."""
    effective_keep = 0 if keep is None else keep
    if effective_keep < 0:
        raise KitaruUsageError("purge `keep` must be >= 0 or None.")

    try:
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            artifact=_memory_artifact_name(scope.scope, key),
            **_memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to fetch versions for purge of key {key!r} "
            f"in scope {scope.scope!r}: {exc}"
        ) from exc

    sorted_artifacts = _sort_memory_artifacts(artifacts)
    to_delete = sorted_artifacts[effective_keep:]

    deleted_count = _delete_preflighted_memory_versions(
        client,
        scope=scope,
        key=key,
        to_delete=to_delete,
        project=project,
    )

    result = PurgeResult(
        versions_deleted=deleted_count,
        keys_affected=1 if deleted_count > 0 else 0,
        scope=scope.scope,
    )

    if deleted_count > 0:
        source_versions = [_parse_memory_version(a.version) for a in to_delete]
        record = CompactionRecord(
            operation="purge",
            scope=scope.scope,
            timestamp=datetime.now(),
            source_keys=[key],
            source_versions=source_versions,
            target_key=None,
            target_version=None,
            instruction=None,
            model=None,
            source_mode=None,
            keys_affected=result.keys_affected,
            versions_deleted=deleted_count,
            keep=keep,
        )
        _write_compaction_record(
            scope,
            record,
            client_factory=client_factory,
            project=project,
        )

    _track_memory_event(
        AnalyticsEvent.MEMORY_PURGED,
        scope=scope,
        metadata={
            "operation": "purge",
            "versions_deleted": result.versions_deleted,
            "keys_affected": result.keys_affected,
            "keep_provided": keep is not None,
        },
    )
    return result


def _purge_scope_impl(
    scope: _MemoryScope,
    *,
    keep: int | None = None,
    include_deleted: bool = False,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> PurgeResult:
    """Purge old versions across all keys in a scope."""
    effective_keep = 0 if keep is None else keep
    if effective_keep < 0:
        raise KitaruUsageError("purge_scope `keep` must be >= 0 or None.")

    try:
        client = _resolve_memory_client_factory(client_factory)()
        artifacts = _paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER, _memory_scope_tag(scope.scope)],
            **_memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to list artifacts for purge of scope {scope.scope!r}: {exc}"
        ) from exc

    by_name: dict[str, _list[ArtifactVersionResponse]] = {}
    for artifact in _sort_memory_artifacts(artifacts):
        by_name.setdefault(artifact.name, []).append(artifact)

    total_deleted = 0
    keys_affected_count = 0
    all_source_keys: _list[str] = []
    all_source_versions: _list[int] = []

    for artifact_name, versions in by_name.items():
        _scope, parsed_key = _parse_memory_artifact_identity(artifact_name)
        if parsed_key.startswith(_COMPACTION_LOG_PREFIX):
            continue

        latest = versions[0] if versions else None
        is_tombstoned = latest is not None and _is_deleted_artifact(latest)

        if is_tombstoned and not include_deleted:
            continue

        if is_tombstoned and include_deleted:
            to_delete = versions
        else:
            to_delete = versions[effective_keep:]

        if not to_delete:
            continue

        key_deleted = _delete_preflighted_memory_versions(
            client,
            scope=scope,
            key=parsed_key,
            to_delete=to_delete,
            project=project,
        )

        if key_deleted > 0:
            total_deleted += key_deleted
            keys_affected_count += 1
            all_source_keys.append(parsed_key)
            all_source_versions.extend(
                _parse_memory_version(a.version) for a in to_delete
            )

    result = PurgeResult(
        versions_deleted=total_deleted,
        keys_affected=keys_affected_count,
        scope=scope.scope,
    )

    if total_deleted > 0:
        record = CompactionRecord(
            operation="purge",
            scope=scope.scope,
            timestamp=datetime.now(),
            source_keys=all_source_keys,
            source_versions=all_source_versions,
            target_key=None,
            target_version=None,
            instruction=None,
            model=None,
            source_mode=None,
            keys_affected=keys_affected_count,
            versions_deleted=total_deleted,
            keep=keep,
        )
        _write_compaction_record(
            scope,
            record,
            client_factory=client_factory,
            project=project,
        )

    _track_memory_event(
        AnalyticsEvent.MEMORY_PURGED,
        scope=scope,
        metadata={
            "operation": "purge_scope",
            "versions_deleted": result.versions_deleted,
            "keys_affected": result.keys_affected,
            "keep_provided": keep is not None,
            "include_deleted": include_deleted,
        },
    )
    return result


def _compact_impl(
    scope: _MemoryScope,
    *,
    key: str | None = None,
    keys: _list[str] | None = None,
    source_mode: _MemoryCompactionSourceMode = "current",
    target_key: str | None = None,
    instruction: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> CompactResult:
    """Summarize memory values using an LLM and write the result."""
    if key is not None and keys is not None:
        raise KitaruUsageError(
            "compact() requires exactly one of `key` or `keys`, not both."
        )
    if key is None and keys is None:
        raise KitaruUsageError(
            "compact() requires either `key` (single-key mode) "
            "or `keys` (multi-key mode)."
        )

    if keys is not None and target_key is None:
        raise KitaruUsageError("compact() in multi-key mode requires `target_key`.")
    if keys is not None and source_mode != "current":
        raise KitaruUsageError(
            "compact() only supports `source_mode='history'` in single-key mode."
        )

    effective_target = target_key if target_key is not None else key
    assert effective_target is not None  # guaranteed by validation above

    client = _resolve_memory_client_factory(client_factory)()
    source_entries: _list[tuple[str, int, Any]]
    if key is not None and source_mode == "current":
        source_entries = _collect_single_key_current_entries(
            client,
            scope,
            key,
            project=project,
        )
    elif key is not None:
        source_entries = _collect_single_key_history_entries(
            client,
            scope,
            key,
            project=project,
        )
    else:
        assert keys is not None
        source_entries = _collect_multi_key_current_entries(
            client,
            scope,
            keys,
            project=project,
        )

    if not source_entries:
        raise KitaruUsageError("compact() found no source entries to summarize.")

    # Build the LLM prompt
    context_parts: _list[str] = []
    for src_key, src_version, src_value in source_entries:
        context_parts.append(f"--- {src_key} (version {src_version}) ---\n{src_value}")
    context_block = "\n\n".join(context_parts)

    default_instruction = (
        "Summarize the following memory entries into a concise, factual summary "
        "preserving all important information. Output only the summary text."
    )
    effective_instruction = instruction or default_instruction

    prompt = (
        f"{effective_instruction}\n\n"
        f"Memory entries ({len(source_entries)} total):\n\n"
        f"{context_block}"
    )

    # Execute LLM call using the shared provider dispatch
    from kitaru.llm import (
        _dispatch_provider_call,
        _normalize_messages,
        _resolve_credential_overlay,
        _track_llm_call_analytics,
        resolve_model_selection,
    )

    model_selection = resolve_model_selection(model)
    messages = _normalize_messages(prompt, system=None)
    env_overlay, credential_source = _resolve_credential_overlay(model_selection)
    result = _dispatch_provider_call(
        model_selection=model_selection,
        messages=messages,
        temperature=None,
        max_tokens=max_tokens,
        env_overlay=env_overlay,
    )
    _track_llm_call_analytics(
        model_selection=model_selection,
        credential_source=credential_source,
        mocked=False,
        extra_metadata={"usage_context": "memory_compaction"},
    )

    summary_text = result.response_text

    # Write the summary as a new version of the target key
    new_entry = _set_entry_impl(
        scope,
        effective_target,
        summary_text,
        client_factory=client_factory,
        project=project,
    )

    # Write compaction record
    record = CompactionRecord(
        operation="compact",
        scope=scope.scope,
        timestamp=datetime.now(),
        source_keys=[src_key for src_key, _, _ in source_entries],
        source_versions=[src_version for _, src_version, _ in source_entries],
        target_key=effective_target,
        target_version=new_entry.version,
        instruction=instruction,
        model=model_selection.resolved_model,
        source_mode=source_mode,
        keys_affected=0,
        versions_deleted=0,
        keep=None,
    )
    _write_compaction_record(
        scope,
        record,
        client_factory=client_factory,
        project=project,
    )

    result_payload = CompactResult(
        entry=new_entry,
        sources_read=len(source_entries),
        scope=scope.scope,
        compaction_record=record,
    )
    _track_memory_event(
        AnalyticsEvent.MEMORY_COMPACTED,
        scope=scope,
        metadata={
            "source_mode": source_mode,
            "sources_read": result_payload.sources_read,
            "multi_key": keys is not None,
            "target_overridden": target_key is not None,
            "custom_instruction": instruction is not None,
            "model_provided": model is not None,
        },
    )
    return result_payload


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
) -> Any:
    """Synthetic non-cacheable step for `memory.get()`.

    Return type is ``Any`` (not ``Any | None``) because ZenML step
    introspection does not reliably handle union return types for
    materializer selection on synthetic memory steps.
    """
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
) -> Any:
    """Synthetic non-cacheable step for `memory.delete()`.

    Return type is ``Any`` (not ``MemoryEntry | None``) because ZenML
    step introspection does not reliably handle union return types for
    materializer selection on synthetic memory steps.
    """
    return _delete_impl(_coerce_memory_scope(scope, scope_type), key)


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


def set(key: str, value: Any) -> None:
    """Persist a new version of a memory key in the active scope.

    Inside a flow, this dispatches through a synthetic non-cacheable ZenML step so
    the write happens at runtime. Outside a flow, it writes directly to the
    artifact store using the configured process-local scope.
    """
    scope = _resolve_memory_scope_for_operation("set")
    normalized_key = _validate_memory_identifier(key, kind="key")
    if _is_inside_flow():
        _memory_set_step(scope.scope, scope.scope_type, normalized_key, value)
    else:
        _set_impl(scope, normalized_key, value)
    return None


def get(key: str, *, version: int | None = None) -> Any | None:
    """Return the current value for a memory key in the active scope.

    Inside a flow, reads run through a synthetic non-cacheable ZenML step so the
    lookup happens at runtime. Outside a flow, reads query the artifact store
    directly using the configured process-local scope.
    """
    scope = _resolve_memory_scope_for_operation("get")
    normalized_key = _validate_memory_identifier(key, kind="key")
    normalized_version = _validate_memory_version(version)
    if _is_inside_flow():
        return _memory_get_step(
            scope.scope,
            scope.scope_type,
            normalized_key,
            normalized_version,
        )
    return _get_impl(scope, normalized_key, normalized_version)


def list() -> _list[MemoryEntry]:
    """List the latest active memory entries for the active scope."""
    scope = _resolve_memory_scope_for_operation("list")
    if _is_inside_flow():
        return _memory_list_step(scope.scope, scope.scope_type)
    return _list_impl(scope)


def history(key: str) -> _list[MemoryEntry]:
    """Return all versions of a memory key, including tombstones."""
    scope = _resolve_memory_scope_for_operation("history")
    normalized_key = _validate_memory_identifier(key, kind="key")
    if _is_inside_flow():
        return _memory_history_step(scope.scope, scope.scope_type, normalized_key)
    return _history_impl(scope, normalized_key)


def delete(key: str) -> MemoryEntry | None:
    """Soft-delete a memory key by writing a tombstone version."""
    scope = _resolve_memory_scope_for_operation("delete")
    normalized_key = _validate_memory_identifier(key, kind="key")
    if _is_inside_flow():
        return _memory_delete_step(scope.scope, scope.scope_type, normalized_key)
    return _delete_impl(scope, normalized_key)


__all__ = [
    "CompactResult",
    "CompactionRecord",
    "MemoryEntry",
    "MemoryReindexIssue",
    "MemoryReindexResult",
    "PurgeResult",
    "configure",
    "delete",
    "get",
    "history",
    "list",
    "set",
]
