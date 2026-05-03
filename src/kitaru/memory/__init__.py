# ruff: noqa: F401
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

from kitaru.memory import _scope
from kitaru.memory._api import delete, get, history, list, set
from kitaru.memory._artifacts import (
    Client,
    _artifact_tag_names,
    _artifact_to_memory_entry,
    _extract_flow_context_from_pipeline,
    _fetch_exact_artifact_version,
    _fetch_memory_artifact,
    _infer_value_type,
    _is_deleted_artifact,
    _iter_matching_memory_artifacts,
    _memory_artifact_name,
    _memory_flow_id_tag,
    _memory_key_tag,
    _memory_metadata,
    _memory_query_kwargs,
    _memory_scope_tag,
    _memory_scope_type_tag,
    _memory_tags,
    _optional_metadata_string,
    _paginate_artifact_versions,
    _parse_memory_artifact_identity,
    _parse_memory_version,
    _resolve_active_flow_scope_context,
    _resolve_execution_flow_context,
    _resolve_memory_client_factory,
    _resolve_scope_type,
    _save_memory_artifact,
    _sort_memory_artifacts,
    _temporary_active_project,
    _warn_flow_context_unresolved,
    logger,
    save_artifact,
)
from kitaru.memory._constants import (
    _COMPACTION_LOG_PREFIX,
    _MEMORY_ARTIFACT_PREFIX,
    _MEMORY_DELETED_METADATA_KEY,
    _MEMORY_FLOW_ID_METADATA_KEY,
    _MEMORY_FLOW_NAME_METADATA_KEY,
    _MEMORY_IDENTIFIER_PATTERN,
    _MEMORY_PAGE_SIZE,
    _MEMORY_REINDEX_ISSUE_SAMPLE_LIMIT,
    _MEMORY_SCOPE_TYPE_METADATA_KEY,
    _MEMORY_SCOPE_TYPE_SORT_ORDER,
    _MEMORY_STEP_EXTRA_PREFIX,
    _MEMORY_TAG_FLOW_ID_PREFIX,
    _MEMORY_TAG_KEY_PREFIX,
    _MEMORY_TAG_MARKER,
    _MEMORY_TAG_SCOPE_PREFIX,
    _MEMORY_TAG_SCOPE_TYPE_PREFIX,
    _MEMORY_VERSION_SORT,
    _list,
)
from kitaru.memory._maintenance import (
    _collect_multi_key_current_entries,
    _collect_single_key_current_entries,
    _collect_single_key_history_entries,
    _compact_impl,
    _compaction_log_impl,
    _delete_preflighted_memory_versions,
    _list_unused_memory_artifact_versions,
    _lookup_reindex_flow_context,
    _purge_impl,
    _purge_scope_impl,
    _record_reindex_issue,
    _reindex_impl,
    _resolve_reindex_flow_context,
    _write_compaction_record,
)
from kitaru.memory._models import (
    CompactionRecord,
    CompactResult,
    MemoryEntry,
    MemoryReindexIssue,
    MemoryReindexResult,
    MemoryScopeInfo,
    MemoryScopeType,
    PurgeResult,
    _ExecutionFlowContext,
    _MemoryCompactionSourceMode,
    _MemoryScope,
    _MemoryScopeType,
    _ReindexCounters,
)
from kitaru.memory._operations import (
    _delete_impl,
    _get_entry_impl,
    _get_impl,
    _history_impl,
    _list_impl,
    _list_scopes_impl,
    _memory_artifact_unavailable_message,
    _set_entry_impl,
    _set_impl,
    _track_memory_event,
)
from kitaru.memory._scope import (
    _coerce_memory_scope,
    _implicit_flow_memory_scope,
    _memory_scope_session,
    _require_memory_boundary,
    _resolve_configured_scope,
    _resolve_memory_scope_for_operation,
    _validate_memory_compaction_source_mode,
    _validate_memory_identifier,
    _validate_memory_scope_type,
    _validate_memory_version,
    configure,
)
from kitaru.memory._steps import (
    _memory_delete_step,
    _memory_get_step,
    _memory_history_step,
    _memory_list_step,
    _memory_set_step,
    _memory_step,
)

_CURRENT_MEMORY_SCOPE = _scope._CURRENT_MEMORY_SCOPE


def __getattr__(name: str) -> object:
    """Resolve mutable private compatibility aliases from their owner module."""
    if name == "_RUNTIME_MEMORY_SCOPE_DEFAULT":
        return _scope._RUNTIME_MEMORY_SCOPE_DEFAULT
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CompactResult",
    "CompactionRecord",
    "MemoryEntry",
    "MemoryReindexIssue",
    "MemoryReindexResult",
    "MemoryScopeInfo",
    "MemoryScopeType",
    "PurgeResult",
    "configure",
    "delete",
    "get",
    "history",
    "list",
    "set",
]
