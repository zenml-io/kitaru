"""Maintenance operations for Kitaru memory."""

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from zenml.client import Client
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import (
    KitaruBackendError,
    KitaruError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.memory import _artifacts as artifact_store
from kitaru.memory import _operations as operations
from kitaru.memory._constants import (
    _COMPACTION_LOG_PREFIX,
    _MEMORY_REINDEX_ISSUE_SAMPLE_LIMIT,
    _MEMORY_TAG_FLOW_ID_PREFIX,
    _MEMORY_TAG_MARKER,
    _list,
)
from kitaru.memory._models import (
    CompactionRecord,
    CompactResult,
    MemoryReindexIssue,
    MemoryReindexResult,
    PurgeResult,
    _ExecutionFlowContext,
    _MemoryCompactionSourceMode,
    _MemoryScope,
    _ReindexCounters,
)
from kitaru.memory._scope import _validate_memory_scope_type
from kitaru.runtime import _is_inside_flow

logger = logging.getLogger(__name__)


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

    flow_context = artifact_store._extract_flow_context_from_pipeline(
        getattr(run, "pipeline", None)
    )
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
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        artifacts = artifact_store._paginate_artifact_versions(
            client,
            tags=[_MEMORY_TAG_MARKER],
            **artifact_store._memory_query_kwargs(project=project),
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
        parsed_scope_type: str | None = None
        key: str | None = None
        issue_recorded = False

        try:
            parsed_scope, key = artifact_store._parse_memory_artifact_identity(
                artifact_name
            )
            scope = parsed_scope.scope
            parsed_scope_type = parsed_scope.scope_type
            scope_type = _validate_memory_scope_type(
                artifact_store._resolve_scope_type(artifact),
                error_type=KitaruRuntimeError,
            )
            if parsed_scope_type != scope_type:
                raise KitaruRuntimeError(
                    "Memory artifact identity mismatch for "
                    f"{artifact_name!r}: artifact name encodes scope_type "
                    f"{parsed_scope_type!r}, but metadata encodes {scope_type!r}."
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

        tag_names = artifact_store._artifact_tag_names(artifact)
        add_tags: _list[str] = []
        added_scope_type_tag = False
        added_flow_tag = False

        scope_type_tag = artifact_store._memory_scope_type_tag(scope_type)
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
                producer_run_id = artifact_store._optional_metadata_string(
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
                    add_tags.append(
                        artifact_store._memory_flow_id_tag(flow_context.flow_id)
                    )
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
                **artifact_store._memory_query_kwargs(project=project),
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
    # calls track() directly instead of operations._track_memory_event().
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


def _write_compaction_record(
    scope: _MemoryScope,
    record: CompactionRecord,
    *,
    client_factory: Callable[[], Client] | None = None,
    project: str | None = None,
) -> None:
    """Persist a compaction audit record under the reserved prefix."""
    log_key = f"{_COMPACTION_LOG_PREFIX}{scope.scope_type}/{scope.scope}"
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        flow_context: _ExecutionFlowContext | None = None
        if scope.scope_type == "execution":
            flow_context = artifact_store._resolve_execution_flow_context(
                client,
                scope=scope,
                project=project,
            )
        elif scope.scope_type == "flow":
            flow_context = artifact_store._resolve_active_flow_scope_context(scope)

        artifact_store._save_memory_artifact(
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
    log_key = f"{_COMPACTION_LOG_PREFIX}{scope.scope_type}/{scope.scope}"
    try:
        client = artifact_store._resolve_memory_client_factory(client_factory)()
        artifacts = artifact_store._paginate_artifact_versions(
            client,
            artifact=artifact_store._memory_artifact_name(scope, log_key),
            **artifact_store._memory_query_kwargs(project=project),
        )
    except KitaruError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to read compaction log for scope {scope.scope!r}: {exc}"
        ) from exc

    records: _list[CompactionRecord] = []
    for artifact, entry in artifact_store._iter_matching_memory_artifacts(
        artifact_store._sort_memory_artifacts(artifacts),
        scope=scope,
    ):
        if entry.is_deleted:
            continue
        try:
            raw = artifact.load()
            records.append(CompactionRecord.model_validate(raw))
        except Exception:
            logger.warning(
                "Skipping unreadable compaction record %s (%s)",
                artifact.name,
                artifact.id,
            )
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
    artifact = artifact_store._fetch_memory_artifact(
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
    if artifact_store._is_deleted_artifact(artifact):
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

    return [(key, artifact_store._parse_memory_version(artifact.version), value)]


def _collect_single_key_history_entries(
    client: Client,
    scope: _MemoryScope,
    key: str,
    *,
    project: str | None = None,
) -> _list[tuple[str, int, Any]]:
    """Collect all non-deleted historical versions of one key for compaction."""
    source_entries: _list[tuple[str, int, Any]] = []
    artifacts = artifact_store._paginate_artifact_versions(
        client,
        artifact=artifact_store._memory_artifact_name(scope, key),
        **artifact_store._memory_query_kwargs(project=project),
    )
    for artifact, entry in artifact_store._iter_matching_memory_artifacts(
        artifact_store._sort_memory_artifacts(artifacts),
        scope=scope,
    ):
        if entry.is_deleted:
            continue
        try:
            value = artifact.load()
            source_entries.append((key, entry.version, value))
        except Exception:
            logger.warning(
                "Skipping unloadable memory version %s v%d for compaction",
                key,
                entry.version,
            )
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
        artifact = artifact_store._fetch_memory_artifact(
            client,
            scope,
            key,
            project=project,
        )
        if artifact is None or artifact_store._is_deleted_artifact(artifact):
            continue
        try:
            value = artifact.load()
            version = artifact_store._parse_memory_version(artifact.version)
            source_entries.append((key, version, value))
        except Exception:
            logger.warning(
                "Skipping unloadable memory entry %s for compaction",
                key,
            )
            continue
    return source_entries


def _list_unused_memory_artifact_versions(
    client: Client,
    *,
    artifact_name: str,
    project: str | None = None,
) -> _list[ArtifactVersionResponse]:
    """List unused versions for one exact memory artifact."""
    return artifact_store._paginate_artifact_versions(
        client,
        hydrate=False,
        artifact=artifact_name,
        only_unused=True,
        **artifact_store._memory_query_kwargs(project=project),
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

    artifact_name = artifact_store._memory_artifact_name(scope, key)
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
        artifact_store._parse_memory_version(artifact.version)
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
            f"Failed to fetch versions for purge of key {key!r} "
            f"in scope {scope.scope!r}: {exc}"
        ) from exc

    sorted_artifacts = [
        artifact
        for artifact, _entry in artifact_store._iter_matching_memory_artifacts(
            artifact_store._sort_memory_artifacts(artifacts),
            scope=scope,
        )
    ]
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
        scope_type=scope.scope_type,
    )

    if deleted_count > 0:
        source_versions = [
            artifact_store._parse_memory_version(a.version) for a in to_delete
        ]
        record = CompactionRecord(
            operation="purge",
            scope=scope.scope,
            scope_type=scope.scope_type,
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

    operations._track_memory_event(
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
            f"Failed to list artifacts for purge of scope {scope.scope!r}: {exc}"
        ) from exc

    by_key: dict[str, _list[ArtifactVersionResponse]] = {}
    for artifact, entry in artifact_store._iter_matching_memory_artifacts(
        artifact_store._sort_memory_artifacts(artifacts),
        scope=scope,
    ):
        by_key.setdefault(entry.key, []).append(artifact)

    total_deleted = 0
    keys_affected_count = 0
    all_source_keys: _list[str] = []
    all_source_versions: _list[int] = []

    for parsed_key, versions in by_key.items():
        if parsed_key.startswith(_COMPACTION_LOG_PREFIX):
            continue

        latest = versions[0] if versions else None
        is_tombstoned = latest is not None and artifact_store._is_deleted_artifact(
            latest
        )

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
                artifact_store._parse_memory_version(a.version) for a in to_delete
            )

    result = PurgeResult(
        versions_deleted=total_deleted,
        keys_affected=keys_affected_count,
        scope=scope.scope,
        scope_type=scope.scope_type,
    )

    if total_deleted > 0:
        record = CompactionRecord(
            operation="purge",
            scope=scope.scope,
            scope_type=scope.scope_type,
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

    operations._track_memory_event(
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

    client = artifact_store._resolve_memory_client_factory(client_factory)()
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

    new_entry = operations._set_entry_impl(
        scope,
        effective_target,
        summary_text,
        client_factory=client_factory,
        project=project,
    )

    record = CompactionRecord(
        operation="compact",
        scope=scope.scope,
        scope_type=scope.scope_type,
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
        scope_type=scope.scope_type,
        compaction_record=record,
    )
    operations._track_memory_event(
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
