"""Public orchestration for importing Langfuse observation exports."""

import re
from collections import Counter
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from zenml.client import Client
from zenml.enums import StackComponentType
from zenml.stack import StackComponent

from kitaru._agent_registration import (
    RegisteredAgentVersionBinding,
    resolve_registered_agent_version,
)
from kitaru._run_identity import extract_run_project_identity
from kitaru.errors import KitaruUsageError
from kitaru.imports._models import ImportedTrace, TraceIntegrity
from kitaru.imports._normalization import (
    NormalizedLangfuseTrace,
    normalize_langfuse_records,
    normalize_selected_langfuse_records,
)
from kitaru.imports._pydantic_ai_replay import (
    PydanticAIReplayEvidence,
    build_pydantic_ai_replay_evidence,
)
from kitaru.imports._replay_evidence import (
    RawImportedEvidence,
    ReplayReadinessSummary,
    SourceAttribution,
    SourceAttributionStatus,
    build_raw_imported_evidence,
    classify_source_attribution,
    extract_langfuse_provider_stamps,
)
from kitaru.imports._source import LangfuseFetchProvenance, resolve_langfuse_source
from kitaru.imports._writer import (
    ImportedTraceConflictError,
    ImportedTracePersistenceError,
    ImportedTracePlan,
    ImportedTraceWriteError,
    persist_imported_trace,
    plan_imported_trace,
)

STORAGE_WARNING = (
    "Writing imported traces stores the selected raw trace rows and normalized "
    "replay evidence, including imported inputs and outputs, in the selected "
    "stack's artifact store. Confirm retention and access controls before "
    "proceeding."
)
_COHORT_TAG_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


class ImportOutcomeStatus(StrEnum):
    """Outcome for one trace in a Langfuse import request."""

    WOULD_CREATE = "would_create"
    WOULD_RESUME = "would_resume"
    UNCHANGED = "unchanged"
    CREATED = "created"
    RESUMED = "resumed"
    CONFLICT = "conflict"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass(frozen=True)
class TraceImportOutcome:
    """Result for one selected source trace."""

    trace_id: str
    integrity: TraceIntegrity
    observation_count: int
    status: ImportOutcomeStatus
    attribution: SourceAttribution | None = None
    raw_evidence_digest: str | None = None
    raw_evidence_artifact_id: str | None = None
    raw_evidence_schema_version: int | None = None
    replay_bundle_digest: str | None = None
    replay_bundle_artifact_id: str | None = None
    replay_bundle_schema_version: int | None = None
    replay_readiness: ReplayReadinessSummary | None = None
    execution_id: str | None = None
    existing_execution_id: str | None = None
    reason: str | None = None
    resolution: str | None = None


@dataclass(frozen=True)
class LangfuseImportResult:
    """Aggregate result of planning or executing a Langfuse import."""

    dry_run: bool
    source_project_id: str
    agent_name: str
    project_name: str
    project_id: str
    stack_name: str
    stack_id: str
    stack_was_explicit: bool
    artifact_store_type: str
    artifact_store_is_local: bool
    artifact_store_is_remotely_accessible: bool
    total_trace_count: int
    selected_trace_count: int
    outcomes: tuple[TraceImportOutcome, ...]
    storage_warning: str = STORAGE_WARNING
    agent_id: str | None = None
    agent_version_id: str | None = None
    pipeline_id: str | None = None
    pipeline_name: str | None = None
    requested_version: str | None = None
    requested_alias: str | None = None
    cohort_tag: str | None = None
    fetch_provenance: LangfuseFetchProvenance | None = None

    @property
    def flow_name(self) -> str:
        """Return the actual registered Pipeline name used by imported executions."""
        if self.pipeline_name is None:
            raise KitaruUsageError(
                "The imported execution Pipeline name is unavailable."
            )
        return self.pipeline_name

    @property
    def counts(self) -> dict[str, int]:
        """Count outcomes by stable status value."""
        counts = Counter(outcome.status.value for outcome in self.outcomes)
        return dict(sorted(counts.items()))

    @property
    def attribution_counts(self) -> dict[str, int]:
        """Count source attribution classifications."""
        counts = Counter(
            outcome.attribution.status.value
            for outcome in self.outcomes
            if outcome.attribution is not None
        )
        return dict(sorted(counts.items()))


@dataclass(frozen=True)
class _PreparedTrace:
    trace: ImportedTrace
    raw_evidence: RawImportedEvidence
    replay_evidence: PydanticAIReplayEvidence
    attribution: SourceAttribution


def import_langfuse(
    source: str | Path,
    *,
    source_project_id: str | None = None,
    agent: str,
    version: str,
    trace_ids: Sequence[str] | None = None,
    limit: int | None = None,
    dry_run: bool = True,
    confirm_data_storage: bool = False,
    allow_fragmented: bool = False,
    max_workers: int = 1,
    stack: str | None = None,
    cohort_tag: str | None = None,
    client: Client | None = None,
) -> LangfuseImportResult:
    """Preview or persist selected Langfuse traces as imported executions."""
    normalized_project_id = (
        _nonempty(source_project_id, "source_project_id")
        if source_project_id is not None
        else None
    )
    normalized_agent = _nonempty(agent, "agent")
    normalized_version = _nonempty(version, "version")
    normalized_cohort_tag = _validate_cohort_tag(cohort_tag)
    _validate_limit(limit)
    _validate_max_workers(max_workers)
    if not dry_run and not confirm_data_storage:
        raise KitaruUsageError(
            "Writing trace payloads requires confirm_data_storage=True."
        )

    zenml_client = client or Client()
    binding = resolve_registered_agent_version(
        zenml_client,
        agent=normalized_agent,
        version=normalized_version,
    )
    target = _resolve_import_target(
        zenml_client,
        binding=binding,
        stack=stack,
    )

    requested_trace_ids = _validate_trace_ids(trace_ids)
    resolved_source = resolve_langfuse_source(
        source,
        source_project_id=normalized_project_id,
        trace_ids=requested_trace_ids,
    )
    normalized_project_id = resolved_source.authoritative_project_id
    if resolved_source.selected_trace_id is not None and requested_trace_ids is None:
        requested_trace_ids = (resolved_source.selected_trace_id,)

    if requested_trace_ids is None:
        normalized_traces = normalize_langfuse_records(
            resolved_source.records,
            project_id=normalized_project_id,
        )
        total_trace_count = len(normalized_traces)
    else:
        normalized_traces, total_trace_count = normalize_selected_langfuse_records(
            resolved_source.records,
            project_id=normalized_project_id,
            trace_ids=set(requested_trace_ids),
        )
    selected = _select_traces(
        normalized_traces,
        trace_ids=requested_trace_ids,
        limit=limit,
    )
    prepared = tuple(_prepare_trace(item, binding=binding) for item in selected)

    if any(
        item.attribution.status is SourceAttributionStatus.CONFLICT for item in prepared
    ):
        outcomes = tuple(
            _outcome(
                item,
                (
                    ImportOutcomeStatus.CONFLICT
                    if item.attribution.status is SourceAttributionStatus.CONFLICT
                    else ImportOutcomeStatus.REJECTED
                ),
                reason=(
                    "Supported provider version evidence conflicts with the declared "
                    "source AgentVersion."
                    if item.attribution.status is SourceAttributionStatus.CONFLICT
                    else "The submission was rejected because another selected trace "
                    "has conflicting source-version evidence."
                ),
                resolution=(
                    "Select the AgentVersion that produced the trace or correct the "
                    "source version fields."
                    if item.attribution.status is SourceAttributionStatus.CONFLICT
                    else None
                ),
            )
            for item in prepared
        )
        return _result(
            dry_run=dry_run,
            source_project_id=normalized_project_id,
            binding=binding,
            requested_version=normalized_version,
            cohort_tag=normalized_cohort_tag,
            target=target,
            total_trace_count=total_trace_count,
            outcomes=outcomes,
            fetch_provenance=resolved_source.fetch_provenance,
        )

    def process(item: _PreparedTrace) -> TraceImportOutcome:
        trace = item.trace
        if trace.integrity is TraceIntegrity.INVALID:
            return _outcome(
                item,
                ImportOutcomeStatus.REJECTED,
                reason="Invalid trace graph cannot be imported.",
            )
        if trace.integrity is TraceIntegrity.FRAGMENTED and not allow_fragmented:
            return _outcome(
                item,
                ImportOutcomeStatus.REJECTED,
                reason="Fragmented trace requires allow_fragmented=True.",
            )
        try:
            if dry_run:
                plan = plan_imported_trace(
                    trace,
                    binding=binding,
                    raw_evidence=item.raw_evidence,
                    replay_evidence=item.replay_evidence,
                    cohort_tag=normalized_cohort_tag,
                    client=zenml_client,
                    stack_id=target.stack_id,
                )
                status = {
                    ImportedTracePlan.CREATE: ImportOutcomeStatus.WOULD_CREATE,
                    ImportedTracePlan.RESUME: ImportOutcomeStatus.WOULD_RESUME,
                    ImportedTracePlan.UNCHANGED: ImportOutcomeStatus.UNCHANGED,
                }[plan]
                return _outcome(item, status)

            persisted = persist_imported_trace(
                trace,
                binding=binding,
                raw_evidence=item.raw_evidence,
                replay_evidence=item.replay_evidence,
                attribution=item.attribution,
                cohort_tag=normalized_cohort_tag,
                client=zenml_client,
                stack_id=target.stack_id,
                artifact_store=target.artifact_store,
            )
            status = ImportOutcomeStatus.UNCHANGED
            if persisted.created:
                status = ImportOutcomeStatus.CREATED
            elif persisted.resumed:
                status = ImportOutcomeStatus.RESUMED
            return _outcome(
                item,
                status,
                execution_id=persisted.execution_id,
                raw_evidence_artifact_id=persisted.raw_evidence_artifact_id,
                raw_evidence_schema_version=persisted.raw_evidence_schema_version,
                replay_bundle_artifact_id=persisted.replay_bundle_artifact_id,
                replay_bundle_schema_version=persisted.replay_bundle_schema_version,
            )
        except ImportedTraceConflictError as exc:
            return _outcome(
                item,
                ImportOutcomeStatus.CONFLICT,
                existing_execution_id=exc.existing_execution_id,
                reason=str(exc),
                resolution=exc.resolution,
            )
        except ImportedTraceWriteError as exc:
            return _outcome(
                item,
                ImportOutcomeStatus.FAILED,
                execution_id=exc.execution_id,
                reason=str(exc),
            )
        except ImportedTracePersistenceError as exc:
            return _outcome(item, ImportOutcomeStatus.REJECTED, reason=str(exc))
        except Exception as exc:  # Backend failures are per-trace outcomes.
            return _outcome(item, ImportOutcomeStatus.FAILED, reason=str(exc))

    if max_workers == 1:
        outcomes = tuple(process(item) for item in prepared)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            outcomes = tuple(executor.map(process, prepared))

    return _result(
        dry_run=dry_run,
        source_project_id=normalized_project_id,
        binding=binding,
        requested_version=normalized_version,
        cohort_tag=normalized_cohort_tag,
        target=target,
        total_trace_count=total_trace_count,
        outcomes=outcomes,
        fetch_provenance=resolved_source.fetch_provenance,
    )


def import_langfuse_jsonl(
    path: str | Path,
    *,
    source_project_id: str,
    agent: str,
    version: str,
    trace_ids: Sequence[str] | None = None,
    limit: int | None = None,
    dry_run: bool = True,
    confirm_data_storage: bool = False,
    allow_fragmented: bool = False,
    max_workers: int = 1,
    stack: str | None = None,
    cohort_tag: str | None = None,
    client: Client | None = None,
) -> LangfuseImportResult:
    """Compatibility adapter for JSONL-only callers."""
    return import_langfuse(
        path,
        source_project_id=source_project_id,
        agent=agent,
        version=version,
        trace_ids=trace_ids,
        limit=limit,
        dry_run=dry_run,
        confirm_data_storage=confirm_data_storage,
        allow_fragmented=allow_fragmented,
        max_workers=max_workers,
        stack=stack,
        cohort_tag=cohort_tag,
        client=client,
    )


@dataclass(frozen=True)
class _ImportTarget:
    stack_name: str
    stack_id: Any
    stack_was_explicit: bool
    artifact_store_type: str
    artifact_store_is_local: bool
    remote_metadata_store: bool
    artifact_store: Any


def _resolve_import_target(
    client: Client,
    *,
    binding: RegisteredAgentVersionBinding,
    stack: str | None,
) -> _ImportTarget:
    normalized_stack = stack.strip() if stack is not None else None
    if stack is not None and not normalized_stack:
        raise KitaruUsageError("stack must be a non-empty name or ID.")

    if normalized_stack is None:
        active_project_id = str(getattr(client.active_project, "id", "")).strip()
        if active_project_id != binding.project_id:
            raise KitaruUsageError(
                "The active stack cannot be selected implicitly for a different "
                "Agent Project. Pass stack= explicitly."
            )
        stack_model = client.active_stack_model
    else:
        try:
            stack_model = client.get_stack(
                normalized_stack,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruUsageError(
                f"Could not resolve stack {normalized_stack!r}: {exc}"
            ) from exc

    stack_project_id = _require_project_scope(
        stack_model,
        expected_project_id=binding.project_id,
        resource_name="stack",
        inherited_project_id=(binding.project_id if normalized_stack is None else None),
    )
    components = stack_model.components.get(StackComponentType.ARTIFACT_STORE, [])
    if len(components) != 1:
        raise KitaruUsageError(
            f"Stack {stack_model.name!r} must contain exactly one artifact store."
        )
    artifact_store_component = components[0]
    _require_project_scope(
        artifact_store_component,
        expected_project_id=binding.project_id,
        resource_name="artifact store",
        inherited_project_id=stack_project_id,
    )
    artifact_store = _load_artifact_store(artifact_store_component)
    _require_project_scope(
        artifact_store,
        expected_project_id=binding.project_id,
        resource_name="artifact store",
        inherited_project_id=stack_project_id,
    )
    is_local = bool(artifact_store.config.is_local)
    return _ImportTarget(
        stack_name=str(stack_model.name),
        stack_id=stack_model.id,
        stack_was_explicit=normalized_stack is not None,
        artifact_store_type=str(artifact_store_component.flavor_name),
        artifact_store_is_local=is_local,
        remote_metadata_store=not client.zen_store.is_local_store(),
        artifact_store=artifact_store,
    )


def _require_project_scope(
    resource: Any,
    *,
    expected_project_id: str,
    resource_name: str,
    inherited_project_id: str | None = None,
) -> str:
    project_id = extract_run_project_identity(resource).project_id
    if project_id is None:
        config = getattr(resource, "config", None)
        candidate = getattr(config, "project_id", None)
        project_id = str(candidate).strip() if candidate is not None else None
    project_id = project_id or inherited_project_id
    if project_id is None:
        raise KitaruUsageError(
            f"The selected {resource_name} has no verifiable Agent Project identity."
        )
    if project_id != expected_project_id:
        raise KitaruUsageError(
            f"The selected {resource_name} belongs to a different Agent Project."
        )
    return project_id


def _load_artifact_store(component: Any) -> Any:
    try:
        return StackComponent.from_model(component)
    except Exception as exc:
        raise KitaruUsageError(
            f"Could not load artifact store {component.name!r} "
            f"({component.flavor_name}): {exc}"
        ) from exc


def _storage_warning(target: _ImportTarget) -> str:
    warnings = [STORAGE_WARNING]
    if not target.stack_was_explicit:
        warnings.append(
            f"No stack was specified, so Kitaru will use the active stack "
            f"{target.stack_name!r} ({target.stack_id}). Pass --stack on the CLI "
            "or stack= in the SDK to select one explicitly before importing."
        )
    if target.remote_metadata_store and target.artifact_store_is_local:
        warnings.append(
            "The execution metadata will be stored on the remote server, but the "
            "artifact payloads will remain on this machine because the selected "
            "artifact store is local. The shared UI may be unable to load them."
        )
    return " ".join(warnings)


def _prepare_trace(
    item: NormalizedLangfuseTrace,
    *,
    binding: RegisteredAgentVersionBinding,
) -> _PreparedTrace:
    raw_evidence = build_raw_imported_evidence(
        source=item.trace.source,
        records=item.records,
    )
    replay_evidence = build_pydantic_ai_replay_evidence(
        item.trace,
        raw_evidence=raw_evidence,
    )
    stamps = extract_langfuse_provider_stamps([record.row for record in item.records])
    attribution = classify_source_attribution(
        stamps,
        git_sha=binding.manifest.git_sha,
        aliases=binding.aliases,
    )
    return _PreparedTrace(
        trace=item.trace,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        attribution=attribution,
    )


def _result(
    *,
    dry_run: bool,
    source_project_id: str,
    binding: RegisteredAgentVersionBinding,
    requested_version: str,
    cohort_tag: str | None,
    target: _ImportTarget,
    total_trace_count: int,
    outcomes: tuple[TraceImportOutcome, ...],
    fetch_provenance: LangfuseFetchProvenance | None,
) -> LangfuseImportResult:
    return LangfuseImportResult(
        dry_run=dry_run,
        source_project_id=source_project_id,
        agent_name=binding.agent_name,
        agent_id=binding.project_id,
        agent_version_id=binding.manifest.agent_version_id,
        pipeline_id=binding.pipeline_id,
        pipeline_name=binding.pipeline_name,
        requested_version=requested_version,
        requested_alias=binding.requested_alias,
        cohort_tag=cohort_tag,
        project_name=binding.project_name,
        project_id=binding.project_id,
        stack_name=target.stack_name,
        stack_id=str(target.stack_id),
        stack_was_explicit=target.stack_was_explicit,
        artifact_store_type=target.artifact_store_type,
        artifact_store_is_local=target.artifact_store_is_local,
        artifact_store_is_remotely_accessible=(not target.artifact_store_is_local),
        total_trace_count=total_trace_count,
        selected_trace_count=len(outcomes),
        outcomes=outcomes,
        storage_warning=_storage_warning(target),
        fetch_provenance=fetch_provenance,
    )


def _outcome(
    item: _PreparedTrace,
    status: ImportOutcomeStatus,
    *,
    execution_id: str | None = None,
    raw_evidence_artifact_id: str | None = None,
    raw_evidence_schema_version: int | None = None,
    replay_bundle_artifact_id: str | None = None,
    replay_bundle_schema_version: int | None = None,
    existing_execution_id: str | None = None,
    reason: str | None = None,
    resolution: str | None = None,
) -> TraceImportOutcome:
    return TraceImportOutcome(
        trace_id=item.trace.source.trace_id,
        integrity=item.trace.integrity,
        observation_count=len(item.trace.observations),
        status=status,
        attribution=item.attribution,
        raw_evidence_digest=item.raw_evidence.raw_content_sha256,
        raw_evidence_artifact_id=raw_evidence_artifact_id,
        raw_evidence_schema_version=raw_evidence_schema_version,
        replay_bundle_digest=item.replay_evidence.bundle.bundle_digest,
        replay_bundle_artifact_id=replay_bundle_artifact_id,
        replay_bundle_schema_version=replay_bundle_schema_version,
        replay_readiness=item.replay_evidence.readiness,
        execution_id=execution_id,
        existing_execution_id=existing_execution_id,
        reason=reason,
        resolution=resolution,
    )


def _select_traces(
    traces: list[NormalizedLangfuseTrace],
    *,
    trace_ids: tuple[str, ...] | None,
    limit: int | None,
) -> list[NormalizedLangfuseTrace]:
    if trace_ids is None:
        selected = traces
    else:
        by_id = {item.trace.source.trace_id: item for item in traces}
        missing = [trace_id for trace_id in trace_ids if trace_id not in by_id]
        if missing:
            raise KitaruUsageError(
                "Requested trace IDs were not found: " + ", ".join(missing)
            )
        selected = [by_id[trace_id] for trace_id in trace_ids]
    return selected if limit is None else selected[:limit]


def _validate_trace_ids(trace_ids: Sequence[str] | None) -> tuple[str, ...] | None:
    if trace_ids is None:
        return None
    normalized = tuple(_nonempty(trace_id, "trace_ids") for trace_id in trace_ids)
    if len(set(normalized)) != len(normalized):
        raise KitaruUsageError("trace_ids cannot contain duplicates.")
    return normalized


def _nonempty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KitaruUsageError(f"{name} must be a non-empty string.")
    return value.strip()


def _validate_cohort_tag(cohort_tag: str | None) -> str | None:
    if cohort_tag is None:
        return None
    if not isinstance(cohort_tag, str):
        raise KitaruUsageError("cohort_tag must be a string.")
    normalized = cohort_tag.strip()
    if not _COHORT_TAG_PATTERN.fullmatch(normalized):
        raise KitaruUsageError(
            "cohort_tag must be 1-64 letters, numbers, dots, underscores, or hyphens."
        )
    if normalized.lower().startswith(("kitaru-", "kitaru.")):
        raise KitaruUsageError("cohort_tag uses a reserved Kitaru prefix.")
    return normalized


def _validate_limit(limit: int | None) -> None:
    if limit is not None and (isinstance(limit, bool) or limit < 1):
        raise KitaruUsageError("limit must be >= 1.")


def _validate_max_workers(max_workers: int) -> None:
    if isinstance(max_workers, bool) or not 1 <= max_workers <= 8:
        raise KitaruUsageError("max_workers must be between 1 and 8.")
