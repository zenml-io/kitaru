"""Public orchestration for importing Langfuse observation exports."""

from collections import Counter
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from zenml.client import Client

from kitaru.errors import KitaruUsageError
from kitaru.imports._langfuse import read_langfuse_jsonl
from kitaru.imports._models import ImportedTrace, TraceIntegrity
from kitaru.imports._normalization import (
    normalize_langfuse_observations,
    normalize_selected_langfuse_observations,
)
from kitaru.imports._writer import (
    ImportedTraceConflictError,
    ImportedTracePersistenceError,
    ImportedTracePlan,
    imported_flow_name,
    persist_imported_trace,
    plan_imported_trace,
)

STORAGE_WARNING = (
    "Writing imported traces stores their full input and output payloads in the "
    "active Kitaru storage backend. Confirm retention and access controls "
    "before proceeding."
)


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
    total_trace_count: int
    selected_trace_count: int
    outcomes: tuple[TraceImportOutcome, ...]
    storage_warning: str = STORAGE_WARNING

    @property
    def flow_name(self) -> str:
        """Return the generated Kitaru flow name for imported executions."""
        return imported_flow_name(provider="langfuse", agent_name=self.agent_name)

    @property
    def counts(self) -> dict[str, int]:
        """Count outcomes by stable status value."""
        counts = Counter(outcome.status.value for outcome in self.outcomes)
        return dict(sorted(counts.items()))


def import_langfuse_jsonl(
    path: str | Path,
    *,
    source_project_id: str,
    agent_name: str,
    trace_ids: Sequence[str] | None = None,
    limit: int | None = None,
    dry_run: bool = True,
    confirm_data_storage: bool = False,
    allow_fragmented: bool = False,
    max_workers: int = 1,
    client: Client | None = None,
) -> LangfuseImportResult:
    """Plan or persist selected Langfuse traces as synthetic executions."""
    normalized_project_id = _nonempty(source_project_id, "source_project_id")
    normalized_agent_name = _nonempty(agent_name, "agent_name")
    _validate_limit(limit)
    _validate_max_workers(max_workers)
    if not dry_run and not confirm_data_storage:
        raise KitaruUsageError(
            "Writing trace payloads requires confirm_data_storage=True."
        )

    requested_trace_ids = _validate_trace_ids(trace_ids)
    if requested_trace_ids is None:
        traces = normalize_langfuse_observations(
            read_langfuse_jsonl(path), project_id=normalized_project_id
        )
        total_trace_count = len(traces)
    else:
        traces, total_trace_count = normalize_selected_langfuse_observations(
            read_langfuse_jsonl(path),
            project_id=normalized_project_id,
            trace_ids=set(requested_trace_ids),
        )
    selected = _select_traces(
        traces,
        trace_ids=requested_trace_ids,
        limit=limit,
    )
    zenml_client = client or Client()

    def process(trace: ImportedTrace) -> TraceImportOutcome:
        if trace.integrity is TraceIntegrity.INVALID:
            return _outcome(
                trace,
                ImportOutcomeStatus.REJECTED,
                reason="Invalid trace graph cannot be imported.",
            )
        if trace.integrity is TraceIntegrity.FRAGMENTED and not allow_fragmented:
            return _outcome(
                trace,
                ImportOutcomeStatus.REJECTED,
                reason="Fragmented trace requires allow_fragmented=True.",
            )
        try:
            if dry_run:
                plan = plan_imported_trace(
                    trace,
                    agent_name=normalized_agent_name,
                    client=zenml_client,
                )
                status = {
                    ImportedTracePlan.CREATE.value: ImportOutcomeStatus.WOULD_CREATE,
                    ImportedTracePlan.RESUME.value: ImportOutcomeStatus.WOULD_RESUME,
                    ImportedTracePlan.UNCHANGED.value: ImportOutcomeStatus.UNCHANGED,
                }[str(plan)]
                return _outcome(trace, status)

            persisted = persist_imported_trace(
                trace,
                agent_name=normalized_agent_name,
                client=zenml_client,
            )
            status = ImportOutcomeStatus.UNCHANGED
            if persisted.created:
                status = ImportOutcomeStatus.CREATED
            elif persisted.resumed:
                status = ImportOutcomeStatus.RESUMED
            return _outcome(
                trace,
                status,
                execution_id=persisted.execution_id,
            )
        except ImportedTraceConflictError as exc:
            return _outcome(
                trace,
                ImportOutcomeStatus.CONFLICT,
                existing_execution_id=exc.existing_execution_id,
                reason=str(exc),
                resolution=exc.resolution,
            )
        except ImportedTracePersistenceError as exc:
            return _outcome(trace, ImportOutcomeStatus.REJECTED, reason=str(exc))
        except Exception as exc:  # Backend failures are per-trace outcomes.
            return _outcome(trace, ImportOutcomeStatus.FAILED, reason=str(exc))

    if max_workers == 1:
        outcomes = tuple(process(trace) for trace in selected)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            outcomes = tuple(executor.map(process, selected))

    return LangfuseImportResult(
        dry_run=dry_run,
        source_project_id=normalized_project_id,
        agent_name=normalized_agent_name,
        total_trace_count=total_trace_count,
        selected_trace_count=len(selected),
        outcomes=outcomes,
    )


def _outcome(
    trace: ImportedTrace,
    status: ImportOutcomeStatus,
    *,
    execution_id: str | None = None,
    existing_execution_id: str | None = None,
    reason: str | None = None,
    resolution: str | None = None,
) -> TraceImportOutcome:
    return TraceImportOutcome(
        trace_id=trace.source.trace_id,
        integrity=trace.integrity,
        observation_count=len(trace.observations),
        status=status,
        execution_id=execution_id,
        existing_execution_id=existing_execution_id,
        reason=reason,
        resolution=resolution,
    )


def _select_traces(
    traces: list[ImportedTrace],
    *,
    trace_ids: tuple[str, ...] | None,
    limit: int | None,
) -> list[ImportedTrace]:
    if trace_ids is None:
        selected = traces
    else:
        by_id = {trace.source.trace_id: trace for trace in traces}
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


def _validate_limit(limit: int | None) -> None:
    if limit is not None and (isinstance(limit, bool) or limit < 1):
        raise KitaruUsageError("limit must be >= 1.")


def _validate_max_workers(max_workers: int) -> None:
    if isinstance(max_workers, bool) or not 1 <= max_workers <= 8:
        raise KitaruUsageError("max_workers must be between 1 and 8.")
