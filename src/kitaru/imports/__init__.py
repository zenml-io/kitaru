"""Canonical data models for importing external agent traces."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from zenml.client import Client

    from kitaru.imports._service import (
        ImportOutcomeStatus,
        LangfuseImportResult,
        TraceImportOutcome,
    )
    from kitaru.imports._writer import (
        ImportedExecutionResult,
        ImportedTraceConflictError,
        ImportedTracePersistenceError,
        ImportedTraceWriteError,
    )

from kitaru.imports._langfuse import (
    LangfuseImportError,
    LangfuseSourceRecord,
    read_langfuse_jsonl,
    read_langfuse_jsonl_records,
)
from kitaru.imports._models import (
    ImportedObservation,
    ImportedTrace,
    ObservationKind,
    ObservationStatus,
    SourceObservationType,
    TraceCost,
    TraceIntegrity,
    TraceSource,
    TraceUsage,
)
from kitaru.imports._normalization import normalize_langfuse_observations
from kitaru.imports._pydantic_ai_replay import (
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    ImportedReplayMode,
    PreparedImportedReplayEvidence,
    PydanticAIReplayBundle,
    PydanticAIReplayEvidence,
    ReplayEvidencePart,
    ReplayObservationEvidence,
    ReplayPartKind,
    build_pydantic_ai_replay_evidence,
)
from kitaru.imports._replay_evidence import (
    CapabilityReadiness,
    EvidenceCaptureStatus,
    EvidenceRedactionStatus,
    ImportedEvidenceArtifactIdentity,
    ImportedReplayEvidenceIdentity,
    ImportedReplayUnsupportedReason,
    ProviderVersionStamp,
    ProviderVersionStampKind,
    RawImportedEvidence,
    RawSourceRow,
    ReplayCapability,
    ReplayDiagnostic,
    ReplayDiagnosticCode,
    ReplayReadinessStatus,
    ReplayReadinessSummary,
    SourceAttribution,
    SourceAttributionStatus,
    build_raw_imported_evidence,
    canonical_json,
    classify_source_attribution,
    extract_langfuse_provider_stamps,
    sha256_canonical_json,
)


def load_imported_replay_evidence(
    execution_id: str,
    *,
    client: Any | None = None,
) -> PreparedImportedReplayEvidence:
    """Load trusted imported replay evidence without eager registration imports."""
    from kitaru.imports._replay_loading import load_imported_replay_evidence as load

    return load(execution_id, client=client)


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
    """Import Langfuse rows without eager Agent registration imports."""
    from kitaru.imports._service import import_langfuse_jsonl as import_rows

    return import_rows(
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


def __getattr__(name: str) -> Any:
    if name == "ImportedReplayEvidenceError":
        from kitaru.imports._replay_loading import ImportedReplayEvidenceError

        return ImportedReplayEvidenceError
    if name in {"ImportOutcomeStatus", "LangfuseImportResult", "TraceImportOutcome"}:
        from kitaru.imports import _service

        return getattr(_service, name)
    if name in {
        "ImportedExecutionResult",
        "ImportedTraceConflictError",
        "ImportedTracePersistenceError",
        "ImportedTraceWriteError",
    }:
        from kitaru.imports import _writer

        return getattr(_writer, name)
    raise AttributeError(name)


__all__ = [
    "CapabilityReadiness",
    "EvidenceCaptureStatus",
    "EvidenceRedactionStatus",
    "ImportOutcomeStatus",
    "ImportedEvidenceArtifactIdentity",
    "ImportedExecutionResult",
    "ImportedObservation",
    "ImportedReplayBoundary",
    "ImportedReplayBoundaryKind",
    "ImportedReplayEvidenceError",
    "ImportedReplayEvidenceIdentity",
    "ImportedReplayMode",
    "ImportedReplayUnsupportedReason",
    "ImportedTrace",
    "ImportedTraceConflictError",
    "ImportedTracePersistenceError",
    "ImportedTraceWriteError",
    "LangfuseImportError",
    "LangfuseImportResult",
    "LangfuseSourceRecord",
    "ObservationKind",
    "ObservationStatus",
    "PreparedImportedReplayEvidence",
    "ProviderVersionStamp",
    "ProviderVersionStampKind",
    "PydanticAIReplayBundle",
    "PydanticAIReplayEvidence",
    "RawImportedEvidence",
    "RawSourceRow",
    "ReplayCapability",
    "ReplayDiagnostic",
    "ReplayDiagnosticCode",
    "ReplayEvidencePart",
    "ReplayObservationEvidence",
    "ReplayPartKind",
    "ReplayReadinessStatus",
    "ReplayReadinessSummary",
    "SourceAttribution",
    "SourceAttributionStatus",
    "SourceObservationType",
    "TraceCost",
    "TraceImportOutcome",
    "TraceIntegrity",
    "TraceSource",
    "TraceUsage",
    "build_pydantic_ai_replay_evidence",
    "build_raw_imported_evidence",
    "canonical_json",
    "classify_source_attribution",
    "extract_langfuse_provider_stamps",
    "import_langfuse_jsonl",
    "load_imported_replay_evidence",
    "normalize_langfuse_observations",
    "read_langfuse_jsonl",
    "read_langfuse_jsonl_records",
    "sha256_canonical_json",
]
