"""Immutable source-attribution and replay-evidence contracts."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from enum import Enum, StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.imports._langfuse import LangfuseSourceRecord, strict_json_loads
from kitaru.imports._models import TraceSource

_MAX_PROVIDER_STAMP_LENGTH = 256
_MAX_PROVIDER_STAMPS = 32
_MAX_ATTRIBUTION_DIAGNOSTICS = 8
_MAX_REPLAY_DIAGNOSTICS = 32
_MAX_DIAGNOSTIC_IDENTIFIER_LENGTH = 256


class ProviderVersionStampKind(StrEnum):
    """Supported Langfuse source-version fields."""

    TRACE_VERSION = "trace_version"
    TRACE_RELEASE = "trace_release"
    GIT_SHA = "git_sha"


class SourceAttributionStatus(StrEnum):
    """How provider evidence supports the declared source AgentVersion."""

    SOURCE_VERIFIED = "source_verified"
    CALLER_ATTRIBUTED = "caller_attributed"
    CONFLICT = "conflict"


class EvidenceCaptureStatus(StrEnum):
    """Whether selected source rows were captured."""

    CAPTURED = "captured"


class EvidenceRedactionStatus(StrEnum):
    """Whether the provider marked any preserved content as redacted."""

    NOT_REDACTED = "not_redacted"
    PARTIALLY_REDACTED = "partially_redacted"
    REDACTED = "redacted"
    UNKNOWN = "unknown"


class ImportedReplayUnsupportedReason(StrEnum):
    """Stable reasons why imported evidence cannot be prepared safely."""

    NOT_IMPORTED = "not_imported"
    LEGACY_IMPORT = "legacy_import"
    IMPORT_INCOMPLETE = "import_incomplete"
    SOURCE_ATTRIBUTION_UNVERIFIED = "source_attribution_unverified"
    SOURCE_BINDING_INVALID = "source_binding_invalid"
    SOURCE_EXECUTION_INVALID = "source_execution_invalid"
    RAW_EVIDENCE_UNAVAILABLE = "raw_evidence_unavailable"
    REPLAY_BUNDLE_UNAVAILABLE = "replay_bundle_unavailable"
    ARTIFACT_SCOPE_MISMATCH = "artifact_scope_mismatch"
    ARTIFACT_ROLE_MISMATCH = "artifact_role_mismatch"
    ARTIFACT_SCHEMA_MISMATCH = "artifact_schema_mismatch"
    ARTIFACT_HASH_MISMATCH = "artifact_hash_mismatch"
    EVIDENCE_INVALID = "evidence_invalid"
    EVIDENCE_REDACTED = "evidence_redacted"
    ROOT_INPUT_MISSING = "root_input_missing"
    REPLAY_PROFILE_UNSUPPORTED = "replay_profile_unsupported"
    READINESS_MISMATCH = "readiness_mismatch"
    MESSAGE_HISTORY_UNAVAILABLE = "message_history_unavailable"
    MESSAGE_HISTORY_INVALID = "message_history_invalid"
    BOUNDARY_UNAVAILABLE = "boundary_unavailable"
    BOUNDARY_INCOMPLETE = "boundary_incomplete"
    TOOL_CONTRACT_INCOMPATIBLE = "tool_contract_incompatible"
    RECORDED_RESPONSE_INVALID = "recorded_response_invalid"
    STREAMING_UNSUPPORTED = "streaming_unsupported"


class ImportedEvidenceArtifactIdentity(BaseModel):
    """One immutable evidence artifact reference trusted for preparation."""

    artifact_id: str
    schema_version: int = Field(ge=1)
    sha256: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("artifact_id")
    @classmethod
    def _require_artifact_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("artifact_id cannot be empty.")
        return normalized

    @field_validator("sha256")
    @classmethod
    def _validate_artifact_digest(cls, value: str) -> str:
        return validate_sha256(value, field_name="sha256")


class ImportedReplayEvidenceIdentity(BaseModel):
    """Verified source and artifact identity for imported replay preparation."""

    execution_id: str
    project_id: str
    source_agent_version_id: str
    source_pipeline_id: str
    source_fingerprint: str
    source_provider: str
    source_project_id: str
    source_trace_id: str
    raw_evidence: ImportedEvidenceArtifactIdentity
    replay_bundle: ImportedEvidenceArtifactIdentity
    replay_profile_version: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator(
        "execution_id",
        "project_id",
        "source_agent_version_id",
        "source_pipeline_id",
        "source_fingerprint",
        "source_provider",
        "source_project_id",
        "source_trace_id",
        "replay_profile_version",
    )
    @classmethod
    def _require_identity_field(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Imported replay identity fields cannot be empty.")
        return normalized

    @model_validator(mode="after")
    def _validate_source_pipeline(self) -> ImportedReplayEvidenceIdentity:
        if self.source_agent_version_id != self.source_pipeline_id:
            raise ValueError("Source AgentVersion and Pipeline IDs must match.")
        return self


class ReplayCapability(StrEnum):
    """Evidence capabilities reported without executing a replay."""

    ROOT_INPUT_CANDIDATE_RERUN = "root_input_candidate_rerun"
    MODEL_MESSAGE_RECONSTRUCTION = "model_message_reconstruction"
    TOOL_RESULT_BOUNDARY_RECONSTRUCTION = "tool_result_boundary_reconstruction"
    RECORDED_RESPONSE_MATCHING = "recorded_response_matching"
    CANDIDATE_TOOL_COMPATIBILITY = "candidate_tool_compatibility"


class ReplayReadinessStatus(StrEnum):
    """Evidence readiness for one future replay capability."""

    READY = "ready"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


class ReplayDiagnosticCode(StrEnum):
    """Stable, non-sensitive replay-evidence diagnostic codes."""

    ROOT_INPUT_MISSING = "root_input_missing"
    MESSAGE_ORDER_AMBIGUOUS = "message_order_ambiguous"
    TOOL_CALL_WITHOUT_RESULT = "tool_call_without_result"
    TOOL_RESULT_WITHOUT_CALL = "tool_result_without_call"
    TOOL_ARGUMENTS_REDACTED = "tool_arguments_redacted"
    CONTENT_REDACTED = "content_redacted"
    CONTENT_INVALID_JSON = "content_invalid_json"
    CONTENT_OMITTED = "content_omitted"
    UNSUPPORTED_MESSAGE_PART = "unsupported_message_part"
    DUPLICATE_TOOL_CALL = "duplicate_tool_call"
    SOURCE_VERSION_CONFLICT = "source_version_conflict"
    SOURCE_VERSION_STAMP_TRUNCATED = "source_version_stamp_truncated"
    SOURCE_VERSION_STAMP_LIMIT_EXCEEDED = "source_version_stamp_limit_exceeded"
    IDENTIFIER_TRUNCATED = "identifier_truncated"
    DIAGNOSTIC_LIMIT_EXCEEDED = "diagnostic_limit_exceeded"
    CANDIDATE_TOOL_CONTRACT_UNKNOWN = "candidate_tool_contract_unknown"


class ProviderVersionStamp(BaseModel):
    """One supported provider version value preserved verbatim."""

    kind: ProviderVersionStampKind
    value: str = Field(max_length=_MAX_PROVIDER_STAMP_LENGTH)
    source_field: str = Field(max_length=64)
    truncated: bool = Field(default=False, exclude=True)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("value", "source_field")
    @classmethod
    def _require_nonempty(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Provider version stamp fields cannot be empty.")
        return normalized


class ReplayDiagnostic(BaseModel):
    """Bounded diagnostic containing no imported user content."""

    code: ReplayDiagnosticCode
    observation_id: str | None = Field(
        default=None, max_length=_MAX_DIAGNOSTIC_IDENTIFIER_LENGTH
    )
    observation_id_truncated: bool = Field(default=False, exclude=True)
    part_kind: str | None = Field(default=None, max_length=64)
    missing_field: str | None = Field(default=None, max_length=64)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="before")
    @classmethod
    def _bound_observation_id(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        observation_id = data.get("observation_id")
        if not isinstance(observation_id, str):
            return data
        normalized = observation_id.strip()
        if len(normalized) <= _MAX_DIAGNOSTIC_IDENTIFIER_LENGTH:
            return data
        bounded = dict(data)
        bounded["observation_id"] = (
            "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        )
        bounded["observation_id_truncated"] = True
        return bounded

    @field_validator("observation_id", "part_kind", "missing_field")
    @classmethod
    def _normalize_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("Diagnostic fields cannot be empty.")
        return normalized


class SourceAttribution(BaseModel):
    """Submission preflight result for one imported trace."""

    status: SourceAttributionStatus
    stamps: tuple[ProviderVersionStamp, ...] = Field(
        default=(), max_length=_MAX_PROVIDER_STAMPS
    )
    diagnostics: tuple[ReplayDiagnostic, ...] = Field(
        default=(), max_length=_MAX_ATTRIBUTION_DIAGNOSTICS
    )

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_conflict_diagnostic(self) -> SourceAttribution:
        has_conflict = any(
            diagnostic.code is ReplayDiagnosticCode.SOURCE_VERSION_CONFLICT
            for diagnostic in self.diagnostics
        )
        if (self.status is SourceAttributionStatus.CONFLICT) != has_conflict:
            raise ValueError(
                "Only conflicting attribution may carry source_version_conflict."
            )
        return self


class RawSourceRow(BaseModel):
    """One selected JSONL row with exact source text and parsed content."""

    line_number: int = Field(ge=1)
    source_order: int = Field(ge=0)
    raw_text: str
    parsed_object: dict[str, Any]

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class RawImportedEvidence(BaseModel):
    """Exact selected source rows and their immutable content digest."""

    schema_version: Literal[1] = 1
    source: TraceSource
    rows: tuple[RawSourceRow, ...]
    raw_content_sha256: str
    capture_status: EvidenceCaptureStatus = EvidenceCaptureStatus.CAPTURED
    redaction_status: EvidenceRedactionStatus

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("raw_content_sha256")
    @classmethod
    def _validate_digest(cls, value: str) -> str:
        return validate_sha256(value, field_name="raw_content_sha256")

    @model_validator(mode="after")
    def _validate_rows(self) -> RawImportedEvidence:
        if not self.rows:
            raise ValueError("Raw imported evidence must contain at least one row.")
        orders = [row.source_order for row in self.rows]
        lines = [row.line_number for row in self.rows]
        if orders != sorted(orders) or len(orders) != len(set(orders)):
            raise ValueError("Raw source rows must have unique ascending source order.")
        if lines != sorted(lines) or len(lines) != len(set(lines)):
            raise ValueError("Raw source rows must have unique ascending line numbers.")
        expected = sha256_text_sequence(row.raw_text for row in self.rows)
        if self.raw_content_sha256 != expected:
            raise ValueError("raw_content_sha256 does not match the preserved rows.")
        for row in self.rows:
            parsed = strict_json_loads(row.raw_text)
            if not isinstance(parsed, dict) or parsed != row.parsed_object:
                raise ValueError(
                    "Raw source text does not match its supplied parsed object."
                )
        return self


class CapabilityReadiness(BaseModel):
    """Evidence status and safe diagnostics for one replay capability."""

    capability: ReplayCapability
    status: ReplayReadinessStatus
    diagnostics: tuple[ReplayDiagnostic, ...] = Field(
        default=(), max_length=_MAX_REPLAY_DIAGNOSTICS
    )

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_ready_capability(self) -> CapabilityReadiness:
        if (
            self.status is ReplayReadinessStatus.READY
            and self.capability is not ReplayCapability.ROOT_INPUT_CANDIDATE_RERUN
        ):
            raise ValueError("Only root-input candidate rerun may be ready.")
        return self


class ReplayReadinessSummary(BaseModel):
    """Capability-specific evidence readiness without imported content."""

    root_input_candidate_rerun: CapabilityReadiness
    model_message_reconstruction: CapabilityReadiness
    tool_result_boundary_reconstruction: CapabilityReadiness
    recorded_response_matching: CapabilityReadiness
    candidate_tool_compatibility: CapabilityReadiness

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_capability_fields(self) -> ReplayReadinessSummary:
        expected = (
            (
                self.root_input_candidate_rerun,
                ReplayCapability.ROOT_INPUT_CANDIDATE_RERUN,
            ),
            (
                self.model_message_reconstruction,
                ReplayCapability.MODEL_MESSAGE_RECONSTRUCTION,
            ),
            (
                self.tool_result_boundary_reconstruction,
                ReplayCapability.TOOL_RESULT_BOUNDARY_RECONSTRUCTION,
            ),
            (
                self.recorded_response_matching,
                ReplayCapability.RECORDED_RESPONSE_MATCHING,
            ),
            (
                self.candidate_tool_compatibility,
                ReplayCapability.CANDIDATE_TOOL_COMPATIBILITY,
            ),
        )
        if any(actual.capability is not capability for actual, capability in expected):
            raise ValueError("Readiness fields must contain their named capability.")
        if (
            self.candidate_tool_compatibility.status
            is not ReplayReadinessStatus.UNKNOWN
        ):
            raise ValueError("Candidate tool compatibility must remain unknown.")
        return self


def canonical_json(value: Any) -> str:
    """Serialize a JSON-compatible value deterministically."""

    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_canonical_json(value: Any) -> str:
    """Return the lowercase SHA-256 of deterministic JSON."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_text_sequence(values: Iterable[str]) -> str:
    """Hash text values exactly and in order without adding separators."""

    digest = hashlib.sha256()
    for value in values:
        digest.update(value.encode("utf-8"))
    return digest.hexdigest()


def validate_sha256(value: str, *, field_name: str) -> str:
    """Validate and return a lowercase SHA-256 digest."""

    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest.")
    return value


def build_raw_imported_evidence(
    *,
    source: TraceSource,
    records: Sequence[LangfuseSourceRecord],
) -> RawImportedEvidence:
    """Build exact raw evidence from selected record-aware reader results."""

    rows = tuple(
        RawSourceRow(
            line_number=int(record.line_number),
            source_order=int(record.source_order),
            raw_text=str(record.raw_text),
            parsed_object=dict(record.row),
        )
        for record in sorted(records, key=lambda item: item.source_order)
    )
    redacted = any(contains_redaction(row.parsed_object) for row in rows)
    return RawImportedEvidence(
        source=source,
        rows=rows,
        raw_content_sha256=sha256_text_sequence(row.raw_text for row in rows),
        redaction_status=(
            EvidenceRedactionStatus.PARTIALLY_REDACTED
            if redacted
            else EvidenceRedactionStatus.NOT_REDACTED
        ),
    )


def extract_langfuse_provider_stamps(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[ProviderVersionStamp, ...]:
    """Extract only allowlisted Langfuse trace version evidence."""

    stamps: list[ProviderVersionStamp] = []
    seen: set[tuple[ProviderVersionStampKind, str, str]] = set()
    for row in rows:
        trace = row.get("trace")
        trace_mapping = trace if isinstance(trace, Mapping) else {}
        for kind, source_field, value in (
            (
                ProviderVersionStampKind.TRACE_VERSION,
                "trace.version",
                _first(row, "traceVersion", "trace_version")
                or trace_mapping.get("version"),
            ),
            (
                ProviderVersionStampKind.TRACE_RELEASE,
                "trace.release",
                _first(row, "traceRelease", "trace_release")
                or trace_mapping.get("release"),
            ),
        ):
            _append_stamp(stamps, seen, kind, source_field, value)

        metadata = _first(row, "traceMetadata", "trace_metadata")
        if not isinstance(metadata, Mapping):
            nested_metadata = trace_mapping.get("metadata")
            metadata = nested_metadata if isinstance(nested_metadata, Mapping) else {}
        _append_stamp(
            stamps,
            seen,
            ProviderVersionStampKind.GIT_SHA,
            "trace.metadata.git_sha",
            metadata.get("git_sha"),
        )
    return tuple(stamps)


def classify_source_attribution(
    stamps: Sequence[ProviderVersionStamp],
    *,
    git_sha: str,
    aliases: Sequence[str],
) -> SourceAttribution:
    """Validate provider stamps against one declared AgentVersion."""

    stamp_limit_exceeded = len(stamps) > _MAX_PROVIDER_STAMPS
    normalized_stamps = tuple(stamps[:_MAX_PROVIDER_STAMPS])
    if not normalized_stamps:
        return SourceAttribution(status=SourceAttributionStatus.CALLER_ATTRIBUTED)

    values_by_kind: dict[ProviderVersionStampKind, set[str]] = defaultdict(set)
    for stamp in normalized_stamps:
        values_by_kind[stamp.kind].add(stamp.value)
    supported_values = {git_sha.strip(), *(alias.strip() for alias in aliases)}
    truncated = any(stamp.truncated for stamp in normalized_stamps)
    conflicts = (
        stamp_limit_exceeded
        or truncated
        or any(len(values) > 1 for values in values_by_kind.values())
        or any(stamp.value not in supported_values for stamp in normalized_stamps)
    )
    if conflicts:
        diagnostics = [
            ReplayDiagnostic(code=ReplayDiagnosticCode.SOURCE_VERSION_CONFLICT)
        ]
        if truncated:
            diagnostics.append(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.SOURCE_VERSION_STAMP_TRUNCATED
                )
            )
        if stamp_limit_exceeded:
            diagnostics.append(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.SOURCE_VERSION_STAMP_LIMIT_EXCEEDED
                )
            )
        return SourceAttribution(
            status=SourceAttributionStatus.CONFLICT,
            stamps=normalized_stamps,
            diagnostics=tuple(diagnostics),
        )
    return SourceAttribution(
        status=SourceAttributionStatus.SOURCE_VERIFIED,
        stamps=normalized_stamps,
    )


def _append_stamp(
    stamps: list[ProviderVersionStamp],
    seen: set[tuple[ProviderVersionStampKind, str, str]],
    kind: ProviderVersionStampKind,
    source_field: str,
    value: Any,
) -> None:
    if not isinstance(value, str):
        return
    normalized = value.strip()
    if not normalized or len(stamps) > _MAX_PROVIDER_STAMPS:
        return
    truncated = len(normalized) > _MAX_PROVIDER_STAMP_LENGTH
    if truncated:
        normalized = "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    identity = (kind, normalized, source_field)
    if identity in seen:
        return
    seen.add(identity)
    stamps.append(
        ProviderVersionStamp(
            kind=kind,
            value=normalized,
            source_field=source_field,
            truncated=truncated,
        )
    )


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
    return None


def contains_redaction(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {
            "[redacted]",
            "<redacted>",
            "__redacted__",
        }
    if isinstance(value, Mapping):
        if value.get("redacted") is True:
            return True
        return any(contains_redaction(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(contains_redaction(item) for item in value)
    return False


def _canonical_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="json"))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value
