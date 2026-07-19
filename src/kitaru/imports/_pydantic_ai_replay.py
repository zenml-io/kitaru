"""PydanticAI replay evidence without a PydanticAI runtime dependency."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.imports._models import ImportedObservation, ImportedTrace, TraceIntegrity
from kitaru.imports._replay_evidence import (
    _MAX_REPLAY_DIAGNOSTICS,
    CapabilityReadiness,
    EvidenceRedactionStatus,
    ImportedReplayEvidenceIdentity,
    RawImportedEvidence,
    ReplayCapability,
    ReplayDiagnostic,
    ReplayDiagnosticCode,
    ReplayReadinessStatus,
    ReplayReadinessSummary,
    contains_redaction,
    sha256_canonical_json,
    validate_sha256,
)

_PYDANTIC_AI_REPLAY_PROFILE_VERSION = "pydantic_ai_replay_v1"


class ReplayPartKind(StrEnum):
    """Explicit structured message and tool parts preserved from the source."""

    SYSTEM_PROMPT = "system_prompt"
    USER_PROMPT = "user_prompt"
    MODEL_TEXT = "model_text"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"


class ImportedReplayMode(StrEnum):
    """Supported ways to start a candidate from imported evidence."""

    ROOT_INPUT = "root_input"
    MESSAGE_HISTORY = "message_history"


class ImportedReplayBoundaryKind(StrEnum):
    """Validated source position selected for a candidate run."""

    ROOT_INPUT = "root_input"
    MODEL_MESSAGE = "model_message"
    TOOL_RESULT = "tool_result"


class ImportedReplayBoundary(BaseModel):
    """Immutable imported evidence position used to start a candidate."""

    kind: ImportedReplayBoundaryKind
    observation_id: str | None = None
    sequence: int | None = Field(default=None, ge=0)
    occurrence: int | None = Field(default=None, ge=0)
    call_id: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("kind", mode="before")
    @classmethod
    def _load_boundary_kind(cls, value: Any) -> ImportedReplayBoundaryKind:
        if isinstance(value, ImportedReplayBoundaryKind):
            return value
        return ImportedReplayBoundaryKind(value)

    @field_validator("observation_id", "call_id")
    @classmethod
    def _normalize_boundary_identity(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("Replay boundary identity fields cannot be empty.")
        return normalized

    @model_validator(mode="after")
    def _validate_boundary(self) -> ImportedReplayBoundary:
        provenance = (self.observation_id, self.sequence, self.occurrence)
        if self.kind is ImportedReplayBoundaryKind.ROOT_INPUT:
            if any(value is not None for value in (*provenance, self.call_id)):
                raise ValueError(
                    "Root-input boundaries cannot carry message provenance."
                )
            return self
        if any(value is None for value in provenance):
            raise ValueError(
                "Message boundaries require complete observation provenance."
            )
        if self.kind is ImportedReplayBoundaryKind.TOOL_RESULT:
            if self.call_id is None:
                raise ValueError("Tool-result boundaries require call_id.")
        elif self.call_id is not None:
            raise ValueError("Model-message boundaries cannot carry call_id.")
        return self


class ReplayEvidencePart(BaseModel):
    """One explicitly structured message or tool part."""

    kind: ReplayPartKind
    observation_id: str
    sequence: int = Field(ge=0)
    occurrence: int = Field(ge=0)
    message_index: int = Field(ge=0)
    call_id: str | None = None
    name: str | None = None
    content: Any = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("observation_id")
    @classmethod
    def _require_observation_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("observation_id cannot be empty.")
        return normalized

    @field_validator("call_id", "name")
    @classmethod
    def _normalize_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def _validate_tool_identity(self) -> ReplayEvidencePart:
        if self.kind in {ReplayPartKind.TOOL_CALL, ReplayPartKind.TOOL_RESULT}:
            if self.call_id is None:
                raise ValueError("Tool evidence requires an explicit call_id.")
        elif self.call_id is not None or self.name is not None:
            raise ValueError("Message text parts cannot carry tool identity.")
        return self


class ReplayObservationEvidence(BaseModel):
    """Provider observation evidence retained in replay order."""

    provider_observation_id: str
    provider_call_id: str | None = None
    parent_observation_id: str | None = None
    sequence: int = Field(ge=0)
    occurrence: int = Field(ge=0)
    kind: str
    input_present: bool
    output_present: bool
    input: Any = None
    output: Any = None
    model: str | None = None
    usage: dict[str, Any] | None = None
    cost: dict[str, Any] | None = None
    started_at: str
    ended_at: str | None = None
    latency_ms: float | None = None
    source_scope: Literal["trace_root", "observation"]
    parts: tuple[ReplayEvidencePart, ...] = ()

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class PydanticAIReplayBundle(BaseModel):
    """Versioned Kitaru wire contract for later PydanticAI replay."""

    schema_version: Literal[1] = 1
    profile_version: Literal["pydantic_ai_replay_v1"] = "pydantic_ai_replay_v1"
    source: dict[str, str]
    root_input_present: bool
    root_input: Any = None
    observations: tuple[ReplayObservationEvidence, ...]
    diagnostics: tuple[ReplayDiagnostic, ...] = Field(
        default=(), max_length=_MAX_REPLAY_DIAGNOSTICS
    )
    raw_evidence_digest: str
    bundle_digest: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("raw_evidence_digest", "bundle_digest")
    @classmethod
    def _validate_digest(cls, value: str, info: Any) -> str:
        return validate_sha256(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_bundle_digest(self) -> PydanticAIReplayBundle:
        payload = self.model_dump(mode="json", exclude={"bundle_digest"})
        if sha256_canonical_json(payload) != self.bundle_digest:
            raise ValueError("bundle_digest does not match the normalized bundle.")
        return self


class PreparedImportedReplayEvidence(BaseModel):
    """Trusted immutable inputs for later PydanticAI replay preparation."""

    identity: ImportedReplayEvidenceIdentity
    raw_evidence: RawImportedEvidence
    replay_bundle: PydanticAIReplayBundle
    readiness: ReplayReadinessSummary

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_prepared_identity(self) -> PreparedImportedReplayEvidence:
        if (
            self.raw_evidence.redaction_status
            is not EvidenceRedactionStatus.NOT_REDACTED
        ):
            raise ValueError("Prepared imported replay evidence cannot be redacted.")
        if (
            self.raw_evidence.source.model_dump(mode="json")
            != self.replay_bundle.source
        ):
            raise ValueError(
                "Raw and normalized replay evidence have different sources."
            )
        if (
            self.raw_evidence.raw_content_sha256
            != self.replay_bundle.raw_evidence_digest
        ):
            raise ValueError("Replay bundle does not bind the loaded raw evidence.")
        if self.identity.raw_evidence.sha256 != self.raw_evidence.raw_content_sha256:
            raise ValueError("Raw evidence identity digest does not match its content.")
        if (
            self.identity.raw_evidence.schema_version
            != self.raw_evidence.schema_version
        ):
            raise ValueError("Raw evidence identity schema does not match its content.")
        if self.identity.replay_bundle.sha256 != self.replay_bundle.bundle_digest:
            raise ValueError(
                "Replay bundle identity digest does not match its content."
            )
        if (
            self.identity.replay_bundle.schema_version
            != self.replay_bundle.schema_version
        ):
            raise ValueError(
                "Replay bundle identity schema does not match its content."
            )
        if self.identity.replay_profile_version != self.replay_bundle.profile_version:
            raise ValueError("Replay profile identity does not match its content.")
        source = self.raw_evidence.source
        if (
            self.identity.source_provider,
            self.identity.source_project_id,
            self.identity.source_trace_id,
        ) != source.identity:
            raise ValueError(
                "Prepared replay source identity does not match its evidence."
            )
        return self


class PydanticAIReplayEvidence(BaseModel):
    """Replay bundle plus its bounded capability summary."""

    bundle: PydanticAIReplayBundle
    readiness: ReplayReadinessSummary

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def build_pydantic_ai_replay_evidence(
    trace: ImportedTrace,
    *,
    raw_evidence: RawImportedEvidence,
) -> PydanticAIReplayEvidence:
    """Derive explicit replay evidence without claiming runtime compatibility."""

    diagnostics: list[ReplayDiagnostic] = []
    parts: list[ReplayEvidencePart] = []
    observations: list[ReplayObservationEvidence] = []
    call_occurrences: dict[str, int] = {}
    call_parts_by_id: dict[str, ReplayEvidencePart] = {}
    result_parts_by_id: dict[str, ReplayEvidencePart] = {}

    if trace.integrity is TraceIntegrity.INVALID or trace.component_count > 1:
        diagnostics.append(
            ReplayDiagnostic(code=ReplayDiagnosticCode.MESSAGE_ORDER_AMBIGUOUS)
        )

    for sequence, observation in enumerate(trace.observations):
        observation_parts = _parts_from_observation(
            observation,
            sequence=sequence,
            diagnostics=diagnostics,
        )
        for part in observation_parts:
            if part.kind is ReplayPartKind.TOOL_CALL and part.call_id is not None:
                call_parts_by_id.setdefault(part.call_id, part)
                call_occurrences[part.call_id] = (
                    call_occurrences.get(part.call_id, 0) + 1
                )
                if call_occurrences[part.call_id] > 1:
                    diagnostics.append(
                        ReplayDiagnostic(
                            code=ReplayDiagnosticCode.DUPLICATE_TOOL_CALL,
                            observation_id=part.observation_id,
                            part_kind=part.kind.value,
                        )
                    )
            elif part.kind is ReplayPartKind.TOOL_RESULT and part.call_id is not None:
                result_parts_by_id.setdefault(part.call_id, part)
        parts.extend(observation_parts)
        observations.append(
            _observation_evidence(
                observation,
                sequence=sequence,
                occurrence=0,
                parts=observation_parts,
                trace=trace,
            )
        )

    call_ids = set(call_occurrences)
    result_call_ids = set(result_parts_by_id)
    for call_id in sorted(call_ids - result_call_ids):
        call_part = call_parts_by_id[call_id]
        diagnostics.append(
            ReplayDiagnostic(
                code=ReplayDiagnosticCode.TOOL_CALL_WITHOUT_RESULT,
                observation_id=call_part.observation_id,
                part_kind=ReplayPartKind.TOOL_CALL.value,
            )
        )
    for call_id in sorted(result_call_ids - call_ids):
        result_part = result_parts_by_id[call_id]
        diagnostics.append(
            ReplayDiagnostic(
                code=ReplayDiagnosticCode.TOOL_RESULT_WITHOUT_CALL,
                observation_id=result_part.observation_id,
                part_kind=ReplayPartKind.TOOL_RESULT.value,
            )
        )

    diagnostics = _deduplicate_diagnostics(diagnostics)
    root_input_redacted = contains_redaction(trace.input)
    root_input_usable = (
        trace.input_present
        and trace.integrity is TraceIntegrity.COMPLETE
        and not root_input_redacted
    )
    if not trace.input_present:
        diagnostics.append(
            ReplayDiagnostic(code=ReplayDiagnosticCode.ROOT_INPUT_MISSING)
        )
    elif root_input_redacted:
        diagnostics.append(ReplayDiagnostic(code=ReplayDiagnosticCode.CONTENT_REDACTED))
    elif trace.integrity is not TraceIntegrity.COMPLETE:
        diagnostics.append(
            ReplayDiagnostic(code=ReplayDiagnosticCode.MESSAGE_ORDER_AMBIGUOUS)
        )
    diagnostics = _deduplicate_diagnostics(diagnostics)

    bundle_values: dict[str, Any] = {
        "schema_version": 1,
        "profile_version": _PYDANTIC_AI_REPLAY_PROFILE_VERSION,
        "source": trace.source.model_dump(mode="json"),
        "root_input_present": trace.input_present,
        "root_input": trace.input,
        "observations": tuple(observations),
        "diagnostics": tuple(diagnostics),
        "raw_evidence_digest": raw_evidence.raw_content_sha256,
    }
    bundle = PydanticAIReplayBundle(
        **bundle_values,
        bundle_digest=sha256_canonical_json(bundle_values),
    )
    return PydanticAIReplayEvidence(
        bundle=bundle,
        readiness=_readiness(
            diagnostics=diagnostics,
            parts=parts,
            root_input_usable=root_input_usable,
        ),
    )


def _observation_evidence(
    observation: ImportedObservation,
    *,
    sequence: int,
    occurrence: int,
    parts: Sequence[ReplayEvidencePart],
    trace: ImportedTrace,
) -> ReplayObservationEvidence:
    provider_call_id = _explicit_call_id(observation.metadata)
    return ReplayObservationEvidence(
        provider_observation_id=observation.id,
        provider_call_id=provider_call_id,
        parent_observation_id=observation.parent_id,
        sequence=sequence,
        occurrence=occurrence,
        kind=observation.kind.value,
        input_present=observation.input_present,
        output_present=observation.output_present,
        input=observation.input,
        output=observation.output,
        model=observation.model,
        usage=(
            observation.usage.model_dump(mode="json")
            if observation.usage is not None
            else None
        ),
        cost=(
            observation.cost.model_dump(mode="json")
            if observation.cost is not None
            else None
        ),
        started_at=observation.started_at.isoformat(),
        ended_at=(
            observation.ended_at.isoformat()
            if observation.ended_at is not None
            else None
        ),
        latency_ms=observation.latency_ms,
        source_scope=(
            "trace_root"
            if observation.id == trace.observations[0].id
            and observation.parent_id is None
            else "observation"
        ),
        parts=tuple(parts),
    )


def _parts_from_observation(
    observation: ImportedObservation,
    *,
    sequence: int,
    diagnostics: list[ReplayDiagnostic],
) -> list[ReplayEvidencePart]:
    messages: list[Mapping[str, Any]] = []
    for payload in (observation.input, observation.output):
        extracted = _explicit_messages(payload)
        messages.extend(extracted)
        if isinstance(payload, str) and payload.lstrip().startswith(("{", "[")):
            diagnostics.append(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.CONTENT_INVALID_JSON,
                    observation_id=observation.id,
                )
            )
    for metadata_field in ("gen_ai.input.messages", "gen_ai.output.messages"):
        messages.extend(_explicit_messages(observation.metadata.get(metadata_field)))

    parts: list[ReplayEvidencePart] = []
    occurrence = 0
    for message_index, message in enumerate(messages):
        role = str(message.get("role", "")).strip().lower()
        content = message.get("content")
        if isinstance(content, list):
            for source_part in content:
                if not isinstance(source_part, Mapping):
                    diagnostics.append(
                        _unsupported_part(observation.id, type(source_part).__name__)
                    )
                    continue
                built = _part_from_mapping(
                    source_part,
                    role=role,
                    observation_id=observation.id,
                    sequence=sequence,
                    occurrence=occurrence,
                    message_index=message_index,
                    diagnostics=diagnostics,
                )
                if built is not None:
                    parts.append(built)
                    occurrence += 1
        elif content is not None:
            built = _text_or_result_part(
                role=role,
                message=message,
                content=content,
                observation_id=observation.id,
                sequence=sequence,
                occurrence=occurrence,
                message_index=message_index,
                diagnostics=diagnostics,
            )
            if built is not None:
                parts.append(built)
                occurrence += 1

        tool_calls = message.get("tool_calls")
        if isinstance(tool_calls, list):
            for tool_call in tool_calls:
                if not isinstance(tool_call, Mapping):
                    diagnostics.append(_unsupported_part(observation.id, "tool_calls"))
                    continue
                built = _tool_call_part(
                    tool_call,
                    observation_id=observation.id,
                    sequence=sequence,
                    occurrence=occurrence,
                    message_index=message_index,
                    diagnostics=diagnostics,
                )
                if built is not None:
                    parts.append(built)
                    occurrence += 1
    return parts


def _explicit_messages(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list) and all(isinstance(item, Mapping) for item in payload):
        return [item for item in payload if isinstance(item.get("role"), str)]
    if isinstance(payload, Mapping):
        messages = payload.get("messages")
        if isinstance(messages, list):
            return [
                item
                for item in messages
                if isinstance(item, Mapping) and isinstance(item.get("role"), str)
            ]
        if isinstance(payload.get("role"), str):
            return [payload]
    return []


def _text_or_result_part(
    *,
    role: str,
    message: Mapping[str, Any],
    content: Any,
    observation_id: str,
    sequence: int,
    occurrence: int,
    message_index: int,
    diagnostics: list[ReplayDiagnostic],
) -> ReplayEvidencePart | None:
    if role == "tool":
        call_id = _normalized_string(
            message.get("tool_call_id") or message.get("call_id")
        )
        if call_id is None:
            diagnostics.append(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.CONTENT_OMITTED,
                    observation_id=observation_id,
                    missing_field="tool_call_id",
                )
            )
            return None
        return ReplayEvidencePart(
            kind=ReplayPartKind.TOOL_RESULT,
            observation_id=observation_id,
            sequence=sequence,
            occurrence=occurrence,
            message_index=message_index,
            call_id=call_id,
            name=_normalized_string(message.get("name")),
            content=content,
        )
    kind = {
        "system": ReplayPartKind.SYSTEM_PROMPT,
        "user": ReplayPartKind.USER_PROMPT,
        "assistant": ReplayPartKind.MODEL_TEXT,
        "model": ReplayPartKind.MODEL_TEXT,
    }.get(role)
    if kind is None:
        diagnostics.append(_unsupported_part(observation_id, role or "missing_role"))
        return None
    return ReplayEvidencePart(
        kind=kind,
        observation_id=observation_id,
        sequence=sequence,
        occurrence=occurrence,
        message_index=message_index,
        content=content,
    )


def _part_from_mapping(
    source: Mapping[str, Any],
    *,
    role: str,
    observation_id: str,
    sequence: int,
    occurrence: int,
    message_index: int,
    diagnostics: list[ReplayDiagnostic],
) -> ReplayEvidencePart | None:
    part_kind = _normalized_string(
        source.get("type") or source.get("part_kind") or source.get("kind")
    )
    normalized_kind = (part_kind or "").lower().replace("-", "_")
    if normalized_kind in {"tool_call", "function_call"}:
        return _tool_call_part(
            source,
            observation_id=observation_id,
            sequence=sequence,
            occurrence=occurrence,
            message_index=message_index,
            diagnostics=diagnostics,
        )
    if normalized_kind in {"tool_result", "tool_return"}:
        call_id = _normalized_string(
            source.get("tool_call_id") or source.get("call_id") or source.get("id")
        )
        if call_id is None:
            diagnostics.append(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.CONTENT_OMITTED,
                    observation_id=observation_id,
                    part_kind=part_kind,
                    missing_field="call_id",
                )
            )
            return None
        return ReplayEvidencePart(
            kind=ReplayPartKind.TOOL_RESULT,
            observation_id=observation_id,
            sequence=sequence,
            occurrence=occurrence,
            message_index=message_index,
            call_id=call_id,
            name=_normalized_string(source.get("tool_name") or source.get("name")),
            content=source.get("content", source.get("result")),
        )
    if normalized_kind in {"text", "input_text", "output_text"}:
        return _text_or_result_part(
            role=role,
            message=source,
            content=source.get("text", source.get("content")),
            observation_id=observation_id,
            sequence=sequence,
            occurrence=occurrence,
            message_index=message_index,
            diagnostics=diagnostics,
        )
    diagnostics.append(_unsupported_part(observation_id, part_kind or "unknown"))
    return None


def _tool_call_part(
    source: Mapping[str, Any],
    *,
    observation_id: str,
    sequence: int,
    occurrence: int,
    message_index: int,
    diagnostics: list[ReplayDiagnostic],
) -> ReplayEvidencePart | None:
    function = source.get("function")
    function_mapping = function if isinstance(function, Mapping) else {}
    call_id = _normalized_string(
        source.get("id") or source.get("tool_call_id") or source.get("call_id")
    )
    name = _normalized_string(
        source.get("name") or source.get("tool_name") or function_mapping.get("name")
    )
    arguments = source.get("arguments", function_mapping.get("arguments"))
    if call_id is None or name is None:
        diagnostics.append(
            ReplayDiagnostic(
                code=ReplayDiagnosticCode.CONTENT_OMITTED,
                observation_id=observation_id,
                part_kind=ReplayPartKind.TOOL_CALL.value,
                missing_field="call_id" if call_id is None else "name",
            )
        )
        return None
    if contains_redaction(arguments):
        diagnostics.append(
            ReplayDiagnostic(
                code=ReplayDiagnosticCode.TOOL_ARGUMENTS_REDACTED,
                observation_id=observation_id,
                part_kind=ReplayPartKind.TOOL_CALL.value,
            )
        )
    return ReplayEvidencePart(
        kind=ReplayPartKind.TOOL_CALL,
        observation_id=observation_id,
        sequence=sequence,
        occurrence=occurrence,
        message_index=message_index,
        call_id=call_id,
        name=name,
        content=arguments,
    )


def _readiness(
    *,
    diagnostics: Sequence[ReplayDiagnostic],
    parts: Sequence[ReplayEvidencePart],
    root_input_usable: bool,
) -> ReplayReadinessSummary:
    blocking_message_codes = {
        ReplayDiagnosticCode.MESSAGE_ORDER_AMBIGUOUS,
        ReplayDiagnosticCode.CONTENT_REDACTED,
        ReplayDiagnosticCode.CONTENT_INVALID_JSON,
        ReplayDiagnosticCode.CONTENT_OMITTED,
        ReplayDiagnosticCode.UNSUPPORTED_MESSAGE_PART,
        ReplayDiagnosticCode.DUPLICATE_TOOL_CALL,
        ReplayDiagnosticCode.IDENTIFIER_TRUNCATED,
        ReplayDiagnosticCode.DIAGNOSTIC_LIMIT_EXCEEDED,
    }
    message_diagnostics = tuple(
        diagnostic
        for diagnostic in diagnostics
        if diagnostic.code in blocking_message_codes
    )
    has_message_parts = any(
        part.kind
        in {
            ReplayPartKind.SYSTEM_PROMPT,
            ReplayPartKind.USER_PROMPT,
            ReplayPartKind.MODEL_TEXT,
        }
        for part in parts
    )
    tool_diagnostics = tuple(
        diagnostic
        for diagnostic in diagnostics
        if diagnostic.code
        in {
            ReplayDiagnosticCode.TOOL_CALL_WITHOUT_RESULT,
            ReplayDiagnosticCode.TOOL_RESULT_WITHOUT_CALL,
            ReplayDiagnosticCode.TOOL_ARGUMENTS_REDACTED,
            ReplayDiagnosticCode.DUPLICATE_TOOL_CALL,
            ReplayDiagnosticCode.MESSAGE_ORDER_AMBIGUOUS,
            ReplayDiagnosticCode.IDENTIFIER_TRUNCATED,
            ReplayDiagnosticCode.DIAGNOSTIC_LIMIT_EXCEEDED,
        }
    )
    has_tool_call = any(part.kind is ReplayPartKind.TOOL_CALL for part in parts)
    has_model_text = any(part.kind is ReplayPartKind.MODEL_TEXT for part in parts)
    return ReplayReadinessSummary(
        root_input_candidate_rerun=CapabilityReadiness(
            capability=ReplayCapability.ROOT_INPUT_CANDIDATE_RERUN,
            status=(
                ReplayReadinessStatus.READY
                if root_input_usable
                else ReplayReadinessStatus.UNSUPPORTED
            ),
            diagnostics=tuple(
                diagnostic
                for diagnostic in diagnostics
                if diagnostic.code
                in {
                    ReplayDiagnosticCode.ROOT_INPUT_MISSING,
                    ReplayDiagnosticCode.CONTENT_REDACTED,
                    ReplayDiagnosticCode.MESSAGE_ORDER_AMBIGUOUS,
                }
            ),
        ),
        model_message_reconstruction=CapabilityReadiness(
            capability=ReplayCapability.MODEL_MESSAGE_RECONSTRUCTION,
            status=(
                ReplayReadinessStatus.UNKNOWN
                if has_message_parts and not message_diagnostics
                else ReplayReadinessStatus.UNSUPPORTED
            ),
            diagnostics=message_diagnostics,
        ),
        tool_result_boundary_reconstruction=CapabilityReadiness(
            capability=ReplayCapability.TOOL_RESULT_BOUNDARY_RECONSTRUCTION,
            status=(
                ReplayReadinessStatus.UNKNOWN
                if has_tool_call and not tool_diagnostics
                else ReplayReadinessStatus.UNSUPPORTED
            ),
            diagnostics=tool_diagnostics,
        ),
        recorded_response_matching=CapabilityReadiness(
            capability=ReplayCapability.RECORDED_RESPONSE_MATCHING,
            status=(
                ReplayReadinessStatus.UNKNOWN
                if has_model_text and not message_diagnostics
                else ReplayReadinessStatus.UNSUPPORTED
            ),
            diagnostics=message_diagnostics,
        ),
        candidate_tool_compatibility=CapabilityReadiness(
            capability=ReplayCapability.CANDIDATE_TOOL_COMPATIBILITY,
            status=ReplayReadinessStatus.UNKNOWN,
            diagnostics=(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.CANDIDATE_TOOL_CONTRACT_UNKNOWN
                ),
            ),
        ),
    )


def _explicit_call_id(metadata: Mapping[str, Any]) -> str | None:
    for key in ("call_id", "tool_call_id", "gen_ai.request.id", "gen_ai.response.id"):
        value = _normalized_string(metadata.get(key))
        if value is not None:
            return value
    return None


def _unsupported_part(observation_id: str, part_kind: str) -> ReplayDiagnostic:
    return ReplayDiagnostic(
        code=ReplayDiagnosticCode.UNSUPPORTED_MESSAGE_PART,
        observation_id=observation_id,
        part_kind=part_kind[:64],
    )


def _normalized_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _deduplicate_diagnostics(
    diagnostics: Sequence[ReplayDiagnostic],
) -> list[ReplayDiagnostic]:
    deduplicated: list[ReplayDiagnostic] = []
    seen: set[tuple[str, str | None, str | None, str | None]] = set()
    for diagnostic in diagnostics:
        identity = (
            diagnostic.code.value,
            diagnostic.observation_id,
            diagnostic.part_kind,
            diagnostic.missing_field,
        )
        if identity not in seen:
            seen.add(identity)
            deduplicated.append(diagnostic)
        if diagnostic.observation_id_truncated:
            truncation = ReplayDiagnostic(
                code=ReplayDiagnosticCode.IDENTIFIER_TRUNCATED,
                observation_id=diagnostic.observation_id,
            )
            truncation_identity = (
                truncation.code.value,
                truncation.observation_id,
                truncation.part_kind,
                truncation.missing_field,
            )
            if truncation_identity not in seen:
                seen.add(truncation_identity)
                deduplicated.append(truncation)
    if len(deduplicated) <= _MAX_REPLAY_DIAGNOSTICS:
        return deduplicated
    return [
        *deduplicated[: _MAX_REPLAY_DIAGNOSTICS - 1],
        ReplayDiagnostic(code=ReplayDiagnosticCode.DIAGNOSTIC_LIMIT_EXCEEDED),
    ]
