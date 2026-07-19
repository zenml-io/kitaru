"""Versioned experiment persistence and planning contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from kitaru._experiments._limits import RegressionLimits
from kitaru.errors import KitaruUsageError
from kitaru.imports._pydantic_ai_replay import (
    ImportedReplayBoundary,
    ImportedReplayMode,
)
from kitaru.imports._replay_evidence import ImportedReplayEvidenceIdentity
from kitaru.replay import (
    _MAX_IMPORTED_REPLAY_TOOL_DECISIONS,
    ReplayPlan,
    ReplayPlanDocument,
)
from kitaru.replay_context import ReplayRuntimeContext
from kitaru.scoring import (
    EvidenceManifestReference,
    GroundedPolicySnapshot,
    ImportedReplayComparability,
    ImportedReplayEvidenceSummary,
    OperationalLimitOutcome,
    ScoreAggregateReference,
    ScorerIdentity,
    ScorerSnapshot,
    VerdictPolicy,
    VerdictResult,
)

_INLINE_TARGET_LIMIT = 500
_MAX_ISSUE_SUMMARIES = 50
_TERMINAL_STATUSES = frozenset({"completed", "partial", "failed", "cancelled"})
ExperimentStatus = Literal[
    "pending", "running", "completed", "partial", "failed", "cancelled"
]
CoveragePolicy = Literal["fail", "skip", "top"]


def _required_string(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return normalized


def _timestamp(value: str, *, field_name: str) -> str:
    normalized = _required_string(value, field_name=field_name)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO 8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone.")
    return normalized


def _canonical_json(value: JsonValue | Mapping[str, Any] | Sequence[Any]) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise KitaruUsageError(
            "Experiment requests must contain canonically JSON-serializable values."
        ) from exc


def _sha256(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _validate_sha256(value: str) -> str:
    normalized = _required_string(value, field_name="SHA-256")
    prefix, separator, digest = normalized.partition(":")
    if separator != ":" or prefix != "sha256" or len(digest) != 64:
        raise ValueError("SHA-256 values must use the sha256:<64 hex characters> form.")
    try:
        int(digest, 16)
    except ValueError as exc:
        raise ValueError("SHA-256 values must contain hexadecimal characters.") from exc
    return normalized


class InlineTargetMembership(BaseModel):
    """Ordered experiment membership stored directly in Project metadata."""

    schema_version: Literal[1] = 1
    storage: Literal["inline"] = "inline"
    execution_ids: list[str]
    count: int = Field(ge=1, le=_INLINE_TARGET_LIMIT)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("execution_ids")
    @classmethod
    def _validate_ids(cls, value: list[str]) -> list[str]:
        normalized = [
            _required_string(item, field_name="Target execution ID") for item in value
        ]
        if len(normalized) != len(set(normalized)):
            raise ValueError("Target execution IDs must be unique.")
        return normalized

    @model_validator(mode="after")
    def _validate_count(self) -> InlineTargetMembership:
        if self.count != len(self.execution_ids):
            raise ValueError("Inline target count must match the ordered ID list.")
        return self


class ArtifactTargetMembership(BaseModel):
    """Reference to a verified immutable ordered target manifest."""

    schema_version: Literal[1] = 1
    storage: Literal["artifact"] = "artifact"
    artifact_version_id: str
    count: int = Field(gt=_INLINE_TARGET_LIMIT)
    sha256: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("artifact_version_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return _required_string(value, field_name="Artifact version ID")

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _validate_sha256(value)


TargetMembership = Annotated[
    InlineTargetMembership | ArtifactTargetMembership,
    Field(discriminator="storage"),
]


class ExperimentExecutable(BaseModel):
    """Exact registered executable frozen into an experiment attempt."""

    kind: Literal["entrypoint"] = "entrypoint"
    entrypoint: str
    repo_root_marker: str = ".kitaru"

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("entrypoint", "repo_root_marker")
    @classmethod
    def _validate_strings(cls, value: str) -> str:
        return _required_string(value, field_name="Executable field")


class ReplayRequestInputs(BaseModel):
    """Exact user replay inputs shared by every target plan."""

    flow_overrides: dict[str, JsonValue] = Field(default_factory=dict)
    checkpoint_overrides: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)
    invocation_overrides: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)
    skip: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class FrozenReplayPlan(BaseModel):
    """Typed replay-engine inputs frozen into the V1 experiment spec."""

    plan_type: Literal["native"] = "native"
    replay_from_start: bool
    resolved_at: str | None
    steps_to_skip: list[str]
    input_overrides: dict[str, JsonValue]
    step_input_overrides: dict[str, dict[str, JsonValue]]
    runtime_context: ReplayRuntimeContext
    document: ReplayPlanDocument

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("runtime_context", mode="before")
    @classmethod
    def _validate_runtime_context(cls, value: Any) -> ReplayRuntimeContext:
        if isinstance(value, ReplayRuntimeContext):
            return value
        if isinstance(value, Mapping):
            return ReplayRuntimeContext.from_json(_canonical_json(value))
        raise ValueError("Frozen replay runtime_context must be an object.")

    @field_validator("document", mode="before")
    @classmethod
    def _validate_document(cls, value: Any) -> ReplayPlanDocument:
        if isinstance(value, ReplayPlanDocument):
            return value
        if isinstance(value, Mapping):
            return ReplayPlanDocument.from_json(value)
        raise ValueError("Frozen replay document must be an object.")

    @field_serializer("runtime_context")
    def _serialize_runtime_context(
        self, value: ReplayRuntimeContext
    ) -> dict[str, JsonValue]:
        return cast(dict[str, JsonValue], json.loads(value.to_json()))

    @field_serializer("document")
    def _serialize_document(self, value: ReplayPlanDocument) -> dict[str, JsonValue]:
        return cast(dict[str, JsonValue], deepcopy(value.to_json()))

    @model_validator(mode="after")
    def _validate_start_shape(self) -> FrozenReplayPlan:
        if self.replay_from_start and self.resolved_at is not None:
            raise ValueError("Replay-from-start plans cannot retain a checkpoint.")
        if not self.replay_from_start and not self.resolved_at:
            raise ValueError("Checkpoint replay plans require a resolved checkpoint.")
        _canonical_json(self.document.to_json())
        json.loads(self.runtime_context.to_json())
        return self

    @classmethod
    def freeze(
        cls,
        plan: ReplayPlan,
        *,
        replay_from_start: bool,
        resolved_at: str | None,
    ) -> FrozenReplayPlan:
        """Freeze one replay-engine plan without discarding its nested types."""
        return cls(
            replay_from_start=replay_from_start,
            resolved_at=resolved_at,
            steps_to_skip=sorted(str(item) for item in plan.steps_to_skip),
            input_overrides=deepcopy(plan.input_overrides),
            step_input_overrides=deepcopy(plan.step_input_overrides),
            runtime_context=plan.runtime_context,
            document=deepcopy(plan.document),
        )

    def thaw(self, *, target_execution_id: str) -> ReplayPlan:
        """Rebuild the existing replay-engine plan for one target."""
        return ReplayPlan(
            original_run_id=target_execution_id,
            steps_to_skip=set(self.steps_to_skip),
            input_overrides=deepcopy(self.input_overrides),
            step_input_overrides=deepcopy(self.step_input_overrides),
            runtime_context=deepcopy(self.runtime_context),
            document=deepcopy(self.document),
        )


class FrozenImportedReplayPlan(BaseModel):
    """Exact imported evidence and candidate start policy frozen for one target."""

    plan_type: Literal["imported"] = "imported"
    mode: ImportedReplayMode
    boundary: ImportedReplayBoundary
    evidence_identity: ImportedReplayEvidenceIdentity
    serving: Literal["recorded_responses"] = "recorded_responses"
    on_miss: Literal["blocked"] = "blocked"
    require_recorded_path_comparability: bool = True

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("mode", mode="before")
    @classmethod
    def _load_imported_mode(cls, value: Any) -> ImportedReplayMode:
        if isinstance(value, ImportedReplayMode):
            return value
        return ImportedReplayMode(value)

    @model_validator(mode="after")
    def _validate_mode_boundary(self) -> FrozenImportedReplayPlan:
        is_root = self.boundary.kind.value == "root_input"
        if (self.mode is ImportedReplayMode.ROOT_INPUT) != is_root:
            raise ValueError(
                "Imported replay mode and boundary must describe the same start."
            )
        return self


FrozenExperimentReplayPlan = Annotated[
    FrozenReplayPlan | FrozenImportedReplayPlan,
    Field(discriminator="plan_type"),
]


class ImportedReplayToolDecision(BaseModel):
    """One value-free recorded-response serving decision."""

    index: int = Field(ge=0)
    decision: Literal["hit", "blocked"]
    logical_tool_id: str
    candidate_tool_name: str
    block_reason: str | None = None
    source_observation_id: str | None = None
    source_sequence: int | None = Field(default=None, ge=0)
    source_occurrence: int | None = Field(default=None, ge=0)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ImportedReplayMemberEvidence(BaseModel):
    """Immutable evidence and lineage for one imported experiment child."""

    schema_version: Literal[1] = 1
    experiment_id: str
    target_execution_id: str
    repeat_index: int = Field(ge=0)
    child_execution_id: str
    candidate_status: Literal["completed", "failed"]
    source_agent_version_id: str
    candidate_agent_version_id: str
    mode: ImportedReplayMode
    boundary: ImportedReplayBoundary
    serving: Literal["recorded_responses"] = "recorded_responses"
    on_miss: Literal["blocked"] = "blocked"
    prefix_complete: bool
    candidate_tool_contract_compatible: bool
    eligible_recorded_responses: int = Field(ge=0)
    recorded_response_hits: int = Field(ge=0)
    recorded_response_misses: int = Field(ge=0)
    blocked_calls: int = Field(ge=0)
    path_diverged: bool
    comparability: ImportedReplayComparability
    parent_execution_id: str
    root_execution_id: str
    decisions: tuple[ImportedReplayToolDecision, ...] = Field(
        default=(),
        max_length=_MAX_IMPORTED_REPLAY_TOOL_DECISIONS,
    )
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("mode", mode="before")
    @classmethod
    def _load_member_mode(cls, value: Any) -> ImportedReplayMode:
        if isinstance(value, ImportedReplayMode):
            return value
        return ImportedReplayMode(value)

    @field_validator("comparability", mode="before")
    @classmethod
    def _load_member_comparability(cls, value: Any) -> ImportedReplayComparability:
        if isinstance(value, ImportedReplayComparability):
            return value
        return ImportedReplayComparability(value)

    @field_validator("decisions", mode="before")
    @classmethod
    def _load_decisions(cls, value: Any) -> Any:
        return tuple(value) if isinstance(value, list) else value

    @field_validator(
        "experiment_id",
        "target_execution_id",
        "child_execution_id",
        "source_agent_version_id",
        "candidate_agent_version_id",
        "parent_execution_id",
        "root_execution_id",
    )
    @classmethod
    def _validate_member_ids(cls, value: str) -> str:
        return _required_string(value, field_name="Imported replay evidence field")

    @field_validator("content_hash")
    @classmethod
    def _validate_member_hash(cls, value: str) -> str:
        return _validate_sha256(value)

    @model_validator(mode="after")
    def _validate_member(self) -> ImportedReplayMemberEvidence:
        if self.recorded_response_hits + self.recorded_response_misses > (
            self.eligible_recorded_responses
        ):
            raise ValueError("Recorded response decisions exceed eligible calls.")
        if [item.index for item in self.decisions] != list(range(len(self.decisions))):
            raise ValueError("Imported replay decisions must use contiguous order.")
        expected = _sha256(
            _canonical_json(self.model_dump(mode="json", exclude={"content_hash"}))
        )
        if self.content_hash != expected:
            raise ValueError(
                "Imported replay member content_hash does not match payload."
            )
        return self

    @classmethod
    def create(cls, **values: Any) -> ImportedReplayMemberEvidence:
        """Create content-addressed member evidence."""
        provisional = cls.model_construct(
            schema_version=1,
            content_hash="sha256:" + "0" * 64,
            **values,
        )
        python_payload = provisional.model_dump(mode="python", exclude={"content_hash"})
        json_payload = provisional.model_dump(mode="json", exclude={"content_hash"})
        return cls(
            **python_payload,
            content_hash=_sha256(_canonical_json(json_payload)),
        )


class TargetPlanningRow(BaseModel):
    """One immutable target-level coverage and replay planning decision."""

    target_execution_id: str
    parent_execution_id: str | None
    root_execution_id: str
    checkpoint_covered: bool
    disposition: Literal["replay", "skip", "top", "imported"]
    reason: str | None = None
    replay_plan: FrozenExperimentReplayPlan | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("target_execution_id", "root_execution_id")
    @classmethod
    def _validate_required_ids(cls, value: str) -> str:
        return _required_string(value, field_name="Execution ID")

    @model_validator(mode="before")
    @classmethod
    def _upgrade_legacy_native_plan(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        replay_plan = value.get("replay_plan")
        if isinstance(replay_plan, Mapping) and "plan_type" not in replay_plan:
            upgraded = dict(value)
            upgraded["replay_plan"] = {"plan_type": "native", **replay_plan}
            return upgraded
        return value

    @model_validator(mode="after")
    def _validate_disposition(self) -> TargetPlanningRow:
        if self.disposition == "replay":
            if not self.checkpoint_covered or self.replay_plan is None:
                raise ValueError("Covered replay targets require a replay plan.")
            if not isinstance(self.replay_plan, FrozenReplayPlan):
                raise ValueError("Native replay targets require a native replay plan.")
            if self.replay_plan.replay_from_start:
                raise ValueError("Covered targets cannot use replay-from-start.")
        elif self.disposition == "imported":
            if not self.checkpoint_covered or not isinstance(
                self.replay_plan, FrozenImportedReplayPlan
            ):
                raise ValueError(
                    "Imported targets require a frozen imported replay plan."
                )
            if self.parent_execution_id != self.target_execution_id:
                raise ValueError(
                    "Imported targets must be their child's immediate parent."
                )
        elif self.disposition == "skip":
            if self.checkpoint_covered or self.replay_plan is not None:
                raise ValueError("Skipped targets must be uncovered and plan-free.")
            if not self.reason:
                raise ValueError("Skipped targets require an audit reason.")
        else:
            if self.checkpoint_covered or self.replay_plan is None:
                raise ValueError("Top-policy targets require a from-start plan.")
            if not isinstance(self.replay_plan, FrozenReplayPlan):
                raise ValueError("Top-policy targets require a native replay plan.")
            if not self.replay_plan.replay_from_start:
                raise ValueError("Top-policy targets must use replay-from-start.")
            if not self.reason:
                raise ValueError("Top-policy targets require an audit reason.")
        return self


class ForkCoverage(BaseModel):
    """Frozen checkpoint coverage summary."""

    selected: int = Field(ge=1)
    covered: int = Field(ge=0)
    policy: CoveragePolicy

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_coverage(self) -> ForkCoverage:
        if self.covered > self.selected:
            raise ValueError("Covered targets cannot exceed selected targets.")
        return self


class CohortRankingRow(BaseModel):
    """One frozen cohort ranking row."""

    execution_id: str
    sort_value: float | None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CohortAudit(BaseModel):
    """Selection evidence copied from a frozen CohortResult."""

    flow: str
    at: str
    deployment: str | None
    deployment_version: int | None
    order_by: str
    scanned: int = Field(ge=0)
    matched: int = Field(ge=1)
    partial: bool
    filtered: dict[str, int]
    ranked: list[CohortRankingRow] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ExperimentSpec(BaseModel):
    """Immutable, versioned replay attempt specification."""

    schema_version: Literal[1] = 1
    experiment_id: str
    kind: Literal["replay"] = "replay"
    name: str | None = None
    display_name: str
    suite_key: str
    source_experiment_id: str | None = None
    idempotency_key: str
    created_at: str
    candidate_project_id: str
    candidate_agent_version_id: str
    candidate_pipeline_id: str
    executable: ExperimentExecutable
    target_membership: TargetMembership
    replay_inputs: ReplayRequestInputs
    at: str
    repeats: int = Field(ge=1)
    wait: bool
    on_error: Literal["collect", "fail"]
    coverage: ForkCoverage
    planning_rows: list[TargetPlanningRow]
    cohort_audit: CohortAudit | None = None
    scorers: list[ScorerSnapshot] = Field(default_factory=list)
    evidence_manifest: EvidenceManifestReference | None = None
    grounded_policy: GroundedPolicySnapshot | None = None
    verdict_policy: VerdictPolicy | None = None
    regression_limits: RegressionLimits | None = None
    request_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator(
        "experiment_id",
        "display_name",
        "suite_key",
        "idempotency_key",
        "candidate_project_id",
        "candidate_agent_version_id",
        "candidate_pipeline_id",
        "at",
    )
    @classmethod
    def _validate_required_strings(cls, value: str) -> str:
        return _required_string(value, field_name="Experiment field")

    @field_validator("source_experiment_id")
    @classmethod
    def _validate_source_experiment_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _required_string(value, field_name="Source experiment ID")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _required_string(value, field_name="Experiment name")

    @field_validator("created_at")
    @classmethod
    def _validate_created_at(cls, value: str) -> str:
        return _timestamp(value, field_name="created_at")

    @field_validator("request_hash")
    @classmethod
    def _validate_request_hash(cls, value: str) -> str:
        return _validate_sha256(value)

    @model_validator(mode="after")
    def _validate_contract(self) -> ExperimentSpec:
        if self.candidate_agent_version_id != self.candidate_pipeline_id:
            raise ValueError(
                "Candidate AgentVersion and Pipeline IDs must be identical."
            )
        count = self.target_membership.count
        if (
            self.regression_limits is not None
            and count * self.repeats > self.regression_limits.max_trials
        ):
            raise ValueError("Replay trial count exceeds the frozen max_trials limit.")
        if self.coverage.selected != count or len(self.planning_rows) != count:
            raise ValueError(
                "Coverage, planning rows, and target membership must have equal counts."
            )
        ordered_row_ids = [row.target_execution_id for row in self.planning_rows]
        if (
            isinstance(self.target_membership, InlineTargetMembership)
            and ordered_row_ids != self.target_membership.execution_ids
        ):
            raise ValueError("Planning rows must preserve target membership order.")
        if len(ordered_row_ids) != len(set(ordered_row_ids)):
            raise ValueError("Planning rows must contain one row per target.")
        if self.coverage.covered != sum(
            row.checkpoint_covered for row in self.planning_rows
        ):
            raise ValueError("Coverage count must match target dispositions.")
        if self.evidence_manifest is not None and self.evidence_manifest.count != count:
            raise ValueError("Evidence manifest count must match target membership.")
        if any(
            not row.checkpoint_covered and row.disposition == "replay"
            for row in self.planning_rows
        ):
            raise ValueError("Uncovered targets require an explicit disposition.")
        _validate_verdict_scorers(self.scorers, self.verdict_policy)
        expected = experiment_request_hash(self)
        if self.request_hash != expected:
            raise ValueError(
                "Experiment request_hash does not match the frozen request."
            )
        return self


ReplayExperimentSpec = ExperimentSpec


class ScoreRequestInputs(BaseModel):
    """Exact user score-only request inputs frozen for idempotency."""

    comparative: bool = False
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    grounded_policy: GroundedPolicySnapshot | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ScoreExperimentSpec(BaseModel):
    """Immutable, versioned score-only attempt specification."""

    schema_version: Literal[1] = 1
    experiment_id: str
    kind: Literal["score"] = "score"
    name: str | None = None
    display_name: str
    suite_key: str
    idempotency_key: str
    created_at: str
    candidate_project_id: str
    target_membership: TargetMembership
    scorers: list[ScorerSnapshot] = Field(min_length=1)
    evidence_manifest: EvidenceManifestReference
    request_inputs: ScoreRequestInputs = Field(default_factory=ScoreRequestInputs)
    verdict_policy: VerdictPolicy | None = None
    request_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator(
        "experiment_id",
        "display_name",
        "suite_key",
        "idempotency_key",
        "candidate_project_id",
    )
    @classmethod
    def _validate_required_strings(cls, value: str) -> str:
        return _required_string(value, field_name="Score experiment field")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _required_string(value, field_name="Score experiment name")

    @field_validator("created_at")
    @classmethod
    def _validate_created_at(cls, value: str) -> str:
        return _timestamp(value, field_name="created_at")

    @field_validator("request_hash")
    @classmethod
    def _validate_request_hash(cls, value: str) -> str:
        return _validate_sha256(value)

    @model_validator(mode="after")
    def _validate_contract(self) -> ScoreExperimentSpec:
        if self.evidence_manifest.count != self.target_membership.count:
            raise ValueError("Evidence manifest count must match target membership.")
        scorer_keys = [
            (scorer.name, scorer.revision, scorer.configuration_hash)
            for scorer in self.scorers
        ]
        if len(scorer_keys) != len(set(scorer_keys)):
            raise ValueError(
                "Score experiment scorers must be unique by revision/config."
            )
        _validate_verdict_scorers(self.scorers, self.verdict_policy)
        expected = experiment_request_hash(self)
        if self.request_hash != expected:
            raise ValueError(
                "Experiment request_hash does not match the frozen request."
            )
        return self


ExperimentSpecRecord = Annotated[
    ExperimentSpec | ScoreExperimentSpec,
    Field(discriminator="kind"),
]


class ExperimentCounts(BaseModel):
    """Cached bounded submission and membership totals."""

    target_count: int = Field(ge=1)
    intended: int = Field(ge=1)
    submitted: int = Field(ge=0, default=0)
    verified: int = Field(ge=0, default=0)
    skipped: int = Field(ge=0, default=0)
    failed: int = Field(ge=0, default=0)
    unverified: int = Field(ge=0, default=0)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_counts(self) -> ExperimentCounts:
        if self.submitted + self.skipped + self.failed > self.intended:
            raise ValueError("Experiment outcome counts exceed the frozen denominator.")
        if self.verified > self.submitted or self.unverified > self.submitted:
            raise ValueError("Membership counts cannot exceed submitted children.")
        if self.verified + self.unverified > self.submitted:
            raise ValueError("Verified and unverified children overlap.")
        return self


class ExperimentIssue(BaseModel):
    """Bounded audit summary for one skipped, failed, or unverified row."""

    target_execution_id: str | None = None
    repeat_index: int | None = Field(default=None, ge=0)
    child_execution_id: str | None = None
    reason: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("reason")
    @classmethod
    def _validate_reason(cls, value: str) -> str:
        return _required_string(value, field_name="Issue reason")


class ExperimentRecord(BaseModel):
    """Durable experiment specification plus monotonic cached outcome state."""

    schema_version: Literal[1] = 1
    spec: ExperimentSpecRecord
    status: ExperimentStatus = "pending"
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    counts: ExperimentCounts
    errors: list[ExperimentIssue] = Field(default_factory=list)
    skips: list[ExperimentIssue] = Field(default_factory=list)
    unverified_children: list[ExperimentIssue] = Field(default_factory=list)
    score_aggregate: ScoreAggregateReference | None = None
    operational_limit: OperationalLimitOutcome | None = None
    imported_replay_members: list[ImportedReplayMemberEvidence] = Field(
        default_factory=list
    )
    imported_replay_evidence: ImportedReplayEvidenceSummary | None = None
    verdict: VerdictResult | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("created_at", "updated_at")
    @classmethod
    def _validate_required_timestamps(cls, value: str) -> str:
        return _timestamp(value, field_name="Experiment timestamp")

    @field_validator("started_at", "finished_at")
    @classmethod
    def _validate_optional_timestamps(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _timestamp(value, field_name="Experiment timestamp")

    @model_validator(mode="after")
    def _validate_record(self) -> ExperimentRecord:
        if self.created_at != self.spec.created_at:
            raise ValueError("Record creation time must match the immutable spec.")
        if self.counts.target_count != self.spec.target_membership.count:
            raise ValueError("Cached target count must match immutable membership.")
        expected_intended = (
            self.counts.target_count * self.spec.repeats
            if self.spec.kind == "replay"
            else self.counts.target_count * len(self.spec.scorers)
        )
        if self.counts.intended != expected_intended:
            raise ValueError(
                "Cached denominator must equal targets multiplied by repeats "
                "or the frozen target/scorer matrix."
            )
        for summaries in (self.errors, self.skips, self.unverified_children):
            if len(summaries) > _MAX_ISSUE_SUMMARIES:
                raise ValueError("Experiment issue summaries exceed the bounded limit.")
        imported_rows = [
            row
            for row in getattr(self.spec, "planning_rows", [])
            if row.disposition == "imported"
        ]
        if imported_rows:
            keys = [
                (item.target_execution_id, item.repeat_index)
                for item in self.imported_replay_members
            ]
            if len(keys) != len(set(keys)):
                raise ValueError(
                    "Imported replay member evidence identities must be unique."
                )
            if self.imported_replay_evidence is not None and (
                self.imported_replay_evidence
                != ImportedReplayEvidenceSummary._from_members(
                    intended=self.counts.intended,
                    members=self.imported_replay_members,
                )
            ):
                raise ValueError(
                    "Imported replay evidence must match member reports and counts."
                )
        elif self.imported_replay_members or self.imported_replay_evidence is not None:
            raise ValueError(
                "Native experiments cannot contain imported replay evidence."
            )
        if self.verdict is not None:
            if self.score_aggregate is None or self.spec.verdict_policy is None:
                raise ValueError(
                    "Verdicts require a frozen policy and score aggregate."
                )
            if (
                self.verdict.experiment_id != self.spec.experiment_id
                or self.verdict.aggregate_artifact_version_id
                != self.score_aggregate.artifact_version_id
                or self.verdict.aggregate_sha256 != self.score_aggregate.sha256
                or self.verdict.policy_sha256 != self.spec.verdict_policy.content_hash
                or self.verdict.operational_limit != self.operational_limit
            ):
                raise ValueError(
                    "Verdict references must match the record's immutable evidence."
                )
        if (
            self.spec.verdict_policy is not None
            and self.score_aggregate is not None
            and self.verdict is None
        ):
            raise ValueError("A graded score aggregate requires an immutable verdict.")
        if self.status == "pending":
            if self.started_at is not None or self.finished_at is not None:
                raise ValueError(
                    "Pending experiments cannot have lifecycle timestamps."
                )
        elif self.status == "running":
            if self.started_at is None or self.finished_at is not None:
                raise ValueError("Running experiments require only started_at.")
        else:
            if self.started_at is None or self.finished_at is None:
                raise ValueError("Terminal experiments require lifecycle timestamps.")
            accounted = self.counts.submitted + self.counts.skipped + self.counts.failed
            if self.status != "cancelled" and accounted != self.counts.intended:
                raise ValueError(
                    "Terminal experiment counts must fill the denominator."
                )
            if self.status == "completed":
                if (
                    self.counts.submitted != self.counts.intended
                    or self.counts.verified != self.counts.intended
                    or self.counts.skipped
                    or self.counts.failed
                    or self.counts.unverified
                ):
                    raise ValueError(
                        "Completed experiments require every intended child to be "
                        "submitted with both membership signals verified."
                    )
            elif self.status == "partial" and self.counts.verified == 0:
                raise ValueError("Partial experiments require a verified child.")
            elif self.status == "failed" and self.counts.verified != 0:
                raise ValueError("Failed experiments cannot contain verified children.")
        return self

    @classmethod
    def pending(cls, spec: ExperimentSpecRecord) -> ExperimentRecord:
        """Create the first durable state for a fully frozen attempt."""
        if spec.kind == "replay":
            skipped = (
                sum(row.disposition == "skip" for row in spec.planning_rows)
                * spec.repeats
            )
            intended = spec.target_membership.count * spec.repeats
        else:
            skipped = 0
            intended = spec.target_membership.count * len(spec.scorers)
        return cls(
            spec=spec,
            created_at=spec.created_at,
            updated_at=spec.created_at,
            counts=ExperimentCounts(
                target_count=spec.target_membership.count,
                intended=intended,
                skipped=skipped,
            ),
        )


class ExperimentReservation(BaseModel):
    """Result of an idempotent catalog reservation."""

    record: ExperimentRecord
    created: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


@dataclass(frozen=True)
class ReplayTrialPlan:
    """One generated target/repeat pair passed to the replay submitter."""

    target: TargetPlanningRow
    repeat_index: int

    def __post_init__(self) -> None:
        if isinstance(self.repeat_index, bool) or self.repeat_index < 0:
            raise KitaruUsageError("repeat_index must be >= 0.")

    @property
    def target_execution_id(self) -> str:
        return self.target.target_execution_id

    @property
    def parent_execution_id(self) -> str:
        return self.target_execution_id

    @property
    def root_execution_id(self) -> str:
        return self.target.root_execution_id

    @property
    def disposition(self) -> Literal["replay", "skip", "top", "imported"]:
        return self.target.disposition

    @property
    def replay_plan(self) -> FrozenExperimentReplayPlan | None:
        return self.target.replay_plan


class ReplayAttemptDraft(BaseModel):
    """Pure, validated replay attempt before membership publication."""

    experiment_id: str
    name: str | None
    display_name: str
    suite_key: str
    source_experiment_id: str | None = None
    idempotency_key: str
    created_at: str
    candidate_project_id: str
    candidate_agent_version_id: str
    candidate_pipeline_id: str
    executable: ExperimentExecutable
    replay_inputs: ReplayRequestInputs
    at: str
    repeats: int
    wait: bool
    on_error: Literal["collect", "fail"]
    coverage: ForkCoverage
    planning_rows: list[TargetPlanningRow]
    cohort_audit: CohortAudit | None
    scorers: list[ScorerSnapshot] = Field(default_factory=list)
    grounded_policy: GroundedPolicySnapshot | None = None
    verdict_policy: VerdictPolicy | None = None
    regression_limits: RegressionLimits | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @property
    def target_execution_ids(self) -> list[str]:
        """Return ordered target IDs derived from the authoritative rows."""
        return [row.target_execution_id for row in self.planning_rows]

    @model_validator(mode="after")
    def _validate_draft(self) -> ReplayAttemptDraft:
        row_ids = self.target_execution_ids
        if len(row_ids) != len(set(row_ids)):
            raise ValueError("Replay attempt targets must be unique.")
        if self.coverage.selected != len(row_ids):
            raise ValueError("Coverage selected count must match frozen targets.")
        if self.coverage.covered != sum(
            row.checkpoint_covered for row in self.planning_rows
        ):
            raise ValueError("Coverage count must match target planning rows.")
        _validate_verdict_scorers(self.scorers, self.verdict_policy)
        return self

    def iter_trials(self) -> Iterator[ReplayTrialPlan]:
        """Generate target-then-repeat trials without storing repeated plans."""
        return _iter_trials(self.planning_rows, self.repeats)


class ReplayAttemptPlan(BaseModel):
    """Frozen publishable specification with derived child trial order."""

    spec: ExperimentSpec

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    def iter_trials(self) -> Iterator[ReplayTrialPlan]:
        """Generate target-then-repeat trials from the immutable specification."""
        return _iter_trials(self.spec.planning_rows, self.spec.repeats)


def _iter_trials(
    rows: Sequence[TargetPlanningRow], repeats: int
) -> Iterator[ReplayTrialPlan]:
    for row in rows:
        for repeat_index in range(repeats):
            yield ReplayTrialPlan(target=row, repeat_index=repeat_index)


class ExperimentPlanningError(KitaruUsageError):
    """Raised with all target-level validation failures from one batch."""

    def __init__(self, issues: Sequence[ExperimentIssue]) -> None:
        self.issues = tuple(issues)
        details = "; ".join(
            f"{issue.target_execution_id or '<request>'}: {issue.reason}"
            for issue in self.issues
        )
        super().__init__(f"Replay attempt planning failed: {details}")


def experiment_request_hash(spec: ExperimentSpecRecord) -> str:
    """Hash the logical frozen request, excluding attempt identity and wall time."""
    payload = spec.model_dump(
        mode="json",
        exclude={"request_hash", "experiment_id", "created_at"},
    )
    if payload.get("kind") == "replay":
        # Native plans predate the discriminator. Keep their logical request
        # hash stable while imported plans retain their explicit plan type.
        for row in payload.get("planning_rows", []):
            replay_plan = row.get("replay_plan")
            if (
                isinstance(replay_plan, dict)
                and replay_plan.get("plan_type") == "native"
            ):
                replay_plan.pop("plan_type")
        if payload.get("source_experiment_id") is None:
            payload.pop("source_experiment_id", None)
        if payload.get("scorers") == []:
            payload.pop("scorers")
        if payload.get("evidence_manifest") is None:
            payload.pop("evidence_manifest")
        if payload.get("regression_limits") is None:
            payload.pop("regression_limits", None)
    if payload.get("grounded_policy") is None:
        payload.pop("grounded_policy", None)
    if payload.get("verdict_policy") is None:
        payload.pop("verdict_policy")
    return _sha256(_canonical_json(payload))


def _validate_verdict_scorers(
    scorers: Sequence[ScorerSnapshot],
    policy: VerdictPolicy | None,
) -> None:
    scorer_identities = [ScorerIdentity.from_snapshot(item) for item in scorers]
    if len(scorer_identities) != len(set(scorer_identities)):
        raise ValueError("Experiment scorers must be unique by revision/config.")
    if policy is None:
        return
    if scorer_identities != policy.scorer_identities:
        raise ValueError(
            "Verdict policy roles must exactly match the frozen scorer identities."
        )
