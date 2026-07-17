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

from kitaru.errors import KitaruUsageError
from kitaru.replay import ReplayPlan, ReplayPlanDocument
from kitaru.replay_context import ReplayRuntimeContext

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


class TargetPlanningRow(BaseModel):
    """One immutable target-level coverage and replay planning decision."""

    target_execution_id: str
    parent_execution_id: str | None
    root_execution_id: str
    checkpoint_covered: bool
    disposition: Literal["replay", "skip", "top"]
    reason: str | None = None
    replay_plan: FrozenReplayPlan | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("target_execution_id", "root_execution_id")
    @classmethod
    def _validate_required_ids(cls, value: str) -> str:
        return _required_string(value, field_name="Execution ID")

    @model_validator(mode="after")
    def _validate_disposition(self) -> TargetPlanningRow:
        if self.disposition == "replay":
            if not self.checkpoint_covered or self.replay_plan is None:
                raise ValueError("Covered replay targets require a replay plan.")
            if self.replay_plan.replay_from_start:
                raise ValueError("Covered targets cannot use replay-from-start.")
        elif self.disposition == "skip":
            if self.checkpoint_covered or self.replay_plan is not None:
                raise ValueError("Skipped targets must be uncovered and plan-free.")
            if not self.reason:
                raise ValueError("Skipped targets require an audit reason.")
        else:
            if self.checkpoint_covered or self.replay_plan is None:
                raise ValueError("Top-policy targets require a from-start plan.")
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
        if any(
            not row.checkpoint_covered and row.disposition == "replay"
            for row in self.planning_rows
        ):
            raise ValueError("Uncovered targets require an explicit disposition.")
        expected = experiment_request_hash(self)
        if self.request_hash != expected:
            raise ValueError(
                "Experiment request_hash does not match the frozen request."
            )
        return self


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
    spec: ExperimentSpec
    status: ExperimentStatus = "pending"
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    counts: ExperimentCounts
    errors: list[ExperimentIssue] = Field(default_factory=list)
    skips: list[ExperimentIssue] = Field(default_factory=list)
    unverified_children: list[ExperimentIssue] = Field(default_factory=list)

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
        if self.counts.intended != self.counts.target_count * self.spec.repeats:
            raise ValueError(
                "Cached denominator must equal targets multiplied by repeats."
            )
        for summaries in (self.errors, self.skips, self.unverified_children):
            if len(summaries) > _MAX_ISSUE_SUMMARIES:
                raise ValueError("Experiment issue summaries exceed the bounded limit.")
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
    def pending(cls, spec: ExperimentSpec) -> ExperimentRecord:
        """Create the first durable state for a fully frozen attempt."""
        skipped = (
            sum(row.disposition == "skip" for row in spec.planning_rows) * spec.repeats
        )
        return cls(
            spec=spec,
            created_at=spec.created_at,
            updated_at=spec.created_at,
            counts=ExperimentCounts(
                target_count=spec.target_membership.count,
                intended=spec.target_membership.count * spec.repeats,
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
    def disposition(self) -> Literal["replay", "skip", "top"]:
        return self.target.disposition

    @property
    def replay_plan(self) -> FrozenReplayPlan | None:
        return self.target.replay_plan


class ReplayAttemptDraft(BaseModel):
    """Pure, validated replay attempt before membership publication."""

    experiment_id: str
    name: str | None
    display_name: str
    suite_key: str
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


def experiment_request_hash(spec: ExperimentSpec) -> str:
    """Hash the logical frozen request, excluding attempt identity and wall time."""
    payload = spec.model_dump(
        mode="json",
        exclude={"request_hash", "experiment_id", "created_at"},
    )
    return _sha256(_canonical_json(payload))
