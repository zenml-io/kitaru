"""Frozen grading policy and immutable experiment verdicts."""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.scoring._aggregates import ScoreAttemptAggregate
from kitaru.scoring._contracts import (
    ProtectionSnapshot,
    ScorerAggregate,
    ScorerSnapshot,
    require_string,
    sha256_json,
    validate_sha256,
)


class ScorerIdentity(BaseModel):
    """Exact scorer identity used to join frozen policy to aggregate rows."""

    name: str
    revision: str
    configuration_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return require_string(value, field_name="Scorer identity name")

    @field_validator("revision", "configuration_hash")
    @classmethod
    def _validate_hashes(cls, value: str) -> str:
        return validate_sha256(value)

    @classmethod
    def from_snapshot(cls, snapshot: ScorerSnapshot) -> ScorerIdentity:
        """Create an identity from one immutable scorer snapshot."""
        return cls(
            name=snapshot.name,
            revision=snapshot.revision,
            configuration_hash=snapshot.configuration_hash,
        )

    @classmethod
    def from_aggregate(cls, aggregate: ScorerAggregate) -> ScorerIdentity:
        """Create an identity from one immutable aggregate row."""
        return cls(
            name=aggregate.scorer_name,
            revision=aggregate.scorer_revision,
            configuration_hash=aggregate.scorer_configuration_hash,
        )


class ObjectivePolicy(BaseModel):
    """Frozen V1 objective scorer and minimum mean threshold."""

    scorer: ScorerIdentity
    minimum_mean: float = Field(ge=0.0, le=1.0)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ProtectionPolicy(BaseModel):
    """Frozen V1 protection identity and fixed passing rule."""

    protection_id: str
    scorer: ScorerIdentity
    pass_rule: Literal["score == 1.0"] = "score == 1.0"

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("protection_id")
    @classmethod
    def _validate_protection_id(cls, value: str) -> str:
        return require_string(value, field_name="Protection ID")

    @classmethod
    def from_snapshot(cls, snapshot: ProtectionSnapshot) -> ProtectionPolicy:
        """Create policy from one registered protection snapshot."""
        return cls(
            protection_id=snapshot.protection_id,
            scorer=ScorerIdentity.from_snapshot(snapshot.scorer),
            pass_rule=snapshot.pass_rule,
        )


class ImportedReplayComparability(StrEnum):
    """How closely an imported candidate followed the recorded source path."""

    RECORDED_PATH_COMPARABLE = "recorded_path_comparable"
    COUNTERFACTUAL = "counterfactual"
    DEGRADED = "degraded"
    NON_COMPARABLE = "non_comparable"


class ImportedReplayVerdictPolicy(BaseModel):
    """Frozen evidence requirements for an imported replay verdict."""

    allowed_comparability: tuple[ImportedReplayComparability, ...] = (
        ImportedReplayComparability.RECORDED_PATH_COMPARABLE,
    )
    require_complete_prefix: bool = True
    require_all_recorded_responses: bool = True

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("allowed_comparability", mode="before")
    @classmethod
    def _load_allowed_comparability(
        cls, value: Any
    ) -> tuple[ImportedReplayComparability, ...]:
        return tuple(
            item
            if isinstance(item, ImportedReplayComparability)
            else ImportedReplayComparability(item)
            for item in value
        )

    @model_validator(mode="after")
    def _validate_allowed_states(self) -> ImportedReplayVerdictPolicy:
        if not self.allowed_comparability:
            raise ValueError(
                "Imported replay verdict policy requires an allowed state."
            )
        if len(self.allowed_comparability) != len(set(self.allowed_comparability)):
            raise ValueError(
                "Allowed imported replay comparability states must be unique."
            )
        return self


class ImportedReplayEvidenceSummary(BaseModel):
    """Bounded immutable evidence facts for all imported replay members."""

    intended: int = Field(ge=1)
    reported: int = Field(ge=0)
    complete_prefixes: int = Field(ge=0)
    eligible_recorded_responses: int = Field(ge=0)
    recorded_response_hits: int = Field(ge=0)
    recorded_response_misses: int = Field(ge=0)
    blocked_calls: int = Field(ge=0)
    path_divergences: int = Field(ge=0)
    comparability: ImportedReplayComparability
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("comparability", mode="before")
    @classmethod
    def _load_summary_comparability(cls, value: Any) -> ImportedReplayComparability:
        if isinstance(value, ImportedReplayComparability):
            return value
        return ImportedReplayComparability(value)

    @field_validator("content_hash")
    @classmethod
    def _validate_summary_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_summary(self) -> ImportedReplayEvidenceSummary:
        if self.reported > self.intended or self.complete_prefixes > self.reported:
            raise ValueError(
                "Imported replay evidence counts exceed their denominator."
            )
        if self.recorded_response_hits + self.recorded_response_misses > (
            self.eligible_recorded_responses
        ):
            raise ValueError("Recorded response decisions exceed eligible calls.")
        expected = sha256_json(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError(
                "Imported replay evidence content_hash does not match payload."
            )
        return self

    @classmethod
    def create(cls, **values: Any) -> ImportedReplayEvidenceSummary:
        """Create a content-addressed evidence summary."""
        provisional = cls.model_construct(
            content_hash="sha256:" + "0" * 64,
            **values,
        )
        python_payload = provisional.model_dump(mode="python", exclude={"content_hash"})
        json_payload = provisional.model_dump(mode="json", exclude={"content_hash"})
        return cls(
            **python_payload,
            content_hash=sha256_json(json_payload),
        )

    @classmethod
    def _from_members(
        cls,
        *,
        intended: int,
        members: Sequence[Any],
    ) -> ImportedReplayEvidenceSummary:
        """Derive the immutable aggregate from its ordered member evidence."""
        ranking = {
            ImportedReplayComparability.RECORDED_PATH_COMPARABLE: 0,
            ImportedReplayComparability.COUNTERFACTUAL: 1,
            ImportedReplayComparability.DEGRADED: 2,
            ImportedReplayComparability.NON_COMPARABLE: 3,
        }
        comparability = max(
            (member.comparability for member in members),
            key=ranking.__getitem__,
            default=ImportedReplayComparability.NON_COMPARABLE,
        )
        return cls.create(
            intended=intended,
            reported=len(members),
            complete_prefixes=sum(member.prefix_complete for member in members),
            eligible_recorded_responses=sum(
                member.eligible_recorded_responses for member in members
            ),
            recorded_response_hits=sum(
                member.recorded_response_hits for member in members
            ),
            recorded_response_misses=sum(
                member.recorded_response_misses for member in members
            ),
            blocked_calls=sum(member.blocked_calls for member in members),
            path_divergences=sum(member.path_diverged for member in members),
            comparability=comparability,
        )


class VerdictPolicy(BaseModel):
    """Complete frozen grading policy for one experiment attempt."""

    schema_version: Literal[1] = 1
    objective: ObjectivePolicy | None = None
    protections: list[ProtectionPolicy] = Field(default_factory=list)
    imported_replay: ImportedReplayVerdictPolicy | None = None
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_policy(self) -> VerdictPolicy:
        if self.objective is None and not self.protections:
            raise ValueError("A verdict policy requires an objective or protection.")
        protection_ids = [item.protection_id for item in self.protections]
        if len(protection_ids) != len(set(protection_ids)):
            raise ValueError("Verdict protection IDs must be unique.")
        identities = [item.scorer for item in self.protections]
        if len(identities) != len(set(identities)):
            raise ValueError("Verdict protection scorer identities must be unique.")
        if self.objective is not None and self.objective.scorer in identities:
            raise ValueError(
                "Objective and protection scorer identities must be disjoint."
            )
        hash_exclude = {"content_hash"}
        if self.imported_replay is None:
            hash_exclude.add("imported_replay")
        expected = sha256_json(self.model_dump(mode="json", exclude=hash_exclude))
        if self.content_hash != expected:
            raise ValueError("Verdict policy content_hash does not match payload.")
        return self

    @property
    def scorer_identities(self) -> list[ScorerIdentity]:
        """Return objective then protection identities in frozen role order."""
        identities = [] if self.objective is None else [self.objective.scorer]
        return [*identities, *(item.scorer for item in self.protections)]

    @classmethod
    def create(
        cls,
        *,
        objective: ScorerSnapshot | None = None,
        minimum_mean: float | None = None,
        protections: Sequence[ProtectionSnapshot] = (),
        imported_replay: ImportedReplayVerdictPolicy | None = None,
    ) -> VerdictPolicy | None:
        """Create a policy, or None when the attempt is intentionally ungraded."""
        protection_items = list(protections)
        if objective is None and minimum_mean is not None:
            raise KitaruUsageError(
                "An objective minimum mean requires exactly one objective scorer."
            )
        if objective is None and not protection_items and imported_replay is None:
            return None
        objective_policy = (
            None
            if objective is None
            else ObjectivePolicy(
                scorer=ScorerIdentity.from_snapshot(objective),
                minimum_mean=1.0 if minimum_mean is None else minimum_mean,
            )
        )
        protection_policies = [
            ProtectionPolicy.from_snapshot(item) for item in protection_items
        ]
        json_payload = {
            "schema_version": 1,
            "objective": (
                None
                if objective_policy is None
                else objective_policy.model_dump(mode="json")
            ),
            "protections": [
                item.model_dump(mode="json") for item in protection_policies
            ],
        }
        if imported_replay is not None:
            json_payload["imported_replay"] = imported_replay.model_dump(mode="json")
        return cls(
            objective=objective_policy,
            protections=protection_policies,
            imported_replay=imported_replay,
            content_hash=sha256_json(json_payload),
        )


class ExperimentVerdict(StrEnum):
    """Stable quality judgment independent from experiment lifecycle status."""

    PASS = "pass"
    FAIL = "fail"
    HOLD = "hold"


class VerdictReasonCode(StrEnum):
    """Bounded machine-readable verdict explanations."""

    LIFECYCLE_INCOMPLETE = "lifecycle_incomplete"
    REPLAY_MEMBERSHIP_INCOMPLETE = "replay_membership_incomplete"
    AGGREGATE_REFERENCE_MISMATCH = "aggregate_reference_mismatch"
    SCORE_MATRIX_INCOMPLETE = "score_matrix_incomplete"
    MISSING_SCORER_AGGREGATE = "missing_scorer_aggregate"
    DUPLICATE_SCORER_AGGREGATE = "duplicate_scorer_aggregate"
    UNEXPECTED_SCORER_AGGREGATE = "unexpected_scorer_aggregate"
    ABSTAINED_OBSERVATIONS = "abstained_observations"
    BLOCKED_OBSERVATIONS = "blocked_observations"
    ERROR_OBSERVATIONS = "error_observations"
    OBJECTIVE_BELOW_THRESHOLD = "objective_below_threshold"
    PROTECTION_BELOW_PASSING_SCORE = "protection_below_passing_score"
    OPERATIONAL_LIMIT_UNVERIFIED = "operational_limit_unverified"
    OPERATIONAL_LIMIT_STOPPED = "operational_limit_stopped"
    IMPORTED_REPLAY_EVIDENCE_MISSING = "imported_replay_evidence_missing"
    IMPORTED_REPLAY_PREFIX_INCOMPLETE = "imported_replay_prefix_incomplete"
    IMPORTED_RECORDED_RESPONSES_INCOMPLETE = "imported_recorded_responses_incomplete"
    IMPORTED_REPLAY_NOT_COMPARABLE = "imported_replay_not_comparable"


_PROTECTION_FAILURE_OVERRIDABLE_HOLD_REASONS = frozenset(
    {
        VerdictReasonCode.IMPORTED_REPLAY_PREFIX_INCOMPLETE,
        VerdictReasonCode.IMPORTED_RECORDED_RESPONSES_INCOMPLETE,
        VerdictReasonCode.IMPORTED_REPLAY_NOT_COMPARABLE,
    }
)


class OperationalLimitReason(StrEnum):
    """Stable reason further regression trials were not submitted."""

    COST_UNVERIFIED = "cost_unverified"
    TOKENS_UNVERIFIED = "tokens_unverified"
    COST_LIMIT_REACHED = "cost_limit_reached"
    TOKEN_LIMIT_REACHED = "token_limit_reached"
    DURATION_LIMIT_REACHED = "duration_limit_reached"


class OperationalLimitThresholds(BaseModel):
    """Frozen regression thresholds used to derive operational facts."""

    schema_version: Literal[1] = 1
    max_trials: int = Field(ge=1)
    max_cost_usd: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    max_incurred_tokens: int | None = Field(default=None, ge=1)
    max_duration_seconds: float | None = Field(
        default=None,
        gt=0,
        allow_inf_nan=False,
    )

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class OperationalLimitFacts(BaseModel):
    """Typed measurements frozen before verdict calculation."""

    limits: OperationalLimitThresholds
    submitted_trials: int = Field(ge=0)
    remaining_trials: int = Field(ge=0)
    incurred_cost_usd: float = Field(ge=0, allow_inf_nan=False)
    incurred_tokens: int = Field(ge=0)
    duration_seconds: float = Field(ge=0, allow_inf_nan=False)
    cost_complete: bool
    tokens_complete: bool
    checked_between_terminal_trials: bool
    one_trial_may_overshoot: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class OperationalLimitOutcome(BaseModel):
    """Immutable usage/limit facts supplied before verdict calculation."""

    schema_version: Literal[1] = 1
    verified: bool
    stopped: bool = False
    reason_code: OperationalLimitReason | None = None
    facts: OperationalLimitFacts
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("reason_code", mode="before")
    @classmethod
    def _validate_reason(cls, value: Any) -> OperationalLimitReason | None:
        if value is None or isinstance(value, OperationalLimitReason):
            return value
        return OperationalLimitReason(
            require_string(value, field_name="Operational limit reason")
        )

    @field_validator("content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_content_hash(self) -> OperationalLimitOutcome:
        expected = sha256_json(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("Operational limit content_hash does not match payload.")
        return self

    @classmethod
    def create(
        cls,
        *,
        verified: bool,
        stopped: bool = False,
        reason_code: OperationalLimitReason | None = None,
        facts: OperationalLimitFacts,
    ) -> OperationalLimitOutcome:
        """Create a content-addressed immutable limit outcome."""
        payload = {
            "schema_version": 1,
            "verified": verified,
            "stopped": stopped,
            "reason_code": None if reason_code is None else reason_code.value,
            "facts": facts.model_dump(mode="json"),
        }
        return cls(
            verified=verified,
            stopped=stopped,
            reason_code=reason_code,
            facts=facts,
            content_hash=sha256_json(payload),
        )


class ObjectiveVerdictFact(BaseModel):
    """Frozen objective inputs and aggregate facts used by one verdict."""

    scorer: ScorerIdentity
    minimum_mean: float
    planned: int
    scored: int
    mean: float | None
    passed: bool | None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ProtectionVerdictFact(BaseModel):
    """Frozen protection inputs and aggregate facts used by one verdict."""

    protection_id: str
    scorer: ScorerIdentity
    pass_rule: Literal["score == 1.0"]
    planned: int
    scored: int
    minimum: float | None
    passed: bool | None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ReplayCompletenessFact(BaseModel):
    """Frozen replay membership denominator used by one verdict."""

    required: bool
    intended: int
    verified: int
    complete: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ScoreMatrixFact(BaseModel):
    """Frozen score-matrix denominator and outcome counts."""

    planned: int
    observed: int
    scored: int
    abstained: int
    blocked: int
    error: int
    complete: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ImportedReplayVerdictFact(BaseModel):
    """Frozen imported replay evidence checked by one verdict."""

    required: bool
    present: bool
    complete_prefixes: bool | None = None
    recorded_responses_complete: bool | None = None
    comparability: ImportedReplayComparability | None = None
    accepted: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("comparability", mode="before")
    @classmethod
    def _load_fact_comparability(cls, value: Any) -> ImportedReplayComparability | None:
        if value is None or isinstance(value, ImportedReplayComparability):
            return value
        return ImportedReplayComparability(value)


class VerdictResult(BaseModel):
    """Immutable verdict and the exact frozen facts that produced it."""

    schema_version: Literal[1] = 1
    experiment_id: str
    verdict: ExperimentVerdict
    aggregate_artifact_version_id: str
    aggregate_sha256: str
    policy_sha256: str
    objective: ObjectiveVerdictFact | None = None
    protections: list[ProtectionVerdictFact] = Field(default_factory=list)
    replay_completeness: ReplayCompletenessFact
    score_matrix: ScoreMatrixFact
    imported_replay: ImportedReplayVerdictFact | None = None
    operational_limit: OperationalLimitOutcome | None = None
    reason_codes: list[VerdictReasonCode] = Field(default_factory=list)
    message: str
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("experiment_id", "aggregate_artifact_version_id", "message")
    @classmethod
    def _validate_strings(cls, value: str) -> str:
        return require_string(value, field_name="Verdict field")

    @field_validator("aggregate_sha256", "policy_sha256", "content_hash")
    @classmethod
    def _validate_hashes(cls, value: str) -> str:
        return validate_sha256(value)

    @field_validator("verdict", mode="before")
    @classmethod
    def _validate_verdict(cls, value: Any) -> ExperimentVerdict:
        return (
            value if isinstance(value, ExperimentVerdict) else ExperimentVerdict(value)
        )

    @field_validator("reason_codes", mode="before")
    @classmethod
    def _validate_reasons(cls, value: Any) -> list[VerdictReasonCode]:
        return [
            item if isinstance(item, VerdictReasonCode) else VerdictReasonCode(item)
            for item in value
        ]

    @model_validator(mode="after")
    def _validate_content_hash(self) -> VerdictResult:
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("Verdict reason codes must be unique.")
        hash_exclude = {"content_hash"}
        if self.imported_replay is None:
            hash_exclude.add("imported_replay")
        expected = sha256_json(self.model_dump(mode="json", exclude=hash_exclude))
        if self.content_hash != expected:
            raise ValueError("Verdict content_hash does not match payload.")
        return self


def evaluate_verdict(
    record: Any,
    aggregate: ScoreAttemptAggregate,
    policy: VerdictPolicy,
    operational_limit: OperationalLimitOutcome | None = None,
) -> VerdictResult:
    """Derive a verdict from frozen record, aggregate, policy, and limit facts."""
    if getattr(record.spec, "verdict_policy", None) != policy:
        raise KitaruStateError(
            "Verdict evaluation requires the record's frozen policy."
        )
    reference = record.score_aggregate
    if reference is None:
        raise KitaruStateError("Verdict evaluation requires an aggregate reference.")

    reasons: list[VerdictReasonCode] = []
    if record.status != "completed":
        reasons.append(VerdictReasonCode.LIFECYCLE_INCOMPLETE)

    replay_required = record.spec.kind == "replay"
    replay_intended = record.counts.intended if replay_required else 0
    replay_verified = record.counts.verified if replay_required else 0
    replay_complete = not replay_required or replay_verified == replay_intended
    if not replay_complete:
        reasons.append(VerdictReasonCode.REPLAY_MEMBERSHIP_INCOMPLETE)
    replay_fact = ReplayCompletenessFact(
        required=replay_required,
        intended=replay_intended,
        verified=replay_verified,
        complete=replay_complete,
    )

    if (
        aggregate.experiment_id != record.spec.experiment_id
        or aggregate.content_hash != reference.sha256
    ):
        reasons.append(VerdictReasonCode.AGGREGATE_REFERENCE_MISMATCH)

    expected_targets = (
        record.counts.verified
        if replay_required
        else record.spec.target_membership.count
    )
    expected_cells = expected_targets * len(policy.scorer_identities)
    observed = len(aggregate.observation_ids)
    outcome_total = (
        aggregate.scored + aggregate.abstained + aggregate.blocked + aggregate.error
    )
    matrix_complete = (
        aggregate.planned == expected_cells
        and observed == expected_cells
        and outcome_total == expected_cells
    )
    if not matrix_complete:
        reasons.append(VerdictReasonCode.SCORE_MATRIX_INCOMPLETE)
    if aggregate.abstained:
        reasons.append(VerdictReasonCode.ABSTAINED_OBSERVATIONS)
    if aggregate.blocked:
        reasons.append(VerdictReasonCode.BLOCKED_OBSERVATIONS)
    if aggregate.error:
        reasons.append(VerdictReasonCode.ERROR_OBSERVATIONS)

    rows: dict[ScorerIdentity, ScorerAggregate] = {}
    duplicate_identities: set[ScorerIdentity] = set()
    for row in aggregate.scorer_aggregates:
        identity = ScorerIdentity.from_aggregate(row)
        if identity in rows:
            duplicate_identities.add(identity)
        else:
            rows[identity] = row
    if duplicate_identities:
        reasons.append(VerdictReasonCode.DUPLICATE_SCORER_AGGREGATE)

    expected_identities = set(policy.scorer_identities)
    if set(rows) - expected_identities:
        reasons.append(VerdictReasonCode.UNEXPECTED_SCORER_AGGREGATE)
    if expected_identities - set(rows):
        reasons.append(VerdictReasonCode.MISSING_SCORER_AGGREGATE)

    objective_fact: ObjectiveVerdictFact | None = None
    if policy.objective is not None:
        objective_row = rows.get(policy.objective.scorer)
        objective_trustworthy = _row_is_complete(objective_row, expected_targets)
        objective_passed = (
            None
            if not objective_trustworthy or objective_row is None
            else objective_row.mean is not None
            and objective_row.mean >= policy.objective.minimum_mean
        )
        objective_fact = ObjectiveVerdictFact(
            scorer=policy.objective.scorer,
            minimum_mean=policy.objective.minimum_mean,
            planned=0 if objective_row is None else objective_row.planned,
            scored=0 if objective_row is None else objective_row.scored,
            mean=None if objective_row is None else objective_row.mean,
            passed=objective_passed,
        )
        if objective_row is not None and not objective_trustworthy:
            reasons.append(VerdictReasonCode.SCORE_MATRIX_INCOMPLETE)
        if objective_passed is False:
            reasons.append(VerdictReasonCode.OBJECTIVE_BELOW_THRESHOLD)

    protection_facts: list[ProtectionVerdictFact] = []
    for protection in policy.protections:
        row = rows.get(protection.scorer)
        trustworthy = _row_is_complete(row, expected_targets)
        passed = (
            None
            if not trustworthy or row is None
            else row.minimum == 1.0 and row.maximum == 1.0
        )
        protection_facts.append(
            ProtectionVerdictFact(
                protection_id=protection.protection_id,
                scorer=protection.scorer,
                pass_rule=protection.pass_rule,
                planned=0 if row is None else row.planned,
                scored=0 if row is None else row.scored,
                minimum=None if row is None else row.minimum,
                passed=passed,
            )
        )
        if row is not None and not trustworthy:
            reasons.append(VerdictReasonCode.SCORE_MATRIX_INCOMPLETE)
        if passed is False:
            reasons.append(VerdictReasonCode.PROTECTION_BELOW_PASSING_SCORE)

    imported_replay_fact: ImportedReplayVerdictFact | None = None
    imported_policy = policy.imported_replay
    if imported_policy is not None:
        evidence = getattr(record, "imported_replay_evidence", None)
        if evidence is None:
            reasons.append(VerdictReasonCode.IMPORTED_REPLAY_EVIDENCE_MISSING)
            imported_replay_fact = ImportedReplayVerdictFact(
                required=True,
                present=False,
                accepted=False,
            )
        else:
            prefixes_complete = (
                evidence.reported == evidence.intended
                and evidence.complete_prefixes == evidence.intended
            )
            responses_complete = (
                evidence.recorded_response_misses == 0
                and evidence.blocked_calls == 0
                and evidence.recorded_response_hits
                == evidence.eligible_recorded_responses
            )
            comparable = evidence.comparability in imported_policy.allowed_comparability
            if imported_policy.require_complete_prefix and not prefixes_complete:
                reasons.append(VerdictReasonCode.IMPORTED_REPLAY_PREFIX_INCOMPLETE)
            if (
                imported_policy.require_all_recorded_responses
                and not responses_complete
            ):
                reasons.append(VerdictReasonCode.IMPORTED_RECORDED_RESPONSES_INCOMPLETE)
            if not comparable:
                reasons.append(VerdictReasonCode.IMPORTED_REPLAY_NOT_COMPARABLE)
            imported_replay_fact = ImportedReplayVerdictFact(
                required=True,
                present=True,
                complete_prefixes=prefixes_complete,
                recorded_responses_complete=responses_complete,
                comparability=evidence.comparability,
                accepted=(
                    (prefixes_complete or not imported_policy.require_complete_prefix)
                    and (
                        responses_complete
                        or not imported_policy.require_all_recorded_responses
                    )
                    and comparable
                ),
            )

    if operational_limit is not None:
        if not operational_limit.verified:
            reasons.append(VerdictReasonCode.OPERATIONAL_LIMIT_UNVERIFIED)
        if operational_limit.stopped:
            reasons.append(VerdictReasonCode.OPERATIONAL_LIMIT_STOPPED)

    reasons = list(dict.fromkeys(reasons))
    reason_set = set(reasons)
    failure_reason_codes = {
        VerdictReasonCode.OBJECTIVE_BELOW_THRESHOLD,
        VerdictReasonCode.PROTECTION_BELOW_PASSING_SCORE,
    }
    hold_reasons = reason_set - failure_reason_codes
    failure_reasons = {
        VerdictReasonCode.OBJECTIVE_BELOW_THRESHOLD,
        VerdictReasonCode.PROTECTION_BELOW_PASSING_SCORE,
    } & reason_set
    protection_failed = (
        VerdictReasonCode.PROTECTION_BELOW_PASSING_SCORE in failure_reasons
    )
    # A complete protection failure is affirmative evidence of forbidden
    # behavior, but it can only outrank present, degraded imported-replay
    # context. Every other hold reason leaves the score evidence untrusted.
    if hold_reasons:
        verdict = (
            ExperimentVerdict.FAIL
            if protection_failed
            and hold_reasons <= _PROTECTION_FAILURE_OVERRIDABLE_HOLD_REASONS
            else ExperimentVerdict.HOLD
        )
    elif failure_reasons:
        verdict = ExperimentVerdict.FAIL
    else:
        verdict = ExperimentVerdict.PASS
    matrix_fact = ScoreMatrixFact(
        planned=expected_cells,
        observed=observed,
        scored=aggregate.scored,
        abstained=aggregate.abstained,
        blocked=aggregate.blocked,
        error=aggregate.error,
        complete=matrix_complete,
    )
    message = f"{verdict.value.upper()}: " + (
        ", ".join(item.value for item in reasons) if reasons else "all policies passed"
    )
    payload = {
        "schema_version": 1,
        "experiment_id": record.spec.experiment_id,
        "verdict": verdict.value,
        "aggregate_artifact_version_id": reference.artifact_version_id,
        "aggregate_sha256": reference.sha256,
        "policy_sha256": policy.content_hash,
        "objective": (
            None if objective_fact is None else objective_fact.model_dump(mode="json")
        ),
        "protections": [item.model_dump(mode="json") for item in protection_facts],
        "replay_completeness": replay_fact.model_dump(mode="json"),
        "score_matrix": matrix_fact.model_dump(mode="json"),
        "operational_limit": (
            None
            if operational_limit is None
            else operational_limit.model_dump(mode="json")
        ),
        "reason_codes": [item.value for item in reasons],
        "message": message,
    }
    if imported_replay_fact is not None:
        payload["imported_replay"] = imported_replay_fact.model_dump(mode="json")
    return VerdictResult(
        experiment_id=record.spec.experiment_id,
        verdict=verdict,
        aggregate_artifact_version_id=reference.artifact_version_id,
        aggregate_sha256=reference.sha256,
        policy_sha256=policy.content_hash,
        objective=objective_fact,
        protections=protection_facts,
        replay_completeness=replay_fact,
        score_matrix=matrix_fact,
        imported_replay=imported_replay_fact,
        operational_limit=operational_limit,
        reason_codes=reasons,
        message=message,
        content_hash=sha256_json(payload),
    )


def _row_is_complete(row: ScorerAggregate | None, expected: int) -> bool:
    if row is None:
        return False
    return (
        row.planned == expected
        and row.denominator == expected
        and row.scored == expected
        and row.abstained == 0
        and row.blocked == 0
        and row.error == 0
    )
