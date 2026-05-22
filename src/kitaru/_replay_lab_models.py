"""Shared data shapes for Replay Lab reports and policy."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Literal

DEFAULT_EXPECTED_ARTIFACTS = ("scorecard", "final_response")
REPLAY_DRIFT_QUALITY_THRESHOLD = 0.10
EFFICIENCY_WIN_THRESHOLD = 0.10
TERMINAL_STATUS_VALUES = {"completed", "failed", "cancelled"}
EVALUATOR_ERROR_POLICIES = {"warn", "fail"}
EVALUATOR_PRECEDENCE_POLICIES = {"override", "fill_missing"}
ANALYTICS_SOURCE_VALUES = {"sdk", "mcp", "cli", "script"}
CandidateVerdictState = Literal["ship", "caution", "hold"]
ReplayTrustState = Literal["steady", "partial", "inspect"]

CANDIDATE_VERDICT_SHIP: CandidateVerdictState = "ship"
CANDIDATE_VERDICT_CAUTION: CandidateVerdictState = "caution"
CANDIDATE_VERDICT_HOLD: CandidateVerdictState = "hold"
REPLAY_TRUST_STEADY: ReplayTrustState = "steady"
REPLAY_TRUST_PARTIAL: ReplayTrustState = "partial"
REPLAY_TRUST_INSPECT: ReplayTrustState = "inspect"

RUNTIME_METRIC_KEYS = {
    "cost",
    "cost_usd",
    "total_cost",
    "estimated_cost",
    "duration_seconds",
    "total_duration_seconds",
    "latency_seconds",
    "latency",
    "response_latency_seconds",
    "latency_ms",
    "duration_ms",
    "output_text",
    "tool_call_count",
    "tool_calls",
    "tools_called",
    "llm_call_count",
    "llm_calls",
    "model_call_count",
    "checkpoint_count",
    "failed_checkpoint_count",
    "artifact_count",
    "error_log_count",
    "warning_log_count",
}


@dataclass(frozen=True)
class ReplayLabCase:
    """One observed execution selected for a Replay Lab cohort."""

    case_id: str
    exec_id: str
    reason: str
    from_checkpoint: str | None = None
    labels: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplayLabManifest:
    """Validated v0 cohort manifest."""

    name: str
    description: str
    default_from_checkpoint: str
    cases: list[ReplayLabCase]
    expected_artifacts: list[str] = field(
        default_factory=lambda: list(DEFAULT_EXPECTED_ARTIFACTS)
    )


@dataclass(frozen=True)
class CandidateDescriptor:
    """Validated v0 candidate replay descriptor."""

    id: str
    label: str
    flow_inputs: dict[str, Any] = field(default_factory=dict)
    checkpoint_overrides: dict[str, Any] = field(default_factory=dict)
    notes: str | None = None


@dataclass(frozen=True)
class EvaluatorDescriptor:
    """Serializable post-replay evaluator configuration."""

    target: str
    id: str | None = None
    on_error: str = "warn"
    precedence: str = "override"


@dataclass(frozen=True)
class EvaluatorInput:
    """Context passed to a Replay Lab evaluator callable."""

    case_id: str
    source_exec_id: str
    from_checkpoint: str
    lane_name: str
    lane: LaneReport
    candidate_id: str | None = None
    candidate_label: str | None = None


ReplayLabEvaluator = Callable[[EvaluatorInput], Mapping[str, Any] | None]


@dataclass(frozen=True)
class EvaluatorConfig:
    """Resolved post-replay evaluator configuration."""

    evaluator: ReplayLabEvaluator
    on_error: str
    precedence: str
    evaluator_id: str | None = None


@dataclass(frozen=True)
class MetricSnapshot:
    """Metrics extracted from one execution lane."""

    cost: float | None = None
    duration_seconds: float | None = None
    latency_seconds: float | None = None
    quality_score: float | None = None
    output_text: str | None = None
    tool_call_count: int | None = None
    llm_call_count: int | None = None
    checkpoint_count: int = 0
    failed_checkpoint_count: int = 0
    artifact_count: int = 0
    error_log_count: int = 0
    warning_log_count: int = 0
    scorecard: dict[str, Any] | None = None
    evaluation: dict[str, Any] | None = None


@dataclass(frozen=True)
class LaneReport:
    """Report data for one case/lane combination."""

    lane: str
    exec_id: str | None
    status: str
    metrics: MetricSnapshot
    timed_out: bool = False
    error_message: str | None = None
    limitations: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class NumericDelta:
    """Absolute and percent change for a numeric metric."""

    baseline: float | None
    comparison: float | None
    absolute: float | None
    percent: float | None


@dataclass(frozen=True)
class CaseDelta:
    """Metric changes between two lanes."""

    cost: NumericDelta
    duration_seconds: NumericDelta
    latency_seconds: NumericDelta
    quality_score: NumericDelta


@dataclass(frozen=True)
class ReplayTrustStatus:
    """Canonical replay trust status for one case or the whole report."""

    status: ReplayTrustState
    label: str
    detail: str
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CandidateResult:
    """Report data for one candidate lane within a case."""

    candidate_id: str
    candidate_label: str
    lane: LaneReport
    effect_vs_baseline: CaseDelta
    output_changed_vs_baseline: bool | None
    verdict: CandidateVerdictState
    limitations: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReplayLabCaseReport:
    """Complete report data for one selected case."""

    case_id: str
    source_exec_id: str
    from_checkpoint: str
    reason: str
    labels: dict[str, str]
    lanes: dict[str, LaneReport]
    replay_drift: CaseDelta
    # Derived legacy flag kept in the report for older renderers/consumers.
    # `replay_trust.status` is the canonical state used by new policy code.
    replay_drift_warning: bool
    replay_trust: ReplayTrustStatus
    candidate_results: list[CandidateResult]
    limitations: list[str]


@dataclass(frozen=True)
class ReplayLabReport:
    """Structured Replay Lab report."""

    name: str
    description: str
    candidates: list[CandidateDescriptor]
    expected_artifacts: list[str]
    cases: list[ReplayLabCaseReport]
    created_at: str
    summary: dict[str, Any]
    report_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report payload."""
        return to_plain_data(self)


def to_plain_data(value: Any) -> Any:
    """Convert dataclasses and nested containers into JSON-safe plain data."""
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: to_plain_data(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {str(key): to_plain_data(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [to_plain_data(item) for item in value]
    return value
