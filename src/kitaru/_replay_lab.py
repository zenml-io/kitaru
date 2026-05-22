"""Internal Replay Lab comparison helpers.

Replay Lab is intentionally internal for now.  It gives MCP tools and examples a
single place to parse a cohort manifest, replay the same cases through baseline
and candidate lanes, and render an honest comparison report.
"""

from __future__ import annotations

import importlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import kitaru.client as client_api
from kitaru.analytics import AnalyticsEvent, track
from kitaru.client import Execution, ExecutionStatus

DEFAULT_EXPECTED_ARTIFACTS = ("scorecard", "final_response")
REPLAY_DRIFT_QUALITY_THRESHOLD = 0.10
EFFICIENCY_WIN_THRESHOLD = 0.10
REQUIRED_EVALUATION_SECTIONS = (
    "has_summary",
    "lists_known_requirements",
    "lists_missing_information",
    "lists_risks",
    "gives_next_action",
)
TERMINAL_STATUS_VALUES = {"completed", "failed", "cancelled"}
EVALUATOR_ERROR_POLICIES = {"warn", "fail"}
EVALUATOR_PRECEDENCE_POLICIES = {"override", "fill_missing"}
ANALYTICS_SOURCE_VALUES = {"sdk", "mcp", "cli"}
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
class CandidateResult:
    """Report data for one candidate lane within a case."""

    candidate_id: str
    candidate_label: str
    lane: LaneReport
    effect_vs_baseline: CaseDelta
    output_changed_vs_baseline: bool | None
    verdict: str
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
    replay_drift_warning: bool
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
        return _to_plain_data(self)


def load_manifest(
    manifest: Mapping[str, Any] | None = None,
    *,
    manifest_path: str | Path | None = None,
) -> ReplayLabManifest:
    """Load and validate a Replay Lab manifest from an object or a path."""
    if manifest is None and manifest_path is None:
        raise ValueError("Provide either `manifest` or `manifest_path`.")
    if manifest is not None and manifest_path is not None:
        raise ValueError("Provide only one of `manifest` or `manifest_path`.")

    raw = dict(manifest) if manifest is not None else _read_manifest_file(manifest_path)
    return validate_manifest(raw)


def validate_manifest(raw: Mapping[str, Any]) -> ReplayLabManifest:
    """Validate a raw v0 cohort manifest mapping."""
    name = _required_str(raw, "name", aliases=("cohort_name",))
    description = _optional_str(raw.get("description"), default="")
    default_from_checkpoint = _required_str(
        raw,
        "default_from_checkpoint",
        aliases=("default_replay_checkpoint", "from_checkpoint", "checkpoint"),
    )

    expected_artifacts = _validate_string_list(
        raw.get("expected_artifacts", list(DEFAULT_EXPECTED_ARTIFACTS)),
        field_name="expected_artifacts",
    )
    if not expected_artifacts:
        expected_artifacts = list(DEFAULT_EXPECTED_ARTIFACTS)

    cases = _validate_cases(raw, default_from_checkpoint=default_from_checkpoint)
    if not cases:
        raise ValueError("Manifest must include at least one case or execution ID.")

    return ReplayLabManifest(
        name=name,
        description=description,
        default_from_checkpoint=default_from_checkpoint,
        cases=cases,
        expected_artifacts=expected_artifacts,
    )


def validate_candidate_descriptor(raw: Mapping[str, Any]) -> CandidateDescriptor:
    """Validate the v0 candidate descriptor shape."""
    candidate_id = _required_str(raw, "id")
    label = _required_str(raw, "label")
    flow_inputs = _optional_mapping(raw.get("flow_inputs"), field_name="flow_inputs")
    checkpoint_overrides = _optional_mapping(
        raw.get("checkpoint_overrides"), field_name="checkpoint_overrides"
    )
    notes = raw.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise ValueError("Candidate `notes` must be a string when provided.")

    unsupported = set(raw) - {
        "id",
        "label",
        "flow_inputs",
        "checkpoint_overrides",
        "notes",
    }
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported candidate descriptor field(s): {names}.")

    return CandidateDescriptor(
        id=candidate_id,
        label=label,
        flow_inputs=flow_inputs,
        checkpoint_overrides=checkpoint_overrides,
        notes=notes,
    )


def validate_candidate_descriptors(
    raw_candidates: Sequence[Mapping[str, Any]],
) -> list[CandidateDescriptor]:
    """Validate the canonical plural candidate descriptor list."""
    if not isinstance(raw_candidates, Sequence) or isinstance(
        raw_candidates, str | bytes
    ):
        raise ValueError("`candidate_descriptors` must be a list of objects.")
    candidates = []
    for index, raw in enumerate(raw_candidates, start=1):
        if not isinstance(raw, Mapping):
            raise ValueError(f"`candidate_descriptors[{index}]` must be an object.")
        candidates.append(validate_candidate_descriptor(raw))
    if not candidates:
        raise ValueError("Provide at least one candidate descriptor.")
    seen_ids: set[str] = set()
    for candidate in candidates:
        if candidate.id in seen_ids:
            raise ValueError(f"Duplicate candidate descriptor id `{candidate.id}`.")
        seen_ids.add(candidate.id)
    return candidates


def validate_evaluator_descriptor(raw: Mapping[str, Any]) -> EvaluatorDescriptor:
    """Validate a serializable evaluator descriptor."""
    target = _required_str(raw, "target")
    if "/" in target or target.endswith(".py"):
        raise ValueError("Evaluator `target` must be a module:function reference.")
    if ":" not in target:
        raise ValueError("Evaluator `target` must use module:function format.")
    evaluator_id = raw.get("id", raw.get("evaluator_id"))
    if evaluator_id is not None and (
        not isinstance(evaluator_id, str) or not evaluator_id.strip()
    ):
        raise ValueError("Evaluator `id` must be a non-empty string when provided.")
    on_error = _optional_policy(
        raw.get("on_error", "warn"),
        field_name="on_error",
        allowed=EVALUATOR_ERROR_POLICIES,
    )
    precedence = _optional_policy(
        raw.get("precedence", "override"),
        field_name="precedence",
        allowed=EVALUATOR_PRECEDENCE_POLICIES,
    )
    unsupported = set(raw) - {"target", "id", "evaluator_id", "on_error", "precedence"}
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported evaluator descriptor field(s): {names}.")
    return EvaluatorDescriptor(
        target=target,
        id=evaluator_id.strip() if isinstance(evaluator_id, str) else None,
        on_error=on_error,
        precedence=precedence,
    )


def compare_replay_lab(
    *,
    manifest: Mapping[str, Any] | None = None,
    manifest_path: str | Path | None = None,
    candidate_descriptor: Mapping[str, Any] | None = None,
    candidate_descriptors: Sequence[Mapping[str, Any]] | None = None,
    evaluator: ReplayLabEvaluator | None = None,
    evaluator_descriptor: Mapping[str, Any] | None = None,
    evaluator_on_error: str = "warn",
    evaluator_precedence: str = "override",
    client: Any | None = None,
    timeout_seconds: float = 300,
    poll_interval_seconds: float = 5,
    report_dir: str | Path | None = None,
    source: str = "sdk",
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayLabReport:
    """Run a multi-candidate Replay Lab comparison and return a report."""
    parsed_manifest: ReplayLabManifest | None = None
    candidates: list[CandidateDescriptor] = []
    has_evaluator = evaluator is not None or evaluator_descriptor is not None
    try:
        if timeout_seconds < 0:
            raise ValueError("`timeout_seconds` must be >= 0.")
        if poll_interval_seconds <= 0:
            raise ValueError("`poll_interval_seconds` must be > 0.")
        if candidate_descriptor is not None and candidate_descriptors is not None:
            raise ValueError(
                "Provide only one of `candidate_descriptor` or `candidate_descriptors`."
            )
        if candidate_descriptor is None and candidate_descriptors is None:
            raise ValueError("Provide `candidate_descriptors`.")
        if evaluator is not None and evaluator_descriptor is not None:
            raise ValueError("Provide only one evaluator callable or descriptor.")

        parsed_manifest = load_manifest(manifest, manifest_path=manifest_path)
        if candidate_descriptor is not None:
            candidates = validate_candidate_descriptors([candidate_descriptor])
        else:
            assert candidate_descriptors is not None
            candidates = validate_candidate_descriptors(candidate_descriptors)

        evaluator_config = _resolve_evaluator_config(
            evaluator=evaluator,
            evaluator_descriptor=evaluator_descriptor,
            evaluator_on_error=evaluator_on_error,
            evaluator_precedence=evaluator_precedence,
        )
        _track_replay_lab_event(
            AnalyticsEvent.REPLAY_LAB_COMPARE_REQUESTED,
            case_count=len(parsed_manifest.cases),
            candidate_count=len(candidates),
            has_evaluator=has_evaluator,
            source=source,
        )
        kitaru_client = client if client is not None else client_api.KitaruClient()

        case_reports = [
            _run_case_compare(
                case=case,
                manifest=parsed_manifest,
                candidates=candidates,
                evaluator_config=evaluator_config,
                client=kitaru_client,
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
                clock=clock,
                sleep=sleep,
            )
            for case in parsed_manifest.cases
        ]

        report_paths: dict[str, str] = {}
        summary = _summarize_cases(case_reports, candidates)
        report = ReplayLabReport(
            name=parsed_manifest.name,
            description=parsed_manifest.description,
            candidates=candidates,
            expected_artifacts=parsed_manifest.expected_artifacts,
            cases=case_reports,
            created_at=datetime.now(UTC).isoformat(),
            summary=summary,
            report_paths=report_paths,
        )
        if report_dir is not None:
            report_paths.update(write_report_files(report, report_dir))
        _track_replay_lab_event(
            AnalyticsEvent.REPLAY_LAB_COMPARE_COMPLETED,
            case_count=len(parsed_manifest.cases),
            candidate_count=len(candidates),
            has_evaluator=has_evaluator,
            source=source,
            completed=True,
            failed_or_timed_out_lane_count=summary["failed_or_timed_out_lane_count"],
        )
        return report
    except Exception:
        _track_replay_lab_event(
            AnalyticsEvent.REPLAY_LAB_COMPARE_FAILED,
            case_count=len(parsed_manifest.cases) if parsed_manifest else 0,
            candidate_count=len(candidates),
            has_evaluator=has_evaluator,
            source=source,
        )
        raise


def render_json_report(report: ReplayLabReport) -> str:
    """Render a report as stable, pretty JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def render_markdown_report(report: ReplayLabReport) -> str:
    """Render a human-readable Markdown report."""
    lines = [
        f"# Replay Lab Report: {report.name}",
        "",
        f"Generated: {report.created_at}",
        f"Candidates: {len(report.candidates)}",
    ]
    if report.description:
        lines.extend(["", report.description])
    lines.extend(["", "## Candidates", ""])
    for candidate in report.candidates:
        suffix = f" — {candidate.notes}" if candidate.notes else ""
        lines.append(f"- `{candidate.id}`: {candidate.label}{suffix}")

    summary = report.summary
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Cases: {summary['case_count']}",
            f"- Candidates: {summary['candidate_count']}",
            f"- Failed or timed-out lanes: {summary['failed_or_timed_out_lane_count']}",
            "- Cases with replay drift warning: "
            f"{summary['replay_drift_warning_count']}",
            f"- Replay trust: {summary['replay_trust']['label']}",
            f"- Recommendation: {summary['overall_recommendation']}",
            "",
            "### Candidate summary",
            "",
            "| Candidate | Aggregate verdict | Completed lanes | Changed outputs | "
            "Efficiency wins | Quality losses | Avg cost | Avg latency | Avg quality |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for candidate_id, candidate_summary in summary["candidates"].items():
        lines.append(
            "| "
            f"`{candidate_id}` | "
            f"{candidate_summary['aggregate_verdict']} | "
            f"{candidate_summary['completed_count']} | "
            f"{candidate_summary['changed_output_count']} | "
            f"{candidate_summary['efficiency_win_count']} | "
            f"{candidate_summary['quality_loss_count']} | "
            f"{_format_number(candidate_summary['average_cost'])} | "
            f"{_format_seconds(candidate_summary['average_latency_seconds'])} | "
            f"{_format_number(candidate_summary['average_quality_score'])} |"
        )

    lines.extend(
        [
            "",
            "## Case comparison",
            "",
            "| Case | Candidate | Observed cost | Baseline cost | Candidate cost | "
            "Cost Δ | Quality Δ | Output changed? | Verdict |",
            "|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for case in report.cases:
        observed = case.lanes["observed"].metrics
        baseline = case.lanes["baseline_replay"].metrics
        for result in case.candidate_results:
            candidate = result.lane.metrics
            lines.append(
                "| "
                f"{case.case_id} | "
                f"`{result.candidate_id}` | "
                f"{_format_number(observed.cost)} | "
                f"{_format_number(baseline.cost)} | "
                f"{_format_number(candidate.cost)} | "
                f"{_format_delta(result.effect_vs_baseline.cost)} | "
                f"{_format_delta(result.effect_vs_baseline.quality_score)} | "
                f"{_format_bool(result.output_changed_vs_baseline)} | "
                f"{result.verdict} |"
            )

    for case in report.cases:
        lines.extend(["", f"## {case.case_id}", "", f"Reason: {case.reason}"])
        lines.extend(
            [
                "",
                "| Lane | Execution | Status | Cost | Duration | Latency | Quality |",
                "|---|---|---|---:|---:|---:|---:|",
            ]
        )
        for lane_name in ("observed", "baseline_replay"):
            lane = case.lanes[lane_name]
            lines.append(_markdown_lane_row(lane_name, lane))
        for result in case.candidate_results:
            lines.append(
                _markdown_lane_row(f"candidate:{result.candidate_id}", result.lane)
            )
        if case.limitations:
            lines.extend(["", "Limitations:"])
            lines.extend(f"- {limitation}" for limitation in case.limitations)
    lines.append("")
    return "\n".join(lines)


def write_report_files(
    report: ReplayLabReport,
    report_dir: str | Path,
) -> dict[str, str]:
    """Write JSON and Markdown reports and return their paths."""
    output_dir = Path(report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = _slugify(report.name)
    json_path = output_dir / f"{slug}.json"
    markdown_path = output_dir / f"{slug}.md"
    json_path.write_text(render_json_report(report), encoding="utf-8")
    markdown_path.write_text(render_markdown_report(report), encoding="utf-8")
    return {"json": str(json_path), "markdown": str(markdown_path)}


def extract_metrics(
    execution: Execution,
    *,
    expected_artifacts: Sequence[str] = DEFAULT_EXPECTED_ARTIFACTS,
    log_entries: Sequence[Any] | None = None,
) -> tuple[MetricSnapshot, list[str]]:
    """Extract Replay Lab metrics from one execution."""
    limitations: list[str] = []
    artifacts = _unique_artifacts(execution)
    artifact_values: dict[str, Any] = {}

    for artifact in artifacts:
        name = str(getattr(artifact, "name", ""))
        if name in expected_artifacts:
            try:
                artifact_values[name] = artifact.load()
            except Exception as exc:  # pragma: no cover - defensive boundary
                limitations.append(
                    f"Could not load `{name}` artifact ({type(exc).__name__}: {exc})."
                )

    scorecard = _scorecard_from_values(artifact_values)
    if scorecard is None:
        limitations.append(
            "Missing `scorecard` artifact; cost, quality, and call metrics may "
            "be unavailable."
        )
        scorecard = {}

    final_response = _final_response_from_values(artifact_values)
    if final_response is None:
        limitations.append(
            "Missing `final_response` artifact; output-change comparison may "
            "be unavailable."
        )

    duration_seconds = _duration_seconds(execution)
    metadata = getattr(execution, "metadata", {}) or {}
    metric_sources = [scorecard, metadata]

    latency_seconds = _first_number(
        metric_sources,
        keys=("latency_seconds", "latency", "response_latency_seconds"),
    )
    latency_ms = _first_number(metric_sources, keys=("latency_ms", "duration_ms"))
    if latency_seconds is None and latency_ms is not None:
        latency_seconds = latency_ms / 1000

    log_summary = _summarize_log_entries(log_entries or [])
    tool_calls = _first_number(
        metric_sources,
        keys=("tool_call_count", "tool_calls", "tools_called"),
    )
    if tool_calls is None and log_summary["tool_call_count"]:
        tool_calls = float(log_summary["tool_call_count"])
    llm_calls = _first_number(
        metric_sources,
        keys=("llm_call_count", "llm_calls", "model_call_count"),
    )
    if llm_calls is None and log_summary["llm_call_count"]:
        llm_calls = float(log_summary["llm_call_count"])

    checkpoints = list(getattr(execution, "checkpoints", []) or [])
    failed_checkpoints = [
        checkpoint
        for checkpoint in checkpoints
        if _status_value(getattr(checkpoint, "status", None)) == "failed"
    ]

    metrics = MetricSnapshot(
        cost=_first_number(
            metric_sources,
            keys=("cost", "cost_usd", "total_cost", "estimated_cost"),
        ),
        duration_seconds=_first_number(
            metric_sources,
            keys=("duration_seconds", "total_duration_seconds"),
        )
        or duration_seconds,
        latency_seconds=latency_seconds,
        quality_score=_first_number(
            metric_sources,
            keys=("quality_score", "score", "evaluator_score", "accuracy"),
        ),
        output_text=final_response,
        tool_call_count=int(tool_calls) if tool_calls is not None else None,
        llm_call_count=int(llm_calls) if llm_calls is not None else None,
        checkpoint_count=len(checkpoints),
        failed_checkpoint_count=len(failed_checkpoints),
        artifact_count=len(artifacts),
        error_log_count=log_summary["error_log_count"],
        warning_log_count=log_summary["warning_log_count"],
        scorecard=dict(scorecard) if scorecard else None,
    )
    return metrics, limitations


def build_case_delta(baseline: MetricSnapshot, comparison: MetricSnapshot) -> CaseDelta:
    """Build deltas for the standard numeric metrics."""
    return CaseDelta(
        cost=_numeric_delta(baseline.cost, comparison.cost),
        duration_seconds=_numeric_delta(
            baseline.duration_seconds, comparison.duration_seconds
        ),
        latency_seconds=_numeric_delta(
            baseline.latency_seconds, comparison.latency_seconds
        ),
        quality_score=_numeric_delta(baseline.quality_score, comparison.quality_score),
    )


def _run_case_compare(
    *,
    case: ReplayLabCase,
    manifest: ReplayLabManifest,
    candidates: Sequence[CandidateDescriptor],
    evaluator_config: EvaluatorConfig | None,
    client: Any,
    timeout_seconds: float,
    poll_interval_seconds: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> ReplayLabCaseReport:
    from_checkpoint = case.from_checkpoint or manifest.default_from_checkpoint
    observed_execution = client.executions.get(case.exec_id)
    observed_lane = _lane_from_execution(
        "observed",
        observed_execution,
        client=client,
        expected_artifacts=manifest.expected_artifacts,
    )

    baseline_started = _start_replay_lane(
        client=client,
        source_exec_id=case.exec_id,
        lane="baseline_replay",
        from_checkpoint=from_checkpoint,
        overrides=None,
        flow_inputs={},
    )
    baseline_lane = _finish_started_lane(
        client=client,
        lane="baseline_replay",
        started_execution=baseline_started,
        expected_artifacts=manifest.expected_artifacts,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        clock=clock,
        sleep=sleep,
    )

    lanes = {
        "observed": observed_lane,
        "baseline_replay": baseline_lane,
    }
    replay_drift = build_case_delta(observed_lane.metrics, baseline_lane.metrics)
    replay_drift_warning = _case_has_replay_drift_warning(lanes, replay_drift)
    candidate_results: list[CandidateResult] = []
    for candidate in candidates:
        candidate_started = _start_replay_lane(
            client=client,
            source_exec_id=case.exec_id,
            lane="candidate_replay",
            from_checkpoint=from_checkpoint,
            overrides=candidate.checkpoint_overrides or None,
            flow_inputs=candidate.flow_inputs,
        )
        candidate_lane = _finish_started_lane(
            client=client,
            lane="candidate_replay",
            started_execution=candidate_started,
            expected_artifacts=manifest.expected_artifacts,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            clock=clock,
            sleep=sleep,
        )
        candidate_results.append(
            _build_candidate_result(
                candidate=candidate,
                lane=candidate_lane,
                baseline_lane=baseline_lane,
                replay_drift_warning=replay_drift_warning,
            )
        )

    if evaluator_config is not None:
        lanes, candidate_results = _apply_evaluator_to_case(
            case=case,
            from_checkpoint=from_checkpoint,
            lanes=lanes,
            candidate_results=candidate_results,
            evaluator_config=evaluator_config,
        )
        replay_drift = build_case_delta(
            lanes["observed"].metrics,
            lanes["baseline_replay"].metrics,
        )
        replay_drift_warning = _case_has_replay_drift_warning(lanes, replay_drift)
        candidate_results = [
            _refresh_candidate_result(
                result,
                baseline_lane=lanes["baseline_replay"],
                replay_drift_warning=replay_drift_warning,
            )
            for result in candidate_results
        ]

    limitations = _case_limitations(
        lanes,
        replay_drift_warning=replay_drift_warning,
        candidate_results=candidate_results,
    )

    return ReplayLabCaseReport(
        case_id=case.case_id,
        source_exec_id=case.exec_id,
        from_checkpoint=from_checkpoint,
        reason=case.reason,
        labels=case.labels,
        lanes=lanes,
        replay_drift=replay_drift,
        replay_drift_warning=replay_drift_warning,
        candidate_results=candidate_results,
        limitations=limitations,
    )


def _build_candidate_result(
    *,
    candidate: CandidateDescriptor,
    lane: LaneReport,
    baseline_lane: LaneReport,
    replay_drift_warning: bool,
) -> CandidateResult:
    return _candidate_result_from_lane(
        candidate_id=candidate.id,
        candidate_label=candidate.label,
        lane=lane,
        baseline_lane=baseline_lane,
        replay_drift_warning=replay_drift_warning,
    )


def _refresh_candidate_result(
    result: CandidateResult,
    *,
    baseline_lane: LaneReport,
    replay_drift_warning: bool,
) -> CandidateResult:
    return _candidate_result_from_lane(
        candidate_id=result.candidate_id,
        candidate_label=result.candidate_label,
        lane=result.lane,
        baseline_lane=baseline_lane,
        replay_drift_warning=replay_drift_warning,
        limitations=result.limitations,
    )


def _candidate_result_from_lane(
    *,
    candidate_id: str,
    candidate_label: str,
    lane: LaneReport,
    baseline_lane: LaneReport,
    replay_drift_warning: bool,
    limitations: Sequence[str] | None = None,
) -> CandidateResult:
    effect = build_case_delta(baseline_lane.metrics, lane.metrics)
    output_changed = _changed_output(
        baseline_lane.metrics.output_text,
        lane.metrics.output_text,
    )
    return CandidateResult(
        candidate_id=candidate_id,
        candidate_label=candidate_label,
        lane=lane,
        effect_vs_baseline=effect,
        output_changed_vs_baseline=output_changed,
        verdict=_candidate_verdict(
            lane,
            replay_drift_warning=replay_drift_warning,
            effect=effect,
            output_changed=output_changed,
        ),
        limitations=list(limitations)
        if limitations is not None
        else list(lane.limitations),
    )


def _candidate_verdict(
    lane: LaneReport,
    *,
    replay_drift_warning: bool,
    effect: CaseDelta,
    output_changed: bool | None,
) -> str:
    if lane.timed_out or lane.status != "completed":
        return "hold"
    if _case_delta_has_quality_loss(effect):
        return "hold"
    if replay_drift_warning or output_changed is True:
        return "caution"
    return "ship"


def _start_replay_lane(
    *,
    client: Any,
    source_exec_id: str,
    lane: str,
    from_checkpoint: str,
    overrides: dict[str, Any] | None,
    flow_inputs: Mapping[str, Any],
) -> Execution | Exception:
    try:
        return client.executions.replay(
            source_exec_id,
            from_=from_checkpoint,
            overrides=overrides,
            **dict(flow_inputs),
        )
    except Exception as exc:  # Lane-level failure: keep cohort processing.
        return exc


def _finish_started_lane(
    *,
    client: Any,
    lane: str,
    started_execution: Execution | Exception,
    expected_artifacts: Sequence[str],
    timeout_seconds: float,
    poll_interval_seconds: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> LaneReport:
    if isinstance(started_execution, Exception):
        return LaneReport(
            lane=lane,
            exec_id=None,
            status="failed",
            metrics=MetricSnapshot(),
            error_message=str(started_execution),
            limitations=[
                f"{lane} replay could not be started "
                f"({type(started_execution).__name__})."
            ],
        )

    final_execution, timed_out = wait_for_terminal_lane(
        client=client,
        execution=started_execution,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        clock=clock,
        sleep=sleep,
    )
    lane_report = _lane_from_execution(
        lane,
        final_execution,
        client=client,
        expected_artifacts=expected_artifacts,
        timed_out=timed_out,
    )
    if timed_out:
        return _replace_lane(
            lane_report,
            status="timeout",
            timed_out=True,
            error_message=(
                "Lane did not reach a terminal status within "
                f"{timeout_seconds:g} seconds."
            ),
            extra_limitations=[
                "Lane timed out; metrics may describe an unfinished run."
            ],
        )
    return lane_report


def wait_for_terminal_lane(
    *,
    client: Any,
    execution: Execution,
    timeout_seconds: float,
    poll_interval_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[Execution, bool]:
    """Poll one replay lane until it reaches a terminal status or times out."""
    current = execution
    deadline = clock() + timeout_seconds
    while not _is_terminal_status(getattr(current, "status", None)):
        if clock() >= deadline:
            return current, True
        sleep(min(poll_interval_seconds, max(0, deadline - clock())))
        current = client.executions.get(current.exec_id)
    return current, False


def _lane_from_execution(
    lane: str,
    execution: Execution,
    *,
    client: Any | None = None,
    expected_artifacts: Sequence[str],
    timed_out: bool = False,
) -> LaneReport:
    log_entries, log_limitations = _load_log_entries(client, execution)
    metrics, limitations = extract_metrics(
        execution,
        expected_artifacts=expected_artifacts,
        log_entries=log_entries,
    )
    limitations.extend(log_limitations)
    return LaneReport(
        lane=lane,
        exec_id=execution.exec_id,
        status=_status_value(execution.status),
        metrics=metrics,
        timed_out=timed_out,
        error_message=_failure_message(execution),
        limitations=limitations,
    )


def _replace_lane(
    lane: LaneReport,
    *,
    status: str,
    timed_out: bool,
    error_message: str,
    extra_limitations: list[str],
) -> LaneReport:
    return replace(
        lane,
        status=status,
        timed_out=timed_out,
        error_message=error_message,
        limitations=[*lane.limitations, *extra_limitations],
    )


def _resolve_evaluator_config(
    *,
    evaluator: ReplayLabEvaluator | None,
    evaluator_descriptor: Mapping[str, Any] | None,
    evaluator_on_error: str,
    evaluator_precedence: str,
) -> EvaluatorConfig | None:
    if evaluator_descriptor is not None:
        descriptor = validate_evaluator_descriptor(evaluator_descriptor)
        return EvaluatorConfig(
            evaluator=_load_evaluator(descriptor),
            on_error=descriptor.on_error,
            precedence=descriptor.precedence,
            evaluator_id=descriptor.id,
        )
    if evaluator is None:
        return None
    return EvaluatorConfig(
        evaluator=evaluator,
        on_error=_optional_policy(
            evaluator_on_error,
            field_name="evaluator_on_error",
            allowed=EVALUATOR_ERROR_POLICIES,
        ),
        precedence=_optional_policy(
            evaluator_precedence,
            field_name="evaluator_precedence",
            allowed=EVALUATOR_PRECEDENCE_POLICIES,
        ),
    )


def _load_evaluator(descriptor: EvaluatorDescriptor) -> ReplayLabEvaluator:
    module_name, function_name = descriptor.target.split(":", 1)
    if not module_name or not function_name:
        raise ValueError("Evaluator `target` must use module:function format.")
    module = importlib.import_module(module_name)
    loaded = getattr(module, function_name)
    if not callable(loaded):
        raise ValueError("Evaluator target must resolve to a callable.")
    return loaded


def _apply_evaluator_to_case(
    *,
    case: ReplayLabCase,
    from_checkpoint: str,
    lanes: Mapping[str, LaneReport],
    candidate_results: Sequence[CandidateResult],
    evaluator_config: EvaluatorConfig,
) -> tuple[dict[str, LaneReport], list[CandidateResult]]:
    updated_lanes = dict(lanes)
    for lane_name, lane in lanes.items():
        updated_lanes[lane_name] = _evaluate_lane(
            evaluator=evaluator_config.evaluator,
            evaluator_id=evaluator_config.evaluator_id,
            on_error=evaluator_config.on_error,
            precedence=evaluator_config.precedence,
            request=EvaluatorInput(
                case_id=case.case_id,
                source_exec_id=case.exec_id,
                from_checkpoint=from_checkpoint,
                lane_name=lane_name,
                lane=lane,
            ),
        )

    updated_results: list[CandidateResult] = []
    for result in candidate_results:
        evaluated_lane = _evaluate_lane(
            evaluator=evaluator_config.evaluator,
            evaluator_id=evaluator_config.evaluator_id,
            on_error=evaluator_config.on_error,
            precedence=evaluator_config.precedence,
            request=EvaluatorInput(
                case_id=case.case_id,
                source_exec_id=case.exec_id,
                from_checkpoint=from_checkpoint,
                lane_name="candidate_replay",
                lane=result.lane,
                candidate_id=result.candidate_id,
                candidate_label=result.candidate_label,
            ),
        )
        updated_results.append(
            replace(
                result,
                lane=evaluated_lane,
                limitations=_dedupe(
                    [
                        *result.limitations,
                        *_new_limitations(result.lane, evaluated_lane),
                    ]
                ),
            )
        )
    return updated_lanes, updated_results


def _evaluate_lane(
    *,
    evaluator: ReplayLabEvaluator,
    evaluator_id: str | None,
    on_error: str,
    precedence: str,
    request: EvaluatorInput,
) -> LaneReport:
    if _lane_failed_or_timed_out(request.lane):
        return request.lane
    try:
        raw_evaluation = evaluator(request)
        if raw_evaluation is None:
            return request.lane
        if not isinstance(raw_evaluation, Mapping):
            raise ValueError("Evaluator must return a mapping or None.")
        return _apply_evaluation_result(
            request.lane,
            raw_evaluation,
            evaluator_id=evaluator_id,
            precedence=precedence,
        )
    except Exception as exc:
        if on_error == "fail":
            raise
        return _append_lane_limitations(
            request.lane,
            [f"Evaluator failed ({type(exc).__name__}: {exc})."],
        )


def _apply_evaluation_result(
    lane: LaneReport,
    raw_evaluation: Mapping[str, Any],
    *,
    evaluator_id: str | None,
    precedence: str,
) -> LaneReport:
    evaluation, limitations = _normalize_evaluation(raw_evaluation, evaluator_id)
    quality_score = _number(evaluation.get("quality_score"))
    final_quality = lane.metrics.quality_score
    if quality_score is not None and (
        precedence == "override" or lane.metrics.quality_score is None
    ):
        final_quality = quality_score
    return replace(
        lane,
        metrics=_replace_metrics(
            lane.metrics,
            quality_score=final_quality,
            evaluation=evaluation,
        ),
        limitations=[*lane.limitations, *limitations],
    )


def _normalize_evaluation(
    raw_evaluation: Mapping[str, Any],
    evaluator_id: str | None,
) -> tuple[dict[str, Any], list[str]]:
    evaluation = dict(raw_evaluation)
    limitations = _validate_optional_string_list(
        evaluation.get("limitations", []), field_name="Evaluator `limitations`"
    )
    for key in RUNTIME_METRIC_KEYS:
        evaluation.pop(key, None)
    if evaluator_id is not None and "evaluator_id" not in evaluation:
        evaluation["evaluator_id"] = evaluator_id
    return evaluation, limitations


def _replace_metrics(
    metrics: MetricSnapshot,
    *,
    quality_score: float | None,
    evaluation: dict[str, Any] | None,
) -> MetricSnapshot:
    return replace(
        metrics,
        quality_score=quality_score,
        evaluation=evaluation,
    )


def _append_lane_limitations(lane: LaneReport, limitations: list[str]) -> LaneReport:
    return replace(lane, limitations=[*lane.limitations, *limitations])


def _new_limitations(before: LaneReport, after: LaneReport) -> list[str]:
    return after.limitations[len(before.limitations) :]


def _lane_failed_or_timed_out(lane: LaneReport) -> bool:
    return lane.timed_out or lane.status != "completed"


def _append_number(values: list[float], value: float | None) -> None:
    if value is not None:
        values.append(value)


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _case_limitations(
    lanes: Mapping[str, LaneReport],
    *,
    replay_drift_warning: bool,
    candidate_results: Sequence[CandidateResult],
) -> list[str]:
    limitations: list[str] = []
    for lane_name, lane in lanes.items():
        limitations.extend(
            f"{lane_name}: {limitation}" for limitation in lane.limitations
        )
        if lane.status != "completed":
            limitations.append(f"{lane_name} lane ended with status `{lane.status}`.")
        if lane.timed_out:
            limitations.append(f"{lane_name} lane timed out.")
    for result in candidate_results:
        candidate_name = f"candidate `{result.candidate_id}`"
        limitations.extend(
            f"{candidate_name}: {limitation}" for limitation in result.limitations
        )
        if result.lane.status != "completed":
            limitations.append(
                f"{candidate_name} lane ended with status `{result.lane.status}`."
            )
        if result.lane.timed_out:
            limitations.append(f"{candidate_name} lane timed out.")
    if replay_drift_warning:
        limitations.append(
            "Observed-to-baseline replay drift is large; candidate effect has "
            "lower confidence."
        )
    return _dedupe(limitations)


def _summarize_cases(
    cases: Sequence[ReplayLabCaseReport],
    candidates: Sequence[CandidateDescriptor],
) -> dict[str, Any]:
    failed_or_timed_out = 0
    drift_warning_case_ids: list[str] = []
    candidate_summaries: dict[str, dict[str, Any]] = {
        candidate.id: {
            "candidate_id": candidate.id,
            "label": candidate.label,
            "aggregate_verdict": "hold",
            "completed_count": 0,
            "changed_output_count": 0,
            "failed_or_timed_out_lane_count": 0,
            "efficiency_win_count": 0,
            "quality_loss_count": 0,
            "cases_to_inspect": [],
            "average_cost": None,
            "average_latency_seconds": None,
            "average_quality_score": None,
        }
        for candidate in candidates
    }
    candidate_costs: dict[str, list[float]] = {
        candidate.id: [] for candidate in candidates
    }
    candidate_latencies: dict[str, list[float]] = {
        candidate.id: [] for candidate in candidates
    }
    candidate_qualities: dict[str, list[float]] = {
        candidate.id: [] for candidate in candidates
    }

    for case in cases:
        all_lanes = [
            *case.lanes.values(),
            *(result.lane for result in case.candidate_results),
        ]
        failed_or_timed_out += sum(
            1 for lane in all_lanes if _lane_failed_or_timed_out(lane)
        )
        if case.replay_drift_warning:
            drift_warning_case_ids.append(case.case_id)
        for result in case.candidate_results:
            summary = candidate_summaries[result.candidate_id]
            if _lane_failed_or_timed_out(result.lane):
                summary["failed_or_timed_out_lane_count"] += 1
            else:
                summary["completed_count"] += 1
                _append_number(
                    candidate_costs[result.candidate_id], result.lane.metrics.cost
                )
                _append_number(
                    candidate_latencies[result.candidate_id],
                    result.lane.metrics.latency_seconds,
                )
                _append_number(
                    candidate_qualities[result.candidate_id],
                    result.lane.metrics.quality_score,
                )
            if result.output_changed_vs_baseline is True:
                summary["changed_output_count"] += 1
                summary["cases_to_inspect"].append(case.case_id)
            if result.verdict != "ship":
                summary["cases_to_inspect"].append(case.case_id)
            if _has_efficiency_win(result):
                summary["efficiency_win_count"] += 1
            if _has_quality_loss(result):
                summary["quality_loss_count"] += 1

    for candidate_id, summary in candidate_summaries.items():
        summary["average_cost"] = _average(candidate_costs[candidate_id])
        summary["average_latency_seconds"] = _average(candidate_latencies[candidate_id])
        summary["average_quality_score"] = _average(candidate_qualities[candidate_id])
        summary["cases_to_inspect"] = _dedupe(summary["cases_to_inspect"])
        summary["aggregate_verdict"] = _aggregate_candidate_verdict(summary)

    candidate_ranking = sorted(candidate_summaries.values(), key=_candidate_rank_key)
    replay_trust = _replay_trust_summary(
        drift_warning_case_ids=drift_warning_case_ids,
        failed_or_timed_out_lane_count=failed_or_timed_out,
    )

    return {
        "case_count": len(cases),
        "candidate_count": len(candidates),
        "candidate_ids": [candidate.id for candidate in candidates],
        "candidates": candidate_summaries,
        "candidate_ranking": candidate_ranking,
        "overall_recommendation": _overall_recommendation(
            candidate_ranking,
            has_replay_drift=bool(drift_warning_case_ids),
            failed_or_timed_out_lane_count=failed_or_timed_out,
        ),
        "failed_or_timed_out_lane_count": failed_or_timed_out,
        "replay_drift_case_ids": drift_warning_case_ids,
        "replay_drift_warning_count": len(drift_warning_case_ids),
        "replay_trust": replay_trust,
    }


def _aggregate_candidate_verdict(summary: Mapping[str, Any]) -> str:
    failed_count = int(summary.get("failed_or_timed_out_lane_count", 0) or 0)
    completed_count = int(summary.get("completed_count", 0) or 0)
    quality_loss_count = int(summary.get("quality_loss_count", 0) or 0)
    efficiency_win_count = int(summary.get("efficiency_win_count", 0) or 0)
    cases_to_inspect = summary.get("cases_to_inspect", []) or []
    if failed_count or not completed_count or quality_loss_count:
        return "hold"
    if cases_to_inspect:
        return "caution"
    if efficiency_win_count:
        return "ship"
    return "caution"


def _candidate_rank_key(summary: Mapping[str, Any]) -> tuple[int, int, int, str]:
    verdict = str(summary.get("aggregate_verdict", "hold"))
    verdict_rank = {"ship": 0, "caution": 1, "hold": 2}.get(verdict, 3)
    return (
        verdict_rank,
        int(summary.get("quality_loss_count", 0) or 0),
        -int(summary.get("efficiency_win_count", 0) or 0),
        str(summary.get("candidate_id", "")),
    )


def _replay_trust_summary(
    *,
    drift_warning_case_ids: Sequence[str],
    failed_or_timed_out_lane_count: int,
) -> dict[str, str]:
    if drift_warning_case_ids:
        return {
            "label": "Replay trust: inspect first",
            "detail": (
                "High replay drift was detected for "
                f"{', '.join(drift_warning_case_ids)}. Treat candidate rankings "
                "as directional until those baseline replays are understood."
            ),
        }
    if failed_or_timed_out_lane_count:
        return {
            "label": "Replay trust: partial",
            "detail": (
                "One or more lanes failed or timed out, so the report can still "
                "teach you where to look but should not be treated as "
                "complete evidence."
            ),
        }
    return {
        "label": "Replay trust: steady",
        "detail": (
            "Observed and baseline replay lanes stayed within the configured drift "
            "threshold for this cohort."
        ),
    }


def _overall_recommendation(
    candidate_ranking: Sequence[Mapping[str, Any]],
    *,
    has_replay_drift: bool,
    failed_or_timed_out_lane_count: int,
) -> str:
    if has_replay_drift or failed_or_timed_out_lane_count:
        return (
            "Hold: inspect replay reliability before using this comparison as "
            "shipping evidence."
        )
    shippable = [
        candidate
        for candidate in candidate_ranking
        if candidate.get("aggregate_verdict") == "ship"
    ]
    if shippable:
        return (
            f"Ship candidate `{shippable[0].get('candidate_id')}` for a guarded trial: "
            "safe enough from this replay cohort, not a blind deployment."
        )
    if any(
        candidate.get("aggregate_verdict") == "caution"
        for candidate in candidate_ranking
    ):
        return (
            "Caution: at least one candidate is promising, but inspect the named "
            "cases before changing production traffic."
        )
    return (
        "Hold: no candidate produced enough efficiency gain without quality risk "
        "in this cohort."
    )


def _has_efficiency_win(result: CandidateResult) -> bool:
    for delta in (
        result.effect_vs_baseline.cost,
        result.effect_vs_baseline.latency_seconds,
    ):
        if delta.percent is not None and delta.percent <= -(
            EFFICIENCY_WIN_THRESHOLD * 100
        ):
            return True
    return False


def _has_quality_loss(result: CandidateResult) -> bool:
    return _case_delta_has_quality_loss(result.effect_vs_baseline)


def _case_delta_has_quality_loss(effect: CaseDelta) -> bool:
    quality_delta = effect.quality_score.absolute
    return quality_delta is not None and quality_delta < -REPLAY_DRIFT_QUALITY_THRESHOLD


def _read_manifest_file(path_like: str | Path | None) -> dict[str, Any]:
    if path_like is None:
        raise ValueError("`manifest_path` cannot be None.")
    path = Path(path_like)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError:
            raise ValueError("YAML manifests require PyYAML to be installed.") from None
        raw = yaml.safe_load(text)
    else:
        raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("Manifest file must contain an object at the top level.")
    return raw


def _validate_cases(
    raw: Mapping[str, Any],
    *,
    default_from_checkpoint: str,
) -> list[ReplayLabCase]:
    raw_cases = raw.get("cases")
    if raw_cases is None and raw.get("observed_execution_ids") is not None:
        raw_cases = [
            {"case_id": str(index + 1), "exec_id": exec_id, "reason": "Selected case"}
            for index, exec_id in enumerate(raw["observed_execution_ids"])
        ]
    if not isinstance(raw_cases, list):
        raise ValueError("Manifest `cases` must be a list.")

    cases: list[ReplayLabCase] = []
    seen_case_ids: set[str] = set()
    for index, item in enumerate(raw_cases, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Manifest case #{index} must be an object.")
        case_id = _required_str(item, "case_id", aliases=("id",))
        if case_id in seen_case_ids:
            raise ValueError(f"Duplicate manifest case_id `{case_id}`.")
        seen_case_ids.add(case_id)
        exec_id = _required_str(
            item, "exec_id", aliases=("execution_id", "observed_exec_id")
        )
        reason = _reason_text(item.get("reason", item.get("reasons")))
        from_checkpoint = item.get("from_checkpoint", default_from_checkpoint)
        if not isinstance(from_checkpoint, str) or not from_checkpoint.strip():
            raise ValueError(
                f"Manifest case `{case_id}` has invalid `from_checkpoint`."
            )
        labels = _labels(item.get("labels", {}), case_id=case_id)
        cases.append(
            ReplayLabCase(
                case_id=case_id,
                exec_id=exec_id,
                reason=reason,
                from_checkpoint=from_checkpoint.strip(),
                labels=labels,
            )
        )
    return cases


def _reason_text(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        text = "; ".join(item.strip() for item in value if item.strip())
        if text:
            return text
    raise ValueError("Manifest case `reason` must be a string or list of strings.")


def _labels(value: Any, *, case_id: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Manifest case `{case_id}` labels must be an object.")
    return {str(key): str(label_value) for key, label_value in value.items()}


def _required_str(
    mapping: Mapping[str, Any],
    key: str,
    *,
    aliases: Sequence[str] = (),
) -> str:
    value = None
    matched_key = key
    for candidate_key in (key, *aliases):
        if candidate_key in mapping:
            value = mapping[candidate_key]
            matched_key = candidate_key
            break
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"`{matched_key}` must be a non-empty string.")
    return value.strip()


def _optional_str(value: Any, *, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise ValueError("Optional string field must be a string when provided.")
    return value


def _optional_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Candidate `{field_name}` must be an object when provided.")
    return dict(value)


def _optional_policy(value: Any, *, field_name: str, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise ValueError(f"`{field_name}` must be one of: {allowed_text}.")
    return value


def _validate_optional_string_list(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field_name} must be a list of strings when provided.")
    return [item for item in value]


def _validate_string_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Manifest `{field_name}` must be a list of strings.")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError(
            f"Manifest `{field_name}` must be a list of non-empty strings."
        )
    return [item.strip() for item in value]


def _unique_artifacts(execution: Execution) -> list[Any]:
    seen: set[str] = set()
    artifacts: list[Any] = []
    for artifact in list(getattr(execution, "artifacts", []) or []):
        key = str(getattr(artifact, "artifact_id", id(artifact)))
        if key not in seen:
            seen.add(key)
            artifacts.append(artifact)
    for checkpoint in list(getattr(execution, "checkpoints", []) or []):
        for artifact in list(getattr(checkpoint, "artifacts", []) or []):
            key = str(getattr(artifact, "artifact_id", id(artifact)))
            if key not in seen:
                seen.add(key)
                artifacts.append(artifact)
    return artifacts


def _scorecard_from_values(values: Mapping[str, Any]) -> dict[str, Any] | None:
    value = values.get("scorecard")
    if isinstance(value, dict):
        return value
    return None


def _final_response_from_values(values: Mapping[str, Any]) -> str | None:
    value = values.get("final_response")
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("text", "content", "response", "output", "final_response"):
            nested = value.get(key)
            if isinstance(nested, str):
                return nested
    return json.dumps(value, sort_keys=True)


def _load_log_entries(
    client: Any | None,
    execution: Execution,
) -> tuple[list[Any], list[str]]:
    if client is None or not hasattr(client, "executions"):
        return [], []
    logs = getattr(client.executions, "logs", None)
    if not callable(logs):
        return [], []
    try:
        entries = logs(execution.exec_id, limit=500)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return [], [
            "Could not read execution logs "
            f"({type(exc).__name__}: {exc}); log-derived metrics are unavailable."
        ]
    return list(entries or []), []


def _summarize_log_entries(entries: Sequence[Any]) -> dict[str, int]:
    summary = {
        "error_log_count": 0,
        "warning_log_count": 0,
        "tool_call_count": 0,
        "llm_call_count": 0,
    }
    for entry in entries:
        level = str(getattr(entry, "level", "") or "").lower()
        message = str(getattr(entry, "message", entry) or "").lower()
        if level == "error":
            summary["error_log_count"] += 1
        if level in {"warn", "warning"}:
            summary["warning_log_count"] += 1
        if "tool" in message:
            summary["tool_call_count"] += 1
        if "llm" in message or "model" in message:
            summary["llm_call_count"] += 1
    return summary


def _duration_seconds(execution: Execution) -> float | None:
    started_at = getattr(execution, "started_at", None)
    ended_at = getattr(execution, "ended_at", None)
    if started_at is None or ended_at is None:
        return None
    return max(0.0, (ended_at - started_at).total_seconds())


def _first_number(
    sources: Sequence[Mapping[str, Any]],
    *,
    keys: Sequence[str],
) -> float | None:
    for source in sources:
        for key in keys:
            number = _number(source.get(key))
            if number is not None:
                return number
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _status_value(status: Any) -> str:
    if isinstance(status, ExecutionStatus):
        return status.value
    value = getattr(status, "value", status)
    return str(value).lower()


def _is_terminal_status(status: Any) -> bool:
    return _status_value(status) in TERMINAL_STATUS_VALUES


def _failure_message(execution: Execution) -> str | None:
    failure = getattr(execution, "failure", None)
    if failure is not None:
        message = getattr(failure, "message", None)
        if message:
            return str(message)
    status_reason = getattr(execution, "status_reason", None)
    return str(status_reason) if status_reason else None


def _numeric_delta(baseline: float | None, comparison: float | None) -> NumericDelta:
    if baseline is None or comparison is None:
        return NumericDelta(
            baseline=baseline,
            comparison=comparison,
            absolute=None,
            percent=None,
        )
    absolute = comparison - baseline
    percent = None if baseline == 0 else (absolute / baseline) * 100
    return NumericDelta(
        baseline=baseline,
        comparison=comparison,
        absolute=absolute,
        percent=percent,
    )


def _changed_output(baseline: str | None, candidate: str | None) -> bool | None:
    if baseline is None or candidate is None:
        return None
    return baseline.strip() != candidate.strip()


def _case_has_replay_drift_warning(
    lanes: Mapping[str, LaneReport], replay_drift: CaseDelta
) -> bool:
    if _has_large_replay_drift(replay_drift):
        return True
    observed = lanes.get("observed")
    baseline = lanes.get("baseline_replay")
    if observed is None or baseline is None:
        return False
    return _required_section_signature(
        observed.metrics.evaluation
    ) != _required_section_signature(baseline.metrics.evaluation)


def _required_section_signature(
    evaluation: Mapping[str, Any] | None,
) -> tuple[bool | None, ...]:
    if not evaluation:
        return tuple(None for _ in REQUIRED_EVALUATION_SECTIONS)
    scorecard = evaluation.get("scorecard")
    if not isinstance(scorecard, Mapping):
        return tuple(None for _ in REQUIRED_EVALUATION_SECTIONS)
    return tuple(
        scorecard.get(section) if isinstance(scorecard.get(section), bool) else None
        for section in REQUIRED_EVALUATION_SECTIONS
    )


def _has_large_replay_drift(delta: CaseDelta) -> bool:
    for numeric_delta in (delta.cost, delta.duration_seconds, delta.latency_seconds):
        if numeric_delta.percent is not None and abs(numeric_delta.percent) >= 20:
            return True
    return (
        delta.quality_score.absolute is not None
        and abs(delta.quality_score.absolute) >= REPLAY_DRIFT_QUALITY_THRESHOLD
    )


def _dedupe(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _to_plain_data(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {
            field_name: _to_plain_data(getattr(value, field_name))
            for field_name in value.__dataclass_fields__
        }
    if isinstance(value, dict):
        return {str(key): _to_plain_data(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_to_plain_data(item) for item in value]
    return value


def _track_replay_lab_event(
    event_name: AnalyticsEvent,
    *,
    case_count: int,
    candidate_count: int,
    has_evaluator: bool,
    source: str,
    completed: bool | None = None,
    failed_or_timed_out_lane_count: int | None = None,
) -> None:
    metadata: dict[str, Any] = {
        "case_count": case_count,
        "candidate_count": candidate_count,
        "has_evaluator": has_evaluator,
        "source": source if source in ANALYTICS_SOURCE_VALUES else "other",
    }
    if completed is not None:
        metadata["completed"] = completed
    if failed_or_timed_out_lane_count is not None:
        metadata["failed_or_timed_out_lane_count"] = failed_or_timed_out_lane_count
    track(event_name, metadata)


def _markdown_lane_row(lane_name: str, lane: LaneReport) -> str:
    metrics = lane.metrics
    return (
        "| "
        f"{lane_name} | "
        f"{lane.exec_id or 'n/a'} | "
        f"{lane.status} | "
        f"{_format_number(metrics.cost)} | "
        f"{_format_seconds(metrics.duration_seconds)} | "
        f"{_format_seconds(metrics.latency_seconds)} | "
        f"{_format_number(metrics.quality_score)} |"
    )


def _format_number(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3g}"


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}s"


def _format_delta(delta: NumericDelta) -> str:
    if delta.absolute is None:
        return "n/a"
    if delta.percent is None:
        return f"{delta.absolute:+.3g}"
    return f"{delta.absolute:+.3g} ({delta.percent:+.1f}%)"


def _format_bool(value: bool | None) -> str:
    if value is None:
        return "unknown"
    return "yes" if value else "no"


def _slugify(value: str) -> str:
    slug = "".join(char.lower() if char.isalnum() else "-" for char in value)
    return "-".join(part for part in slug.split("-") if part) or "replay-lab-report"
