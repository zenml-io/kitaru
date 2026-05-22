"""Internal Replay Lab comparison helpers.

Replay Lab is intentionally internal for now. It gives MCP tools and examples a
single place to parse a cohort manifest, replay the same cases through baseline
and candidate lanes, and render an honest comparison report.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import kitaru.client as client_api
from kitaru._replay_lab_evaluators import (
    apply_evaluator_to_case,
    load_evaluator,
    resolve_evaluator_config,
)
from kitaru._replay_lab_models import (
    ANALYTICS_SOURCE_VALUES,
    DEFAULT_EXPECTED_ARTIFACTS,
    EFFICIENCY_WIN_THRESHOLD,
    EVALUATOR_ERROR_POLICIES,
    EVALUATOR_PRECEDENCE_POLICIES,
    REPLAY_DRIFT_QUALITY_THRESHOLD,
    RUNTIME_METRIC_KEYS,
    TERMINAL_STATUS_VALUES,
    CandidateDescriptor,
    CandidateResult,
    CaseDelta,
    EvaluatorConfig,
    EvaluatorDescriptor,
    EvaluatorInput,
    LaneReport,
    MetricSnapshot,
    NumericDelta,
    ReplayLabCase,
    ReplayLabCaseReport,
    ReplayLabEvaluator,
    ReplayLabManifest,
    ReplayLabReport,
    ReplayTrustStatus,
)
from kitaru._replay_lab_policy import (
    build_case_delta,
    build_case_replay_trust,
    candidate_result_from_lane,
    case_has_replay_drift_warning,
    dedupe,
    summarize_cases,
)
from kitaru._replay_lab_reporting import (
    render_json_report,
    render_markdown_report,
    write_report_files,
)
from kitaru._replay_lab_utils import number_or_none
from kitaru._replay_lab_validation import (
    load_manifest,
    validate_candidate_descriptor,
    validate_candidate_descriptors,
    validate_evaluator_descriptor,
    validate_manifest,
)
from kitaru.analytics import AnalyticsEvent, track
from kitaru.client import Execution, ExecutionStatus

__all__ = [
    "ANALYTICS_SOURCE_VALUES",
    "DEFAULT_EXPECTED_ARTIFACTS",
    "EFFICIENCY_WIN_THRESHOLD",
    "EVALUATOR_ERROR_POLICIES",
    "EVALUATOR_PRECEDENCE_POLICIES",
    "REPLAY_DRIFT_QUALITY_THRESHOLD",
    "RUNTIME_METRIC_KEYS",
    "TERMINAL_STATUS_VALUES",
    "CandidateDescriptor",
    "CandidateResult",
    "CaseDelta",
    "EvaluatorConfig",
    "EvaluatorDescriptor",
    "EvaluatorInput",
    "LaneReport",
    "MetricSnapshot",
    "NumericDelta",
    "ReplayLabCase",
    "ReplayLabCaseReport",
    "ReplayLabEvaluator",
    "ReplayLabManifest",
    "ReplayLabReport",
    "build_case_delta",
    "compare_replay_lab",
    "extract_metrics",
    "load_evaluator",
    "load_manifest",
    "render_json_report",
    "render_markdown_report",
    "validate_candidate_descriptor",
    "validate_candidate_descriptors",
    "validate_evaluator_descriptor",
    "validate_manifest",
    "wait_for_terminal_lane",
    "write_report_files",
]


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

        evaluator_config = resolve_evaluator_config(
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
        summary = summarize_cases(case_reports, candidates)
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
    replay_drift_warning = case_has_replay_drift_warning(lanes, replay_drift)
    replay_trust = build_case_replay_trust(
        case_id=case.case_id,
        lanes=lanes,
        replay_drift_warning=replay_drift_warning,
    )
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
                replay_trust=replay_trust,
            )
        )

    if evaluator_config is not None:
        lanes, candidate_results = apply_evaluator_to_case(
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
        replay_drift_warning = case_has_replay_drift_warning(lanes, replay_drift)
        replay_trust = build_case_replay_trust(
            case_id=case.case_id,
            lanes=lanes,
            replay_drift_warning=replay_drift_warning,
        )
        candidate_results = [
            _refresh_candidate_result(
                result,
                baseline_lane=lanes["baseline_replay"],
                replay_trust=replay_trust,
            )
            for result in candidate_results
        ]

    limitations = _case_limitations(
        lanes,
        replay_trust=replay_trust,
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
        replay_trust=replay_trust,
        candidate_results=candidate_results,
        limitations=limitations,
    )


def _build_candidate_result(
    *,
    candidate: CandidateDescriptor,
    lane: LaneReport,
    baseline_lane: LaneReport,
    replay_trust: ReplayTrustStatus,
) -> CandidateResult:
    return candidate_result_from_lane(
        candidate_id=candidate.id,
        candidate_label=candidate.label,
        lane=lane,
        baseline_lane=baseline_lane,
        replay_trust=replay_trust,
    )


def _refresh_candidate_result(
    result: CandidateResult,
    *,
    baseline_lane: LaneReport,
    replay_trust: ReplayTrustStatus,
) -> CandidateResult:
    return candidate_result_from_lane(
        candidate_id=result.candidate_id,
        candidate_label=result.candidate_label,
        lane=result.lane,
        baseline_lane=baseline_lane,
        replay_trust=replay_trust,
        limitations=result.lane.limitations,
    )


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


def _case_limitations(
    lanes: Mapping[str, LaneReport],
    *,
    replay_trust: ReplayTrustStatus,
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
    if replay_trust.status == "inspect":
        limitations.append(
            "Observed-to-baseline replay drift is large; candidate effect has "
            "lower confidence."
        )
    return dedupe(limitations)


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
            number = number_or_none(source.get(key))
            if number is not None:
                return number
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
