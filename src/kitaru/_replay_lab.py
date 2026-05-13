"""Internal Replay Lab comparison helpers.

Replay Lab is intentionally internal for now.  It gives MCP tools and examples a
single place to parse a cohort manifest, replay the same cases through baseline
and candidate lanes, and render an honest comparison report.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import kitaru.client as client_api
from kitaru.client import Execution, ExecutionStatus

DEFAULT_EXPECTED_ARTIFACTS = ("scorecard", "final_response")
TERMINAL_STATUS_VALUES = {"completed", "failed", "cancelled"}


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

    label: str
    flow_inputs: dict[str, Any] = field(default_factory=dict)
    checkpoint_overrides: dict[str, Any] = field(default_factory=dict)
    notes: str | None = None


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
class ReplayLabCaseReport:
    """Complete report data for one selected case."""

    case_id: str
    source_exec_id: str
    from_checkpoint: str
    reason: str
    labels: dict[str, str]
    lanes: dict[str, LaneReport]
    replay_drift: CaseDelta
    candidate_effect: CaseDelta
    output_changed_vs_baseline: bool | None
    limitations: list[str]


@dataclass(frozen=True)
class ReplayLabReport:
    """Structured Replay Lab report."""

    name: str
    description: str
    candidate: CandidateDescriptor
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
    label = _required_str(raw, "label")
    flow_inputs = _optional_mapping(raw.get("flow_inputs"), field_name="flow_inputs")
    checkpoint_overrides = _optional_mapping(
        raw.get("checkpoint_overrides"), field_name="checkpoint_overrides"
    )
    notes = raw.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise ValueError("Candidate `notes` must be a string when provided.")

    unsupported = set(raw) - {
        "label",
        "flow_inputs",
        "checkpoint_overrides",
        "notes",
    }
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported candidate descriptor field(s): {names}.")

    return CandidateDescriptor(
        label=label,
        flow_inputs=flow_inputs,
        checkpoint_overrides=checkpoint_overrides,
        notes=notes,
    )


def compare_replay_lab(
    *,
    manifest: Mapping[str, Any] | None = None,
    manifest_path: str | Path | None = None,
    candidate_descriptor: Mapping[str, Any] | None = None,
    client: Any | None = None,
    timeout_seconds: float = 300,
    poll_interval_seconds: float = 5,
    report_dir: str | Path | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayLabReport:
    """Run a three-lane Replay Lab comparison and return a report."""
    if timeout_seconds < 0:
        raise ValueError("`timeout_seconds` must be >= 0.")
    if poll_interval_seconds <= 0:
        raise ValueError("`poll_interval_seconds` must be > 0.")
    if candidate_descriptor is None:
        raise ValueError("Provide `candidate_descriptor`.")

    parsed_manifest = load_manifest(manifest, manifest_path=manifest_path)
    candidate = validate_candidate_descriptor(candidate_descriptor)
    kitaru_client = client if client is not None else client_api.KitaruClient()

    case_reports = [
        _run_case_compare(
            case=case,
            manifest=parsed_manifest,
            candidate=candidate,
            client=kitaru_client,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            clock=clock,
            sleep=sleep,
        )
        for case in parsed_manifest.cases
    ]

    report_paths: dict[str, str] = {}
    report = ReplayLabReport(
        name=parsed_manifest.name,
        description=parsed_manifest.description,
        candidate=candidate,
        expected_artifacts=parsed_manifest.expected_artifacts,
        cases=case_reports,
        created_at=datetime.now(UTC).isoformat(),
        summary=_summarize_cases(case_reports),
        report_paths=report_paths,
    )
    if report_dir is not None:
        report_paths.update(write_report_files(report, report_dir))
    return report


def render_json_report(report: ReplayLabReport) -> str:
    """Render a report as stable, pretty JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def render_markdown_report(report: ReplayLabReport) -> str:
    """Render a human-readable Markdown report."""
    lines = [
        f"# Replay Lab Report: {report.name}",
        "",
        f"Generated: {report.created_at}",
        f"Candidate: {report.candidate.label}",
    ]
    if report.description:
        lines.extend(["", report.description])
    if report.candidate.notes:
        lines.extend(["", f"Candidate notes: {report.candidate.notes}"])

    summary = report.summary
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Cases: {summary['case_count']}",
            f"- Completed candidate lanes: {summary['candidate_completed_count']}",
            f"- Failed or timed-out lanes: {summary['failed_or_timed_out_lane_count']}",
            f"- Cases with changed candidate output: {summary['changed_output_count']}",
            "- Cases with replay drift warning: "
            f"{summary['replay_drift_warning_count']}",
            "",
            "## Case comparison",
            "",
            "| Case | Observed | Baseline | Candidate | Candidate cost Δ | "
            "Candidate quality Δ | Output changed? |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )

    for case in report.cases:
        observed = case.lanes["observed"].metrics
        baseline = case.lanes["baseline_replay"].metrics
        candidate = case.lanes["candidate_replay"].metrics
        lines.append(
            "| "
            f"{case.case_id} | "
            f"{_format_number(observed.cost)} | "
            f"{_format_number(baseline.cost)} | "
            f"{_format_number(candidate.cost)} | "
            f"{_format_delta(case.candidate_effect.cost)} | "
            f"{_format_delta(case.candidate_effect.quality_score)} | "
            f"{_format_bool(case.output_changed_vs_baseline)} |"
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
        for lane_name in ("observed", "baseline_replay", "candidate_replay"):
            lane = case.lanes[lane_name]
            metrics = lane.metrics
            lines.append(
                "| "
                f"{lane_name} | "
                f"{lane.exec_id or 'n/a'} | "
                f"{lane.status} | "
                f"{_format_number(metrics.cost)} | "
                f"{_format_seconds(metrics.duration_seconds)} | "
                f"{_format_seconds(metrics.latency_seconds)} | "
                f"{_format_number(metrics.quality_score)} |"
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
    candidate: CandidateDescriptor,
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

    lanes = {
        "observed": observed_lane,
        "baseline_replay": baseline_lane,
        "candidate_replay": candidate_lane,
    }
    replay_drift = build_case_delta(observed_lane.metrics, baseline_lane.metrics)
    candidate_effect = build_case_delta(baseline_lane.metrics, candidate_lane.metrics)
    output_changed = _changed_output(
        baseline_lane.metrics.output_text,
        candidate_lane.metrics.output_text,
    )
    limitations = _case_limitations(lanes, replay_drift)

    return ReplayLabCaseReport(
        case_id=case.case_id,
        source_exec_id=case.exec_id,
        from_checkpoint=from_checkpoint,
        reason=case.reason,
        labels=case.labels,
        lanes=lanes,
        replay_drift=replay_drift,
        candidate_effect=candidate_effect,
        output_changed_vs_baseline=output_changed,
        limitations=limitations,
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
    return LaneReport(
        lane=lane.lane,
        exec_id=lane.exec_id,
        status=status,
        metrics=lane.metrics,
        timed_out=timed_out,
        error_message=error_message,
        limitations=[*lane.limitations, *extra_limitations],
    )


def _case_limitations(
    lanes: Mapping[str, LaneReport], replay_drift: CaseDelta
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
    if _has_large_replay_drift(replay_drift):
        limitations.append(
            "Observed-to-baseline replay drift is large; candidate effect has "
            "lower confidence."
        )
    return _dedupe(limitations)


def _summarize_cases(cases: Sequence[ReplayLabCaseReport]) -> dict[str, Any]:
    failed_or_timed_out = 0
    candidate_completed = 0
    changed_outputs = 0
    drift_warnings = 0
    for case in cases:
        failed_or_timed_out += sum(
            1
            for lane in case.lanes.values()
            if lane.timed_out
            or lane.status not in TERMINAL_STATUS_VALUES
            or lane.status != "completed"
        )
        candidate_lane = case.lanes["candidate_replay"]
        if candidate_lane.status == "completed" and not candidate_lane.timed_out:
            candidate_completed += 1
        if case.output_changed_vs_baseline is True:
            changed_outputs += 1
        if _has_large_replay_drift(case.replay_drift):
            drift_warnings += 1
    return {
        "case_count": len(cases),
        "candidate_completed_count": candidate_completed,
        "failed_or_timed_out_lane_count": failed_or_timed_out,
        "changed_output_count": changed_outputs,
        "replay_drift_warning_count": drift_warnings,
    }


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


def _has_large_replay_drift(delta: CaseDelta) -> bool:
    for numeric_delta in (delta.cost, delta.duration_seconds, delta.latency_seconds):
        if numeric_delta.percent is not None and abs(numeric_delta.percent) >= 20:
            return True
    return (
        delta.quality_score.absolute is not None
        and abs(delta.quality_score.absolute) >= 0.1
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
