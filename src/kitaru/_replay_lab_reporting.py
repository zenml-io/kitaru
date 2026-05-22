"""JSON, Markdown, and file rendering for Replay Lab reports."""

from __future__ import annotations

import json
from pathlib import Path

from kitaru._replay_lab_models import LaneReport, NumericDelta, ReplayLabReport


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
        lines.append(f"Replay trust: {case.replay_trust.label}")
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
