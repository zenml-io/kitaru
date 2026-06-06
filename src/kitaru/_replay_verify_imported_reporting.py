"""JSON, Markdown, and file rendering for imported Replay Verify reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kitaru._replay_verify_imported_models import (
    FIXTURE_HARNESS_EXECUTION_MODE,
    ImportedVerificationReport,
    execution_mode_detail,
)


def render_json_report(report: ImportedVerificationReport) -> str:
    """Render an imported verification report as stable, pretty JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def render_markdown_report(report: ImportedVerificationReport) -> str:
    """Render a product-shaped Markdown report for imported verification."""
    summary = report.summary
    rag = summary.get("rag_metadata_coverage", {})
    mode_detail = summary.get(
        "mode_detail",
        execution_mode_detail(report.execution_mode),
    )
    lines = [
        f"# Imported Replay Verify Report: {report.name}",
        "",
        f"Generated: {report.created_at}",
        f"Source system: {report.source_system}",
        f"Execution mode: `{report.execution_mode}`",
        f"Mode detail: {mode_detail}",
        "Recorded-response control: unavailable",
        "",
        "This report uses imported-input fresh execution. It does not convert "
        "foreign traces into deterministic Kitaru checkpoint replays.",
    ]
    if report.execution_mode == FIXTURE_HARNESS_EXECUTION_MODE:
        lines.extend(
            [
                "",
                "Fixture note: this report is harness evidence, not a real-agent "
                "candidate comparison.",
            ]
        )

    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Imported cases: {_count(summary, 'imported_count')}",
            f"- Eligible cases: {_count(summary, 'eligible_count')}",
            f"- Partial cases: {_count(summary, 'partial_count')}",
            f"- Ineligible cases: {_count(summary, 'ineligible_count')}",
            f"- Non-comparable cases: {_count(summary, 'non_comparable_count')}",
            f"- Unsafe cases: {_count(summary, 'unsafe_count')}",
            f"- Stopped cases: {_count(summary, 'stopped_count')}",
            f"- Candidate executions: {_count(summary, 'candidate_execution_count')}",
            "- Candidate executions for stopped cases: "
            f"{_count(summary, 'candidate_executions_for_stopped_cases')}",
            "- Unsafe live executions: "
            f"{_count(summary, 'unsafe_live_execution_count')}",
            "- Observed-vs-baseline mismatch cases: "
            f"{_count(summary, 'observed_vs_baseline_mismatch_count')}",
            "- Candidate-vs-baseline drift cases: "
            f"{_count(summary, 'candidate_vs_baseline_drift_count')}",
            "",
            "### RAG metadata coverage",
            "",
            f"- RAG metadata present: {_format_bool(bool(rag.get('available')))}",
            f"- RAG cases: {_count(rag, 'rag_case_count')}",
            f"- Complete RAG metadata: {_count(rag, 'complete_count')}",
            "- Missing or incomplete RAG metadata: "
            f"{_count(rag, 'missing_or_incomplete_count')}",
            "",
            "### Verdict counts",
            "",
        ]
    )
    verdict_counts = summary.get("verdict_counts", {})
    if isinstance(verdict_counts, dict) and verdict_counts:
        for verdict, count in sorted(verdict_counts.items()):
            lines.append(f"- `{verdict}`: {count}")
    else:
        lines.append("- n/a")

    lines.extend(
        [
            "",
            "## Stopped cases",
            "",
        ]
    )
    stopped_reasons = summary.get("stopped_case_reasons", {})
    if isinstance(stopped_reasons, dict) and stopped_reasons:
        for case_id, raw_reasons in sorted(stopped_reasons.items()):
            reasons = raw_reasons if isinstance(raw_reasons, list) else []
            reason_text = ", ".join(str(reason) for reason in reasons) or "unknown"
            lines.append(f"- `{case_id}`: {reason_text}")
    else:
        lines.append("No cases stopped before candidate execution.")

    lines.extend(
        [
            "",
            "## Case comparison",
            "",
            "| Case | Status | Verdict | Candidate executed? | "
            "Observed/base mismatches | Candidate/base drift | "
            "Unsafe live executions |",
            "|---|---|---|---|---:|---:|---:|",
        ]
    )
    for case_result in _case_results(summary):
        lines.append(
            "| "
            f"`{case_result.get('case_id', 'unknown')}` | "
            f"{case_result.get('status', 'unknown')} | "
            f"{case_result.get('verdict', 'unknown')} | "
            f"{_format_bool(bool(case_result.get('candidate_executed')))} | "
            f"{_mismatch_count(case_result.get('observed_vs_baseline'))} | "
            f"{_mismatch_count(case_result.get('candidate_vs_baseline'))} | "
            f"{case_result.get('unsafe_live_execution_count', 0)} |"
        )

    lines.append("")
    return "\n".join(lines)


def write_report_files(
    report: ImportedVerificationReport,
    report_dir: str | Path,
) -> dict[str, str]:
    """Write product-shaped JSON and Markdown report files."""
    output_dir = Path(report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "verification_report.json"
    markdown_path = output_dir / "verification_report.md"
    json_path.write_text(render_json_report(report), encoding="utf-8")
    markdown_path.write_text(render_markdown_report(report), encoding="utf-8")
    return {"json": str(json_path), "markdown": str(markdown_path)}


def _case_results(summary: dict[str, Any]) -> list[dict[str, Any]]:
    results = summary.get("case_results", [])
    if not isinstance(results, list):
        return []
    return [item for item in results if isinstance(item, dict)]


def _mismatch_count(value: Any) -> int:
    if not isinstance(value, list):
        return 0
    return sum(
        1 for item in value if isinstance(item, dict) and item.get("matches") is False
    )


def _count(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key, 0)
    return value if isinstance(value, int) else 0


def _format_bool(value: bool) -> str:
    return "yes" if value else "no"
