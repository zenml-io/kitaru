"""JSON, Markdown, and file rendering for imported Replay Verify reports."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from kitaru._replay_verify_imported_html import render_html_report
from kitaru._replay_verify_imported_models import (
    FIXTURE_HARNESS_EXECUTION_MODE,
    FidelityReport,
    ImportedCaseValidation,
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
        f"Overall verdict: `{summary.get('overall_verdict', 'unknown')}`",
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


def render_fidelity_checklist(
    validations: Sequence[ImportedCaseValidation],
    *,
    summary: Mapping[str, Any] | None = None,
) -> str:
    """Render a Markdown fidelity checklist for scanned imported cases.

    Built for scan mode over arbitrary (often uninstrumented) traces: each
    case lists what was recovered, what is missing, and the reasons as a
    checklist of what to add before the case becomes verifiable. The
    ``retrieval`` row is suppressed for non-RAG cases so generic chat traces
    are not penalized for metadata they never needed.
    """
    verifiable = [item for item in validations if item.candidate_execution_allowed]
    missing_counts = Counter(
        field_name
        for item in validations
        for field_name in _checklist_missing_fields(item.fidelity)
    )
    lines = [
        "# Imported Trace Fidelity Checklist",
        "",
        f"- Cases scanned: {len(validations)}",
        f"- Verifiable today: {len(verifiable)}",
    ]
    if summary is not None:
        ignored = summary.get("ignored_observation_count")
        if isinstance(ignored, int):
            lines.append(f"- Observations ignored (no replayable call kind): {ignored}")
    lines.extend(["", "## Top missing fields", ""])
    if missing_counts:
        for rank, (field_name, count) in enumerate(
            missing_counts.most_common(), start=1
        ):
            lines.append(
                f"{rank}. `{field_name}` — missing in {count} of "
                f"{len(validations)} cases"
            )
    else:
        lines.append("All scanned cases recovered every checked field.")
    for item in validations:
        fidelity = item.fidelity
        rag_relevant = _rag_fields_relevant(fidelity)
        lines.extend(
            [
                "",
                f"## Case `{item.case_id}`",
                "",
                f"- Eligibility: `{fidelity.eligibility}` "
                f"(confidence: {fidelity.level})",
                f"- Verdict: `{fidelity.verdict}`",
                f"- Fidelity score: {fidelity.score}",
                f"- Verifiable today: {_format_bool(item.candidate_execution_allowed)}",
                "",
                "### Recovered fields",
                "",
            ]
        )
        for field_name, present in fidelity.recovered_fields.items():
            if field_name == "retrieval" and not rag_relevant:
                continue
            marker = "x" if present else " "
            lines.append(f"- [{marker}] {field_name}")
        lines.extend(["", "### To make this case verifiable, add", ""])
        if item.candidate_execution_allowed:
            lines.append("Nothing — this case is verifiable as imported.")
        else:
            lines.extend(f"- [ ] {reason}" for reason in fidelity.reasons)
    lines.append("")
    return "\n".join(lines)


def _rag_fields_relevant(fidelity: FidelityReport) -> bool:
    """Return whether the retrieval row carries signal for this case.

    For non-RAG cases the validator always reports ``retrieval`` as missing;
    showing that row in a checklist for a plain chat trace is noise. Retrieval
    is relevant only when it was recovered or when RAG-specific reasons exist.
    """
    if fidelity.recovered_fields.get("retrieval", False):
        return True
    return any(
        reason.startswith("missing_rag_metadata:")
        or reason == "stale_corpus_index_version"
        for reason in fidelity.reasons
    )


def _checklist_missing_fields(fidelity: FidelityReport) -> list[str]:
    if _rag_fields_relevant(fidelity):
        return list(fidelity.missing_fields)
    return [
        field_name
        for field_name in fidelity.missing_fields
        if field_name != "retrieval"
    ]


def write_report_files(
    report: ImportedVerificationReport,
    report_dir: str | Path,
) -> dict[str, str]:
    """Write product-shaped JSON, Markdown, and HTML report files."""
    output_dir = Path(report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "verification_report.json"
    markdown_path = output_dir / "verification_report.md"
    html_path = output_dir / "verification_report.html"
    report_paths = {
        "json": str(json_path),
        "markdown": str(markdown_path),
        "html": str(html_path),
    }
    # Assign paths before rendering so the serialized JSON itself records
    # where the report files live (needed by downstream HTML rendering).
    report_with_paths = replace(report, report_paths=report_paths)
    json_path.write_text(render_json_report(report_with_paths), encoding="utf-8")
    markdown_path.write_text(
        render_markdown_report(report_with_paths), encoding="utf-8"
    )
    html_path.write_text(render_html_report(report_with_paths), encoding="utf-8")
    return report_paths


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
