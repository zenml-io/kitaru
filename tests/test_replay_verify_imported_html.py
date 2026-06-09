"""Tests for the imported Replay Verify HTML verdict report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from examples.replay_verify_imported_cases.run_langfuse_pydanticai_demo import (
    DEFAULT_CASE_FILE,
    run_demo,
)

from kitaru._replay_verify_imported_html import render_html_report
from kitaru._replay_verify_imported_models import (
    ImportedCaseSourceRef,
    ImportedReplayCase,
    ReplayTraceContract,
    RunnerContract,
)
from kitaru._replay_verify_imported_reporting import write_report_files
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerInvocation,
    verify_imported_cases,
)

RUNNER_ENTRYPOINT = "run_support_copilot_case"


def _output(policy_label: str) -> dict[str, Any]:
    return {
        "policy_label": policy_label,
        "risk_status": "safe",
        "tool_names": [],
        "retrieval_document_ids": [],
    }


def _case(case_id: str, *, observed_output: Any) -> ImportedReplayCase:
    return ImportedReplayCase(
        case_id=case_id,
        source_ref=ImportedCaseSourceRef(
            source_system="langfuse",
            source_id=f"trace-{case_id}",
        ),
        root_input={"user_message": "What is the policy?"},
        observed_output=observed_output,
        recorded_calls=[],
        trace_contract=ReplayTraceContract(
            trace_contract_version="trace-contract-v1",
            app_name="support-copilot",
            app_version="2026-06-07",
            model="openai-chat:gpt-5",
            prompt_version="support-copilot-v1",
            prompt_hash="abc123",
            available_tools=[],
            side_effect_policy="safe",
        ),
        runner_contract=RunnerContract(entrypoint=RUNNER_ENTRYPOINT),
    )


def _runner_returning(
    policy_label: str,
) -> Any:
    def _runner(
        _case: ImportedReplayCase,
        _invocation: ImportedRunnerInvocation,
    ) -> dict[str, Any]:
        return _output(policy_label)

    return _runner


def test_demo_html_report_shows_verdict_counts_and_stopped_cases(
    tmp_path: Path,
) -> None:
    """The fixture demo produces an HTML page with verdict, counts, and holds."""
    run_demo(source="jsonl", case_file=DEFAULT_CASE_FILE, report_dir=tmp_path)

    html_path = tmp_path / "verification_report.html"
    assert html_path.exists()
    page = html_path.read_text(encoding="utf-8")
    summary = json.loads(
        (tmp_path / "verification_report.json").read_text(encoding="utf-8")
    )["summary"]

    assert summary["overall_verdict"] == "hold"
    assert ">HOLD</p>" in page
    # The fixture mode lines must appear under the verdict banner.
    assert summary["execution_mode"] in page
    assert "recorded-response control:</strong> unavailable" in page
    assert "source system:</strong> fixture-jsonl" in page

    expected_counts = {
        "imported_count": 8,
        "eligible_count": 4,
        "stopped_count": 4,
        "candidate_vs_baseline_drift_count": 0,
        "candidate_executions_for_stopped_cases": 0,
        "unsafe_live_execution_count": 0,
    }
    for metric, value in expected_counts.items():
        assert summary[metric] == value
        assert (
            f'data-metric="{metric}"><span class="count-value">{value}</span>' in page
        )

    # A held case stays visible with its stop reason, no expansion required.
    assert "rv-missing-output-stopped" in page
    assert "missing_observed_output_or_evaluator_signal" in page
    assert ">HELD</span>" in page

    # Footer provenance plus the honesty line about what this run is not.
    assert "cohorts: next-week-demo" in page
    assert "trace contract versions: trace-contract-v1" in page
    assert "comparison fields: " in page
    assert "imported-input fresh execution, not deterministic checkpoint replay" in page


def test_field_table_relabels_observed_baseline_and_candidate_columns() -> None:
    """observed_vs_baseline's 'baseline_value' is the observed production value.

    The HTML field table must place it in the observed column, the
    'comparison_value' from the same list in the baseline column, and the
    candidate_vs_baseline 'comparison_value' in the candidate column.
    """
    report = verify_imported_cases(
        [_case("relabel", observed_output=_output("from-production"))],
        baseline_runner=_runner_returning("from-baseline"),
        candidate_runner=_runner_returning("from-candidate"),
        created_at="2026-06-09T12:00:00+00:00",
    )
    case_result = report.summary["case_results"][0]
    observed_entry = next(
        item
        for item in case_result["observed_vs_baseline"]
        if item["field"] == "policy_label"
    )
    candidate_entry = next(
        item
        for item in case_result["candidate_vs_baseline"]
        if item["field"] == "policy_label"
    )
    # Pin the runner's key meaning so a renamed field upstream fails loudly.
    assert observed_entry["baseline_value"] == "from-production"
    assert observed_entry["comparison_value"] == "from-baseline"
    assert candidate_entry["baseline_value"] == "from-baseline"
    assert candidate_entry["comparison_value"] == "from-candidate"

    page = render_html_report(report)

    # Both comparisons mismatched on policy_label: the observed cell is tinted
    # orange (production differs) and the candidate cell red (real divergence).
    assert (
        '<td class="cell-field">policy_label</td>'
        '<td class="cell-observed observed-drift">from-production</td>'
        '<td class="cell-baseline">from-baseline</td>'
        '<td class="cell-candidate candidate-drift">from-candidate</td>'
        '<td class="cell-result drift">drift</td>' in page
    )
    # Candidate drifted from the fresh baseline, so the case verdict is hold
    # and the badge reads DRIFT.
    assert ">DRIFT</span>" in page
    # Matching fields keep the green match result cell.
    assert (
        '<td class="cell-field">risk_status</td>'
        '<td class="cell-observed">safe</td>'
        '<td class="cell-baseline">safe</td>'
        '<td class="cell-candidate">safe</td>'
        '<td class="cell-result match">match</td>' in page
    )


def test_caution_case_renders_caution_badge_not_drift() -> None:
    """candidate==baseline!=observed is verdict 'caution', never a DRIFT badge.

    The candidate reproduced the fresh baseline exactly; only the recorded
    production output differs. That is an environment/source discrepancy, so
    the badge must be the orange CAUTION, with the observed cell tinted orange
    and the candidate cell left untinted.
    """
    report = verify_imported_cases(
        [_case("caution-case", observed_output=_output("from-production"))],
        baseline_runner=_runner_returning("same-fresh-answer"),
        candidate_runner=_runner_returning("same-fresh-answer"),
        created_at="2026-06-09T12:00:00+00:00",
    )
    case_result = report.summary["case_results"][0]
    assert case_result["verdict"] == "caution"

    page = render_html_report(report)

    assert 'style="background: #b35309;">CAUTION</span>' in page
    assert ">DRIFT</span>" not in page
    # Only the observed-vs-baseline comparison mismatched.
    assert '<td class="cell-observed observed-drift">from-production</td>' in page
    assert '<td class="cell-candidate">same-fresh-answer</td>' in page
    assert 'class="cell-candidate candidate-drift"' not in page


def test_html_escapes_case_ids_and_reasons() -> None:
    """User-ish strings must be escaped, never emitted as raw markup."""
    report = verify_imported_cases(
        [_case("<script>alert(1)</script>", observed_output=None)],
        baseline_runner=_runner_returning("from-baseline"),
        candidate_runner=_runner_returning("from-candidate"),
        created_at="2026-06-09T12:00:00+00:00",
    )

    page = render_html_report(report)

    assert "<script>" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page


def test_write_report_files_includes_html_path(tmp_path: Path) -> None:
    """write_report_files writes the HTML page and reports its path."""
    report = verify_imported_cases(
        [_case("eligible", observed_output=_output("support_policy"))],
        baseline_runner=_runner_returning("support_policy"),
        candidate_runner=_runner_returning("support_policy"),
        created_at="2026-06-09T12:00:00+00:00",
    )

    paths = write_report_files(report, tmp_path)

    html_path = Path(paths["html"])
    assert html_path == tmp_path / "verification_report.html"
    assert html_path.exists()
    written = json.loads(
        (tmp_path / "verification_report.json").read_text(encoding="utf-8")
    )
    assert written["report_paths"]["html"] == str(html_path)
    page = html_path.read_text(encoding="utf-8")
    assert ">SHIP</p>" in page
    assert ">MATCH</span>" in page
