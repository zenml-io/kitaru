"""Tests for imported Replay Verify report rendering."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kitaru._replay_verify_imported_models import (
    FIXTURE_HARNESS_EXECUTION_MODE,
    IMPORTED_INPUT_EXECUTION_MODE,
    ImportedCaseSourceRef,
    ImportedReplayCase,
    ReplayTraceContract,
    RunnerContract,
)
from kitaru._replay_verify_imported_reporting import (
    render_json_report,
    render_markdown_report,
    write_report_files,
)
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerInvocation,
    verify_imported_cases,
)

RUNNER_ENTRYPOINT = "run_support_copilot_case"


def _observed_output(risk_status: str = "safe") -> dict[str, Any]:
    return {
        "policy_label": "support_policy",
        "risk_status": risk_status,
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


def _runner(
    _case: ImportedReplayCase,
    _invocation: ImportedRunnerInvocation,
) -> dict[str, Any]:
    return _observed_output()


def test_rendered_reports_show_product_counts_and_limitations() -> None:
    report = verify_imported_cases(
        [
            _case("eligible", observed_output=_observed_output()),
            _case("stopped", observed_output=None),
        ],
        baseline_runner=_runner,
        candidate_runner=_runner,
        created_at="2026-06-07T12:00:00+00:00",
    )

    payload = json.loads(render_json_report(report))
    markdown = render_markdown_report(report)

    assert payload["execution_mode"] == IMPORTED_INPUT_EXECUTION_MODE
    assert payload["summary"]["recorded_response_control_status"] == "unavailable"
    assert payload["summary"]["imported_count"] == 2
    assert payload["summary"]["candidate_execution_count"] == 1
    assert payload["summary"]["candidate_executions_for_stopped_cases"] == 0
    assert payload["summary"]["stopped_case_reasons"] == {
        "stopped": ["missing_observed_output_or_evaluator_signal"]
    }
    assert payload["summary"]["failed_case_reasons"] == {}
    assert payload["summary"]["overall_verdict"] == "hold"
    assert payload["summary"]["cohorts"] == []
    assert payload["summary"]["trace_contract_versions"] == ["trace-contract-v1"]
    assert "Overall verdict: `hold`" in markdown
    assert "Source system: langfuse" in markdown
    assert f"Execution mode: `{IMPORTED_INPUT_EXECUTION_MODE}`" in markdown
    assert "Recorded-response control: unavailable" in markdown
    assert "Imported cases: 2" in markdown
    assert "Candidate executions for stopped cases: 0" in markdown
    assert "Unsafe live executions: 0" in markdown
    assert "`stopped`: missing_observed_output_or_evaluator_signal" in markdown


def test_fixture_mode_markdown_names_harness_evidence() -> None:
    report = verify_imported_cases(
        [_case("fixture", observed_output=_observed_output())],
        baseline_runner=_runner,
        candidate_runner=_runner,
        execution_mode=FIXTURE_HARNESS_EXECUTION_MODE,
        created_at="2026-06-07T12:00:00+00:00",
    )

    markdown = render_markdown_report(report)

    assert f"Execution mode: `{FIXTURE_HARNESS_EXECUTION_MODE}`" in markdown
    assert "Fixture note:" in markdown
    assert "not a real-agent candidate comparison" in markdown


def test_write_report_files_uses_product_filenames(tmp_path: Path) -> None:
    report = verify_imported_cases(
        [_case("eligible", observed_output=_observed_output())],
        baseline_runner=_runner,
        candidate_runner=_runner,
        created_at="2026-06-07T12:00:00+00:00",
    )

    paths = write_report_files(report, tmp_path)

    assert paths == {
        "json": str(tmp_path / "verification_report.json"),
        "markdown": str(tmp_path / "verification_report.md"),
        "html": str(tmp_path / "verification_report.html"),
    }
    assert (tmp_path / "verification_report.html").exists()
    written = json.loads((tmp_path / "verification_report.json").read_text())
    assert written["summary"]["eligible_count"] == 1
    # The serialized JSON must self-describe its own file locations so an
    # HTML verdict page can be rendered from the JSON alone.
    assert written["report_paths"] == paths
    markdown = (tmp_path / "verification_report.md").read_text()
    assert "# Imported Replay Verify Report" in markdown
    assert "Overall verdict: `ship`" in markdown
