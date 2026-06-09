"""Focused tests for the imported-input Replay Verify demo."""

from __future__ import annotations

import json
from pathlib import Path

from examples.replay_verify_imported_cases.run_langfuse_pydanticai_demo import (
    DEFAULT_CASE_FILE,
    run_demo,
)
from examples.replay_verify_imported_cases.support_copilot_demo import (
    run_candidate_support_copilot_case,
)

from kitaru._replay_verify_imported_models import ImportedReplayCase
from kitaru._replay_verify_imported_runner import ImportedRunnerInvocation


def test_demo_writes_reports_and_stopped_cases_do_not_execute_candidate(
    tmp_path: Path,
) -> None:
    """The demo writes product files and never calls candidate for stopped cases."""
    candidate_calls: list[str] = []

    def candidate_spy(
        case: ImportedReplayCase,
        invocation: ImportedRunnerInvocation,
    ):
        candidate_calls.append(case.case_id)
        return run_candidate_support_copilot_case(case, invocation)

    result = run_demo(
        source="jsonl",
        case_file=DEFAULT_CASE_FILE,
        report_dir=tmp_path,
        candidate_runner=candidate_spy,
    )

    expected_files = {
        "imported_cases.jsonl",
        "fidelity_report.json",
        "verification_report.json",
        "verification_report.md",
        "verification_report.html",
    }
    assert {path.name for path in result.paths.values()} == expected_files
    for path in result.paths.values():
        assert path.exists()

    verification = json.loads((tmp_path / "verification_report.json").read_text())
    fidelity = json.loads((tmp_path / "fidelity_report.json").read_text())
    stopped_case_ids = set(verification["summary"]["stopped_case_reasons"])
    executed_case_ids = {
        item["case_id"]
        for item in verification["summary"]["case_results"]
        if item["candidate_executed"]
    }

    assert verification["summary"]["imported_count"] == 8
    assert verification["summary"]["eligible_count"] == 4
    assert verification["summary"]["stopped_count"] == 4
    assert verification["summary"]["candidate_execution_count"] == 4
    assert verification["summary"]["candidate_executions_for_stopped_cases"] == 0
    assert verification["summary"]["unsafe_live_execution_count"] == 0
    assert stopped_case_ids == {
        "rv-missing-output-stopped",
        "rv-missing-tools-stopped",
        "rv-unsafe-live-write-stopped",
        "rv-incomplete-rag-stopped",
    }
    assert set(candidate_calls) == executed_case_ids
    assert stopped_case_ids.isdisjoint(candidate_calls)
    assert (
        "missing_observed_output_or_evaluator_signal"
        in verification["summary"]["stopped_case_reasons"]["rv-missing-output-stopped"]
    )
    assert (
        "missing_available_tools"
        in verification["summary"]["stopped_case_reasons"]["rv-missing-tools-stopped"]
    )
    assert (
        "unsafe_or_unknown_write_like_tool_blocked"
        in verification["summary"]["stopped_case_reasons"][
            "rv-unsafe-live-write-stopped"
        ]
    )
    assert (
        "stale_corpus_index_version"
        in verification["summary"]["stopped_case_reasons"]["rv-incomplete-rag-stopped"]
    )
    assert fidelity["cohort_kind"] == "curated_jsonl_fixture"


def test_demo_fixture_does_not_use_manifest_expected_output() -> None:
    """The checked-in fixture contains observed output, not expected output."""
    rows = [
        json.loads(line)
        for line in DEFAULT_CASE_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert rows
    assert all("expected_output" not in row for row in rows)
    assert all("manifest_case" not in row for row in rows)
