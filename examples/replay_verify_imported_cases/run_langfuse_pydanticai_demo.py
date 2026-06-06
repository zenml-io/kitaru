"""Run the imported-input Replay Verify support-copilot demo."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from examples.replay_verify_imported_cases.prompt_config import (
    BASELINE_CONFIG,
    CANDIDATE_CONFIG,
)
from examples.replay_verify_imported_cases.support_copilot_demo import (
    RUNNER_ENTRYPOINT,
    run_baseline_support_copilot_case,
    run_candidate_support_copilot_case,
)
from examples.replay_verify_imported_cases.tool_registry import SAFE_TOOL_NAMES
from kitaru._replay_verify_imported_models import (
    IMPORTED_INPUT_EXECUTION_MODE,
    to_plain_data,
)
from kitaru._replay_verify_imported_reporting import write_report_files
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerCallable,
    verify_imported_cases,
)
from kitaru._replay_verify_imported_sources.jsonl import (
    read_imported_cases_jsonl,
    validate_imported_cases_jsonl,
    write_imported_cases_jsonl,
)
from kitaru._replay_verify_imported_validation import (
    EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
)

DEMO_DIR = Path(__file__).resolve().parent
DEFAULT_CASE_FILE = DEMO_DIR / "fixtures" / "support_copilot_imported_cases.jsonl"
DEFAULT_REPORT_DIR = DEMO_DIR / "reports"


@dataclass(frozen=True)
class DemoRunResult:
    """Files and summary returned by one demo run."""

    report_dir: Path
    paths: dict[str, Path]
    summary: dict[str, Any]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=["jsonl"],
        default="jsonl",
        help="Imported-case source. The checked-in demo currently uses JSONL.",
    )
    parser.add_argument(
        "--case-file",
        type=Path,
        default=DEFAULT_CASE_FILE,
        help="Neutral imported-case JSONL file to validate and run.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=DEFAULT_REPORT_DIR,
        help="Directory for imported cases, fidelity report, and verification report.",
    )
    parser.add_argument(
        "--baseline",
        default="support-copilot-v1",
        help="Baseline support-copilot config id shown in runner metadata.",
    )
    parser.add_argument(
        "--candidate",
        default="support-copilot-v2",
        help="Candidate support-copilot config id shown in runner metadata.",
    )
    return parser.parse_args(argv)


def run_demo(
    *,
    source: str = "jsonl",
    case_file: Path = DEFAULT_CASE_FILE,
    report_dir: Path = DEFAULT_REPORT_DIR,
    baseline: str = "support-copilot-v1",
    candidate: str = "support-copilot-v2",
    baseline_runner: ImportedRunnerCallable = run_baseline_support_copilot_case,
    candidate_runner: ImportedRunnerCallable = run_candidate_support_copilot_case,
) -> DemoRunResult:
    """Run validation, imported-input execution, and report writing."""
    if source != "jsonl":
        msg = "This demo currently supports --source jsonl only."
        raise ValueError(msg)

    cases = read_imported_cases_jsonl(case_file)
    report_dir.mkdir(parents=True, exist_ok=True)
    imported_cases_path = report_dir / "imported_cases.jsonl"
    write_imported_cases_jsonl(cases, imported_cases_path)

    fidelity = validate_imported_cases_jsonl(
        imported_cases_path,
        expected_runner_entrypoint=RUNNER_ENTRYPOINT,
        expected_corpus_index_version=EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
        allowed_tool_names=SAFE_TOOL_NAMES,
    )
    fidelity_report_path = report_dir / "fidelity_report.json"
    fidelity_report_path.write_text(
        json.dumps(
            {
                "name": "Replay Verify imported-case fidelity report",
                "source": source,
                "cohort_kind": "curated_jsonl_fixture",
                "case_file": str(case_file),
                "summary": fidelity.summary,
                "cases": [to_plain_data(item) for item in fidelity.validations],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    baseline_config = {**BASELINE_CONFIG, "agent_id": baseline}
    candidate_config = {**CANDIDATE_CONFIG, "agent_id": candidate}
    verification_report = verify_imported_cases(
        fidelity.cases,
        baseline_runner=baseline_runner,
        candidate_runner=candidate_runner,
        baseline_config=baseline_config,
        candidate_config=candidate_config,
        report_name="Support Copilot imported-input demo",
        execution_mode=IMPORTED_INPUT_EXECUTION_MODE,
        expected_runner_entrypoint=RUNNER_ENTRYPOINT,
        expected_corpus_index_version=EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
        allowed_tool_names=SAFE_TOOL_NAMES,
    )
    verification_paths = write_report_files(verification_report, report_dir)
    paths = {
        "imported_cases": imported_cases_path,
        "fidelity_json": fidelity_report_path,
        "verification_json": Path(verification_paths["json"]),
        "verification_markdown": Path(verification_paths["markdown"]),
    }
    return DemoRunResult(
        report_dir=report_dir,
        paths=paths,
        summary=verification_report.summary,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the demo from the command line."""
    args = parse_args(argv)
    result = run_demo(
        source=args.source,
        case_file=args.case_file,
        report_dir=args.report_dir,
        baseline=args.baseline,
        candidate=args.candidate,
    )
    print("Imported-input Replay Verify demo complete.")
    for name, path in sorted(result.paths.items()):
        print(f"{name}: {path}")
    headline_summary = {
        "imported_count": result.summary["imported_count"],
        "eligible_count": result.summary["eligible_count"],
        "stopped_count": result.summary["stopped_count"],
        "candidate_execution_count": result.summary["candidate_execution_count"],
        "candidate_executions_for_stopped_cases": result.summary[
            "candidate_executions_for_stopped_cases"
        ],
        "unsafe_live_execution_count": result.summary["unsafe_live_execution_count"],
        "verdict_counts": result.summary["verdict_counts"],
    }
    print("summary:")
    print(json.dumps(headline_summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
