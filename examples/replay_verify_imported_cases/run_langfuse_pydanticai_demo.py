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

from examples.replay_verify_imported_cases.live_prompt_config import (
    BASELINE_LIVE_CONFIG,
    CANDIDATE_LIVE_CONFIG,
    LIVE_RUNNER_ENTRYPOINT,
)
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
DEFAULT_LIVE_CASE_FILE = DEMO_DIR / "fixtures" / "support_copilot_live_cases.jsonl"
DEFAULT_REPORT_DIR = DEMO_DIR / "reports"


@dataclass(frozen=True)
class DemoRunResult:
    """Files and summary returned by one demo run."""

    report_dir: Path
    paths: dict[str, Path]
    summary: dict[str, Any]


@dataclass(frozen=True)
class RunnerSelection:
    """Resolved runner mode: case file, entrypoint, callables, and configs."""

    case_file: Path
    expected_runner_entrypoint: str
    baseline_runner: ImportedRunnerCallable
    candidate_runner: ImportedRunnerCallable
    baseline_config: dict[str, Any]
    candidate_config: dict[str, Any]
    report_name: str


def select_runner(
    runner: str,
    *,
    baseline: str,
    candidate: str,
    baseline_model: str | None,
    candidate_model: str | None,
) -> RunnerSelection:
    if runner == "deterministic":
        if baseline_model or candidate_model:
            msg = "--baseline-model/--candidate-model require --runner live."
            raise ValueError(msg)
        return RunnerSelection(
            case_file=DEFAULT_CASE_FILE,
            expected_runner_entrypoint=RUNNER_ENTRYPOINT,
            baseline_runner=run_baseline_support_copilot_case,
            candidate_runner=run_candidate_support_copilot_case,
            baseline_config={**BASELINE_CONFIG, "agent_id": baseline},
            candidate_config={**CANDIDATE_CONFIG, "agent_id": candidate},
            report_name="Support Copilot imported-input demo",
        )
    if runner == "live":
        # Imported lazily so the deterministic path works without pydantic_ai.
        from examples.replay_verify_imported_cases.support_copilot_live import (
            run_baseline_support_copilot_case_live,
            run_candidate_support_copilot_case_live,
        )

        return RunnerSelection(
            case_file=DEFAULT_LIVE_CASE_FILE,
            expected_runner_entrypoint=LIVE_RUNNER_ENTRYPOINT,
            baseline_runner=run_baseline_support_copilot_case_live,
            candidate_runner=run_candidate_support_copilot_case_live,
            baseline_config={
                "agent_id": baseline,
                "prompt_version": BASELINE_LIVE_CONFIG.prompt_version,
                "prompt_hash": BASELINE_LIVE_CONFIG.prompt_hash,
                "model": baseline_model or BASELINE_LIVE_CONFIG.model,
            },
            candidate_config={
                "agent_id": candidate,
                "prompt_version": CANDIDATE_LIVE_CONFIG.prompt_version,
                "prompt_hash": CANDIDATE_LIVE_CONFIG.prompt_hash,
                "model": candidate_model or CANDIDATE_LIVE_CONFIG.model,
            },
            report_name="Support Copilot imported-input live demo",
        )
    msg = f"Unknown runner {runner!r}; use 'deterministic' or 'live'."
    raise ValueError(msg)


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
        default=None,
        help=(
            "Neutral imported-case JSONL file to validate and run. Defaults to "
            "the deterministic fixture, or the live fixture with --runner live."
        ),
    )
    parser.add_argument(
        "--runner",
        choices=["deterministic", "live"],
        default="deterministic",
        help=(
            "deterministic: credential-free local runner (default). "
            "live: real PydanticAI agent calls (requires OPENAI_API_KEY)."
        ),
    )
    parser.add_argument(
        "--baseline-model",
        default=None,
        help="Override the baseline model for --runner live.",
    )
    parser.add_argument(
        "--candidate-model",
        default=None,
        help="Override the candidate model for --runner live.",
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
    case_file: Path | None = None,
    report_dir: Path = DEFAULT_REPORT_DIR,
    runner: str = "deterministic",
    baseline: str = "support-copilot-v1",
    candidate: str = "support-copilot-v2",
    baseline_model: str | None = None,
    candidate_model: str | None = None,
    baseline_runner: ImportedRunnerCallable | None = None,
    candidate_runner: ImportedRunnerCallable | None = None,
) -> DemoRunResult:
    """Run validation, imported-input execution, and report writing."""
    if source != "jsonl":
        msg = "This demo currently supports --source jsonl only."
        raise ValueError(msg)
    selection = select_runner(
        runner,
        baseline=baseline,
        candidate=candidate,
        baseline_model=baseline_model,
        candidate_model=candidate_model,
    )
    resolved_case_file = case_file if case_file is not None else selection.case_file

    cases = read_imported_cases_jsonl(resolved_case_file)
    report_dir.mkdir(parents=True, exist_ok=True)
    imported_cases_path = report_dir / "imported_cases.jsonl"
    write_imported_cases_jsonl(cases, imported_cases_path)

    fidelity = validate_imported_cases_jsonl(
        imported_cases_path,
        expected_runner_entrypoint=selection.expected_runner_entrypoint,
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
                "case_file": str(resolved_case_file),
                "summary": fidelity.summary,
                "cases": [to_plain_data(item) for item in fidelity.validations],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    verification_report = verify_imported_cases(
        fidelity.cases,
        baseline_runner=baseline_runner or selection.baseline_runner,
        candidate_runner=candidate_runner or selection.candidate_runner,
        baseline_config=selection.baseline_config,
        candidate_config=selection.candidate_config,
        report_name=selection.report_name,
        execution_mode=IMPORTED_INPUT_EXECUTION_MODE,
        expected_runner_entrypoint=selection.expected_runner_entrypoint,
        expected_corpus_index_version=EXPECTED_CURRENT_CORPUS_INDEX_VERSION,
        allowed_tool_names=SAFE_TOOL_NAMES,
    )
    verification_paths = write_report_files(verification_report, report_dir)
    paths = {
        "imported_cases": imported_cases_path,
        "fidelity_json": fidelity_report_path,
        "verification_json": Path(verification_paths["json"]),
        "verification_markdown": Path(verification_paths["markdown"]),
        "verification_html": Path(verification_paths["html"]),
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
        runner=args.runner,
        baseline=args.baseline,
        candidate=args.candidate,
        baseline_model=args.baseline_model,
        candidate_model=args.candidate_model,
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
