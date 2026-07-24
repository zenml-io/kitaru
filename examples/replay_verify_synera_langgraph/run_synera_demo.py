"""Run the Synera LangGraph imported-input Replay Verify demo.

Same engine as ``run_langfuse_pydanticai_demo.py``, but the agent under test is a
real LangGraph mechanical-engineering assistant (Synera's stack) and the cohort
is fabricated mechanical-engineering traces (no customer data).

Deterministic and credential-free: no OPENAI_API_KEY, no Langfuse credentials,
no Kitaru server, no login. It prints a plain-English Match / Drift / Skipped
summary with a Ship / Ship with caution / Don't ship recommendation, and writes
the same JSON / Markdown / self-contained HTML reports the engine produces.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from examples.replay_verify_synera_langgraph.generate_synera_cohort import (
    BASELINE_ID,
    CANDIDATE_ID,
    SYNERA_CORPUS_INDEX_VERSION,
    write_cohort,
)
from examples.replay_verify_synera_langgraph.synera_agent import SAFE_TOOL_NAMES
from examples.replay_verify_synera_langgraph.synera_runner import (
    RUNNER_ENTRYPOINT,
    run_baseline_synera_case,
    run_candidate_synera_case,
)
from kitaru._replay_verify_imported_models import (
    IMPORTED_INPUT_EXECUTION_MODE,
    to_plain_data,
)
from kitaru._replay_verify_imported_reporting import write_report_files
from kitaru._replay_verify_imported_runner import verify_imported_cases
from kitaru._replay_verify_imported_sources.jsonl import (
    read_imported_cases_jsonl,
    validate_imported_cases_jsonl,
    write_imported_cases_jsonl,
)

DEMO_DIR = Path(__file__).resolve().parent
DEFAULT_CASE_FILE = DEMO_DIR / "fixtures" / "synera_imported_cases.jsonl"
DEFAULT_REPORT_DIR = DEMO_DIR / "reports"

# The candidate runs the same agent with a "cheaper / faster" config: it skips
# the FEA validation step on simulation requests. That is the planted regression.
BASELINE_CONFIG: dict[str, Any] = {"agent_id": BASELINE_ID, "skip_fea_validation": False}
CANDIDATE_CONFIG: dict[str, Any] = {"agent_id": CANDIDATE_ID, "skip_fea_validation": True}

# --- Human reframe vocabulary (Match / Drift / Skipped, Ship / caution / hold) ---
# Mirrors docs/superpowers/plans/2026-06-15-replay-verify-reframe.md. Derived
# inline here because the shared label helper has not landed on this branch yet.
MATCH = "Match"
DRIFT = "Drift"
SKIPPED = "Skipped"
_RECOMMENDATION_BY_VERDICT = {
    "ship": "Ship",
    "caution": "Ship with caution",
    "hold": "Don't ship",
}


def _case_outcome(case: Mapping[str, Any]) -> str:
    if case.get("status") != "completed":
        return SKIPPED
    return DRIFT if str(case.get("verdict", "hold")) == "hold" else MATCH


def _outcome_counts(case_results: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {MATCH: 0, DRIFT: 0, SKIPPED: 0}
    for case in case_results:
        counts[_case_outcome(case)] += 1
    return counts


def _recommendation_reason(counts: Mapping[str, int]) -> str:
    drift = counts.get(DRIFT, 0)
    skipped = counts.get(SKIPPED, 0)
    if drift:
        return f"{drift} case{'s' if drift != 1 else ''} drifted"
    if skipped:
        return f"{skipped} case{'s' if skipped != 1 else ''} couldn't be tested safely"
    return "all tested cases matched"


def format_hero_summary(summary: Mapping[str, Any], html_path: Path) -> str:
    """Render the plain-English Match / Drift / Skipped result summary."""
    case_results = summary.get("case_results", [])
    counts = _outcome_counts(case_results)
    imported = summary.get("imported_count", len(case_results))
    recommendation = _RECOMMENDATION_BY_VERDICT.get(
        str(summary.get("overall_verdict", "hold")), "Don't ship"
    )
    reason = _recommendation_reason(counts)
    title = "Replay Verify — Synera design assistant (LangGraph), candidate vs baseline"
    return "\n".join(
        [
            title,
            "─" * len(title),
            f"  {imported} imported cases",
            f"  ✓ {counts[MATCH]} Match     (cheaper config agrees with the current agent)",
            f"  ✗ {counts[DRIFT]} Drift     (cheaper config diverged — see report)",
            f"  ⤼ {counts[SKIPPED]} Skipped   "
            "(couldn't test safely — missing evidence / stale corpus)",
            "",
            f"  RECOMMENDATION:  {recommendation}  ({reason})",
            "",
            f"  Full report:  open {html_path}",
        ]
    )


def run_demo(
    *,
    case_file: Path | None = None,
    report_dir: Path = DEFAULT_REPORT_DIR,
) -> dict[str, Any]:
    """Validate, run baseline vs candidate, and write reports. Returns the summary."""
    resolved_case_file = case_file or DEFAULT_CASE_FILE
    if not resolved_case_file.exists():
        write_cohort(resolved_case_file)

    cases = read_imported_cases_jsonl(resolved_case_file)
    report_dir.mkdir(parents=True, exist_ok=True)
    imported_cases_path = report_dir / "imported_cases.jsonl"
    write_imported_cases_jsonl(cases, imported_cases_path)

    fidelity = validate_imported_cases_jsonl(
        imported_cases_path,
        expected_runner_entrypoint=RUNNER_ENTRYPOINT,
        expected_corpus_index_version=SYNERA_CORPUS_INDEX_VERSION,
        allowed_tool_names=SAFE_TOOL_NAMES,
    )
    (report_dir / "fidelity_report.json").write_text(
        json.dumps(
            {
                "name": "Synera Replay Verify imported-case fidelity report",
                "source": "jsonl",
                "case_file": str(resolved_case_file),
                "summary": fidelity.summary,
                "cases": [to_plain_data(item) for item in fidelity.validations],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    report = verify_imported_cases(
        fidelity.cases,
        baseline_runner=run_baseline_synera_case,
        candidate_runner=run_candidate_synera_case,
        baseline_config=BASELINE_CONFIG,
        candidate_config=CANDIDATE_CONFIG,
        report_name="Synera design assistant imported-input demo (LangGraph)",
        execution_mode=IMPORTED_INPUT_EXECUTION_MODE,
        expected_runner_entrypoint=RUNNER_ENTRYPOINT,
        expected_corpus_index_version=SYNERA_CORPUS_INDEX_VERSION,
        allowed_tool_names=SAFE_TOOL_NAMES,
    )
    paths = write_report_files(report, report_dir)
    return {
        "summary": report.summary,
        "report_dir": report_dir,
        "html": Path(paths["html"]),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-file", type=Path, default=None)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_demo(case_file=args.case_file, report_dir=args.report_dir)
    print(format_hero_summary(result["summary"], result["html"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
