"""Run Replay Lab for the live LangGraph requirements-triage demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from kitaru._replay_lab import compare_replay_lab

DEFAULT_DIR = Path(__file__).parent
DEFAULT_MANIFEST_PATH = DEFAULT_DIR / "manifests" / "requirements_triage.json"
DEFAULT_MATRIX_PATH = DEFAULT_DIR / "candidates" / "model_matrix.example.json"
DEFAULT_REPORT_DIR = DEFAULT_DIR / "reports"
EVALUATOR_TARGET = (
    "examples.end_to_end.replay_lab.langgraph_requirements_triage."
    "evaluator:evaluate_requirements_triage"
)


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def load_candidate_descriptors(
    *,
    matrix_path: Path | None,
    candidate_paths: list[Path] | None,
    candidate_limit: int | None = None,
) -> list[dict[str, Any]]:
    """Load candidates from a matrix file or repeated descriptor paths."""
    if matrix_path is not None and candidate_paths:
        raise ValueError(
            "Pass either --matrix-path or repeated --candidate-path, not both."
        )
    if candidate_limit is not None and candidate_limit < 1:
        raise ValueError("candidate limit must be at least 1.")

    if candidate_paths:
        candidates = [load_json(path) for path in candidate_paths]
    else:
        matrix = load_json(matrix_path or DEFAULT_MATRIX_PATH)
        raw_candidates = matrix.get("candidates")
        if not isinstance(raw_candidates, list):
            raise ValueError("Matrix file must contain a `candidates` list.")
        candidates = []
        for index, item in enumerate(raw_candidates, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"Matrix candidate #{index} must be an object.")
            candidates.append(dict(item))

    if candidate_limit is not None:
        candidates = candidates[:candidate_limit]
    return candidates


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Generated live cohort manifest from seed_observed.py.",
    )
    parser.add_argument(
        "--matrix-path",
        type=Path,
        help="Candidate matrix JSON file with a top-level `candidates` list.",
    )
    parser.add_argument(
        "--candidate-path",
        action="append",
        type=Path,
        help="Candidate descriptor JSON file. Repeat for multiple candidates.",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        help=(
            "Use only the first N loaded candidates, useful for a "
            "two-candidate first run."
        ),
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=DEFAULT_REPORT_DIR,
        help="Directory for JSON and Markdown report files.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=300.0,
        help="Per-lane replay timeout.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=5.0,
        help="How often to poll replay executions.",
    )
    args = parser.parse_args(argv)
    if args.matrix_path is not None and args.candidate_path:
        parser.error(
            "Pass either --matrix-path or repeated --candidate-path, not both."
        )
    if args.candidate_limit is not None and args.candidate_limit < 1:
        parser.error("--candidate-limit must be at least 1.")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the live comparison and print generated report paths."""
    args = parse_args(argv)
    candidates = load_candidate_descriptors(
        matrix_path=args.matrix_path,
        candidate_paths=args.candidate_path,
        candidate_limit=args.candidate_limit,
    )
    report = compare_replay_lab(
        manifest_path=args.manifest_path,
        candidate_descriptors=candidates,
        evaluator_descriptor={
            "target": EVALUATOR_TARGET,
            "id": "requirements_triage_v1",
            "on_error": "warn",
            "precedence": "override",
        },
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        report_dir=args.report_dir,
        source="script",
    )

    print("Replay Lab requirements-triage comparison complete.")
    for report_type, report_path in sorted(report.report_paths.items()):
        print(f"{report_type}: {report_path}")
    print("summary:")
    print(json.dumps(report.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
