"""Seed live observed requirements-triage executions and write a manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:  # Package import path used by tests and repo-root execution.
    from .requirements_cases import get_case, select_case_ids
    from .requirements_flow import REPLAY_ANCHOR, requirements_triage_case
except ImportError:  # Direct script path used by example commands.
    from requirements_cases import get_case, select_case_ids  # type: ignore[no-redef]
    from requirements_flow import (  # type: ignore[no-redef]
        REPLAY_ANCHOR,
        requirements_triage_case,
    )

DEFAULT_MANIFEST_PATH = Path(__file__).parent / "manifests" / "requirements_triage.json"
EXPECTED_ARTIFACTS = ["scorecard", "final_response"]
DEFAULT_MODEL_ALIAS = "balanced"


def build_manifest_payload(observed_runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a Replay Lab manifest from live observed run records."""
    return {
        "name": "Requirements triage LangGraph demo",
        "description": (
            "Live opt-in Replay Lab cohort for comparing model aliases on a "
            "LangGraph requirements-triage flow."
        ),
        "default_from_checkpoint": REPLAY_ANCHOR,
        "expected_artifacts": EXPECTED_ARTIFACTS,
        "cases": [
            {
                "case_id": run["case_id"],
                "exec_id": run["exec_id"],
                "reason": run["reason"],
                "labels": _string_labels(run.get("labels", {})),
            }
            for run in observed_runs
        ],
    }


def write_manifest(payload: dict[str, Any], path: Path) -> Path:
    """Write a manifest JSON file and return its path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        dest="case_ids",
        action="append",
        help="Case ID to seed. Repeat to choose multiple cases.",
    )
    parser.add_argument(
        "--small",
        action="store_true",
        help="Seed the first two live requirements-triage cases.",
    )
    parser.add_argument(
        "--count",
        type=int,
        help="Cap the deterministic default case order to N cases.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL_ALIAS,
        help="Observed-lane Kitaru model alias. Defaults to `balanced`.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Where to write the generated manifest JSON.",
    )
    args = parser.parse_args(argv)
    _validate_arg_combinations(parser, args)
    return args


def main(argv: list[str] | None = None) -> int:
    """Run observed live executions and write the generated manifest."""
    args = parse_args(argv)
    case_ids = select_case_ids(
        case_ids=args.case_ids,
        small=args.small,
        count=args.count,
    )
    observed_runs: list[dict[str, Any]] = []

    for case_id in case_ids:
        case = get_case(case_id)
        print(f"Seeding observed requirements-triage execution for {case_id}...")
        handle = requirements_triage_case.run(case_id, model=args.model)
        final_response = handle.wait()
        observed_runs.append(
            {
                "case_id": case_id,
                "exec_id": handle.exec_id,
                "reason": case["reason"],
                "labels": case["labels"],
            }
        )
        print(f"  execution: {handle.exec_id}")
        print(f"  final response: {final_response}")

    manifest_path = write_manifest(
        build_manifest_payload(observed_runs),
        args.manifest_path,
    )
    print(f"\nWrote manifest: {manifest_path}")
    return 0


def _validate_arg_combinations(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> None:
    if args.case_ids and (args.small or args.count is not None):
        parser.error("--case cannot be combined with --small or --count.")
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1.")


def _string_labels(labels: Any) -> dict[str, str]:
    if not isinstance(labels, dict):
        return {}
    return {str(key): str(value) for key, value in labels.items()}


if __name__ == "__main__":
    raise SystemExit(main())
