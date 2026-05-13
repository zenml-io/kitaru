"""Seed observed champion executions and write a Replay Lab manifest.

This script creates the observed-production lane for the demo. By default it
runs a richer deterministic production-like cohort: the three original support
cases plus three synthetic history variants for each case. Use ``--small`` to
preserve the old quick three-case path.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:  # Package import path used by tests and repo-root execution.
    from .scenarios import (
        DEFAULT_VARIANTS_PER_BASE,
        MAX_VARIANTS_PER_BASE,
        get_scenario,
        list_base_scenarios,
        list_seed_scenarios,
    )
    from .support_flow import support_replay_lab_case
except ImportError:  # Direct script path used by README commands.
    from scenarios import (  # type: ignore[no-redef]
        DEFAULT_VARIANTS_PER_BASE,
        MAX_VARIANTS_PER_BASE,
        get_scenario,
        list_base_scenarios,
        list_seed_scenarios,
    )
    from support_flow import support_replay_lab_case  # type: ignore[no-redef]

DEFAULT_MANIFEST_PATH = Path(__file__).parent / "manifests" / "support_demo.json"
DEFAULT_FROM_CHECKPOINT = "draft_response"
EXPECTED_ARTIFACTS = ["scorecard", "final_response"]


def build_manifest_payload(observed_runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a manifest payload from observed run records."""
    return {
        "name": "Support Replay Lab demo",
        "description": (
            "Synthetic customer-support regression cohort. Observed executions "
            "use the champion profile over a deterministic production-like "
            "history; Replay Lab then compares baseline replay against a "
            "cheaper candidate profile."
        ),
        "default_from_checkpoint": DEFAULT_FROM_CHECKPOINT,
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


def select_case_ids(
    *,
    case_ids: list[str] | None,
    small: bool,
    count: int | None,
    variants_per_base: int | None,
) -> list[str]:
    """Select deterministic case IDs for the seed run.

    Args:
        case_ids: Explicit case IDs passed with repeated ``--case`` flags.
        small: Whether to use the old three-case demo cohort.
        count: Optional cap applied after deterministic ordering.
        variants_per_base: Optional variant count for the rich default cohort.

    Returns:
        Stable case IDs to run sequentially.
    """
    if count is not None and count < 1:
        raise ValueError("count must be at least 1.")

    if case_ids:
        return _validate_explicit_case_ids(case_ids)

    if small:
        selected = [scenario["case_id"] for scenario in list_base_scenarios()]
    else:
        variant_count = (
            DEFAULT_VARIANTS_PER_BASE
            if variants_per_base is None
            else variants_per_base
        )
        selected = [
            scenario["case_id"]
            for scenario in list_seed_scenarios(variants_per_base=variant_count)
        ]

    if count is not None:
        selected = selected[:count]
    return selected


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        dest="case_ids",
        action="append",
        help=(
            "Case ID to seed. Repeat to choose multiple cases. Defaults to the "
            "rich deterministic production-like cohort."
        ),
    )
    parser.add_argument(
        "--small",
        action="store_true",
        help="Seed only the original three demo cases.",
    )
    parser.add_argument(
        "--count",
        type=int,
        help="Cap the deterministically ordered selected cases to this count.",
    )
    parser.add_argument(
        "--variants-per-base",
        type=int,
        help=(
            "Number of generated history variants per base case. Defaults to "
            f"{DEFAULT_VARIANTS_PER_BASE} for the rich seed path."
        ),
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
    """Run champion executions and write the generated manifest."""
    args = parse_args(argv)
    case_ids = select_case_ids(
        case_ids=args.case_ids,
        small=args.small,
        count=args.count,
        variants_per_base=args.variants_per_base,
    )
    observed_runs: list[dict[str, Any]] = []

    for case_id in case_ids:
        scenario = get_scenario(case_id)
        print(f"Seeding observed champion execution for {case_id}...")
        handle = support_replay_lab_case.run(case_id, agent_profile="champion")
        final_response = handle.wait()
        observed_runs.append(
            {
                "case_id": case_id,
                "exec_id": handle.exec_id,
                "reason": scenario["reason"],
                "labels": scenario["labels"],
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
    """Reject confusing seed-selection flag combinations."""
    if args.case_ids and (
        args.small or args.count is not None or args.variants_per_base is not None
    ):
        parser.error(
            "--case cannot be combined with --small, --count, or --variants-per-base."
        )
    if args.small and args.variants_per_base is not None:
        parser.error("--small cannot be combined with --variants-per-base.")
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1.")
    if args.variants_per_base is not None and args.variants_per_base < 0:
        parser.error("--variants-per-base must be at least 0.")
    if (
        args.variants_per_base is not None
        and args.variants_per_base > MAX_VARIANTS_PER_BASE
    ):
        parser.error(f"--variants-per-base must be {MAX_VARIANTS_PER_BASE} or fewer.")


def _validate_explicit_case_ids(case_ids: list[str]) -> list[str]:
    """Validate explicit case IDs while preserving caller order."""
    for case_id in case_ids:
        get_scenario(case_id)
    return list(case_ids)


def _string_labels(labels: Any) -> dict[str, str]:
    """Return manifest labels as flat strings for backend compatibility."""
    if not isinstance(labels, dict):
        return {}
    return {str(key): str(value) for key, value in labels.items()}


if __name__ == "__main__":
    raise SystemExit(main())
