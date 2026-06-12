"""Seed observed champion requirements-triage executions and write a manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent))

try:
    from .requirements_scenarios import get_scenario, select_case_ids
    from .triage_flow import REPLAY_ANCHOR, requirements_triage_case
except ImportError:
    from requirements_scenarios import (  # type: ignore[no-redef]
        get_scenario,
        select_case_ids,
    )
    from triage_flow import (  # type: ignore[no-redef]
        REPLAY_ANCHOR,
        requirements_triage_case,
    )

DEFAULT_MANIFEST_PATH = Path(__file__).parent / "manifests" / "requirements_triage.json"
EXPECTED_ARTIFACTS = ["scorecard", "final_response"]


def build_manifest_payload(observed_runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a Replay Lab manifest from observed run records."""
    return {
        "name": "Requirements triage Replay Lab demo",
        "description": (
            "Deterministic engineering requirements-triage cohort. Observed runs "
            "use the current model (champion); Replay Lab replays each case "
            "against a cheaper candidate model and flags any dropped requirement."
        ),
        "default_from_checkpoint": REPLAY_ANCHOR,
        "expected_artifacts": EXPECTED_ARTIFACTS,
        "cases": [
            {
                "case_id": run["case_id"],
                "exec_id": run["exec_id"],
                "reason": run["reason"],
                "labels": {str(k): str(v) for k, v in run.get("labels", {}).items()},
            }
            for run in observed_runs
        ],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", dest="case_ids", action="append")
    parser.add_argument("--small", action="store_true")
    parser.add_argument("--count", type=int)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run observed champion executions and write the manifest."""
    args = parse_args(argv)
    case_ids = select_case_ids(
        case_ids=args.case_ids, small=args.small, count=args.count
    )
    observed_runs: list[dict[str, Any]] = []
    for case_id in case_ids:
        scenario = get_scenario(case_id)
        print(f"Seeding observed requirements-triage execution for {case_id}...")
        handle = requirements_triage_case.run(case_id, agent_profile="champion")
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

    args.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    args.manifest_path.write_text(
        json.dumps(build_manifest_payload(observed_runs), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote manifest: {args.manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
