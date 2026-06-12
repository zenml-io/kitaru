"""Seed live observed PydanticAI support executions and write a manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[4]))

try:  # Package import path.
    from .support_cases import get_case, select_case_ids
    from .support_flow import REPLAY_ANCHOR, support_agent_case
except ImportError:  # Direct script path.
    from support_cases import get_case, select_case_ids  # type: ignore[no-redef]
    from support_flow import (  # type: ignore[no-redef]
        REPLAY_ANCHOR,
        support_agent_case,
    )

DEFAULT_MANIFEST_PATH = Path(__file__).parent / "manifests" / "pydantic_support.json"
EXPECTED_ARTIFACTS = ["scorecard", "final_response"]
DEFAULT_MODEL_ALIAS = "current"


def build_manifest_payload(observed_runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a Replay Lab manifest from live observed run records."""
    return {
        "name": "PydanticAI support Replay Lab demo",
        "description": (
            "Live Replay Lab cohort: a PydanticAI customer-support agent with "
            "deterministic faked tools. Observed runs use the `current` alias; "
            "Replay Lab replays each case against a cheaper candidate alias so "
            "only the model differs."
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
    parser.add_argument("--model", default=DEFAULT_MODEL_ALIAS)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run observed live executions and write the generated manifest."""
    args = parse_args(argv)
    case_ids = select_case_ids(
        case_ids=args.case_ids, small=args.small, count=args.count
    )
    observed_runs: list[dict[str, Any]] = []
    for case_id in case_ids:
        case = get_case(case_id)
        print(f"Seeding observed support execution for {case_id}...", flush=True)
        handle = support_agent_case.run(case_id, model=args.model)
        final_response = handle.wait()
        observed_runs.append(
            {
                "case_id": case_id,
                "exec_id": handle.exec_id,
                "reason": case["reason"],
                "labels": case["labels"],
            }
        )
        print(f"  execution: {handle.exec_id}", flush=True)
        print(f"  final response: {final_response}", flush=True)

    args.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    args.manifest_path.write_text(
        json.dumps(build_manifest_payload(observed_runs), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote manifest: {args.manifest_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
