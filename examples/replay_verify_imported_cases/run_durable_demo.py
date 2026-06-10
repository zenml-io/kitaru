"""Run the imported-input verifier as a durable Kitaru flow.

Requires an initialized Kitaru project (``kitaru init``) or an active Kitaru
connection; the deterministic default needs no provider credentials.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runner",
        choices=("deterministic", "live"),
        default="deterministic",
        help="Lane runner mode. 'live' makes real model calls (OPENAI_API_KEY).",
    )
    parser.add_argument("--case-file", type=Path, default=None)
    parser.add_argument("--baseline", default="support-copilot-v1")
    parser.add_argument("--candidate", default="support-copilot-v2")
    parser.add_argument("--baseline-model", default=None)
    parser.add_argument("--candidate-model", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from examples.replay_verify_imported_cases.durable_verify_flow import (
        run_durable_demo,
    )

    args = parse_args(argv)
    result = run_durable_demo(
        case_file=str(args.case_file) if args.case_file else None,
        runner_mode=args.runner,
        baseline=args.baseline,
        candidate=args.candidate,
        baseline_model=args.baseline_model,
        candidate_model=args.candidate_model,
    )
    print("Durable Replay Verify run complete.")
    print(f"execution: {result['exec_id']}")
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    print(
        "Inspect with: kitaru executions get "
        f"{result['exec_id']}  (artifacts include verification_report_html)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
