"""Run the Replay Lab comparison for the deterministic support demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from kitaru._replay_lab import compare_replay_lab

DEFAULT_DIR = Path(__file__).parent
DEFAULT_MANIFEST_PATH = DEFAULT_DIR / "manifests" / "support_demo.json"
DEFAULT_CANDIDATE_PATH = DEFAULT_DIR / "candidates" / "cheaper_support_agent.json"
DEFAULT_REPORT_DIR = DEFAULT_DIR / "reports"


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Generated cohort manifest from seed_observed.py.",
    )
    parser.add_argument(
        "--candidate-path",
        type=Path,
        default=DEFAULT_CANDIDATE_PATH,
        help="Candidate descriptor JSON file.",
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
        default=180.0,
        help="Per-lane replay timeout.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="How often to poll replay executions.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the comparison and print the generated report paths."""
    args = parse_args(argv)
    candidate = load_json(args.candidate_path)
    report = compare_replay_lab(
        manifest_path=args.manifest_path,
        candidate_descriptor=candidate,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        report_dir=args.report_dir,
    )

    print("Replay Lab comparison complete.")
    for report_type, report_path in sorted(report.report_paths.items()):
        print(f"{report_type}: {report_path}")
    print("summary:")
    print(json.dumps(report.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
