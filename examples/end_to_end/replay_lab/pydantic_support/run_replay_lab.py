"""Run Replay Lab for the live PydanticAI support demo."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[4]))

from kitaru._replay_lab import compare_replay_lab

DEFAULT_DIR = Path(__file__).parent
DEFAULT_MANIFEST_PATH = DEFAULT_DIR / "manifests" / "pydantic_support.json"
DEFAULT_MATRIX_PATH = DEFAULT_DIR / "candidates" / "model_matrix.json"
DEFAULT_REPORT_DIR = DEFAULT_DIR / "reports"
EVALUATOR_TARGET = (
    "examples.end_to_end.replay_lab.pydantic_support.evaluator"
    ":evaluate_support_response"
)


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def load_candidate_descriptors(
    *, matrix_path: Path | None, candidate_paths: list[Path] | None
) -> list[dict[str, Any]]:
    """Load candidates from a matrix file or repeated descriptor paths."""
    if matrix_path is not None and candidate_paths:
        raise ValueError("Pass either --matrix-path or --candidate-path, not both.")
    if candidate_paths:
        return [load_json(path) for path in candidate_paths]
    matrix = load_json(matrix_path or DEFAULT_MATRIX_PATH)
    raw = matrix.get("candidates")
    if not isinstance(raw, list):
        raise ValueError("Matrix file must contain a `candidates` list.")
    return [dict(item) for item in raw]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--matrix-path", type=Path)
    parser.add_argument("--candidate-path", action="append", type=Path)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the live comparison and print generated report paths."""
    args = parse_args(argv)
    candidates = load_candidate_descriptors(
        matrix_path=args.matrix_path, candidate_paths=args.candidate_path
    )
    report = compare_replay_lab(
        manifest_path=args.manifest_path,
        candidate_descriptors=candidates,
        evaluator_descriptor={
            "target": EVALUATOR_TARGET,
            "id": "pydantic_support_v1",
            "on_error": "warn",
            "precedence": "override",
        },
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        report_dir=args.report_dir,
        source="script",
    )
    print("Replay Lab PydanticAI support comparison complete.")
    for report_type, report_path in sorted(report.report_paths.items()):
        print(f"{report_type}: {report_path}")
    print("summary:")
    print(json.dumps(report.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
