"""Scan uninstrumented Synera LangFuse traces for replay fidelity.

This is the "first answer before anything is installed" for Synera: point the
imported-input validators at *uninstrumented* mechanical-engineering chat traces
(ordinary LangFuse generations with none of the replay trace-contract metadata)
and get a checklist back -- how many traces are replayable today, and exactly
which fields each one is missing -- instead of an error.

It answers Ruben's #1 blocker ("are my traces even replayable, and can I tell
without sharing anything sensitive?") with zero setup, zero credentials, and no
data leaving the machine.

    uv run python -m examples.replay_verify_synera_langgraph.run_synera_scan

Outputs (default ``reports/`` next to this script):

- ``scan_checklist.md`` — per-trace fidelity checklist with an aggregate header
- ``scan_report.json`` — full scan payload

To scan a real export, pass ``--observations path/to/rows.jsonl`` (one LangFuse
observation object per line; see the shipped ``fetch_langfuse_observations.py``).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from kitaru._replay_verify_imported_models import to_plain_data
from kitaru._replay_verify_imported_reporting import render_fidelity_checklist
from kitaru._replay_verify_imported_sources.scan import scan_langfuse_observations

DEMO_DIR = Path(__file__).resolve().parent
DEFAULT_OBSERVATIONS_FILE = DEMO_DIR / "fixtures" / "uninstrumented_observations.jsonl"
DEFAULT_REPORT_DIR = DEMO_DIR / "reports"


def read_observation_rows(path: Path) -> list[dict[str, Any]]:
    """Read raw LangFuse observation rows (one JSON object per line)."""
    rows: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            msg = f"Expected a JSON object on line {line_number} of {path}"
            raise TypeError(msg)
        rows.append(row)
    return rows


def run_scan(observations_file: Path, report_dir: Path) -> dict[str, Any]:
    """Scan observation rows; write the checklist + JSON report; return the summary."""
    rows = read_observation_rows(observations_file)
    result = scan_langfuse_observations(rows, source_ref=str(observations_file))

    report_dir.mkdir(parents=True, exist_ok=True)
    checklist_path = report_dir / "scan_checklist.md"
    checklist_path.write_text(
        render_fidelity_checklist(result.validations, summary=result.summary),
        encoding="utf-8",
    )
    (report_dir / "scan_report.json").write_text(
        json.dumps(
            {
                "observations_file": str(observations_file),
                "summary": result.summary,
                "cases": [to_plain_data(case) for case in result.cases],
                "validations": [to_plain_data(v) for v in result.validations],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return {"summary": result.summary, "checklist": checklist_path}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, default=DEFAULT_OBSERVATIONS_FILE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_scan(args.observations, args.report_dir)
    summary = result["summary"]
    total = summary.get("case_count", summary.get("total", 0))
    verifiable = summary.get("verifiable_count", 0)
    print("Synera trace scan — can these LangFuse traces be replayed yet?")
    print("─" * 60)
    print(f"  {verifiable} of {total} traces are verifiable as-is")
    print(f"  Checklist (which fields unlock the rest):  open {result['checklist']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
