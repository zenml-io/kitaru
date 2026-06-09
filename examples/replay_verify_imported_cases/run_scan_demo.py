"""Scan arbitrary Langfuse observation rows and report replay fidelity.

This demo points the imported-input verifier at *uninstrumented* traces:
ordinary chat generations exported from any Langfuse project, with none of the
replay trace contract metadata that the support-copilot demo emits. Instead of
errors or registry noise, the scan produces a checklist that says, per trace,
exactly which fields are missing before the trace could be verified.

Run it:

    uv run examples/replay_verify_imported_cases/run_scan_demo.py

Outputs (default ``reports/`` next to this script):

- ``scan_checklist.md`` — per-case fidelity checklist with an aggregate header
- ``scan_report.json`` — full scan payload (cases, validations, summaries)

To scan your own export, produce an observation-rows JSONL file (one Langfuse
observation object per line — see ``fetch_langfuse_observations.py``) and pass
``--observations path/to/rows.jsonl``.
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
    """Read raw Langfuse observation rows from a JSONL file."""
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                msg = f"Invalid JSON on line {line_number} of {path}: {exc.msg}"
                raise ValueError(msg) from exc
            if not isinstance(row, dict):
                msg = f"Expected JSON object on line {line_number} of {path}"
                raise ValueError(msg)
            rows.append(row)
    return rows


def run_scan(
    observations_file: Path,
    report_dir: Path,
    *,
    base_url: str | None = None,
) -> dict[str, str]:
    """Scan observation rows and write checklist + JSON report files."""
    rows = read_observation_rows(observations_file)
    result = scan_langfuse_observations(
        rows,
        base_url=base_url,
        source_ref=str(observations_file),
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    checklist_path = report_dir / "scan_checklist.md"
    json_path = report_dir / "scan_report.json"
    checklist_path.write_text(
        render_fidelity_checklist(result.validations, summary=result.summary),
        encoding="utf-8",
    )
    report_payload = {
        "observations_file": str(observations_file),
        "summary": result.summary,
        "source_import_summary": result.source_import_summary,
        "cases": [to_plain_data(case) for case in result.cases],
        "validations": [to_plain_data(validation) for validation in result.validations],
    }
    json_path.write_text(
        json.dumps(report_payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return {"checklist": str(checklist_path), "json": str(json_path)}


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the scan demo."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--observations",
        type=Path,
        default=DEFAULT_OBSERVATIONS_FILE,
        help="Observation-rows JSONL file (one Langfuse observation per line).",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=DEFAULT_REPORT_DIR,
        help="Directory for scan_checklist.md and scan_report.json.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Langfuse base URL used to build trace links (optional).",
    )
    args = parser.parse_args(argv)
    paths = run_scan(args.observations, args.report_dir, base_url=args.base_url)
    print(f"Wrote fidelity checklist: {paths['checklist']}")
    print(f"Wrote scan report:        {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
