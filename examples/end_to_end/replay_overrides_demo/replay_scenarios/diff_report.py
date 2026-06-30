"""Diff report — compare one original execution with a replay child.

CLI equivalent:

    uv run kitaru executions diff "$PROD_ID" "$REPLAY_ID" -o json
"""

from __future__ import annotations

import json
from pathlib import Path

from kitaru import diff
from kitaru.diff import serialize_execution_diff

REPORTS = Path("reports")


def report_execution_diff(prod_id: str, replay_id: str) -> None:
    result = diff(prod_id, replay_id)
    REPORTS.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS / "diff_report.json"
    payload = {
        "original_exec_id": prod_id,
        "replay_exec_ids": [replay_id],
        "diff": serialize_execution_diff(result),
    }
    report_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
