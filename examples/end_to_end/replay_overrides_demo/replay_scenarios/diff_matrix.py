"""Diff matrix — compare batch originals with their tagged replay children.

CLI equivalent:

    uv run kitaru executions diff-matrix "$ID_1" "$ID_2" "$ID_3" -o json
"""

from __future__ import annotations

import json
from pathlib import Path

from kitaru.diff import diff_matrix, serialize_diff_matrix

REPORTS = Path("reports")


def report_diff_matrix(prod_ids: list[str]) -> None:
    result = diff_matrix(prod_ids)
    REPORTS.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS / "diff_matrix.json"
    report_path.write_text(
        json.dumps(serialize_diff_matrix(result), indent=2, sort_keys=True),
        encoding="utf-8",
    )
