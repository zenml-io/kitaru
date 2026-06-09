# /// script
# requires-python = ">=3.11"
# dependencies = ["langfuse>=2.0"]
# ///
"""Export Langfuse observation rows for a project to JSONL.

Standalone script — run it with uv so the ``langfuse`` dependency is resolved
from the inline metadata above (it is intentionally not a Kitaru dependency):

    LANGFUSE_PUBLIC_KEY=pk-... LANGFUSE_SECRET_KEY=sk-... \
    LANGFUSE_HOST=https://cloud.langfuse.com \
    uv run examples/replay_verify_imported_cases/fetch_langfuse_observations.py \
        --output observations.jsonl --limit 200

The output file feeds directly into ``run_scan_demo.py --observations ...``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REQUIRED_ENV_VARS = ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST")
PAGE_SIZE = 50


def _row_from_observation(observation: Any) -> dict[str, Any]:
    """Convert a Langfuse API observation model into a plain JSON row."""
    # Langfuse's generated API models are pydantic models across 2.x/3.x;
    # ``json()``/``model_dump_json`` keeps datetimes serializable.
    if hasattr(observation, "model_dump_json"):
        return json.loads(observation.model_dump_json())
    if hasattr(observation, "json"):
        return json.loads(observation.json())
    return dict(observation)


def fetch_observation_rows(
    *,
    limit: int,
    trace_id: str | None = None,
    observation_type: str | None = None,
) -> list[dict[str, Any]]:
    """Page through the Langfuse observations API and collect raw rows."""
    from langfuse import Langfuse

    client = Langfuse()
    rows: list[dict[str, Any]] = []
    page = 1
    while len(rows) < limit:
        response = client.api.observations.get_many(
            page=page,
            limit=min(PAGE_SIZE, limit - len(rows)),
            trace_id=trace_id,
            type=observation_type,
        )
        data = response.data
        if not data:
            break
        rows.extend(_row_from_observation(observation) for observation in data)
        if page >= response.meta.total_pages:
            break
        page += 1
    return rows[:limit]


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("observations.jsonl"),
        help="JSONL file to write (one observation row per line).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of observation rows to export.",
    )
    parser.add_argument(
        "--trace-id",
        default=None,
        help="Only export observations for this trace id (optional).",
    )
    parser.add_argument(
        "--type",
        dest="observation_type",
        default=None,
        help="Only export observations of this type, e.g. GENERATION (optional).",
    )
    args = parser.parse_args(argv)

    missing = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]
    if missing:
        print(
            "Missing required environment variables: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 1

    rows = fetch_observation_rows(
        limit=args.limit,
        trace_id=args.trace_id,
        observation_type=args.observation_type,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, default=str) + "\n")
    print(f"Wrote {len(rows)} observation rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
