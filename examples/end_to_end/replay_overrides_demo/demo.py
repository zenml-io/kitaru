"""Replay overrides demo — seed prod runs and dispatch replay scenarios.

Run from this directory:

    uv run python demo.py seed
    uv run python demo.py seed --count 15
    uv run python demo.py flow-override
    uv run python demo.py publish-input
    uv run python demo.py model-override
    uv run python demo.py explicit-skip
    uv run python demo.py tagged-batch
    uv run python demo.py diff-report [REPLAY_ID]
    uv run python demo.py diff-matrix

Each replay command delegates to a module under ``replay_scenarios/`` where the
SDK call and CLI equivalent are spelled out inline.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from replay_scenarios import (
    diff_matrix,
    diff_report,
    explicit_skip,
    flow_override,
    invocation_model_override,
    publish_input,
    tagged_batch,
)
from seed_prod_runs import DEFAULT_COUNT, seed_prod_runs
from utils.runtime import quiet_runtime_logs

PROD_EXEC_IDS = Path("fixtures/prod_exec_ids")


def resolve_prod_id(explicit: str | None = None) -> str:
    """Return the primary prod execution ID from env, argv, or fixtures."""
    if explicit and explicit.strip():
        return explicit.strip()
    env_prod_id = os.environ.get("PROD_ID", "").strip()
    if env_prod_id:
        return env_prod_id
    if PROD_EXEC_IDS.is_file():
        first = PROD_EXEC_IDS.read_text(encoding="utf-8").splitlines()[0].strip()
        if first:
            return first
    raise SystemExit(
        "Set PROD_ID or run demo.py seed first (fixtures/prod_exec_ids)."
    )


def resolve_prod_ids(explicit: list[str] | None = None) -> list[str]:
    """Return prod execution IDs for batch scenarios."""
    if explicit:
        return explicit
    env_ids = os.environ.get("PROD_IDS", "").strip()
    if env_ids:
        return [item.strip() for item in env_ids.split(",") if item.strip()]
    if PROD_EXEC_IDS.is_file():
        ids = [
            line.strip()
            for line in PROD_EXEC_IDS.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if ids:
            return ids
    raise SystemExit(
        "Pass execution IDs as arguments, set PROD_IDS, or run demo.py seed "
        "first (fixtures/prod_exec_ids)."
    )


def resolve_replay_id(explicit: str | None = None) -> str:
    """Return a replay execution ID from argv or REPLAY_ID."""
    if explicit and explicit.strip():
        return explicit.strip()
    env_replay = os.environ.get("REPLAY_ID", "").strip()
    if env_replay:
        return env_replay
    raise SystemExit(
        "Pass the replay execution ID as an argument or set REPLAY_ID. "
        "Copy it from the dashboard compare view on the prod execution."
    )


def _parse_flag(argv: list[str], flag: str) -> tuple[list[str], str | None]:
    if flag not in argv:
        return argv, None
    index = argv.index(flag)
    if index + 1 >= len(argv):
        raise SystemExit(f"{flag} requires a value")
    return argv[:index] + argv[index + 2 :], argv[index + 1]


def main(argv: list[str]) -> None:
    load_dotenv(".env")
    quiet_runtime_logs()
    if not argv:
        raise SystemExit(
            "Usage: demo.py seed [--count N] | flow-override | publish-input | "
            "model-override | explicit-skip | tagged-batch [ID ...] | "
            "diff-report [REPLAY_ID] | diff-matrix [ID ...]"
        )

    command = argv[0]
    rest = argv[1:]

    if command == "seed":
        rest, count_raw = _parse_flag(rest, "--count")
        if rest:
            raise SystemExit(f"Unknown arguments: {' '.join(rest)}")
        seed_prod_runs(count=int(count_raw or DEFAULT_COUNT))
    elif command == "flow-override":
        flow_override.replay_with_flow_overrides(resolve_prod_id())
    elif command == "publish-input":
        publish_input.replay_with_publish_input_override(resolve_prod_id())
    elif command == "model-override":
        invocation_model_override.replay_with_invocation_model_override(resolve_prod_id())
    elif command == "explicit-skip":
        explicit_skip.replay_with_explicit_skip(resolve_prod_id())
    elif command == "tagged-batch":
        tagged_batch.replay_tagged_batch(resolve_prod_ids(rest or None))
    elif command == "diff-report":
        diff_report.report_execution_diff(
            resolve_prod_id(),
            resolve_replay_id(rest[0] if rest else None),
        )
    elif command == "diff-matrix":
        diff_matrix.report_diff_matrix(resolve_prod_ids(rest or None))
    else:
        raise SystemExit(f"Unknown command: {command}")


if __name__ == "__main__":
    main(sys.argv[1:])
