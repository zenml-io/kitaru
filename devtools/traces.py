"""Generate dummy JSONL traces for the kitaru/kitaru-jsonl importer."""

import argparse
import asyncio
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from simulation import (
    BASE_STARTED_AT,
    FRAMEWORK,
    FixedClock,
    SimulationConfig,
    build_session_inputs,
    simulate_session,
)

from kitaru.task.importer import ImportedSession

MALFORMED_LINES = (
    '{"external_id": "trace-bad-json", "inputs": {"question": ',
    '{"inputs": {"question": "Where?"}, "outputs": {"answer": "There."}}',
    '{"external_id": "trace-bad-nodes", "status": "completed", "inputs": {}, '
    '"outputs": {}, "nodes": [{"name": "orphan"}]}',
)


def malformed_line_count(requested: int) -> int:
    """Return how many malformed lines a request actually yields."""
    return min(requested, len(MALFORMED_LINES))


async def build_trace(config: SimulationConfig, index: int) -> dict[str, Any]:
    """Build one deterministic trace record as a JSON-serializable dict."""
    inputs = build_session_inputs(config, index)
    clock = FixedClock(BASE_STARTED_AT + timedelta(minutes=index))
    simulated = await simulate_session(inputs, clock=clock)
    metadata = dict(simulated.metadata)
    if simulated.outputs is not None:
        metadata["expected"] = simulated.outputs
    record = {
        "external_id": f"{config.seed}-trace-{index:06d}",
        "name": simulated.name,
        "status": simulated.status.value,
        "inputs": simulated.inputs,
        "outputs": simulated.outputs,
        "error": simulated.error,
        "started_at": simulated.started_at.isoformat(),
        "ended_at": simulated.ended_at.isoformat(),
        "metadata": metadata,
        "framework": FRAMEWORK,
        "nodes": [node.model_dump(mode="json") for node in simulated.nodes],
    }
    # Reject schema drift at generation time instead of at import time.
    ImportedSession.model_validate(record)
    return record


async def build_payload(
    config: SimulationConfig,
    count: int,
    start_index: int = 0,
    malformed: int = 0,
) -> bytes:
    """Build a JSONL import payload of deterministic dummy traces."""
    lines = [
        json.dumps(await build_trace(config, index))
        for index in range(start_index, start_index + count)
    ]
    lines += list(MALFORMED_LINES[:malformed])
    return ("\n".join(lines) + "\n").encode("utf-8")


def add_simulation_args(parser: argparse.ArgumentParser) -> None:
    """Add the shared simulation shape flags to a parser."""
    parser.add_argument("--seed", default="kitaru-dev", help="Generation seed.")
    parser.add_argument("--min-turns", type=int, default=1)
    parser.add_argument("--max-turns", type=int, default=3)
    parser.add_argument(
        "--failure-rate",
        type=float,
        default=0.0,
        help="Fraction of sessions that end failed.",
    )
    parser.add_argument(
        "--big-payload-every",
        type=int,
        default=0,
        help="Attach a large context blob to every Nth session, 0 disables.",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=4096,
        help="Size of the large context blob.",
    )


def config_from_args(args: argparse.Namespace) -> SimulationConfig:
    """Build a simulation config from parsed CLI flags."""
    return SimulationConfig(
        seed=args.seed,
        min_turns=args.min_turns,
        max_turns=args.max_turns,
        failure_rate=args.failure_rate,
        big_payload_every=args.big_payload_every,
        payload_bytes=args.payload_bytes,
    )


def main() -> int:
    """Generate a JSONL trace file from CLI flags."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=25, help="Traces to generate.")
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="First session index, for appending distinct batches.",
    )
    parser.add_argument(
        "--malformed",
        type=int,
        default=0,
        help=(
            f"Broken lines to append, at most {len(MALFORMED_LINES)}, "
            "for import failure handling."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file, stdout when omitted.",
    )
    add_simulation_args(parser)
    args = parser.parse_args()

    payload = asyncio.run(
        build_payload(
            config_from_args(args),
            count=args.count,
            start_index=args.start_index,
            malformed=args.malformed,
        )
    )
    malformed = malformed_line_count(args.malformed)
    if malformed < args.malformed:
        print(
            f"Only {malformed} malformed lines are available, "
            f"ignoring the remaining {args.malformed - malformed}.",
            file=sys.stderr,
        )
    if args.output is None:
        sys.stdout.buffer.write(payload)
    else:
        args.output.write_bytes(payload)
        print(f"Wrote {args.count + malformed} lines to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
