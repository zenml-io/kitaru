"""Video-friendly checkpoint streaming demo.

Terminal A:
    cd examples/features/checkpoint_streaming
    uv run python checkpoint_streaming_flow.py "garden robot"

Terminal B:
    cd examples/features/checkpoint_streaming
    uv run python watch_checkpoint_events.py <execution-id>

This example deliberately avoids external APIs. The checkpoint sleeps for a
moment between each phase so the watcher has something visible to print.
"""

import os
import sys
import time

import kitaru

DEFAULT_DELAY_SECONDS = 5.0


@kitaru.checkpoint
def draft_brief(topic: str) -> str:
    """Pretend to do a few slow pieces of work for a short research brief."""
    steps = [
        ("Collecting source notes", 0.2, {"documents": 3}),
        ("Comparing the strongest claims", 0.45, {"claims_checked": 7}),
        ("Writing the first version", 0.75, {"sections": 2}),
    ]
    delay_seconds = float(
        os.environ.get("CHECKPOINT_STREAMING_DELAY_SECONDS", DEFAULT_DELAY_SECONDS)
    )
    for message, percent, fields in steps:
        kitaru.progress(message, percent=percent, **fields)
        time.sleep(delay_seconds)

    kitaru.events.publish(
        "demo.brief.section.ready",
        {"section": "recommendation", "topic": topic},
        message="Recommendation section is ready",
    )
    time.sleep(delay_seconds)
    kitaru.progress("Brief complete", percent=1.0)
    return f"A short practical brief about {topic}."


@kitaru.flow
def streaming_brief(topic: str) -> str:
    """Run one checkpoint that emits live progress while it works."""
    return draft_brief(topic)


def run_workflow(topic: str = "garden robot") -> tuple[str, str]:
    """Start the flow and return its execution ID and final result."""
    handle = streaming_brief.run(topic)
    print(f"Execution ID: {handle.exec_id}", flush=True)
    print("If Terminal B is not already watching, run:", flush=True)
    print(f"  uv run python watch_checkpoint_events.py {handle.exec_id}", flush=True)
    result = handle.wait()
    return handle.exec_id, result


def main(argv: list[str] | None = None) -> int:
    """Run the demo from the command line."""
    args = list(sys.argv[1:] if argv is None else argv)
    topic = args[0] if args else "garden robot"
    _execution_id, result = run_workflow(topic)
    print(f"\nFinal result: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
