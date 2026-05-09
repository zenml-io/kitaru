"""Watch checkpoint-level Kitaru events for one execution.

Run:
    uv run python watch_checkpoint_events.py <execution-id>
"""

import sys
import time
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from kitaru import ExecutionEvent
from kitaru.events import (
    CHECKPOINT_COMPLETED_KIND,
    CHECKPOINT_FAILED_KIND,
    CHECKPOINT_PROGRESS_KIND,
    CHECKPOINT_STARTED_KIND,
)

DEMO_SECTION_READY_KIND = "demo.brief.section.ready"
FLOW_NAME = "streaming_brief"
POLL_SECONDS = 1.0
DEFAULT_WAIT_TIMEOUT_SECONDS = 120.0
CHECKPOINT_EVENT_KINDS = [
    CHECKPOINT_STARTED_KIND,
    CHECKPOINT_PROGRESS_KIND,
    CHECKPOINT_COMPLETED_KIND,
    CHECKPOINT_FAILED_KIND,
    DEMO_SECTION_READY_KIND,
]


def _checkpoint_name(event: ExecutionEvent) -> str:
    return event.checkpoint_name or "checkpoint"


def _format_percent(data: dict[str, Any]) -> str:
    percent = data.get("percent")
    if isinstance(percent, int | float):
        return f" {percent * 100:>5.1f}%"
    return ""


def _format_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")
    return "--:--:--"


def format_event(event: ExecutionEvent) -> str:
    """Return one terminal-friendly line for a checkpoint event."""
    payload = event.payload
    kind = event.kind
    checkpoint = _checkpoint_name(event)
    timestamp = _format_timestamp(event.timestamp)

    if kind == CHECKPOINT_STARTED_KIND:
        return f"{timestamp}  {checkpoint:<18} started"
    if kind == CHECKPOINT_COMPLETED_KIND:
        return f"{timestamp}  {checkpoint:<18} completed"
    if kind == CHECKPOINT_FAILED_KIND:
        message = payload.get("message") or "failed"
        return f"{timestamp}  {checkpoint:<18} failed: {message}"

    message = payload.get("message") or kind or "event"
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    percent = _format_percent(data)
    return f"{timestamp}  {checkpoint:<18}{percent}  {message}"


def iter_formatted_events(events: Iterable[ExecutionEvent]) -> Iterable[str]:
    """Yield printable lines for incoming execution events."""
    for event in events:
        yield format_event(event)


def watch_execution(execution_id: str) -> None:
    """Print checkpoint events for a Kitaru execution until the stream ends."""
    import kitaru

    client = kitaru.KitaruClient()
    events = client.executions.events(execution_id, kinds=CHECKPOINT_EVENT_KINDS)
    for line in iter_formatted_events(events):
        print(line, flush=True)


def wait_for_next_execution(
    timeout_seconds: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
) -> str:
    """Wait for the next running streaming demo execution and return its ID."""
    import kitaru

    client = kitaru.KitaruClient()
    deadline = time.monotonic() + timeout_seconds
    print(
        f"Waiting for the next running `{FLOW_NAME}` execution...",
        flush=True,
    )
    while time.monotonic() < deadline:
        try:
            execution = client.executions.latest(flow=FLOW_NAME, status="running")
        except LookupError:
            time.sleep(POLL_SECONDS)
            continue
        print(f"Watching execution: {execution.exec_id}", flush=True)
        return execution.exec_id

    raise TimeoutError(
        f"No running `{FLOW_NAME}` execution appeared within {timeout_seconds:.0f}s."
    )


def main(argv: list[str] | None = None) -> int:
    """Run the watcher from the command line."""
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) > 1 or (args and args[0] in {"-h", "--help"}):
        print(
            "Usage: uv run python watch_checkpoint_events.py [execution-id]\n"
            "\n"
            "With no execution ID, the watcher waits for the next running "
            f"`{FLOW_NAME}` demo execution.",
            file=sys.stderr,
        )
        return 0 if args and args[0] in {"-h", "--help"} else 2

    execution_id = args[0] if args else wait_for_next_execution()
    watch_execution(execution_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
