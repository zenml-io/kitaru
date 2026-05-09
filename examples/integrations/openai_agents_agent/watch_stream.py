"""Watch OpenAI Agents stream events for the local example.

Run:
    uv run python watch_stream.py <execution-id>
"""

import sys
from collections.abc import Iterable
from typing import Any

STREAM_EVENT_KIND = "openai_agents.stream.event"


def _event_payload(event: Any) -> dict[str, Any]:
    payload = (
        event.get("payload")
        if isinstance(event, dict)
        else getattr(event, "payload", None)
    )
    return payload if isinstance(payload, dict) else {}


def display_text(event: Any) -> str:
    """Return the text worth printing for one stream event."""
    payload = _event_payload(event)
    for key in ("text_delta", "display"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return ""


def iter_display_text(events: Iterable[Any]) -> Iterable[str]:
    """Yield printable text chunks from stream events."""
    for event in events:
        text = display_text(event)
        if text:
            yield text


def watch_execution(execution_id: str) -> None:
    """Print OpenAI Agents stream text for a Kitaru execution."""
    from zenml.client import Client

    events = Client().iter_run_events(execution_id, kinds=[STREAM_EVENT_KIND])
    for text in iter_display_text(events):
        print(text, end="", flush=True)
    print()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("Usage: uv run python watch_stream.py <execution-id>", file=sys.stderr)
        return 2
    watch_execution(args[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
