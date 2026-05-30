"""Publish live progress events from Kitaru checkpoints.

The checkpoint result is still the durable record. These live events are
best-effort updates for anything watching the active backend stream.
"""

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def prepare_outline(topic: str) -> list[str]:
    """Create a tiny outline and publish progress while it runs."""
    kitaru.progress("Choosing sections", percent=0.25, topic=topic)
    sections = ["context", "why it matters", "next step"]
    kitaru.events.publish(
        "report.outline.ready",
        {"section_count": len(sections)},
        message="Outline ready",
    )
    return sections


@checkpoint
def write_summary(topic: str, sections: list[str]) -> str:
    """Turn the outline into a short summary."""
    kitaru.progress("Writing summary", percent=0.75, sections=len(sections))
    return f"{topic}: " + "; ".join(sections)


@flow
def streaming_demo(topic: str) -> str:
    """Run a small flow that emits checkpoint live events."""
    sections = prepare_outline(topic)
    return write_summary(topic, sections)


def run_workflow(topic: str = "checkpoint live events") -> str:
    """Execute the example workflow and return the final summary."""
    return streaming_demo.run(topic).wait()


def main() -> None:
    """Run the example as a script."""
    print(run_workflow())


if __name__ == "__main__":
    main()
