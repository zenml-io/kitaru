"""Smallest end-to-end ``@flow`` + ``@checkpoint`` example.

Two checkpoints, one flow — the minimum needed to get durable execution.
If this flow crashes after ``gather_sources``, a replay skips it and
resumes from ``summarize``.
"""

from kitaru import checkpoint, flow


@checkpoint
def gather_sources(topic: str) -> str:
    """Collect raw material for a topic."""
    return f"Source notes on {topic}: key trends, recent breakthroughs, open questions."


@checkpoint
def summarize(notes: str) -> str:
    """Distill raw notes into a one-line summary."""
    return f"Summary: {notes.split(':')[0].lower()} are evolving rapidly."


@flow
def research_agent(topic: str) -> str:
    """Gather sources and summarize — the smallest durable agent."""
    notes = gather_sources(topic)
    return summarize(notes)


def run_workflow(topic: str = "renewable energy") -> str:
    """Execute the workflow and return its output."""
    return research_agent.run(topic).wait()


def main() -> None:
    """Run the example as a script."""
    result = run_workflow()
    print(result)


if __name__ == "__main__":
    main()
