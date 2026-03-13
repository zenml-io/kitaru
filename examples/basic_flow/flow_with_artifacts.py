"""Explicit artifact save/load across executions.

This example demonstrates:
- implicit checkpoint output reuse via ``kitaru.load(exec_id, "research")``
- explicit named artifact reuse via ``kitaru.save(...)`` / ``kitaru.load(...)``
"""

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def research(topic: str) -> str:
    """Generate notes and persist extra research context.

    Args:
        topic: Topic to investigate.

    Returns:
        Primary notes output.
    """
    notes = f"Research notes about {topic}."
    kitaru.save(
        "research_context",
        {"topic": topic, "notes": notes},
        type="context",
    )
    return notes


@flow
def first_pass(topic: str) -> str:
    """Run the first pass that produces reusable artifacts.

    Args:
        topic: Topic to investigate.

    Returns:
        Research notes from the first pass.
    """
    return research(topic)


@checkpoint
def follow_up_from_previous(prev_exec_id: str) -> str:
    """Build follow-up output by loading artifacts from a prior execution.

    Args:
        prev_exec_id: Previous execution ID.

    Returns:
        Follow-up text generated from loaded artifacts.
    """
    previous_notes = kitaru.load(prev_exec_id, "research")
    saved_context = kitaru.load(prev_exec_id, "research_context")
    return f"{previous_notes} [topic={saved_context['topic']}]"


@flow
def second_pass(prev_exec_id: str) -> str:
    """Run a second pass that reads artifacts from the first pass.

    Args:
        prev_exec_id: Previous execution ID.

    Returns:
        Follow-up text.
    """
    return follow_up_from_previous(prev_exec_id)


def run_workflow(topic: str = "kitaru") -> tuple[str, str, str]:
    """Execute both passes and return their key outputs.

    Args:
        topic: Topic to investigate.

    Returns:
        Tuple of `(first_exec_id, first_result, second_result)`.
    """
    first_handle = first_pass.run(topic)
    first_result = first_handle.wait()
    second_result = second_pass.run(first_handle.exec_id).wait()
    return first_handle.exec_id, first_result, second_result


def main() -> None:
    """Run the example as a script."""
    exec_id, first_result, second_result = run_workflow()
    print(f"First execution: {exec_id}")
    print(first_result)
    print(second_result)


if __name__ == "__main__":
    main()
