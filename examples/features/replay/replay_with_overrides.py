"""Replay a flow from a checkpoint boundary with overrides.

This example demonstrates:
- running an execution end-to-end
- replaying from a later checkpoint with a modified input
- inspecting how the downstream output changes
"""

import time

import kitaru
from kitaru import checkpoint, flow
from kitaru.client import ExecutionStatus


@checkpoint
def research(topic: str) -> str:
    """Generate research notes for a topic."""
    return f"notes about {topic}"


@checkpoint
def write_draft(research_notes: str) -> str:
    """Create a draft from research notes."""
    return f"Draft from {research_notes}"


@checkpoint
def publish(draft: str) -> str:
    """Produce the final published string."""
    return f"PUBLISHED: {draft}"


@flow
def content_pipeline(topic: str) -> str:
    """Simple durable content pipeline used for replay demonstration."""
    notes = research(topic)
    draft = write_draft(notes)
    return publish(draft)


def run_workflow(topic: str = "kitaru") -> tuple[str, str, str, str]:
    """Run original execution, then replay with an override.

    Returns:
        Tuple of (source_exec_id, replay_exec_id, original_result, replay_output).
    """
    source_handle = content_pipeline.run(topic)
    original_result = source_handle.wait()
    print(f"Original result: {original_result}")

    # Replay from write_draft, swapping the research checkpoint's output.
    client = kitaru.KitaruClient()
    edited_notes = f"edited notes for {topic}"
    replayed = client.executions.replay(
        source_handle.exec_id,
        from_="write_draft",
        overrides={"checkpoint.research": edited_notes},
    )
    print(f"Replay execution started: {replayed.exec_id}")

    # Poll until the replay execution reaches a terminal state.
    deadline = time.time() + 120
    while time.time() <= deadline:
        execution = client.executions.get(replayed.exec_id)
        if execution.status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            break
        time.sleep(1)
    else:
        raise TimeoutError("Replay execution did not finish within 120s.")

    if execution.status != ExecutionStatus.COMPLETED:
        raise RuntimeError(f"Replay finished with status '{execution.status.value}'.")

    # Load the replayed output from the publish checkpoint.
    publish_cp = next(cp for cp in execution.checkpoints if cp.name == "publish")
    replay_output = str(publish_cp.artifacts[0].load())
    print(f"Replay output:  {replay_output}")

    return source_handle.exec_id, replayed.exec_id, original_result, replay_output


def main() -> None:
    """Run the example as a script."""
    source_id, replay_id, _, _ = run_workflow()
    print(f"\nSource execution:  {source_id}")
    print(f"Replay execution:  {replay_id}")


if __name__ == "__main__":
    main()
