"""Phase 16 example: replay a flow with checkpoint overrides.

This example demonstrates:
- running an execution end-to-end
- replaying from a checkpoint boundary
- overriding a prior checkpoint outcome via `checkpoint.*`
- inspecting replay output artifacts through `KitaruClient`
"""

import time

import kitaru
from kitaru import checkpoint, flow
from kitaru.client import Execution, ExecutionStatus


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


# TODO: remove explicit base_image once kitaru and ZenML
# feature/kitaru are released to PyPI — the auto-injection
# of kitaru into Docker requirements will handle this automatically.
# Build the dev image with: just dev-image
@flow(image={"base_image": "strickvl/kitaru-dev:latest"})
def content_pipeline(topic: str) -> str:
    """Simple durable content pipeline used for replay demonstration."""
    notes = research(topic)
    draft = write_draft(notes)
    return publish(draft)


def _wait_for_terminal_execution(
    *,
    client: kitaru.KitaruClient,
    exec_id: str,
    timeout_seconds: int = 120,
) -> Execution:
    """Poll execution state until it reaches a terminal status."""
    deadline = time.time() + timeout_seconds
    while time.time() <= deadline:
        execution = client.executions.get(exec_id)
        if execution.status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            return execution
        time.sleep(1)

    raise TimeoutError(
        f"Replay execution '{exec_id}' did not finish within {timeout_seconds}s."
    )


def _published_output(execution: Execution) -> str:
    """Extract publish output from replayed execution artifacts."""
    publish_checkpoint = next(
        (
            checkpoint
            for checkpoint in execution.checkpoints
            if checkpoint.name == "publish"
        ),
        None,
    )
    if publish_checkpoint is None:
        raise RuntimeError("Replay output checkpoint 'publish' was not found.")
    if not publish_checkpoint.artifacts:
        raise RuntimeError("Replay output checkpoint has no artifacts to load.")

    return str(publish_checkpoint.artifacts[0].load())


def _print_phase(title: str, *, enabled: bool) -> None:
    """Print a short phase header when verbose mode is enabled."""
    if enabled:
        print(f"\n=== {title} ===")


def _print_step(message: str, *, enabled: bool) -> None:
    """Print one progress line when verbose mode is enabled."""
    if enabled:
        print(f"- {message}")


def run_workflow(
    topic: str = "kitaru",
    *,
    verbose: bool = False,
) -> tuple[str, str, str, str]:
    """Run the replay example workflow end-to-end.

    Returns:
        Tuple: `(source_exec_id, replay_exec_id, original_result, replay_output)`.
    """
    _print_phase("Phase 1 · Run original execution", enabled=verbose)
    _print_step(f"1) Start flow with topic={topic!r}", enabled=verbose)
    source_handle = content_pipeline.run(topic)
    _print_step(
        f"2) Source execution ID: {source_handle.exec_id}",
        enabled=verbose,
    )
    _print_step("3) Wait for source execution to finish", enabled=verbose)
    original_result = source_handle.wait()
    _print_step(f"4) Source result: {original_result}", enabled=verbose)

    edited_notes = f"edited notes for {topic}"
    _print_phase("Phase 2 · Replay with override", enabled=verbose)
    _print_step("5) Replay selector: from_='write_draft'", enabled=verbose)
    _print_step(
        f"6) Override: checkpoint.research={edited_notes!r}",
        enabled=verbose,
    )

    client = kitaru.KitaruClient()
    replayed = client.executions.replay(
        source_handle.exec_id,
        from_="write_draft",
        overrides={"checkpoint.research": edited_notes},
    )
    _print_step(f"7) Replay execution ID: {replayed.exec_id}", enabled=verbose)
    _print_step("8) Wait for replay execution to finish", enabled=verbose)

    terminal = _wait_for_terminal_execution(
        client=client,
        exec_id=replayed.exec_id,
    )
    _print_phase("Phase 3 · Inspect replay outcome", enabled=verbose)
    _print_step(
        f"9) Replay terminal status: {terminal.status.value}",
        enabled=verbose,
    )
    if terminal.status != ExecutionStatus.COMPLETED:
        raise RuntimeError(
            f"Replay execution '{terminal.exec_id}' finished with "
            f"status '{terminal.status.value}'."
        )

    replay_output = _published_output(terminal)
    _print_step(f"10) Replay output: {replay_output}", enabled=verbose)

    return source_handle.exec_id, terminal.exec_id, original_result, replay_output


def main() -> None:
    """Run the example as a script."""
    source_exec_id, replay_exec_id, original_result, replay_output = run_workflow(
        verbose=True
    )
    print("\n--- Final summary ---")
    print(f"Source execution: {source_exec_id}")
    print(f"Replay execution: {replay_exec_id}")
    print(f"Original result: {original_result}")
    print(f"Replay output: {replay_output}")


if __name__ == "__main__":
    main()
