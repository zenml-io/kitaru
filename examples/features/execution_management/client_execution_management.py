"""Inspect and browse executions with ``KitaruClient``.

This example demonstrates:
- starting a flow and waiting for completion
- reading execution details with `client.executions.get(...)`
- finding the latest execution for a flow
- browsing execution artifacts and loading an explicit saved artifact
"""

from typing import Any

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def write_summary(topic: str) -> str:
    """Produce a summary and persist related context.

    Args:
        topic: Topic to summarize.

    Returns:
        Summary text.
    """
    summary = f"Summary for {topic}."
    kitaru.save(
        "summary_context",
        {"topic": topic, "summary": summary},
        type="context",
    )
    return summary


@flow
def summarize_topic(topic: str) -> str:
    """Run the summary flow.

    Args:
        topic: Topic to summarize.

    Returns:
        Summary text.
    """
    return write_summary(topic)


def run_workflow(
    topic: str = "kitaru",
) -> tuple[str, str, str, list[str], dict[str, Any]]:
    """Run the client example workflow end-to-end.

    Args:
        topic: Topic to summarize.

    Returns:
        Tuple of `(execution_id, status, result, artifact_names, loaded_context)`.
    """
    handle = summarize_topic.run(topic)
    result = handle.wait()

    client = kitaru.KitaruClient()
    execution = client.executions.get(handle.exec_id)
    latest = client.executions.latest(flow="summarize_topic")
    artifacts = client.artifacts.list(handle.exec_id)

    context_artifact = next(
        artifact for artifact in artifacts if artifact.name == "summary_context"
    )
    loaded_context = context_artifact.load()

    if latest.exec_id != execution.exec_id:
        raise RuntimeError("Expected latest execution to match the just-started run.")

    artifact_names = [artifact.name for artifact in artifacts]
    return (
        execution.exec_id,
        execution.status.value,
        result,
        artifact_names,
        loaded_context,
    )


def main() -> None:
    """Run the example as a script."""
    execution_id, status, result, artifact_names, loaded_context = run_workflow()
    print(f"Execution ID: {execution_id}")
    print(f"Status: {status}")
    print(f"Result: {result}")
    print(f"Artifacts: {artifact_names}")
    print(f"Loaded context topic: {loaded_context['topic']}")


if __name__ == "__main__":
    main()
