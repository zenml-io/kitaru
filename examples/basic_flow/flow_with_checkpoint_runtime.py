"""Checkpoint runtime selection with concurrent fan-out.

This example demonstrates:
- ``@checkpoint(runtime="isolated")`` to request isolated container execution
- ``.submit()`` for concurrent fan-out (returns ``KitaruStepFuture``)
- ``.result()`` to collect ordered results from each future

When run on a remote orchestrator that supports isolated steps (Kubernetes,
Vertex, SageMaker, AzureML), each submitted checkpoint runs in its own
container.  Locally, the runtime hint is ignored and checkpoints execute
inline via threads — so this example works anywhere.
"""

import time

from kitaru import checkpoint, flow


@checkpoint(runtime="isolated")
def transform_item(item: str) -> str:
    """Transform a single item in an isolated checkpoint.

    Args:
        item: Input string.

    Returns:
        Uppercased and tagged result.
    """
    return f"[processed] {item.upper()}"


@flow
def parallel_transform(items: list[str]) -> list[str]:
    """Fan out item transformations concurrently.

    Each ``transform_item`` invocation is submitted as an independent
    checkpoint.  On a stack with isolated-step support, every submission
    runs in its own container.

    Args:
        items: Strings to transform.

    Returns:
        Ordered list of transformed results.
    """
    futures = [transform_item.submit(item) for item in items]
    return [f.result() for f in futures]


def run_workflow(items: list[str] | None = None) -> str:
    """Execute the parallel-transform workflow.

    Args:
        items: Items to process.  Defaults to a small sample list.

    Returns:
        The execution ID.
    """
    if items is None:
        items = ["alpha", "bravo", "charlie"]

    handle = parallel_transform.run(items)
    while not handle.status.is_finished:
        time.sleep(1)
    return handle.exec_id


def main() -> None:
    """Run the example as a script."""
    execution_id = run_workflow()
    print(f"Execution ID: {execution_id}")


if __name__ == "__main__":
    main()
