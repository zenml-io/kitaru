"""Runtime configuration and precedence resolution.

This example demonstrates:
- process-local defaults via ``kitaru.configure(...)``
- flow-level defaults via ``@flow(...)``
- invocation-time overrides via ``.run(..., retries=...)``
"""

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def draft(topic: str) -> str:
    """Generate a simple draft output."""
    return f"draft:{topic}".upper()


@flow(cache=True, retries=2)
def configured_flow(topic: str) -> str:
    """Run a configured flow."""
    return draft(topic)


def run_workflow(topic: str = "kitaru") -> tuple[str, str]:
    """Run the configuration workflow.

    Returns:
        Tuple of (execution_id, result).
    """
    kitaru.configure(
        cache=False,
        retries=1,
        image=kitaru.ImageSettings(
            base_image="python:3.12-slim",
            environment={"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
        ),
    )

    handle = configured_flow.run(topic, retries=3)
    result = handle.wait()
    return handle.exec_id, result


def main() -> None:
    """Run the example as a script."""
    execution_id, result = run_workflow()
    print(f"Execution ID: {execution_id}")
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
