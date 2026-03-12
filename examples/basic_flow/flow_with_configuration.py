"""Phase 10 example: runtime configuration and precedence resolution.

This example demonstrates:
- process-local defaults via ``kitaru.configure(...)``
- flow-level defaults via ``@flow(...)``
- invocation-time overrides via ``.run(..., retries=...)``
- frozen execution spec persistence on the resulting run metadata
"""

from typing import Any, cast

from zenml.client import Client

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def draft(topic: str) -> str:
    """Generate a simple draft output.

    Args:
        topic: Topic to process.

    Returns:
        Upper-cased draft output.
    """
    return f"draft:{topic}".upper()


@flow(cache=True, retries=2)
def configured_flow(topic: str) -> str:
    """Run a configured flow.

    Args:
        topic: Topic to process.

    Returns:
        Draft output.
    """
    return draft(topic)


def run_workflow(topic: str = "kitaru") -> tuple[str, str, dict[str, Any]]:
    """Run the Phase 10 configuration workflow.

    Args:
        topic: Topic to process.

    Returns:
        Tuple of ``(execution_id, result, frozen_execution_spec)``.
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

    run = Client().get_pipeline_run(
        handle.exec_id,
        allow_name_prefix_match=False,
    )
    hydrated_run = run.get_hydrated_version()
    raw_frozen_execution_spec = hydrated_run.run_metadata["kitaru_execution_spec"]
    if not isinstance(raw_frozen_execution_spec, dict):
        raise RuntimeError(
            "Expected run metadata key 'kitaru_execution_spec' to contain a dict."
        )
    frozen_execution_spec = cast(dict[str, Any], raw_frozen_execution_spec)

    return handle.exec_id, result, frozen_execution_spec


def main() -> None:
    """Run the example as a script."""
    execution_id, result, frozen_execution_spec = run_workflow()
    print(f"Execution ID: {execution_id}")
    print(f"Result: {result}")
    print(f"Resolved retries: {frozen_execution_spec['resolved_execution']['retries']}")


if __name__ == "__main__":
    main()
