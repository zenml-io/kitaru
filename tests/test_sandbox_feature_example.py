"""Integration test for the active-stack sandbox command flow example."""

from examples.features.sandbox.active_stack_sandbox_command import (
    format_result,
    run_workflow,
)

import kitaru
from kitaru.client import KitaruClient


def test_active_stack_sandbox_command_flow_runs_end_to_end(
    primed_zenml: None,
) -> None:
    """Create a local stack, run the public example, and verify stable output."""
    del primed_zenml

    stack_name = "sandbox-example-test"
    kitaru.create_stack(stack_name)

    result = run_workflow()

    assert result.stdout == "kitaru sandbox ready\n"
    assert result.stderr == ""
    assert result.exit_code == 0
    assert result.stack_name == stack_name
    assert result.cleanup == "destroy"
    assert result.cleanup_succeeded is True

    executions = KitaruClient().executions.list(
        flow="sandbox_command_flow",
        status="completed",
        limit=5,
    )
    assert executions

    output = format_result(result)

    assert output == "\n".join(
        [
            "Sandbox command flow result:",
            "stdout: kitaru sandbox ready",
            "exit_code: 0",
            f"stack_name: {stack_name}",
            f"sandbox_name: {result.sandbox_name or '<not reported>'}",
            "cleanup: destroy",
            "cleanup_succeeded: True",
        ]
    )
    assert result.stack_id not in output
    assert result.sandbox_id is None or result.sandbox_id not in output
    assert result.session_id is None or result.session_id not in output
