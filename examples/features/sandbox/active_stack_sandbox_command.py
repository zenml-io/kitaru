"""Run one command through the active stack's sandbox inside a Kitaru flow.

This example demonstrates the tracked execution path for
``kitaru.run_sandbox_command(...)``: a flow starts, a checkpoint runs, and that
checkpoint asks the active stack's sandbox component to execute one command.

The example does not create or switch stacks. It uses whichever Kitaru stack is
currently active, and that stack must have exactly one sandbox component.
"""

import kitaru
from kitaru import SandboxCommandResult, checkpoint, flow

DEMO_COMMAND = ["python", "-c", "print('kitaru sandbox ready')"]
DEMO_MAX_CHARS = 2_000


@checkpoint
def inspect_active_sandbox() -> SandboxCommandResult:
    """Run a harmless deterministic command in the active stack's sandbox."""
    return kitaru.run_sandbox_command(
        DEMO_COMMAND,
        max_chars=DEMO_MAX_CHARS,
        cleanup="destroy",
    )


@flow
def sandbox_command_flow() -> SandboxCommandResult:
    """Track one sandbox command as part of a Kitaru flow execution."""
    return inspect_active_sandbox()


def run_workflow() -> SandboxCommandResult:
    """Execute the tracked sandbox flow and return its final result."""
    return sandbox_command_flow.run().wait()


def format_result(result: SandboxCommandResult) -> str:
    """Format the stable user-facing result fields.

    Random runtime IDs are intentionally omitted so the example output is safe
    to compare in tests and easy for users to read.
    """
    sandbox_name = result.sandbox_name or "<not reported>"
    return "\n".join(
        [
            "Sandbox command flow result:",
            f"stdout: {result.stdout.strip()}",
            f"exit_code: {result.exit_code}",
            f"stack_name: {result.stack_name}",
            f"sandbox_name: {sandbox_name}",
            f"cleanup: {result.cleanup}",
            f"cleanup_succeeded: {result.cleanup_succeeded}",
        ]
    )


def main() -> None:
    """Run the example as a script."""
    result = run_workflow()
    print(format_result(result))
    if result.exit_code != 0 or not result.cleanup_succeeded:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
