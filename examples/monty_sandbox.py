"""Phase 20 example: keep Monty sandbox state alive across wait/resume.

This example demonstrates an execution-scoped ``kitaru.sandbox()`` session
remembering Python state across a ``kitaru.wait()`` suspension/resume boundary.
It is intended as a practical local verification workflow before you open a PR
for the Monty sandbox feature.

Setup:

    uv sync --extra local --extra sandbox

Optional preflight:

    kitaru sandbox set monty
    kitaru sandbox test

Run in auto mode (single terminal):

    uv run python -m examples.monty_sandbox --mode auto

Run in interactive mode (two terminals):

    uv run python -m examples.monty_sandbox --mode interactive

Auto mode finds the pending wait and resolves it for you via ``KitaruClient``.
Interactive mode prints the exact ``kitaru executions input`` / ``resume``
commands you can run in another terminal.
"""

import argparse
import threading
import time
from contextlib import suppress
from typing import Any, TypedDict

import kitaru
from kitaru import checkpoint, flow
from kitaru.client import Execution, ExecutionStatus, KitaruClient

_WAIT_DISCOVERY_TIMEOUT_SECONDS = 900.0


class _StartState(TypedDict):
    """Shared state between the starter thread and the main thread."""

    handle: Any | None
    error: Exception | None


class _SandboxSnapshot(TypedDict):
    """User-visible values captured from one sandbox call."""

    counter: int
    draft: str


class _SandboxWorkflowResult(TypedDict):
    """Stable result payload returned by the sandbox example flow."""

    approved: bool
    before_wait: _SandboxSnapshot
    after_wait: _SandboxSnapshot | None


def _prime_zenml_runtime() -> None:
    """Force ZenML's lazy store initialization on the current thread."""
    from zenml.client import Client

    _ = Client().zen_store


@checkpoint
def finalize_sandbox_result(
    approved: bool,
    before_wait: dict[str, Any],
    after_wait: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return one explicit terminal artifact for the example result."""
    return {
        "approved": approved,
        "before_wait": before_wait,
        "after_wait": after_wait,
    }


@flow(
    sandbox={
        "provider": "monty",
        "monty": {"max_duration_secs": 5.0, "max_memory_mb": 128},
    }
)
def monty_sandbox_flow(topic: str) -> dict[str, Any]:
    """Run stateful sandbox code before and after a durable wait boundary."""
    session = kitaru.sandbox(name="scratchpad")
    before_wait = session.run_code(
        """
counter = 40
draft = f"Draft for {topic}"
{
    "counter": counter,
    "draft": draft,
}
""".strip(),
        inputs={"topic": topic},
        name="before_wait",
    )

    approved = kitaru.wait(
        schema=bool,
        name="approve_sandbox_resume",
        question=f"Resume the sandbox workflow for {topic}?",
        metadata={"topic": topic},
    )
    if not approved:
        return finalize_sandbox_result(
            approved=False,
            before_wait=before_wait.value,
            after_wait=None,
        )

    after_wait = session.run_code(
        """
counter += 2
{
    "counter": counter,
    "draft": draft,
}
""".strip(),
        name="after_wait",
    )
    return finalize_sandbox_result(
        approved=True,
        before_wait=before_wait.value,
        after_wait=after_wait.value,
    )


def _coerce_snapshot(value: Any, *, label: str) -> _SandboxSnapshot:
    """Validate one sandbox snapshot and normalize it into a stable shape."""
    if not isinstance(value, dict):
        raise RuntimeError(
            f"Expected {label} to be a dict, got {type(value).__name__}."
        )

    counter = value.get("counter")
    draft = value.get("draft")
    if not isinstance(counter, int):
        raise RuntimeError(f"Expected {label}['counter'] to be an int.")
    if not isinstance(draft, str):
        raise RuntimeError(f"Expected {label}['draft'] to be a str.")

    return {
        "counter": counter,
        "draft": draft,
    }


def _coerce_workflow_result(value: Any) -> _SandboxWorkflowResult:
    """Validate the flow result before printing or asserting on it."""
    if not isinstance(value, dict):
        raise RuntimeError(
            "Expected monty_sandbox_flow() to return a dict result payload."
        )

    approved = value.get("approved")
    if not isinstance(approved, bool):
        raise RuntimeError("Expected result['approved'] to be a bool.")

    before_wait = _coerce_snapshot(value.get("before_wait"), label="before_wait")
    after_wait_raw = value.get("after_wait")
    after_wait = None
    if after_wait_raw is not None:
        after_wait = _coerce_snapshot(after_wait_raw, label="after_wait")

    return {
        "approved": approved,
        "before_wait": before_wait,
        "after_wait": after_wait,
    }


def _find_pending_wait_for_topic(
    *,
    client: KitaruClient,
    topic: str,
) -> tuple[str, str] | None:
    """Return ``(exec_id, wait_id)`` for the matching topic metadata."""
    executions = client.executions.list(flow="monty_sandbox_flow", limit=20)
    for execution in executions:
        detailed_execution = client.executions.get(execution.exec_id)
        pending_wait = detailed_execution.pending_wait
        if pending_wait is None:
            continue
        if pending_wait.metadata.get("topic") != topic:
            continue
        return detailed_execution.exec_id, pending_wait.wait_id
    return None


def _watch_and_print_unblock_commands(
    *,
    client: KitaruClient,
    topic: str,
    stop_event: threading.Event,
) -> None:
    """Watch for a pending wait and print manual unblock commands once."""
    printed = False
    while not stop_event.is_set() and not printed:
        found = _find_pending_wait_for_topic(client=client, topic=topic)
        if found is not None:
            exec_id, wait_id = found
            print("\n⏸️  Flow is waiting and the sandbox session has been snapshotted.")
            print("Run these commands in another terminal to continue:\n")
            print(f"kitaru executions input {exec_id} --wait {wait_id} --value true")
            print(f"kitaru executions resume {exec_id}\n")
            print("(Use --value false to stop before the post-wait sandbox step.)\n")
            printed = True
            break
        time.sleep(1.0)


def _start_flow_in_background(topic: str) -> tuple[threading.Thread, _StartState]:
    """Start the flow in a background thread to avoid local ``run()`` blocking."""
    state: _StartState = {"handle": None, "error": None}

    def _runner() -> None:
        try:
            state["handle"] = monty_sandbox_flow.run(topic, cache=False)
        except Exception as exc:  # pragma: no cover - surfaced via state
            state["error"] = exc

    thread = threading.Thread(
        target=_runner,
        name="kitaru-monty-sandbox-example",
        daemon=True,
    )
    thread.start()
    return thread, state


def _wait_for_pending_wait(
    *,
    client: KitaruClient,
    topic: str,
    start_state: _StartState,
    timeout_seconds: float = _WAIT_DISCOVERY_TIMEOUT_SECONDS,
) -> tuple[str, str]:
    """Wait until this run reaches waiting state and return ``(exec_id, wait_id)``."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        start_error = start_state["error"]
        if start_error is not None:
            raise RuntimeError(
                "Flow run failed before reaching a wait condition."
            ) from start_error

        found = _find_pending_wait_for_topic(client=client, topic=topic)
        if found is not None:
            return found

        time.sleep(0.5)

    raise TimeoutError(
        "Timed out waiting for execution to reach a pending wait after "
        f"{timeout_seconds:.0f}s. On remote stacks, first-run image builds can "
        "take several minutes before the flow reaches wait()."
    )


def _wait_for_terminal_execution(
    *,
    client: KitaruClient,
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
        f"Execution {exec_id!r} did not finish within {timeout_seconds}s."
    )


def _result_from_execution(execution: Execution) -> _SandboxWorkflowResult:
    """Load the final example result from the explicit terminal checkpoint."""
    finalize_checkpoint = next(
        (
            checkpoint
            for checkpoint in execution.checkpoints
            if checkpoint.name == "finalize_sandbox_result"
        ),
        None,
    )
    if finalize_checkpoint is None:
        raise RuntimeError(
            "Expected checkpoint 'finalize_sandbox_result' was not found."
        )
    if not finalize_checkpoint.artifacts:
        raise RuntimeError(
            "The finalize_sandbox_result checkpoint has no artifacts to load."
        )
    return _coerce_workflow_result(finalize_checkpoint.artifacts[0].load())


def run_workflow(
    topic: str | None = None,
    *,
    approve: bool = True,
    wait_discovery_timeout_seconds: float = _WAIT_DISCOVERY_TIMEOUT_SECONDS,
) -> tuple[str, str, _SandboxWorkflowResult]:
    """Run the sandbox example and auto-resolve its pending wait via the client."""
    resolved_topic = topic or f"monty-sandbox-{int(time.time())}"
    client = KitaruClient()
    _prime_zenml_runtime()

    starter_thread, start_state = _start_flow_in_background(resolved_topic)
    exec_id, wait_id = _wait_for_pending_wait(
        client=client,
        topic=resolved_topic,
        start_state=start_state,
        timeout_seconds=wait_discovery_timeout_seconds,
    )

    execution_after_input = client.executions.input(
        exec_id,
        wait=wait_id,
        value=approve,
    )

    with suppress(kitaru.KitaruStateError):
        client.executions.resume(exec_id)

    starter_thread.join(timeout=60.0)
    if starter_thread.is_alive():
        raise TimeoutError(
            "Timed out waiting for the background flow run call to finish."
        )
    if start_state["error"] is not None:
        raise RuntimeError("Background flow start failed.") from start_state["error"]

    execution = _wait_for_terminal_execution(client=client, exec_id=exec_id)
    if execution.status is not ExecutionStatus.COMPLETED:
        raise RuntimeError(
            f"Execution {exec_id} finished with status {execution.status.value!r}."
        )

    result = _result_from_execution(execution)
    return exec_id, execution_after_input.status.value, result


def run_workflow_interactive(
    topic: str | None = None,
) -> tuple[str, _SandboxWorkflowResult]:
    """Run the workflow in the main thread and print manual unblock commands."""
    resolved_topic = topic or f"monty-sandbox-{int(time.time())}"
    client = KitaruClient()
    _prime_zenml_runtime()
    stop_event = threading.Event()

    watcher = threading.Thread(
        target=_watch_and_print_unblock_commands,
        kwargs={
            "client": client,
            "topic": resolved_topic,
            "stop_event": stop_event,
        },
        name="kitaru-monty-sandbox-watcher",
        daemon=True,
    )
    watcher.start()

    print("Starting interactive sandbox wait/resume workflow...")
    print("Keep this terminal open; execute the printed commands in another terminal.")

    handle = monty_sandbox_flow.run(resolved_topic, cache=False)
    print(f"Execution ID: {handle.exec_id}")

    try:
        execution = _wait_for_terminal_execution(client=client, exec_id=handle.exec_id)
        if execution.status is not ExecutionStatus.COMPLETED:
            raise RuntimeError(
                "Execution "
                f"{handle.exec_id} finished with status "
                f"{execution.status.value!r}."
            )
        result = _result_from_execution(execution)
    finally:
        stop_event.set()
        watcher.join(timeout=2.0)

    return handle.exec_id, result


def _print_result_summary(
    *,
    execution_id: str,
    result: _SandboxWorkflowResult,
    status_after_input: str | None = None,
) -> None:
    """Render the example result as a short story-like summary."""
    print(f"Execution ID: {execution_id}")
    if status_after_input is not None:
        print(f"Status after input: {status_after_input}")
    print(f"Approved: {result['approved']}")
    print(f"Before wait counter: {result['before_wait']['counter']}")

    after_wait = result["after_wait"]
    if after_wait is None:
        print("After wait counter: not resumed")
        print(f"Remembered draft before wait: {result['before_wait']['draft']}")
        return

    print(f"After wait counter: {after_wait['counter']}")
    print(f"Remembered draft: {after_wait['draft']}")


def _build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for example execution modes."""
    parser = argparse.ArgumentParser(
        description="Monty sandbox wait/resume workflow example."
    )
    parser.add_argument(
        "--mode",
        choices=["interactive", "auto"],
        default="auto",
        help="auto resolves the wait itself; interactive prints manual commands.",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Optional topic label. Defaults to a timestamp-based unique topic.",
    )
    parser.add_argument(
        "--approve",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Auto-mode approval value (`--approve` / `--no-approve`).",
    )
    parser.add_argument(
        "--wait-timeout",
        type=float,
        default=_WAIT_DISCOVERY_TIMEOUT_SECONDS,
        help="Auto-mode wait-discovery timeout in seconds.",
    )
    return parser


def main() -> None:
    """Run the example as a script."""
    args = _build_parser().parse_args()

    if args.mode == "interactive":
        execution_id, result = run_workflow_interactive(topic=args.topic)
        _print_result_summary(execution_id=execution_id, result=result)
        return

    execution_id, status_after_input, result = run_workflow(
        topic=args.topic,
        approve=args.approve,
        wait_discovery_timeout_seconds=args.wait_timeout,
    )
    _print_result_summary(
        execution_id=execution_id,
        status_after_input=status_after_input,
        result=result,
    )


if __name__ == "__main__":
    main()
