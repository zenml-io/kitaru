"""Phase 15 example: wait for external input and resume the same execution.

This module supports two usage modes:

- interactive (default): run the flow in the main thread and print exact CLI
  commands you can run in another terminal to resolve the wait.
- auto: start the flow in a background thread and auto-resolve wait input via
  the Python client API.
"""

import argparse
import threading
import time
from contextlib import suppress
from typing import Any, TypedDict

import kitaru
from kitaru import checkpoint, flow
from kitaru.client import KitaruClient

_WAIT_DISCOVERY_TIMEOUT_SECONDS = 900.0


def _prime_zenml_runtime() -> None:
    """Force ZenML's lazy store initialization on the current thread.

    ZenML's ``GlobalConfiguration().zen_store`` is lazy and not thread-safe.
    When two threads first access it concurrently they race on SQLite table
    creation (``CREATE TABLE … already exists``) or alembic stamping.  Calling
    this once on the main thread before spawning worker threads eliminates the
    race entirely.
    """
    from zenml.client import Client

    _ = Client().zen_store


class _StartState(TypedDict):
    """Shared state between the starter thread and the main thread."""

    handle: Any | None
    error: Exception | None


@checkpoint
def draft_release_note(topic: str) -> str:
    """Create a draft release note for the requested topic."""
    return f"Draft about {topic}."


@checkpoint
def publish_release_note(draft: str) -> str:
    """Publish a previously approved draft release note."""
    return f"PUBLISHED: {draft}"


# TODO: remove explicit base_image once kitaru and ZenML
# feature/pause-pipeline-runs are released to PyPI — the auto-injection
# of kitaru into Docker requirements will handle this automatically.
# Build the dev image with: just dev-image
@flow(image={"base_image": "strickvl/kitaru-dev:latest"})
def wait_for_approval_flow(topic: str) -> str:
    """Gate publication behind a durable human-approval wait."""
    draft = draft_release_note(topic)
    approved = kitaru.wait(
        schema=bool,
        name="approve_release",
        question=f"Approve publishing release notes for {topic}?",
        metadata={"topic": topic},
    )
    if not approved:
        return f"REJECTED: {topic}"
    return publish_release_note(draft)


def _find_pending_wait_for_topic(
    *,
    client: KitaruClient,
    topic: str,
) -> tuple[str, str] | None:
    """Return (exec_id, wait_id) for the flow run that matches topic metadata."""
    executions = client.executions.list(flow="wait_for_approval_flow", limit=20)
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
    """Watch for a pending wait and print manual CLI unblock commands once."""
    printed = False
    while not stop_event.is_set() and not printed:
        found = _find_pending_wait_for_topic(client=client, topic=topic)
        if found is not None:
            exec_id, wait_id = found
            print("\n⏸️  Flow is waiting for external input.")
            print("Run these commands in another terminal to continue:\n")
            print(f"kitaru executions input {exec_id} --wait {wait_id} --value true")
            print(f"kitaru executions resume {exec_id}\n")
            print("(Use --value false to reject instead.)\n")
            printed = True
            break
        time.sleep(1.0)


def _start_flow_in_background(topic: str) -> tuple[threading.Thread, _StartState]:
    """Start the flow in a background thread to avoid local run() blocking."""
    state: _StartState = {"handle": None, "error": None}

    def _runner() -> None:
        try:
            state["handle"] = wait_for_approval_flow.run(topic, cache=False)
        except Exception as exc:  # pragma: no cover - surfaced via state
            state["error"] = exc

    thread = threading.Thread(target=_runner, name="kitaru-wait-example", daemon=True)
    thread.start()
    return thread, state


def _wait_for_pending_wait(
    *,
    client: KitaruClient,
    topic: str,
    start_state: _StartState,
    timeout_seconds: float = _WAIT_DISCOVERY_TIMEOUT_SECONDS,
) -> tuple[str, str]:
    """Wait until this run reaches waiting state and return (exec_id, wait_id)."""
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


def run_workflow(
    topic: str | None = None,
    *,
    approve: bool = True,
    wait_discovery_timeout_seconds: float = _WAIT_DISCOVERY_TIMEOUT_SECONDS,
) -> tuple[str, str, str]:
    """Run the wait example and resolve its pending input via the client API."""
    resolved_topic = topic or f"kitaru-{int(time.time())}"
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

    # Auto-resume backends can continue immediately after input resolution.
    # Manual-resume backends only succeed once the execution is actually paused
    # and ready to restart.
    with suppress(kitaru.KitaruStateError):
        client.executions.resume(exec_id)

    starter_thread.join(timeout=60.0)
    if starter_thread.is_alive():
        raise TimeoutError(
            "Timed out waiting for the background flow run call to finish."
        )
    if start_state["error"] is not None:
        raise RuntimeError("Background flow start failed.") from start_state["error"]

    handle = start_state["handle"]
    if handle is None:
        raise RuntimeError("Flow handle was not captured from background run.")

    result = handle.wait()
    if not isinstance(result, str):
        raise RuntimeError("Expected wait_for_approval_flow to return a string.")

    return exec_id, execution_after_input.status.value, result


def run_workflow_interactive(topic: str | None = None) -> str:
    """Run workflow in main thread and print manual unblock commands."""
    resolved_topic = topic or f"kitaru-{int(time.time())}"
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
        name="kitaru-wait-unblock-watcher",
        daemon=True,
    )
    watcher.start()

    print("Starting interactive wait/resume workflow...")
    print("Keep this terminal open; execute the printed commands in another terminal.")

    try:
        result = wait_for_approval_flow.run(resolved_topic).wait()
    finally:
        stop_event.set()
        watcher.join(timeout=2.0)

    if not isinstance(result, str):
        raise RuntimeError("Expected wait_for_approval_flow to return a string.")
    return result


def _build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for example execution modes."""
    parser = argparse.ArgumentParser(description="Wait/resume workflow example.")
    parser.add_argument(
        "--mode",
        choices=["interactive", "auto"],
        default="interactive",
        help="interactive prints manual unblock commands; auto resolves input itself.",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Optional topic label. Defaults to timestamp-based unique topic.",
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
        result = run_workflow_interactive(topic=args.topic)
        print(f"Result: {result}")
        return

    execution_id, status_after_input, result = run_workflow(
        topic=args.topic,
        approve=args.approve,
        wait_discovery_timeout_seconds=args.wait_timeout,
    )
    print(f"Execution ID: {execution_id}")
    print(f"Status after input: {status_after_input}")
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
