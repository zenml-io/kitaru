"""Wait for external input and resume the same execution.

Run this example, then open a second terminal and execute the CLI commands
it prints to approve (or reject) the release note and resume the flow.
"""

import argparse
import threading
import time

import kitaru
from kitaru import checkpoint, flow
from kitaru.client import KitaruClient


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


def run_workflow(topic: str | None = None) -> str:
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

    print("Starting wait/resume workflow...")
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
    """Build CLI parser for the wait/resume example."""
    parser = argparse.ArgumentParser(
        description="Wait/resume workflow example — pauses for manual CLI input.",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Optional topic label. Defaults to timestamp-based unique topic.",
    )
    return parser


def main() -> None:
    """Run the example as a script."""
    args = _build_parser().parse_args()
    result = run_workflow(topic=args.topic)
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
