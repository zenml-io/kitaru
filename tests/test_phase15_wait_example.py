"""Integration test for the wait/resume example workflow.

The example itself is manual (prints CLI commands for a second terminal).
This test drives the same flow and APIs programmatically so CI can validate
the full wait → input → resume → result sequence without human interaction.
"""

from __future__ import annotations

import threading
import time
from contextlib import suppress

import pytest
from examples.execution_management.wait_and_resume import (
    _find_pending_wait_for_topic,
    _prime_zenml_runtime,
    wait_for_approval_flow,
)

from kitaru.client import KitaruClient
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruStateError
from kitaru.wait import _resolve_zenml_wait

_WAIT_DISCOVERY_TIMEOUT_SECONDS = 900.0


def test_phase15_wait_example_runs_end_to_end(primed_zenml) -> None:
    """Verify wait input resumes the same execution and produces output."""
    try:
        _resolve_zenml_wait()
    except KitaruFeatureNotAvailableError:
        pytest.skip("Installed ZenML build does not expose wait support yet.")

    topic = "kitaru"
    client = KitaruClient()
    _prime_zenml_runtime()

    # Start the flow in a background thread (the example blocks the main
    # thread, but we need the main thread free to drive input/resume).
    state: dict[str, object] = {"handle": None, "error": None}

    def _runner() -> None:
        try:
            state["handle"] = wait_for_approval_flow.run(topic, cache=False)
        except Exception as exc:
            state["error"] = exc

    starter = threading.Thread(target=_runner, name="test-wait-starter", daemon=True)
    starter.start()

    # Poll until the flow reaches its pending wait.
    deadline = time.time() + _WAIT_DISCOVERY_TIMEOUT_SECONDS
    found = None
    while time.time() < deadline:
        if state["error"] is not None:
            raise RuntimeError(
                "Flow run failed before reaching a wait condition."
            ) from state["error"]
        found = _find_pending_wait_for_topic(client=client, topic=topic)
        if found is not None:
            break
        time.sleep(0.5)

    if found is None:
        raise TimeoutError(
            f"Timed out after {_WAIT_DISCOVERY_TIMEOUT_SECONDS:.0f}s waiting for "
            "execution to reach a pending wait. On remote stacks, first-run "
            "image builds can take several minutes before the flow reaches wait()."
        )

    exec_id, wait_id = found

    # Resolve the wait with approval.
    execution_after_input = client.executions.input(
        exec_id,
        wait=wait_id,
        value=True,
    )

    # Some backends auto-resume after input; others need an explicit resume.
    with suppress(KitaruStateError):
        client.executions.resume(exec_id)

    starter.join(timeout=60.0)
    assert not starter.is_alive(), "Background flow-start thread did not finish."
    assert state["error"] is None, f"Background flow start failed: {state['error']}"

    handle = state["handle"]
    assert handle is not None, "Flow handle was not captured from background run."

    result = handle.wait()
    assert execution_after_input.status.value in {"running", "waiting", "completed"}
    assert result == "PUBLISHED: Draft about kitaru."
