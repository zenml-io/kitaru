"""Integration test for the wait/resume example workflow.

This test drives the same flow and APIs programmatically so CI can validate
the full wait -> input -> optional resume -> result sequence without human
interaction.

The example has two wait points:
1. A boolean gate ("approve_release") — resolved with True to approve
2. A structured input wait ("release_details") — resolved with a ReleaseDetails dict
"""

from __future__ import annotations

import threading
import time
from contextlib import suppress

import pytest
from examples.execution_management.wait_and_resume import wait_for_approval_flow

from kitaru.client import KitaruClient
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruStateError
from kitaru.wait import _resolve_zenml_wait

_WAIT_DISCOVERY_TIMEOUT_SECONDS = 900.0


def _find_pending_wait(*, client: KitaruClient, topic: str) -> str | None:
    """Return the exec_id for the flow run that has a pending wait matching topic."""
    executions = client.executions.list(flow="wait_for_approval_flow", limit=20)
    for execution in executions:
        detailed = client.executions.get(execution.exec_id)
        if detailed.pending_wait is None:
            continue
        if detailed.pending_wait.metadata.get("topic") != topic:
            continue
        return detailed.exec_id
    return None


def _wait_for_pending_wait(
    *,
    client: KitaruClient,
    topic: str,
    state: dict[str, object],
) -> str:
    """Poll until the flow reaches a pending wait, return exec_id."""
    deadline = time.time() + _WAIT_DISCOVERY_TIMEOUT_SECONDS
    found = None
    while time.time() < deadline:
        if state["error"] is not None:
            raise RuntimeError(
                "Flow run failed before reaching a wait condition."
            ) from state["error"]  # type: ignore[arg-type]
        try:
            found = _find_pending_wait(client=client, topic=topic)
        except ValueError:
            found = None
        if found is not None:
            return found
        time.sleep(0.5)

    raise TimeoutError(
        f"Timed out after {_WAIT_DISCOVERY_TIMEOUT_SECONDS:.0f}s waiting for "
        "execution to reach a pending wait. On remote stacks, first-run "
        "image builds can take several minutes before the flow reaches wait()."
    )


def test_phase15_wait_example_runs_end_to_end(primed_zenml) -> None:
    """Verify wait input resumes the same execution and produces output."""
    try:
        _resolve_zenml_wait()
    except KitaruFeatureNotAvailableError:
        pytest.skip("Installed ZenML build does not expose wait support yet.")

    topic = "v1.0"
    client = KitaruClient()

    state: dict[str, object] = {"handle": None, "error": None}

    def _runner() -> None:
        try:
            state["handle"] = wait_for_approval_flow.run(topic, cache=False)
        except Exception as exc:
            state["error"] = exc

    starter = threading.Thread(target=_runner, name="test-wait-starter", daemon=True)
    starter.start()

    try:
        # --- Wait 1: boolean approval gate ("approve_release") ---
        exec_id = _wait_for_pending_wait(client=client, topic=topic, state=state)

        pending = client.executions.pending_waits(exec_id)
        assert pending, f"No pending waits found for execution {exec_id}"
        client.executions.input(
            exec_id,
            wait=pending[0].wait_id,
            value=True,
        )

        with suppress(KitaruStateError):
            client.executions.resume(exec_id)

        # --- Wait 2: structured input ("release_details") ---
        exec_id = _wait_for_pending_wait(client=client, topic=topic, state=state)

        pending = client.executions.pending_waits(exec_id)
        assert pending, f"No pending waits found for execution {exec_id}"
        execution_after_input = client.executions.input(
            exec_id,
            wait=pending[0].wait_id,
            value={"notes": "Bug fixes", "major_version": 2},
        )

        with suppress(KitaruStateError):
            client.executions.resume(exec_id)
    finally:
        starter.join(timeout=60.0)

    assert not starter.is_alive(), "Background flow-start thread did not finish."
    assert state["error"] is None, f"Background flow start failed: {state['error']}"

    handle = state["handle"]
    assert handle is not None, "Flow handle was not captured from background run."

    result = handle.wait()
    assert execution_after_input.status.value in {"running", "waiting", "completed"}
    assert result == "PUBLISHED v2: Draft about v1.0.\nNotes: Bug fixes"
