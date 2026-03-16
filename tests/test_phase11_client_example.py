"""Integration test for the client execution example workflow."""

from __future__ import annotations

from examples.execution_management.client_execution_management import run_workflow


def test_phase11_client_example_runs_end_to_end(primed_zenml) -> None:
    """Verify execution browsing and artifact loading via KitaruClient."""
    execution_id, status, result, artifact_names, loaded_context = run_workflow(
        "kitaru"
    )

    assert execution_id
    assert status == "completed"
    assert result == "Summary for kitaru."
    assert "summary_context" in artifact_names
    assert loaded_context["topic"] == "kitaru"
    assert loaded_context["summary"] == "Summary for kitaru."
