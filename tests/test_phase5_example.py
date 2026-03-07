"""Integration test for the Phase 5 first working workflow example."""

from __future__ import annotations

from examples.first_working_flow import run_workflow


def test_phase5_first_working_example_runs_end_to_end() -> None:
    """Verify the first end-to-end example executes successfully."""
    result = run_workflow("https://example.com")
    assert result == "SOME DATA"
