"""Integration test for the Phase 15 wait/resume example workflow."""

from __future__ import annotations

import pytest
from examples.execution_management.wait_and_resume import run_workflow

from kitaru.errors import KitaruFeatureNotAvailableError
from kitaru.wait import _resolve_zenml_wait


def test_phase15_wait_example_runs_end_to_end() -> None:
    """Verify wait input resumes the same execution and produces output."""
    try:
        _resolve_zenml_wait()
    except KitaruFeatureNotAvailableError:
        pytest.skip("Installed ZenML build does not expose wait support yet.")

    execution_id, status_after_input, result = run_workflow(topic="kitaru")

    assert execution_id
    assert status_after_input in {"running", "waiting", "completed"}
    assert result == "PUBLISHED: Draft about kitaru."
