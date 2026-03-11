"""Integration test for the Phase 20 Monty sandbox example workflow."""

from __future__ import annotations

import pytest
from examples.monty_sandbox import run_workflow

from kitaru.errors import KitaruFeatureNotAvailableError
from kitaru.wait import _resolve_zenml_wait


def test_phase20_sandbox_example_runs_end_to_end() -> None:
    """Verify sandbox state survives a wait/resume boundary in the example."""
    pytest.importorskip("pydantic_monty")

    try:
        _resolve_zenml_wait()
    except KitaruFeatureNotAvailableError:
        pytest.skip("Installed ZenML build does not expose wait support yet.")

    execution_id, status_after_input, result = run_workflow(topic="kitaru")

    assert execution_id
    assert status_after_input in {"running", "waiting", "completed"}
    assert result["approved"] is True
    assert result["before_wait"]["counter"] == 40
    assert result["after_wait"] is not None
    assert result["after_wait"]["counter"] == 42
    assert result["before_wait"]["draft"] == result["after_wait"]["draft"]
