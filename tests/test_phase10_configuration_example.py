"""Integration test for the Phase 10 configuration example workflow."""

from __future__ import annotations

from examples.flow_with_configuration import run_workflow


def test_phase10_configuration_example_runs_end_to_end() -> None:
    """Verify configuration precedence and frozen spec persistence."""
    execution_id, result, frozen_execution_spec = run_workflow("kitaru")

    assert execution_id
    assert result == "DRAFT:KITARU"
    assert frozen_execution_spec["version"] == 1

    resolved = frozen_execution_spec["resolved_execution"]
    assert resolved["stack"] == "local"
    assert resolved["cache"] is True
    assert resolved["retries"] == 3
    assert resolved["image"]["base_image"] == "python:3.12-slim"
    assert resolved["image"]["environment"] == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}
