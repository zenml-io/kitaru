"""Integration test for the configuration example workflow."""

from __future__ import annotations

from typing import Any, cast

from examples.basic_flow.flow_with_configuration import run_workflow
from zenml.client import Client


def test_phase10_configuration_example_runs_end_to_end(primed_zenml) -> None:
    """Verify configuration precedence and frozen spec persistence."""
    execution_id, result = run_workflow("kitaru")

    assert execution_id
    assert result == "DRAFT:KITARU"

    # Verify frozen execution spec persisted on ZenML run metadata.
    run = Client().get_pipeline_run(execution_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()
    frozen_execution_spec = cast(
        dict[str, Any],
        hydrated_run.run_metadata["kitaru_execution_spec"],
    )

    assert frozen_execution_spec["version"] == 1

    resolved = frozen_execution_spec["resolved_execution"]
    assert resolved["stack"] == "default"
    assert resolved["cache"] is True
    assert resolved["retries"] == 3
    assert resolved["image"]["base_image"] == "python:3.12-slim"
    # Secret-looking env vars are redacted in the frozen spec.
    assert resolved["image"]["environment"] == {"OPENAI_API_KEY": "***"}
