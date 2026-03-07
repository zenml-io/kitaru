"""Integration test for the Phase 7 logging example workflow."""

from __future__ import annotations

from typing import Any

from examples.flow_with_logging import writing_agent
from zenml.client import Client


def _find_step_metadata(
    *,
    metadata_by_step: list[dict[str, Any]],
    key: str,
) -> dict[str, Any]:
    """Return the first step metadata map containing a given key."""
    for step_metadata in metadata_by_step:
        if key in step_metadata:
            return step_metadata
    raise AssertionError(f"No step metadata contained key '{key}'.")


def test_phase7_logging_example_runs_end_to_end() -> None:
    """Verify the logging example executes and persists structured metadata."""
    handle = writing_agent.start("kitaru")
    result = handle.wait()

    assert result == "DRAFT ABOUT KITARU. [reviewed]"

    run = Client().get_pipeline_run(
        handle.exec_id,
        allow_name_prefix_match=False,
    )
    hydrated_run = run.get_hydrated_version()

    assert hydrated_run.run_metadata["topic"] == "kitaru"
    assert hydrated_run.run_metadata["stage"] == "completed"
    assert hydrated_run.run_metadata["output_kind"] == "text"
    assert "draft_cost" not in hydrated_run.run_metadata
    assert "quality_score" not in hydrated_run.run_metadata

    metadata_by_step = [step.run_metadata for step in hydrated_run.steps.values()]

    draft_metadata = _find_step_metadata(
        metadata_by_step=metadata_by_step, key="draft_cost"
    )
    assert draft_metadata["draft_cost"] == {"usd": 0.001, "tokens": 42}
    assert draft_metadata["model"] == "demo-model"
    assert draft_metadata["latency_ms"] == 120

    polish_metadata = _find_step_metadata(
        metadata_by_step=metadata_by_step,
        key="quality_score",
    )
    assert polish_metadata["quality_score"] == 0.93
