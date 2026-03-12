"""Integration test for the Phase 8 save/load artifact example workflow."""

from __future__ import annotations

from typing import Any

from examples.basic_flow.flow_with_artifacts import run_workflow
from zenml.client import Client
from zenml.enums import ArtifactSaveType


def _find_artifact_by_name(
    *,
    outputs_by_step: list[dict[str, list[Any]]],
    name: str,
) -> Any:
    """Return the first artifact in step outputs with a given name."""
    for step_outputs in outputs_by_step:
        for artifacts in step_outputs.values():
            for artifact in artifacts:
                if artifact.name == name:
                    return artifact
    raise AssertionError(f"No artifact named '{name}' found in step outputs.")


def test_phase8_artifacts_example_runs_end_to_end() -> None:
    """Verify save/load works for both checkpoint outputs and manual artifacts."""
    exec_id, first_result, second_result = run_workflow("kitaru")

    assert first_result == "Research notes about kitaru."
    assert second_result == "Research notes about kitaru. [topic=kitaru]"

    run = Client().get_pipeline_run(
        exec_id,
        allow_name_prefix_match=False,
    )
    hydrated_run = run.get_hydrated_version()

    outputs_by_step = [step.outputs for step in hydrated_run.steps.values()]
    saved_context_artifact = _find_artifact_by_name(
        outputs_by_step=outputs_by_step,
        name="research_context",
    )

    assert saved_context_artifact.save_type == ArtifactSaveType.MANUAL
    assert saved_context_artifact.run_metadata["kitaru_artifact_type"] == "context"
