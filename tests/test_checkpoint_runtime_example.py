"""Example-level integration test for checkpoint runtime selection."""

from __future__ import annotations

from examples.features.basic_flow.flow_with_checkpoint_runtime import run_workflow

from kitaru._client._models import ExecutionStatus
from kitaru.client import KitaruClient


def test_checkpoint_runtime_example_runs_fan_out_checkpoints(primed_zenml) -> None:
    """Run the public example entrypoint and inspect the recorded fan-out."""
    execution_id = run_workflow(["alpha", "bravo"])

    assert execution_id

    execution = KitaruClient().executions.get(execution_id)
    assert execution.status == ExecutionStatus.COMPLETED
    assert execution.flow_name == "parallel_transform"

    transform_calls = [
        checkpoint
        for checkpoint in execution.checkpoints
        if checkpoint.name.startswith("transform_item")
    ]
    assert len(transform_calls) == 2
    assert all(call.status == ExecutionStatus.COMPLETED for call in transform_calls)

    loaded_outputs = [
        artifact.load()
        for call in transform_calls
        for artifact in call.artifacts
        if artifact.direction == "output"
    ]
    assert sorted(loaded_outputs) == ["[processed] ALPHA", "[processed] BRAVO"]
