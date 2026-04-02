"""Tests for the ZenML → ExecutionGraphSnapshot mapper."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

from zenml.models import PipelineRunResponse

from kitaru.engines._types import ExecutionGraphSnapshot
from kitaru.engines.zenml.snapshots import execution_graph_from_run


def _input_spec(step_name: str, output_name: str) -> Any:
    return SimpleNamespace(step_name=step_name, output_name=output_name)


def _step(
    *,
    name: str,
    invocation_id: str,
    started_at: datetime,
    upstream_steps: list[str] | None = None,
    inputs_v2: dict[str, list[Any]] | None = None,
) -> Any:
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        start_time=started_at,
        end_time=started_at + timedelta(seconds=1),
        spec=SimpleNamespace(
            invocation_id=invocation_id,
            upstream_steps=upstream_steps or [],
            inputs_v2=inputs_v2 or {},
        ),
        outputs={"output": [object()]},
        regular_outputs={"output": object()},
    )


def _run(*steps: Any) -> PipelineRunResponse:
    return cast(
        PipelineRunResponse,
        SimpleNamespace(
            id=uuid4(),
            steps={step.name: step for step in steps},
            pipeline=None,
        ),
    )


def test_basic_snapshot_conversion() -> None:
    """A simple three-step pipeline converts to a snapshot with correct fields."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(name="fetch", invocation_id="fetch", started_at=t0)
    write = _step(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["fetch"],
        inputs_v2={"research": [_input_spec("fetch", "output")]},
    )
    publish = _step(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["write"],
        inputs_v2={"draft": [_input_spec("write", "output")]},
    )

    run = _run(fetch, write, publish)
    snapshot = execution_graph_from_run(run)

    assert isinstance(snapshot, ExecutionGraphSnapshot)
    assert snapshot.exec_id == str(run.id)
    assert len(snapshot.checkpoints) == 3

    by_name = {node.name: node for node in snapshot.checkpoints}

    assert by_name["fetch"].upstream_invocation_ids == ()
    assert by_name["fetch"].output_names == ("output",)

    assert by_name["write"].upstream_invocation_ids == ("fetch",)
    assert len(by_name["write"].input_bindings) == 1
    binding = by_name["write"].input_bindings[0]
    assert binding.input_name == "research"
    assert binding.upstream_invocation_id == "fetch"
    assert binding.upstream_output_name == "output"


def test_snapshot_preserves_timestamps() -> None:
    """Snapshot nodes should carry start/end timestamps from the source."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step = _step(name="fetch", invocation_id="fetch", started_at=t0)
    snapshot = execution_graph_from_run(_run(step))

    node = snapshot.checkpoints[0]
    assert node.start_time == t0
    assert node.end_time == t0 + timedelta(seconds=1)


def test_snapshot_with_no_steps() -> None:
    """An empty run produces an empty snapshot."""
    run = cast(
        PipelineRunResponse,
        SimpleNamespace(id=uuid4(), steps={}, pipeline=None),
    )
    snapshot = execution_graph_from_run(run)
    assert snapshot.checkpoints == ()


def test_snapshot_normalizes_checkpoint_names() -> None:
    """Checkpoint source alias prefixes should be stripped during conversion."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step = _step(
        name="__kitaru_checkpoint_source_my_step",
        invocation_id="my_step",
        started_at=t0,
    )
    snapshot = execution_graph_from_run(_run(step))
    assert snapshot.checkpoints[0].name == "my_step"
