"""Unit tests for replay planning utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from zenml.models import PipelineRunResponse

from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.replay import build_replay_plan


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
        ),
    )


def test_build_replay_plan_skips_steps_before_checkpoint_selector() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="__kitaru_checkpoint_source_fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    write = _step(
        name="__kitaru_checkpoint_source_write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["fetch"],
        inputs_v2={"research": [_input_spec("fetch", "output")]},
    )
    publish = _step(
        name="__kitaru_checkpoint_source_publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["write"],
        inputs_v2={"draft": [_input_spec("write", "output")]},
    )

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="write",
    )

    assert plan.steps_to_skip == {"fetch"}
    assert plan.input_overrides == {}
    assert plan.step_input_overrides == {}


def test_checkpoint_override_anchors_frontier_at_source_step() -> None:
    """Checkpoint override frontier should use source.index, not consumer."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="__kitaru_checkpoint_source_fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    write = _step(
        name="__kitaru_checkpoint_source_write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["fetch"],
        inputs_v2={"research": [_input_spec("fetch", "output")]},
    )
    publish = _step(
        name="__kitaru_checkpoint_source_publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["write"],
        inputs_v2={"draft": [_input_spec("write", "output")]},
    )

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="publish",
        overrides={"checkpoint.fetch": "edited research"},
    )

    # source.index anchoring: fetch (index 0) is the frontier, so nothing is
    # skipped. The override is injected on the consumer (write).
    assert plan.steps_to_skip == set()
    assert plan.step_input_overrides == {"write": {"research": "edited research"}}


def test_skip_override_disjointness_is_enforced() -> None:
    """Steps with input overrides must not appear in steps_to_skip."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="__kitaru_checkpoint_source_fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    transform = _step(
        name="__kitaru_checkpoint_source_transform",
        invocation_id="transform",
        started_at=t0 + timedelta(seconds=5),
        upstream_steps=["fetch"],
        inputs_v2={"data": [_input_spec("fetch", "output")]},
    )
    train = _step(
        name="__kitaru_checkpoint_source_train",
        invocation_id="train",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["transform"],
        inputs_v2={"features": [_input_spec("transform", "output")]},
    )

    # from_="train" means skip fetch and transform.
    # But checkpoint.transform overrides inject into train, and the frontier
    # from source.index (transform=1) should keep transform out of skip.
    plan = build_replay_plan(
        run=_run(fetch, transform, train),
        from_="train",
        overrides={"checkpoint.transform": "new features"},
    )

    # transform has step_input_overrides on its consumer (train), and
    # source.index=1 means only fetch (index 0) is skipped.
    assert "train" not in plan.steps_to_skip
    assert plan.step_input_overrides == {"train": {"features": "new features"}}


def test_dag_ordering_not_timestamp_ordering() -> None:
    """Steps should be ordered by DAG topology, not timestamps.

    In this test, validate starts *before* extract in wall-clock time but
    depends on extract in the DAG. DAG order must win.
    """
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)

    # validate starts first in wall clock, but depends on extract
    extract = _step(
        name="__kitaru_checkpoint_source_extract",
        invocation_id="extract",
        started_at=t0 + timedelta(seconds=5),
    )
    validate = _step(
        name="__kitaru_checkpoint_source_validate",
        invocation_id="validate",
        started_at=t0,  # earlier timestamp!
        upstream_steps=["extract"],
        inputs_v2={"data": [_input_spec("extract", "output")]},
    )

    plan = build_replay_plan(
        run=_run(extract, validate),
        from_="validate",
    )

    # DAG order: extract(0) → validate(1). from_="validate" means skip extract.
    assert plan.steps_to_skip == {"extract"}


def test_parallel_branches_ordered_deterministically() -> None:
    """Parallel branches (no dependency) should get a deterministic order."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)

    branch_a = _step(
        name="__kitaru_checkpoint_source_branch_a",
        invocation_id="branch_a",
        started_at=t0 + timedelta(seconds=10),
    )
    branch_b = _step(
        name="__kitaru_checkpoint_source_branch_b",
        invocation_id="branch_b",
        started_at=t0,
    )
    merge = _step(
        name="__kitaru_checkpoint_source_merge",
        invocation_id="merge",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["branch_a", "branch_b"],
        inputs_v2={
            "a": [_input_spec("branch_a", "output")],
            "b": [_input_spec("branch_b", "output")],
        },
    )

    plan = build_replay_plan(
        run=_run(branch_a, branch_b, merge),
        from_="merge",
    )

    # branch_a and branch_b are in the same layer (no deps) — both skipped
    assert plan.steps_to_skip == {"branch_a", "branch_b"}


def test_wait_overrides_are_rejected() -> None:
    """Wait overrides should raise a clear error."""
    step = _step(
        name="__kitaru_checkpoint_source_fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruUsageError, match="not supported in replay"):
        build_replay_plan(
            run=_run(step),
            from_="fetch",
            overrides={"wait.approve": True},
        )


def test_build_replay_plan_rejects_invalid_override_prefix() -> None:
    step = _step(
        name="__kitaru_checkpoint_source_fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruUsageError, match="Override keys must start"):
        build_replay_plan(
            run=_run(step),
            from_="fetch",
            overrides={"artifact.fetch": "x"},
        )


def test_build_replay_plan_rejects_unknown_selector() -> None:
    step = _step(
        name="__kitaru_checkpoint_source_fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruStateError, match="Unknown checkpoint selector"):
        build_replay_plan(
            run=_run(step),
            from_="unknown",
        )
