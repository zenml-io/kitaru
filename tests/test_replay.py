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
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
    )
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

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="write",
    )

    assert plan.steps_to_skip == {"fetch"}
    assert plan.input_overrides == {}
    assert plan.step_input_overrides == {}


def test_checkpoint_override_reexecutes_override_source_path() -> None:
    """Checkpoint overrides should replay from direct override consumers."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
    )
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

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="publish",
        overrides={"checkpoint.fetch": "edited research"},
    )

    # Replay roots include publish (from_) and direct consumers of the override
    # source (write). fetch remains skipped.
    assert plan.steps_to_skip == {"fetch"}
    assert plan.step_input_overrides == {"write": {"research": "edited research"}}


def test_skip_override_disjointness_is_enforced() -> None:
    """Steps with input overrides must not appear in steps_to_skip."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    transform = _step(
        name="transform",
        invocation_id="transform",
        started_at=t0 + timedelta(seconds=5),
        upstream_steps=["fetch"],
        inputs_v2={"data": [_input_spec("fetch", "output")]},
    )
    train = _step(
        name="train",
        invocation_id="train",
        started_at=t0 + timedelta(seconds=10),
        upstream_steps=["transform"],
        inputs_v2={"features": [_input_spec("transform", "output")]},
    )

    # from_="train" skips fetch/transform. Override on transform injects into
    # train (already a replay root), so skip set remains unchanged.
    plan = build_replay_plan(
        run=_run(fetch, transform, train),
        from_="train",
        overrides={"checkpoint.transform": "new features"},
    )

    assert plan.steps_to_skip == {"fetch", "transform"}
    assert "train" not in plan.steps_to_skip
    assert plan.step_input_overrides == {"train": {"features": "new features"}}


def test_replay_from_branch_leaf_skips_unrelated_branch() -> None:
    """Replay from a branch leaf should not re-execute sibling-branch work."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step_a = _step(
        name="a",
        invocation_id="a",
        started_at=t0,
    )
    step_b = _step(
        name="b",
        invocation_id="b",
        started_at=t0 + timedelta(seconds=1),
        upstream_steps=["a"],
        inputs_v2={"a_input": [_input_spec("a", "output")]},
    )
    step_c = _step(
        name="c",
        invocation_id="c",
        started_at=t0 + timedelta(seconds=2),
        upstream_steps=["b"],
        inputs_v2={"b_input": [_input_spec("b", "output")]},
    )
    step_d = _step(
        name="d",
        invocation_id="d",
        started_at=t0 + timedelta(seconds=3),
        upstream_steps=["a"],
        inputs_v2={"a_input": [_input_spec("a", "output")]},
    )

    plan = build_replay_plan(run=_run(step_a, step_b, step_c, step_d), from_="d")

    assert plan.steps_to_skip == {"a", "b", "c"}


def test_checkpoint_override_fanout_reexecutes_consumer_branches() -> None:
    """Override fan-out re-executes every direct consumer branch."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step_a = _step(
        name="a",
        invocation_id="a",
        started_at=t0,
    )
    step_b = _step(
        name="b",
        invocation_id="b",
        started_at=t0 + timedelta(seconds=1),
        upstream_steps=["a"],
        inputs_v2={"a_input": [_input_spec("a", "output")]},
    )
    step_c = _step(
        name="c",
        invocation_id="c",
        started_at=t0 + timedelta(seconds=2),
        upstream_steps=["b"],
        inputs_v2={"b_input": [_input_spec("b", "output")]},
    )
    step_d = _step(
        name="d",
        invocation_id="d",
        started_at=t0 + timedelta(seconds=3),
        upstream_steps=["a"],
        inputs_v2={"a_input": [_input_spec("a", "output")]},
    )

    plan = build_replay_plan(
        run=_run(step_a, step_b, step_c, step_d),
        from_="d",
        overrides={"checkpoint.a": "edited"},
    )

    assert plan.steps_to_skip == {"a"}
    assert plan.step_input_overrides == {
        "b": {"a_input": "edited"},
        "d": {"a_input": "edited"},
    }


def test_dag_ordering_not_timestamp_ordering() -> None:
    """Steps should be ordered by DAG topology, not timestamps.

    In this test, validate starts *before* extract in wall-clock time but
    depends on extract in the DAG. DAG order must win.
    """
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)

    # validate starts first in wall clock, but depends on extract
    extract = _step(
        name="extract",
        invocation_id="extract",
        started_at=t0 + timedelta(seconds=5),
    )
    validate = _step(
        name="validate",
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


def test_replay_from_parallel_branch_only_skips_unrelated_peer() -> None:
    """Replay should skip unrelated peer branch work."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)

    branch_a = _step(
        name="branch_a",
        invocation_id="branch_a",
        started_at=t0 + timedelta(seconds=10),
    )
    branch_b = _step(
        name="branch_b",
        invocation_id="branch_b",
        started_at=t0,
    )
    merge = _step(
        name="merge",
        invocation_id="merge",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["branch_a", "branch_b"],
        inputs_v2={
            "a": [_input_spec("branch_a", "output")],
            "b": [_input_spec("branch_b", "output")],
        },
    )

    plan = build_replay_plan(run=_run(branch_a, branch_b, merge), from_="branch_a")

    # Replaying from branch_a should not re-execute its sibling peer branch.
    assert plan.steps_to_skip == {"branch_b"}


def test_replay_from_parallel_branch_with_tied_start_times() -> None:
    """Replay should be stable when peer branches share start times."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)

    beta = _step(
        name="beta",
        invocation_id="beta",
        started_at=t0,
    )
    alpha = _step(
        name="alpha",
        invocation_id="alpha",
        started_at=t0,
    )
    merge = _step(
        name="merge",
        invocation_id="merge",
        started_at=t0 + timedelta(seconds=20),
        upstream_steps=["alpha", "beta"],
        inputs_v2={
            "a": [_input_spec("alpha", "output")],
            "b": [_input_spec("beta", "output")],
        },
    )

    plan = build_replay_plan(run=_run(beta, alpha, merge), from_="beta")

    # Replaying from beta still skips the sibling alpha branch.
    assert plan.steps_to_skip == {"alpha"}


def test_wait_overrides_are_rejected() -> None:
    """Wait overrides should raise a clear error."""
    step = _step(
        name="fetch",
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
        name="fetch",
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
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruStateError, match="Unknown checkpoint selector"):
        build_replay_plan(
            run=_run(step),
            from_="unknown",
        )
