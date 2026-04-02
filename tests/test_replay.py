"""Unit tests for replay planning utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from kitaru.engines._types import (
    CheckpointGraphNode,
    CheckpointInputBinding,
    ExecutionGraphSnapshot,
)
from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.replay import build_replay_plan


def _binding(
    input_name: str, upstream_id: str, output_name: str
) -> CheckpointInputBinding:
    return CheckpointInputBinding(
        input_name=input_name,
        upstream_invocation_id=upstream_id,
        upstream_output_name=output_name,
    )


def _node(
    *,
    name: str,
    invocation_id: str,
    started_at: datetime,
    upstream: list[str] | None = None,
    input_bindings: list[CheckpointInputBinding] | None = None,
    output_names: list[str] | None = None,
) -> CheckpointGraphNode:
    return CheckpointGraphNode(
        call_id=str(uuid4()),
        invocation_id=invocation_id,
        name=name,
        upstream_invocation_ids=tuple(upstream or []),
        input_bindings=tuple(input_bindings or []),
        output_names=tuple(output_names or ["output"]),
        start_time=started_at,
        end_time=started_at + timedelta(seconds=1),
    )


def _snapshot(*nodes: CheckpointGraphNode) -> ExecutionGraphSnapshot:
    return ExecutionGraphSnapshot(
        exec_id=str(uuid4()),
        checkpoints=nodes,
    )


def test_build_replay_plan_skips_steps_before_checkpoint_selector() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _node(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    write = _node(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream=["fetch"],
        input_bindings=[_binding("research", "fetch", "output")],
    )
    publish = _node(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream=["write"],
        input_bindings=[_binding("draft", "write", "output")],
    )

    plan = build_replay_plan(
        snapshot=_snapshot(fetch, write, publish),
        from_="write",
    )

    assert plan.steps_to_skip == {"fetch"}
    assert plan.input_overrides == {}
    assert plan.step_input_overrides == {}


def test_checkpoint_override_anchors_frontier_at_source_step() -> None:
    """Checkpoint override frontier should use source.index, not consumer."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _node(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    write = _node(
        name="write",
        invocation_id="write",
        started_at=t0 + timedelta(seconds=10),
        upstream=["fetch"],
        input_bindings=[_binding("research", "fetch", "output")],
    )
    publish = _node(
        name="publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        upstream=["write"],
        input_bindings=[_binding("draft", "write", "output")],
    )

    plan = build_replay_plan(
        snapshot=_snapshot(fetch, write, publish),
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
    fetch = _node(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
    )
    transform = _node(
        name="transform",
        invocation_id="transform",
        started_at=t0 + timedelta(seconds=5),
        upstream=["fetch"],
        input_bindings=[_binding("data", "fetch", "output")],
    )
    train = _node(
        name="train",
        invocation_id="train",
        started_at=t0 + timedelta(seconds=10),
        upstream=["transform"],
        input_bindings=[_binding("features", "transform", "output")],
    )

    # from_="train" means skip fetch and transform.
    # But checkpoint.transform overrides inject into train, and the frontier
    # from source.index (transform=1) should keep transform out of skip.
    plan = build_replay_plan(
        snapshot=_snapshot(fetch, transform, train),
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
    extract = _node(
        name="extract",
        invocation_id="extract",
        started_at=t0 + timedelta(seconds=5),
    )
    validate = _node(
        name="validate",
        invocation_id="validate",
        started_at=t0,  # earlier timestamp!
        upstream=["extract"],
        input_bindings=[_binding("data", "extract", "output")],
    )

    plan = build_replay_plan(
        snapshot=_snapshot(extract, validate),
        from_="validate",
    )

    # DAG order: extract(0) → validate(1). from_="validate" means skip extract.
    assert plan.steps_to_skip == {"extract"}


def test_parallel_branches_ordered_deterministically() -> None:
    """Parallel branches (no dependency) should get a deterministic order."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)

    branch_a = _node(
        name="branch_a",
        invocation_id="branch_a",
        started_at=t0 + timedelta(seconds=10),
    )
    branch_b = _node(
        name="branch_b",
        invocation_id="branch_b",
        started_at=t0,
    )
    merge = _node(
        name="merge",
        invocation_id="merge",
        started_at=t0 + timedelta(seconds=20),
        upstream=["branch_a", "branch_b"],
        input_bindings=[
            _binding("a", "branch_a", "output"),
            _binding("b", "branch_b", "output"),
        ],
    )

    plan = build_replay_plan(
        snapshot=_snapshot(branch_a, branch_b, merge),
        from_="merge",
    )

    # branch_a and branch_b are in the same layer (no deps) — both skipped
    assert plan.steps_to_skip == {"branch_a", "branch_b"}


def test_wait_overrides_are_rejected() -> None:
    """Wait overrides should raise a clear error."""
    node = _node(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruUsageError, match="not supported in replay"):
        build_replay_plan(
            snapshot=_snapshot(node),
            from_="fetch",
            overrides={"wait.approve": True},
        )


def test_build_replay_plan_rejects_invalid_override_prefix() -> None:
    node = _node(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruUsageError, match="Override keys must start"):
        build_replay_plan(
            snapshot=_snapshot(node),
            from_="fetch",
            overrides={"artifact.fetch": "x"},
        )


def test_build_replay_plan_rejects_unknown_selector() -> None:
    node = _node(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruStateError, match="Unknown checkpoint selector"):
        build_replay_plan(
            snapshot=_snapshot(node),
            from_="unknown",
        )
