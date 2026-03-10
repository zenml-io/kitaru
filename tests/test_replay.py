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
    inputs_v2: dict[str, list[Any]] | None = None,
) -> Any:
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        start_time=started_at,
        end_time=started_at + timedelta(seconds=1),
        spec=SimpleNamespace(invocation_id=invocation_id, inputs_v2=inputs_v2 or {}),
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
        inputs_v2={"research": [_input_spec("fetch", "output")]},
    )
    publish = _step(
        name="__kitaru_checkpoint_source_publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        inputs_v2={"draft": [_input_spec("write", "output")]},
    )

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="write",
    )

    assert plan.steps_to_skip == {"fetch"}
    assert plan.input_overrides == {}
    assert plan.step_input_overrides == {}


def test_checkpoint_override_moves_frontier_to_consumer_step() -> None:
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
        inputs_v2={"research": [_input_spec("fetch", "output")]},
    )
    publish = _step(
        name="__kitaru_checkpoint_source_publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        inputs_v2={"draft": [_input_spec("write", "output")]},
    )

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="publish",
        overrides={"checkpoint.fetch": "edited research"},
    )

    assert plan.steps_to_skip == {"fetch"}
    assert plan.step_input_overrides == {"write": {"research": "edited research"}}


def test_wait_override_is_resolved_to_full_wait_key() -> None:
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
        inputs_v2={"research": [_input_spec("fetch", "output")]},
    )
    publish = _step(
        name="__kitaru_checkpoint_source_publish",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=20),
        inputs_v2={"draft": [_input_spec("write", "output")]},
    )

    wait = SimpleNamespace(
        id=uuid4(),
        wait_condition_key="approve:0",
        created=t0 + timedelta(seconds=15),
        upstream_step_names=["write"],
        downstream_step_names=["publish"],
    )

    plan = build_replay_plan(
        run=_run(fetch, write, publish),
        from_="publish",
        overrides={"wait.approve": True},
        wait_conditions=[wait],
    )

    assert plan.wait_overrides == {"approve:0": True}
    assert plan.steps_to_skip == {"fetch", "write"}


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

    with pytest.raises(KitaruStateError, match="not found"):
        build_replay_plan(
            run=_run(step),
            from_="unknown",
        )
