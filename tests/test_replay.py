"""Unit tests for replay planning utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from zenml.models import PipelineRunResponse

from kitaru.errors import KitaruStateError
from kitaru.replay import build_replay_plan, replay_at_status


def _input_spec(step_name: str, output_name: str) -> Any:
    return SimpleNamespace(step_name=step_name, output_name=output_name)


def _step(
    *,
    name: str,
    invocation_id: str,
    started_at: datetime,
    upstream_steps: list[str] | None = None,
    inputs_v2: dict[str, list[Any]] | None = None,
    step_type: str | None = None,
) -> Any:
    step_type_obj = None
    if step_type is not None:
        step_type_obj = SimpleNamespace(value=step_type)
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        type=step_type_obj,
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


def test_replay_at_status_detects_present_missing_and_ambiguous() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    write_a = _step(name="write", invocation_id="write-a", started_at=t0)
    write_b = _step(name="write", invocation_id="write-b", started_at=t0)
    ambiguous_run = cast(
        PipelineRunResponse,
        SimpleNamespace(
            id=uuid4(),
            steps={"write-a": write_a, "write-b": write_b},
        ),
    )

    assert replay_at_status(run=_run(write_a), at="write") == "present"
    assert replay_at_status(run=ambiguous_run, at="write") == "ambiguous"
    assert replay_at_status(run=_run(write_a), at="missing") == "missing"
    assert replay_at_status(run=_run(), at="write") == "no_checkpoints"


def test_build_replay_plan_skips_steps_before_at_selector() -> None:
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

    plan = build_replay_plan(run=_run(fetch, write, publish), at="write")

    assert plan.steps_to_skip == {"fetch"}
    assert plan.input_overrides == {}
    assert plan.step_input_overrides == {}


def test_output_override_skips_source_and_injects_downstream() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
        step_type="tool_call",
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
        at="publish",
        output={"fetch": "edited research"},
    )

    assert "fetch" in plan.steps_to_skip
    assert plan.step_input_overrides == {"write": {"research": "edited research"}}


def test_input_override_forces_checkpoint_reexecution() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    fetch = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=t0,
        step_type="tool_call",
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

    plan = build_replay_plan(
        run=_run(fetch, transform, train),
        at="train",
        input={"transform": {"data": "new features"}},
    )

    assert "transform" not in plan.steps_to_skip
    assert plan.step_input_overrides["transform"] == {"data": "new features"}


def test_replay_at_branch_leaf_skips_unrelated_branch() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step_a = _step(name="a", invocation_id="a", started_at=t0)
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

    plan = build_replay_plan(run=_run(step_a, step_b, step_c, step_d), at="d")

    assert plan.steps_to_skip == {"a", "b", "c"}


def test_output_scoped_to_at_when_tool_key_matches_cut() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    policy = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=t0,
        step_type="tool_call",
    )
    decide = _step(
        name="decide",
        invocation_id="decide",
        started_at=t0 + timedelta(seconds=5),
        upstream_steps=["lookup_policy_tool"],
        inputs_v2={"policy": [_input_spec("lookup_policy_tool", "output")]},
    )

    plan = build_replay_plan(
        run=_run(policy, decide),
        at="lookup_policy_tool",
        output={"lookup_policy": {"policy_label": "mock"}},
    )

    assert "lookup_policy_tool" in plan.steps_to_skip
    assert plan.step_input_overrides["decide"]["policy"] == {"policy_label": "mock"}


def test_build_replay_plan_rejects_ambiguous_output_target() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    first = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=t0,
        step_type="tool_call",
    )
    second = _step(
        name="lookup_policy_tool_2",
        invocation_id="lookup_policy_tool_2",
        started_at=t0 + timedelta(seconds=1),
        step_type="tool_call",
    )
    decide = _step(
        name="decide",
        invocation_id="decide",
        started_at=t0 + timedelta(seconds=5),
        upstream_steps=["lookup_policy_tool", "lookup_policy_tool_2"],
        inputs_v2={
            "policy_a": [_input_spec("lookup_policy_tool", "output")],
            "policy_b": [_input_spec("lookup_policy_tool_2", "output")],
        },
    )

    with pytest.raises(KitaruStateError, match="ambiguous"):
        build_replay_plan(
            run=_run(first, second, decide),
            at="decide",
            output={"lookup_policy": "mock"},
        )


def test_build_replay_plan_rejects_unknown_selector() -> None:
    step = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruStateError, match="Unknown checkpoint selector"):
        build_replay_plan(run=_run(step), at="unknown")


def test_runtime_context_carries_tool_and_llm_model() -> None:
    step = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )
    plan = build_replay_plan(
        run=_run(step),
        at="fetch",
        tool={"lookup_policy": "mocks.lookup_policy"},
        llm_model="openai/gpt-5-nano",
    )
    assert plan.runtime_context.tool_overrides == {
        "lookup_policy": "mocks.lookup_policy"
    }
    assert plan.runtime_context.llm_model == "openai/gpt-5-nano"
