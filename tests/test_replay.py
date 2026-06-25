"""Unit tests for replay planning utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from zenml.models import PipelineRunResponse

from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.replay import (
    ReplayPlanDocument,
    ReplayResultRow,
    ReplaySubmission,
    build_replay_plan,
    replay_at_status,
)


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
        invocation_overrides={"fetch": {"output": "edited research"}},
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
        invocation_overrides={"transform": {"input": {"data": "new features"}}},
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


def test_replay_at_includes_linear_tail_without_upstream_edges() -> None:
    """Adapter call checkpoints may lack DAG edges; time-ordered tail still re-runs."""
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    model_1 = _step(name="support_copilot_model_request", invocation_id="m1", started_at=t0)
    gather = _step(
        name="gather_context_tool",
        invocation_id="gather",
        started_at=t0 + timedelta(seconds=1),
    )
    model_2 = _step(
        name="support_copilot_model_request_2",
        invocation_id="m2",
        started_at=t0 + timedelta(seconds=2),
    )
    lookup = _step(
        name="lookup_policy_tool",
        invocation_id="lookup",
        started_at=t0 + timedelta(seconds=3),
        step_type="tool_call",
    )
    model_3 = _step(
        name="support_copilot_model_request_3",
        invocation_id="m3",
        started_at=t0 + timedelta(seconds=4),
    )
    publish = _step(
        name="publish_support_decision",
        invocation_id="publish",
        started_at=t0 + timedelta(seconds=5),
    )

    plan = build_replay_plan(
        run=_run(model_1, gather, model_2, lookup, model_3, publish),
        at="lookup_policy_tool",
    )

    assert plan.steps_to_skip == {"m1", "gather", "m2"}
    assert "lookup" not in plan.steps_to_skip
    assert "m3" not in plan.steps_to_skip
    assert "publish" not in plan.steps_to_skip


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
        checkpoint_overrides={
            "lookup_policy": {"output": {"policy_label": "mock"}}
        },
    )

    assert "lookup_policy_tool" in plan.steps_to_skip
    assert plan.step_input_overrides["decide"]["policy"] == {"policy_label": "mock"}


def test_checkpoint_overrides_fan_out_to_matching_invocations() -> None:
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

    plan = build_replay_plan(
        run=_run(first, second, decide),
        at="decide",
        checkpoint_overrides={"lookup_policy": {"output": "mock"}},
    )

    assert plan.step_input_overrides["decide"] == {
        "policy_a": "mock",
        "policy_b": "mock",
    }
    assert plan.document.matched_targets["checkpoint:lookup_policy"] == [
        "lookup_policy_tool",
        "lookup_policy_tool_2",
    ]


def test_build_replay_plan_rejects_unknown_selector() -> None:
    step = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruStateError, match="Unknown checkpoint selector"):
        build_replay_plan(run=_run(step), at="unknown")


def test_runtime_context_carries_targeted_code_and_model_overrides() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    tool_step = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=t0,
        step_type="tool_call",
    )
    llm_step = _step(
        name="support_copilot_model_request_2",
        invocation_id="support_copilot_model_request_2",
        started_at=t0 + timedelta(seconds=1),
        step_type="llm_call",
    )
    plan = build_replay_plan(
        run=_run(tool_step, llm_step),
        at="lookup_policy_tool",
        checkpoint_overrides={
            "lookup_policy": {"code": "mocks.lookup_policy"}
        },
        invocation_overrides={
            "support_copilot_model_request_2": {"model": "openai/gpt-5-nano"}
        },
    )

    assert plan.runtime_context.code_overrides["lookup_policy_tool"] == (
        "mocks.lookup_policy"
    )
    assert plan.runtime_context.model_overrides[
        "support_copilot_model_request_2"
    ] == "openai/gpt-5-nano"


def test_explicit_skip_forces_playback_in_live_tail() -> None:
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
        at="write",
        skip=["write"],
    )

    assert "write" in plan.steps_to_skip
    assert "fetch" in plan.steps_to_skip
    assert "publish" not in plan.steps_to_skip


def test_explicit_skip_conflicts_with_input_override() -> None:
    step = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
    )
    with pytest.raises(KitaruUsageError, match="Cannot skip and override"):
        build_replay_plan(
            run=_run(step),
            at="fetch",
            skip=["fetch"],
            invocation_overrides={"fetch": {"input": {"tool_args": {"topic": "new"}}}},
        )

def test_invocation_override_wins_over_checkpoint_override() -> None:
    step = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
    )
    plan = build_replay_plan(
        run=_run(step),
        at="lookup_policy_tool",
        checkpoint_overrides={"lookup_policy": {"code": "mocks.default"}},
        invocation_overrides={"lookup_policy_tool": {"code": "mocks.specific"}},
    )

    assert plan.runtime_context.code_overrides["lookup_policy_tool"] == "mocks.specific"


def test_invalid_override_field_fails_before_submission() -> None:
    step = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )
    with pytest.raises(KitaruUsageError, match="Unknown replay override field"):
        build_replay_plan(
            run=_run(step),
            at="fetch",
            invocation_overrides={"fetch": {"bogus": True}},
        )


def test_input_and_output_same_target_fails_before_submission() -> None:
    step = _step(
        name="fetch",
        invocation_id="fetch",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )
    with pytest.raises(KitaruUsageError, match="cannot include both input and output"):
        build_replay_plan(
            run=_run(step),
            at="fetch",
            invocation_overrides={"fetch": {"input": {"x": 1}, "output": "y"}},
        )


def test_model_override_rejects_non_llm_checkpoint() -> None:
    step = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
    )
    with pytest.raises(KitaruUsageError, match="not an LLM checkpoint"):
        build_replay_plan(
            run=_run(step),
            at="lookup_policy_tool",
            invocation_overrides={"lookup_policy_tool": {"model": "openai/gpt-5-nano"}},
        )


def test_replay_submission_to_json_excludes_handles() -> None:
    handle = object()
    submission = ReplaySubmission.create(
        submission_id="rs-test",
        tag="eval",
        at="lookup_policy_tool",
        wait=False,
        plan=ReplayPlanDocument(flow_overrides={"model": "openai:gpt-5-nano"}),
        results=[
            ReplayResultRow(
                original_exec_ref="kr-a",
                original_exec_id="orig-a",
                replay_exec_id="replay-a",
                status="submitted",
                compare_url="https://example.test/compare",
                handle=handle,
            )
        ],
    )

    payload = submission.to_json()

    assert payload["submission_id"] == "rs-test"
    assert payload["summary"] == {
        "submitted": 1,
        "completed": 0,
        "failed": 0,
        "skipped": 0,
    }
    assert "handle" not in payload["results"][0]
