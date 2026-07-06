"""Unit tests for replay planning utilities."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from zenml.models import PipelineRunResponse, PipelineRunUpdate

from kitaru._checkpoint_metadata import (
    KITARU_METADATA_NAMESPACE,
    REPLAY_INPUT_SLOTS_KEY,
    adapter_checkpoint_metadata,
)
from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.replay import (
    REPLAY_SKIPPED_STEPS_METADATA_KEY,
    ReplayPlanDocument,
    ReplayResultRow,
    ReplaySubmission,
    build_replay_plan,
    parse_replay_skipped_steps_metadata,
    plan_requires_runtime_transport,
    replay_at_status,
    replay_skipped_steps_metadata,
    safe_persist_replay_submission_metadata,
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
    run_metadata: dict[str, Any] | None = None,
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
        run_metadata=run_metadata or {},
    )


def _run(*steps: Any) -> PipelineRunResponse:
    return cast(
        PipelineRunResponse,
        SimpleNamespace(
            id=uuid4(),
            steps={step.name: step for step in steps},
        ),
    )


def _adapter_metadata(
    *,
    kind: str = "tool_call",
    input_slots: list[str] | None = None,
    output_slots: list[str] | None = None,
) -> dict[str, Any]:
    return {
        KITARU_METADATA_NAMESPACE: adapter_checkpoint_metadata(
            adapter="pydantic_ai",
            kind=kind,
            input_slots=input_slots or [],
            output_slots=output_slots or ["output"],
        )
    }


def _support_copilot_model_request_steps(t0: datetime) -> tuple[Any, Any, Any]:
    return (
        _step(
            name="support_copilot_model_request",
            invocation_id="m1",
            started_at=t0,
            step_type="llm_call",
        ),
        _step(
            name="support_copilot_model_request_2",
            invocation_id="m2",
            started_at=t0 + timedelta(seconds=1),
            step_type="llm_call",
        ),
        _step(
            name="support_copilot_model_request_3",
            invocation_id="m3",
            started_at=t0 + timedelta(seconds=2),
            step_type="llm_call",
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


def test_leaf_output_override_explains_no_downstream_consumer() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    charge_card = _step(
        name="charge_card",
        invocation_id="charge_card",
        started_at=t0,
        step_type="tool_call",
    )

    with pytest.raises(KitaruStateError) as exc_info:
        build_replay_plan(
            run=_run(charge_card),
            at="charge_card",
            invocation_overrides={"charge_card": {"output": "already charged"}},
        )

    message = str(exc_info.value)
    assert "no downstream consumer" in message
    assert "replacing inputs to later checkpoints" in message
    assert "kitaru.is_replay()" in message


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


def test_generic_input_override_does_not_require_runtime_transport() -> None:
    transform = _step(
        name="transform",
        invocation_id="transform",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        inputs_v2={"data": []},
    )

    plan = build_replay_plan(
        run=_run(transform),
        at="transform",
        invocation_overrides={"transform": {"input": {"data": "new features"}}},
    )

    assert plan.step_input_overrides == {"transform": {"data": "new features"}}
    assert plan.runtime_context.input_overrides == {}
    assert plan_requires_runtime_transport(plan) is False


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
    model_1 = _step(
        name="support_copilot_model_request",
        invocation_id="m1",
        started_at=t0,
    )
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
        checkpoint_overrides={"lookup_policy": {"output": {"policy_label": "mock"}}},
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
    third = _step(
        name="lookup_policy_tool_3",
        invocation_id="lookup_policy_tool_3",
        started_at=t0 + timedelta(seconds=2),
        step_type="tool_call",
    )
    decide = _step(
        name="decide",
        invocation_id="decide",
        started_at=t0 + timedelta(seconds=5),
        upstream_steps=[
            "lookup_policy_tool",
            "lookup_policy_tool_2",
            "lookup_policy_tool_3",
        ],
        inputs_v2={
            "policy_a": [_input_spec("lookup_policy_tool", "output")],
            "policy_b": [_input_spec("lookup_policy_tool_2", "output")],
            "policy_c": [_input_spec("lookup_policy_tool_3", "output")],
        },
    )

    plan = build_replay_plan(
        run=_run(first, second, third, decide),
        at="decide",
        checkpoint_overrides={"lookup_policy": {"output": "mock"}},
    )

    assert plan.step_input_overrides["decide"] == {
        "policy_a": "mock",
        "policy_b": "mock",
        "policy_c": "mock",
    }
    assert plan.document.matched_targets["checkpoint:lookup_policy"] == [
        "lookup_policy_tool",
        "lookup_policy_tool_2",
        "lookup_policy_tool_3",
    ]


def test_unsuffixed_tool_checkpoint_override_selector_fans_out() -> None:
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
    third = _step(
        name="lookup_policy_tool_3",
        invocation_id="lookup_policy_tool_3",
        started_at=t0 + timedelta(seconds=2),
        step_type="tool_call",
    )

    plan = build_replay_plan(
        run=_run(first, second, third),
        at="lookup_policy_tool",
        checkpoint_overrides={"lookup_policy_tool": {"code": "mocks.lookup_policy"}},
    )

    assert plan.document.matched_targets["checkpoint:lookup_policy_tool"] == [
        "lookup_policy_tool",
        "lookup_policy_tool_2",
        "lookup_policy_tool_3",
    ]
    assert plan.runtime_context.code_overrides["lookup_policy_tool"] == (
        "mocks.lookup_policy"
    )
    assert plan.runtime_context.code_overrides["lookup_policy_tool_2"] == (
        "mocks.lookup_policy"
    )
    assert plan.runtime_context.code_overrides["lookup_policy_tool_3"] == (
        "mocks.lookup_policy"
    )


def test_suffixed_tool_checkpoint_override_selector_stays_exact() -> None:
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
    third = _step(
        name="lookup_policy_tool_3",
        invocation_id="lookup_policy_tool_3",
        started_at=t0 + timedelta(seconds=2),
        step_type="tool_call",
    )

    plan = build_replay_plan(
        run=_run(first, second, third),
        at="lookup_policy_tool",
        checkpoint_overrides={"lookup_policy_tool_2": {"code": "mocks.lookup_policy"}},
    )

    assert plan.document.matched_targets["checkpoint:lookup_policy_tool_2"] == [
        "lookup_policy_tool_2"
    ]
    assert "lookup_policy_tool" not in plan.runtime_context.code_overrides
    assert plan.runtime_context.code_overrides["lookup_policy_tool_2"] == (
        "mocks.lookup_policy"
    )
    assert "lookup_policy_tool_3" not in plan.runtime_context.code_overrides


def test_checkpoint_model_override_fans_out_to_repeated_model_requests() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    first, second, third = _support_copilot_model_request_steps(t0)

    plan = build_replay_plan(
        run=_run(first, second, third),
        at="support_copilot_model_request",
        checkpoint_overrides={
            "support_copilot_model_request": {"model": "openai/gpt-5-nano"}
        },
    )

    assert plan.document.matched_targets[
        "checkpoint:support_copilot_model_request"
    ] == ["m1", "m2", "m3"]
    assert plan.runtime_context.model_overrides["m1"] == "openai/gpt-5-nano"
    assert plan.runtime_context.model_overrides["m2"] == "openai/gpt-5-nano"
    assert plan.runtime_context.model_overrides["m3"] == "openai/gpt-5-nano"


def test_invocation_model_override_wins_over_repeated_checkpoint_fanout() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    first, second, third = _support_copilot_model_request_steps(t0)

    plan = build_replay_plan(
        run=_run(first, second, third),
        at="support_copilot_model_request",
        checkpoint_overrides={
            "support_copilot_model_request": {"model": "openai/gpt-5-nano"}
        },
        invocation_overrides={
            "support_copilot_model_request_2": {"model": "anthropic/claude-opus-4"}
        },
    )

    assert plan.runtime_context.model_overrides["m1"] == "openai/gpt-5-nano"
    assert plan.runtime_context.model_overrides["m2"] == "anthropic/claude-opus-4"
    assert plan.runtime_context.model_overrides["m3"] == "openai/gpt-5-nano"


def test_suffixed_checkpoint_model_override_selector_stays_exact() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    first, second, third = _support_copilot_model_request_steps(t0)

    plan = build_replay_plan(
        run=_run(first, second, third),
        at="support_copilot_model_request",
        checkpoint_overrides={
            "support_copilot_model_request_2": {"model": "openai/gpt-5-nano"}
        },
    )

    assert plan.document.matched_targets[
        "checkpoint:support_copilot_model_request_2"
    ] == ["m2"]
    assert "m1" not in plan.runtime_context.model_overrides
    assert plan.runtime_context.model_overrides["m2"] == "openai/gpt-5-nano"
    assert "m3" not in plan.runtime_context.model_overrides


def test_unknown_model_override_target_suggests_family_and_invocation() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    first, second, third = _support_copilot_model_request_steps(t0)

    with pytest.raises(KitaruStateError) as exc_info:
        build_replay_plan(
            run=_run(first, second, third),
            at="support_copilot_model_request",
            checkpoint_overrides={
                "support_copilot_model_request_4": {"model": "openai/gpt-5-nano"}
            },
        )

    message = str(exc_info.value)
    assert (
        "Unknown checkpoint override target 'support_copilot_model_request_4'"
        in message
    )
    assert "Available checkpoints:" in message
    assert "support_copilot_model_request" in message
    assert "support_copilot_model_request_2" in message
    assert "support_copilot_model_request_3" in message
    assert "'support_copilot_model_request' in checkpoint_overrides" in message
    assert "call ID or invocation ID in invocation_overrides" in message


def test_unknown_tool_override_target_suggests_family_and_invocation() -> None:
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
    third = _step(
        name="lookup_policy_tool_3",
        invocation_id="lookup_policy_tool_3",
        started_at=t0 + timedelta(seconds=2),
        step_type="tool_call",
    )

    with pytest.raises(KitaruStateError) as exc_info:
        build_replay_plan(
            run=_run(first, second, third),
            at="lookup_policy_tool",
            checkpoint_overrides={
                "lookup_policy_tool_4": {"code": "mocks.lookup_policy"}
            },
        )

    message = str(exc_info.value)
    assert "Unknown checkpoint override target 'lookup_policy_tool_4'" in message
    assert "Available checkpoints:" in message
    assert "lookup_policy_tool" in message
    assert "lookup_policy_tool_2" in message
    assert "lookup_policy_tool_3" in message
    assert "'lookup_policy_tool' in checkpoint_overrides" in message
    assert "call ID or invocation ID in invocation_overrides" in message


def test_unknown_override_target_typo_stays_plain() -> None:
    t0 = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    first, second, third = _support_copilot_model_request_steps(t0)

    with pytest.raises(KitaruStateError) as exc_info:
        build_replay_plan(
            run=_run(first, second, third),
            at="support_copilot_model_request",
            checkpoint_overrides={
                "support_copilot_model_request_extra_4": {"model": "openai/gpt-5-nano"}
            },
        )

    message = str(exc_info.value)
    assert (
        "Unknown checkpoint override target 'support_copilot_model_request_extra_4'"
        in message
    )
    assert "Available checkpoints:" in message
    assert "support_copilot_model_request" in message
    assert "To change every recorded call in this family" not in message
    assert "checkpoint_overrides" not in message
    assert "invocation_overrides" not in message


def test_checkpoint_override_does_not_strip_user_numbered_checkpoint_names() -> None:
    phase_2 = _step(
        name="phase_2",
        invocation_id="phase_2",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
    )

    with pytest.raises(KitaruStateError, match="Unknown checkpoint override target"):
        build_replay_plan(
            run=_run(phase_2),
            at="phase_2",
            checkpoint_overrides={"phase": {"input": "new value"}},
        )


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
        checkpoint_overrides={"lookup_policy": {"code": "mocks.lookup_policy"}},
        invocation_overrides={
            "support_copilot_model_request_2": {"model": "openai/gpt-5-nano"}
        },
    )

    assert plan.runtime_context.code_overrides["lookup_policy_tool"] == (
        "mocks.lookup_policy"
    )
    assert (
        plan.runtime_context.model_overrides["support_copilot_model_request_2"]
        == "openai/gpt-5-nano"
    )


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


def test_adapter_replay_input_slots_enable_tool_args_shorthand() -> None:
    step = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
        run_metadata=_adapter_metadata(input_slots=["tool_args"]),
    )

    plan = build_replay_plan(
        run=_run(step),
        at="lookup_policy_tool",
        invocation_overrides={
            "lookup_policy_tool": {"input": {"account_id": "acct-2"}}
        },
    )

    assert plan.step_input_overrides == {
        "lookup_policy_tool": {"tool_args": {"account_id": "acct-2"}}
    }
    assert plan.runtime_context.input_overrides["lookup_policy_tool"] == {
        "tool_args": {"account_id": "acct-2"}
    }
    assert plan.runtime_context.input_overrides[str(step.id)] == {
        "tool_args": {"account_id": "acct-2"}
    }
    assert plan_requires_runtime_transport(plan) is True


def test_adapter_replay_input_slots_accept_explicit_tool_args() -> None:
    step = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
        run_metadata=_adapter_metadata(input_slots=["tool_args"]),
    )

    plan = build_replay_plan(
        run=_run(step),
        at="lookup_policy_tool",
        invocation_overrides={
            "lookup_policy_tool": {"input": {"tool_args": {"account_id": "acct-2"}}}
        },
    )

    assert plan.step_input_overrides == {
        "lookup_policy_tool": {"tool_args": {"account_id": "acct-2"}}
    }


def test_adapter_replay_input_rejects_unknown_explicit_slot() -> None:
    step = _step(
        name="lookup_policy_tool",
        invocation_id="lookup_policy_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
        run_metadata=_adapter_metadata(input_slots=["tool_args"]),
    )

    with pytest.raises(KitaruUsageError, match="Unknown replay input slot"):
        build_replay_plan(
            run=_run(step),
            at="lookup_policy_tool",
            invocation_overrides={
                "lookup_policy_tool": {
                    "input": {
                        "tool_args": {"account_id": "acct-2"},
                        "mystery": "unexpected",
                    }
                }
            },
        )


def test_pydantic_ai_model_request_rejects_input_override() -> None:
    step = _step(
        name="support_copilot_model_request",
        invocation_id="support_copilot_model_request",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="llm_call",
        run_metadata=_adapter_metadata(kind="model_request", input_slots=[]),
    )

    with pytest.raises(
        KitaruUsageError,
        match="does not expose replayable inputs",
    ):
        build_replay_plan(
            run=_run(step),
            at="support_copilot_model_request",
            invocation_overrides={
                "support_copilot_model_request": {
                    "input": {"messages": [{"role": "user", "content": "edited"}]}
                }
            },
        )


def test_pydantic_ai_turn_rejects_input_override() -> None:
    step = _step(
        name="support_copilot_turn",
        invocation_id="support_copilot_turn",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="llm_call",
        run_metadata=_adapter_metadata(kind="turn", input_slots=[]),
    )

    with pytest.raises(
        KitaruUsageError,
        match="does not expose replayable inputs",
    ):
        build_replay_plan(
            run=_run(step),
            at="support_copilot_turn",
            invocation_overrides={
                "support_copilot_turn": {
                    "input": {
                        "user_prompt": "edited prompt",
                        "message_history": [],
                    }
                }
            },
        )


def test_stale_pydantic_ai_model_request_metadata_rejects_input_override() -> None:
    step = _step(
        name="support_copilot_model_request",
        invocation_id="support_copilot_model_request",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="llm_call",
        run_metadata=_adapter_metadata(kind="model_request", input_slots=["messages"]),
    )

    with pytest.raises(
        KitaruUsageError,
        match="does not expose replayable inputs",
    ):
        build_replay_plan(
            run=_run(step),
            at="support_copilot_model_request",
            invocation_overrides={
                "support_copilot_model_request": {
                    "input": {"messages": [{"role": "user", "content": "edited"}]}
                }
            },
        )


def test_stale_pydantic_ai_turn_metadata_rejects_input_override() -> None:
    step = _step(
        name="support_copilot_turn",
        invocation_id="support_copilot_turn",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="llm_call",
        run_metadata=_adapter_metadata(
            kind="turn",
            input_slots=["user_prompt", "message_history"],
        ),
    )

    with pytest.raises(
        KitaruUsageError,
        match="does not expose replayable inputs",
    ):
        build_replay_plan(
            run=_run(step),
            at="support_copilot_turn",
            invocation_overrides={
                "support_copilot_turn": {
                    "input": {
                        "user_prompt": "edited prompt",
                        "message_history": [],
                    }
                }
            },
        )


def test_user_authored_tool_call_without_tool_args_rejects_input_replay() -> None:
    step = _step(
        name="user_tool_checkpoint",
        invocation_id="user_tool_checkpoint",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
    )

    with pytest.raises(KitaruUsageError, match="does not expose replayable inputs"):
        build_replay_plan(
            run=_run(step),
            at="user_tool_checkpoint",
            invocation_overrides={
                "user_tool_checkpoint": {
                    "input": {"tool_args": {"account_id": "acct-2"}}
                }
            },
        )


def test_recorded_inputs_win_over_tool_call_type_guess() -> None:
    step = _step(
        name="user_tool_checkpoint",
        invocation_id="user_tool_checkpoint",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
        inputs_v2={"payload": []},
    )

    plan = build_replay_plan(
        run=_run(step),
        at="user_tool_checkpoint",
        invocation_overrides={
            "user_tool_checkpoint": {"input": {"account_id": "acct-2"}}
        },
    )

    assert plan.step_input_overrides == {
        "user_tool_checkpoint": {"payload": {"account_id": "acct-2"}}
    }


def test_user_checkpoint_mixed_recorded_and_literal_inputs_pass_through() -> None:
    step = _step(
        name="transform",
        invocation_id="transform",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        inputs_v2={"data": []},
    )

    plan = build_replay_plan(
        run=_run(step),
        at="transform",
        invocation_overrides={
            "transform": {
                "input": {
                    "data": {"account_id": "acct-2"},
                    "config": {"mode": "strict"},
                }
            }
        },
    )

    assert plan.step_input_overrides == {
        "transform": {
            "data": {"account_id": "acct-2"},
            "config": {"mode": "strict"},
        }
    }


def test_explicit_empty_replay_input_slots_disable_type_guess() -> None:
    step = _step(
        name="redacted_tool",
        invocation_id="redacted_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
        run_metadata=_adapter_metadata(input_slots=[]),
    )

    with pytest.raises(KitaruUsageError, match="does not expose replayable inputs"):
        build_replay_plan(
            run=_run(step),
            at="redacted_tool",
            invocation_overrides={"redacted_tool": {"input": {"topic": "new"}}},
        )


def test_malformed_explicit_replay_input_slots_raise_usage_error() -> None:
    step = _step(
        name="broken_tool",
        invocation_id="broken_tool",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="tool_call",
        run_metadata={
            KITARU_METADATA_NAMESPACE: {
                **adapter_checkpoint_metadata(
                    adapter="pydantic_ai",
                    kind="tool_call",
                    input_slots=["tool_args"],
                    output_slots=["output"],
                ),
                REPLAY_INPUT_SLOTS_KEY: "tool_args",
            }
        },
    )

    with pytest.raises(KitaruUsageError, match="Malformed replay input slot metadata"):
        build_replay_plan(
            run=_run(step),
            at="broken_tool",
            invocation_overrides={"broken_tool": {"input": {"topic": "new"}}},
        )


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


def test_code_override_rejects_non_tool_checkpoint() -> None:
    step = _step(
        name="summarize",
        invocation_id="summarize",
        started_at=datetime(2026, 3, 9, 10, 0, tzinfo=UTC),
        step_type="checkpoint",
    )
    with pytest.raises(KitaruUsageError, match="not a tool checkpoint"):
        build_replay_plan(
            run=_run(step),
            at="summarize",
            invocation_overrides={"summarize": {"code": "mocks.summarize"}},
        )


def test_replay_skipped_steps_metadata_is_deterministic() -> None:
    assert replay_skipped_steps_metadata({"write", "fetch"}) == {
        REPLAY_SKIPPED_STEPS_METADATA_KEY: ["fetch", "write"]
    }


def test_parse_replay_skipped_steps_metadata_is_tolerant() -> None:
    assert parse_replay_skipped_steps_metadata({}) == set()
    assert parse_replay_skipped_steps_metadata(
        {REPLAY_SKIPPED_STEPS_METADATA_KEY: '["fetch", "write"]'}
    ) == {"fetch", "write"}
    assert parse_replay_skipped_steps_metadata(
        {REPLAY_SKIPPED_STEPS_METADATA_KEY: ["fetch", 3, None, "write"]}
    ) == {"fetch", "write"}
    assert (
        parse_replay_skipped_steps_metadata(
            {REPLAY_SKIPPED_STEPS_METADATA_KEY: "not json"}
        )
        == set()
    )
    assert (
        parse_replay_skipped_steps_metadata(
            {REPLAY_SKIPPED_STEPS_METADATA_KEY: {"fetch": True}}
        )
        == set()
    )


def test_replay_submission_metadata_uses_pipeline_run_update_for_tags(
    monkeypatch,
) -> None:
    calls: dict[str, Any] = {}
    logged: dict[str, Any] = {}

    class _Store:
        def update_run(self, **kwargs: Any) -> None:
            calls.update(kwargs)

    class _Client:
        zen_store = _Store()

    monkeypatch.setattr("zenml.client.Client", lambda: _Client())

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        logged["run_id"] = run_id
        logged.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    safe_persist_replay_submission_metadata(
        replay_exec_id="replay-a",
        original_exec_id="orig-a",
        submission_id="rs-test",
        tag="batch-eval",
        steps_to_skip={"write", "fetch"},
    )

    assert logged == {
        "run_id": "replay-a",
        "submission_id": "rs-test",
        "original_exec_id": "orig-a",
        "replay_tag": "batch-eval",
        REPLAY_SKIPPED_STEPS_METADATA_KEY: ["fetch", "write"],
    }
    assert calls["run_id"] == "replay-a"
    assert isinstance(calls["run_update"], PipelineRunUpdate)
    assert calls["run_update"].add_tags == ["batch-eval"]


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
