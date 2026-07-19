"""Focused contracts for experiment persistence and replay preplanning."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from zenml.models import PipelineRunResponse

from kitaru._experiments import (
    ExperimentPlanningError,
    FrozenReplayPlan,
    preplan_replay_attempt,
)
from kitaru.cohort import CohortResult
from kitaru.errors import (
    KitaruUsageError,
)
from tests.experiments._helpers import (
    _binding,
    _run,
    _RunClient,
    _step,
)


def test_preplanning_collects_all_failures_without_writes_or_submissions() -> None:
    missing = _run("missing", _step("other"))
    running = _run("running", _step("at"), status="running")
    client = _RunClient({"missing": missing, "running": running})

    with pytest.raises(ExperimentPlanningError) as exc_info:
        preplan_replay_attempt(
            ["missing", "running"],
            binding=_binding(),
            at="at",
            on_error="collect",
            uncovered_policy="fail",
            idempotency_key="request",
            repeats=1,
            wait=False,
            client=client,
            pipeline_verifier=lambda _client, _binding: None,
        )

    assert len(exc_info.value.issues) == 2
    assert client.get_calls == ["missing", "running"]
    assert not hasattr(client, "update_project")


@pytest.mark.parametrize(
    ("policy", "disposition", "has_plan"),
    [("skip", "skip", False), ("top", "top", True)],
)
def test_uncovered_policies_are_frozen(
    policy: str,
    disposition: str,
    has_plan: bool,
) -> None:
    run = _run("run-1", _step("first"))
    draft = preplan_replay_attempt(
        ["run-1"],
        binding=_binding(),
        at="missing",
        on_error="collect",
        uncovered_policy=cast(Any, policy),
        idempotency_key=f"request-{policy}",
        repeats=2,
        wait=False,
        client=_RunClient({"run-1": run}),
        pipeline_verifier=lambda _client, _binding: None,
    )

    assert draft.coverage.covered == 0
    assert draft.planning_rows[0].disposition == disposition
    assert (draft.planning_rows[0].replay_plan is not None) is has_plan
    assert [trial.repeat_index for trial in list(draft.iter_trials())] == [0, 1]
    assert "trials" not in draft.model_dump()
    assert "target_execution_ids" not in draft.model_dump()


def test_ambiguous_selector_is_reported_and_checkpoint_free_top_is_planned() -> None:
    at_one = _step("at", invocation_id="at-one")
    at_two = _step("at", invocation_id="at-two")
    ambiguous = cast(
        PipelineRunResponse,
        SimpleNamespace(
            id="ambiguous",
            project_id="project-id",
            status=SimpleNamespace(value="completed"),
            original_run=None,
            orchestrator_environment={},
            steps={"at-one": at_one, "at-two": at_two},
            config=SimpleNamespace(parameters={}),
        ),
    )
    checkpoint_free = _run("checkpoint-free")
    client = _RunClient({"ambiguous": ambiguous, "checkpoint-free": checkpoint_free})

    with pytest.raises(ExperimentPlanningError, match="ambiguous"):
        preplan_replay_attempt(
            ["ambiguous"],
            binding=_binding(),
            at="at",
            on_error="fail",
            uncovered_policy="fail",
            idempotency_key="ambiguous-request",
            repeats=1,
            wait=False,
            client=client,
            pipeline_verifier=lambda _client, _binding: None,
        )

    draft = preplan_replay_attempt(
        ["checkpoint-free"],
        binding=_binding(),
        at="at",
        on_error="collect",
        uncovered_policy="top",
        idempotency_key="top-request",
        repeats=1,
        wait=False,
        client=client,
        pipeline_verifier=lambda _client, _binding: None,
    )
    row = draft.planning_rows[0]
    assert row.disposition == "top"
    assert isinstance(row.replay_plan, FrozenReplayPlan)
    assert row.replay_plan.steps_to_skip == []


def test_single_string_target_is_not_split_and_resolved_duplicates_fail() -> None:
    run = _run("run-1", _step("at"))
    client = _RunClient({"run-1": run, "alias": run})

    draft = preplan_replay_attempt(
        "run-1",
        binding=_binding(),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key="single-request",
        repeats=1,
        wait=False,
        client=client,
        pipeline_verifier=lambda _client, _binding: None,
    )
    assert draft.target_execution_ids == ["run-1"]

    with pytest.raises(ExperimentPlanningError, match="same execution ID"):
        preplan_replay_attempt(
            ["run-1", "alias"],
            binding=_binding(),
            at="at",
            on_error="fail",
            uncovered_policy="fail",
            idempotency_key="duplicate-request",
            repeats=1,
            wait=False,
            client=client,
            pipeline_verifier=lambda _client, _binding: None,
        )


def test_repeat_order_is_target_then_zero_based_repeat() -> None:
    first = _run("first", _step("at"))
    second = _run("second", _step("at"))
    draft = preplan_replay_attempt(
        ["first", "second"],
        binding=_binding(),
        at="at",
        on_error="collect",
        uncovered_policy="fail",
        idempotency_key="request",
        repeats=2,
        wait=False,
        client=_RunClient({"first": first, "second": second}),
        pipeline_verifier=lambda _client, _binding: None,
    )

    assert [
        (trial.target_execution_id, trial.repeat_index)
        for trial in list(draft.iter_trials())
    ] == [("first", 0), ("first", 1), ("second", 0), ("second", 1)]


def test_lineage_cycles_and_foreign_project_targets_are_rejected() -> None:
    first = _run("first", _step("at"), original_run=SimpleNamespace(id="second"))
    second = _run("second", _step("at"), original_run=SimpleNamespace(id="first"))
    foreign = _run("foreign", _step("at"), project_id="other-project")
    client = _RunClient({"first": first, "second": second, "foreign": foreign})

    with pytest.raises(ExperimentPlanningError) as exc_info:
        preplan_replay_attempt(
            ["first", "foreign"],
            binding=_binding(),
            at="at",
            on_error="collect",
            uncovered_policy="fail",
            idempotency_key="request",
            repeats=1,
            wait=False,
            client=client,
            pipeline_verifier=lambda _client, _binding: None,
        )

    reasons = " ".join(issue.reason for issue in exc_info.value.issues)
    assert "lineage cycle" in reasons
    assert "different Agent Project" in reasons


def test_trial_parent_is_target_and_root_is_resolved_across_replay_chain() -> None:
    root = _run("root", _step("at"))
    parent = _run(
        "parent",
        _step("at"),
        original_run=SimpleNamespace(id="root"),
    )
    target = _run(
        "target",
        _step("at"),
        original_run=SimpleNamespace(id="parent"),
    )
    draft = preplan_replay_attempt(
        ["target"],
        binding=_binding(),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key="lineage-request",
        repeats=1,
        wait=False,
        client=_RunClient({"root": root, "parent": parent, "target": target}),
        pipeline_verifier=lambda _client, _binding: None,
    )

    trial = next(draft.iter_trials())
    assert draft.planning_rows[0].parent_execution_id == "parent"
    assert trial.parent_execution_id == "target"
    assert trial.root_execution_id == "root"


def test_cohort_at_and_partial_acknowledgement_are_required() -> None:
    cohort = CohortResult(
        exec_ids=["run-1"],
        flow="support",
        at="at",
        deployment=None,
        deployment_version=None,
        order_by="-started_at",
        scanned=500,
        matched=1,
        partial=True,
        filtered={"checkpoint": 2},
    )
    run = _run("run-1", _step("at"))
    client = _RunClient({"run-1": run})
    kwargs: dict[str, Any] = {
        "binding": _binding(),
        "on_error": "collect",
        "uncovered_policy": "fail",
        "idempotency_key": "request",
        "repeats": 1,
        "wait": False,
        "client": client,
        "pipeline_verifier": lambda _client, _binding: None,
    }

    with pytest.raises(KitaruUsageError, match="must equal"):
        preplan_replay_attempt(cohort, at="other", **kwargs)
    with pytest.raises(KitaruUsageError, match="explicit acknowledgement"):
        preplan_replay_attempt(cohort, at="at", **kwargs)

    draft = preplan_replay_attempt(
        cohort,
        at="at",
        acknowledge_partial_cohort=True,
        **kwargs,
    )
    assert draft.cohort_audit is not None
    assert draft.cohort_audit.partial is True
    assert draft.coverage.covered == 1
