"""Focused contracts for experiment persistence and replay preplanning."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru._experiments import (
    execute_replay_attempt,
    freeze_replay_attempt,
    preplan_replay_attempt,
    reserve_experiment,
    transition_experiment_to_running,
)
from kitaru._experiments._limits import RegressionLimitTracker
from kitaru.errors import KitaruStateError
from kitaru.experiments import RegressionLimits
from kitaru.replay import (
    EXPERIMENT_ID_METADATA_KEY,
    EXPERIMENT_PARENT_EXECUTION_ID_METADATA_KEY,
    EXPERIMENT_REPEAT_INDEX_METADATA_KEY,
    EXPERIMENT_ROOT_EXECUTION_ID_METADATA_KEY,
    EXPERIMENT_TAG_PREFIX,
    EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY,
    ExperimentMemberVerification,
    ExperimentReplayContext,
    ExperimentReplayOutcome,
    ReplayFailureRow,
    ReplayPlanDocument,
    ReplayResultRow,
    ReplaySubmission,
)
from kitaru.scoring import OperationalLimitReason
from tests.experiments._helpers import (
    _base_envelope,
    _binding,
    _ProjectClient,
    _run,
    _RunClient,
    _step,
)


def _attempt_plan(
    *,
    repeats: int = 1,
    on_error: str = "collect",
    name: str | None = None,
) -> Any:
    runs = {
        "first": _run("first", _step("at")),
        "second": _run("second", _step("at")),
    }
    draft = preplan_replay_attempt(
        ["first", "second"],
        binding=_binding(),
        at="at",
        on_error=cast(Any, on_error),
        uncovered_policy="fail",
        idempotency_key=f"attempt-{repeats}-{on_error}-{name}",
        repeats=repeats,
        wait=False,
        name=name,
        client=_RunClient(runs),
        pipeline_verifier=lambda _client, _binding: None,
    )
    return freeze_replay_attempt(draft)


def _experiment_outcome(
    *,
    experiment_id: str,
    trial: Any,
    verified: bool,
    reason: str | None = None,
) -> ExperimentReplayOutcome:
    return ExperimentReplayOutcome(
        context=ExperimentReplayContext(
            experiment_id=experiment_id,
            target_execution_id=trial.target_execution_id,
            repeat_index=trial.repeat_index,
            parent_execution_id=trial.parent_execution_id,
            root_execution_id=trial.root_execution_id,
        ),
        verification=ExperimentMemberVerification(
            verified=verified,
            reason=reason,
        ),
    )


class _RecoveryProjectClient(_ProjectClient):
    def __init__(self) -> None:
        super().__init__(_base_envelope())
        self.member_runs: list[Any] = []

    def add_verified_member(self, *, plan: Any, target_id: str, repeat: int) -> None:
        self.member_runs.append(
            SimpleNamespace(
                id=f"child-{target_id}-{repeat}",
                project_id="project-id",
                status=SimpleNamespace(value="completed"),
                original_run=SimpleNamespace(id=target_id),
                snapshot=SimpleNamespace(
                    project_id="project-id",
                    pipeline_id=plan.spec.candidate_pipeline_id,
                ),
                tags=[f"{EXPERIMENT_TAG_PREFIX}{plan.spec.experiment_id}"],
                run_metadata={
                    EXPERIMENT_ID_METADATA_KEY: plan.spec.experiment_id,
                    EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY: target_id,
                    EXPERIMENT_REPEAT_INDEX_METADATA_KEY: repeat,
                    EXPERIMENT_PARENT_EXECUTION_ID_METADATA_KEY: target_id,
                    EXPERIMENT_ROOT_EXECUTION_ID_METADATA_KEY: target_id,
                },
            )
        )

    def list_pipeline_runs(self, **kwargs: Any) -> Any:
        self.list_run_calls.append(kwargs)
        page = kwargs["page"]
        size = kwargs["size"]
        start = (page - 1) * size
        return SimpleNamespace(items=self.member_runs[start : start + size])

    def get_pipeline_run(self, *, name_id_or_prefix: str, **_kwargs: Any) -> Any:
        return next(run for run in self.member_runs if run.id == name_id_or_prefix)


def test_execute_attempt_completes_named_repeats_and_is_idempotent() -> None:
    plan = _attempt_plan(repeats=2, name="candidate regression")
    client = _ProjectClient(_base_envelope())
    calls: list[tuple[str, int, str]] = []

    def submit_trial(*, trial: Any, replay_plan: Any, submission_id: str) -> Any:
        calls.append((trial.target_execution_id, trial.repeat_index, submission_id))
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag="custom-tag",
            at="at",
            wait=False,
            plan=replay_plan.document,
            results=[
                ReplayResultRow(
                    original_exec_ref=trial.target_execution_id,
                    original_exec_id=trial.target_execution_id,
                    replay_exec_id=(
                        f"child-{trial.target_execution_id}-{trial.repeat_index}"
                    ),
                    status=(
                        "failed"
                        if trial.target_execution_id == "first"
                        and trial.repeat_index == 0
                        else "submitted"
                    ),
                    experiment=_experiment_outcome(
                        experiment_id=plan.spec.experiment_id,
                        trial=trial,
                        verified=True,
                    ),
                )
            ],
        )

    result = execute_replay_attempt(
        plan,
        submit_trial=submit_trial,
        tag="custom-tag",
        client_factory=lambda: client,
    )

    assert result.record.status == "completed"
    assert len(client.update_calls) == 3
    assert result.record.counts.intended == 4
    assert result.record.counts.verified == 4
    assert result.spec.name == "candidate regression"
    assert [(target, repeat) for target, repeat, _ in calls] == [
        ("first", 0),
        ("first", 1),
        ("second", 0),
        ("second", 1),
    ]
    assert len({submission_id for _, _, submission_id in calls}) == 1
    assert result.submission.summary.submitted == 4
    assert result.submission.summary.failed == 1
    assert set(result.to_json()) == {"record", "submission", "regression"}

    result.runs.list(page=2, size=25)
    assert client.list_run_calls == [
        {
            "sort_by": "asc:created",
            "page": 2,
            "size": 25,
            "project": "project-id",
            "hydrate": False,
            "tags": [f"kitaru-experiment:{plan.spec.experiment_id}"],
        }
    ]

    retry = execute_replay_attempt(
        plan,
        submit_trial=lambda **_kwargs: pytest.fail("retry submitted a child"),
        tag="custom-tag",
        client_factory=lambda: client,
    )
    assert retry.record == result.record
    assert retry.submission.submission_id == result.submission.submission_id
    assert retry.submission.results == []


def test_execute_attempt_stops_between_trials_and_freezes_usage() -> None:
    plan = _attempt_plan(repeats=2, name="bounded")
    client = _ProjectClient(_base_envelope())
    tracker = RegressionLimitTracker(
        RegressionLimits(max_trials=4, max_incurred_tokens=10)
    )
    calls: list[str] = []

    def submit_trial(*, trial: Any, replay_plan: Any, submission_id: str) -> Any:
        calls.append(f"{trial.target_execution_id}:{trial.repeat_index}")
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=None,
            at="at",
            wait=True,
            plan=replay_plan.document,
            results=[
                ReplayResultRow(
                    original_exec_ref=trial.target_execution_id,
                    original_exec_id=trial.target_execution_id,
                    replay_exec_id=f"child-{len(calls)}",
                    status="completed",
                    experiment=_experiment_outcome(
                        experiment_id=plan.spec.experiment_id,
                        trial=trial,
                        verified=True,
                    ),
                )
            ],
        )

    def observe_trial(_trial: Any, _child: Any) -> OperationalLimitReason | None:
        return tracker.observe_trial(
            [
                {
                    "cost_policy": "non_reused_is_incurred_v1",
                    "display_cost_usd": 0.02,
                    "records_without_cost_count": 0,
                    "incurred_total_tokens": 5,
                }
            ]
        )

    result = execute_replay_attempt(
        plan,
        submit_trial=submit_trial,
        observe_trial=observe_trial,
        finalize_operational_limit=lambda remaining, started_at: tracker.outcome(
            remaining_trials=remaining,
            started_at=started_at,
        ),
        client_factory=lambda: client,
    )

    assert calls == ["first:0", "first:1"]
    assert result.record.status == "partial"
    assert result.record.counts.submitted == 2
    assert result.record.counts.failed == 2
    assert result.record.operational_limit is not None
    assert (
        result.record.operational_limit.reason_code
        is OperationalLimitReason.TOKEN_LIMIT_REACHED
    )
    assert result.record.operational_limit.facts.incurred_tokens == 10
    assert result.record.operational_limit.facts.remaining_trials == 2
    assert result.record.operational_limit.facts.one_trial_may_overshoot is True

    retry = execute_replay_attempt(
        plan,
        submit_trial=lambda **_kwargs: pytest.fail("retry submitted a child"),
        observe_trial=lambda *_args: pytest.fail("retry observed a child"),
        finalize_operational_limit=lambda _remaining, _started_at: pytest.fail(
            "retry replaced the frozen limit outcome"
        ),
        client_factory=lambda: client,
    )
    assert retry.record.operational_limit == result.record.operational_limit


def test_execute_attempt_recovers_after_lost_reservation_response() -> None:
    plan = _attempt_plan(repeats=1)
    client = _ProjectClient(_base_envelope())
    client.raise_after_first_commit = True
    calls: list[str] = []

    def submit_trial(*, trial: Any, replay_plan: Any, submission_id: str) -> Any:
        calls.append(trial.target_execution_id)
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=None,
            at="at",
            wait=False,
            plan=replay_plan.document,
            results=[
                ReplayResultRow(
                    original_exec_ref=trial.target_execution_id,
                    original_exec_id=trial.target_execution_id,
                    replay_exec_id=f"child-{trial.target_execution_id}",
                    status="submitted",
                    experiment=_experiment_outcome(
                        experiment_id=plan.spec.experiment_id,
                        trial=trial,
                        verified=True,
                    ),
                )
            ],
        )

    result = execute_replay_attempt(
        plan,
        submit_trial=submit_trial,
        client_factory=lambda: client,
    )

    assert calls == ["first", "second"]
    assert result.record.status == "completed"
    assert result.record.counts.verified == 2


def test_execute_attempt_finalizes_fully_submitted_running_attempt() -> None:
    plan = _attempt_plan(repeats=1)
    client = _RecoveryProjectClient()
    reserve_experiment("project-id", plan.spec, client_factory=lambda: client)
    transition_experiment_to_running(
        "project-id",
        plan.spec.experiment_id,
        client_factory=lambda: client,
    )
    client.add_verified_member(plan=plan, target_id="first", repeat=0)
    client.add_verified_member(plan=plan, target_id="second", repeat=0)

    result = execute_replay_attempt(
        plan,
        submit_trial=lambda **_kwargs: pytest.fail("recovery duplicated a child"),
        client_factory=lambda: client,
    )

    assert result.record.status == "completed"
    assert [row.replay_exec_id for row in result.submission.results] == [
        "child-first-0",
        "child-second-0",
    ]


def test_recovered_children_all_contribute_usage_after_frozen_stop_reason() -> None:
    plan = _attempt_plan(repeats=1)
    client = _RecoveryProjectClient()
    reserve_experiment("project-id", plan.spec, client_factory=lambda: client)
    transition_experiment_to_running(
        "project-id",
        plan.spec.experiment_id,
        client_factory=lambda: client,
    )
    client.add_verified_member(plan=plan, target_id="first", repeat=0)
    client.add_verified_member(plan=plan, target_id="second", repeat=0)
    tracker = RegressionLimitTracker(
        RegressionLimits(
            max_trials=2,
            max_cost_usd=0.015,
            max_incurred_tokens=5,
        )
    )
    observed_children: list[str] = []

    def observe_trial(
        _trial: Any, child: ReplaySubmission | None
    ) -> OperationalLimitReason | None:
        assert child is not None
        observed_children.append(child.results[0].replay_exec_id)
        return tracker.observe_trial(
            [
                {
                    "cost_policy": "non_reused_is_incurred_v1",
                    "display_cost_usd": 0.01,
                    "records_without_cost_count": 0,
                    "incurred_total_tokens": 5,
                }
            ]
        )

    result = execute_replay_attempt(
        plan,
        submit_trial=lambda **_kwargs: pytest.fail("recovery duplicated a child"),
        observe_trial=observe_trial,
        finalize_operational_limit=lambda remaining, started_at: tracker.outcome(
            remaining_trials=remaining,
            started_at=started_at,
        ),
        client_factory=lambda: client,
    )

    assert observed_children == ["child-first-0", "child-second-0"]
    assert result.record.operational_limit is not None
    assert (
        result.record.operational_limit.reason_code
        is OperationalLimitReason.TOKEN_LIMIT_REACHED
    )
    assert result.record.operational_limit.facts.submitted_trials == 2
    assert result.record.operational_limit.facts.incurred_tokens == 10
    assert result.record.operational_limit.facts.incurred_cost_usd == 0.02
    assert result.record.operational_limit.facts.remaining_trials == 0


def test_execute_attempt_recovers_frozen_top_trials() -> None:
    draft = preplan_replay_attempt(
        ["top-target"],
        binding=_binding(),
        at="missing",
        on_error="fail",
        uncovered_policy="top",
        idempotency_key="top-recovery",
        repeats=1,
        wait=False,
        client=_RunClient({"top-target": _run("top-target", _step("other"))}),
        pipeline_verifier=lambda _client, _binding: None,
    )
    plan = freeze_replay_attempt(draft)
    assert plan.spec.planning_rows[0].disposition == "top"
    client = _RecoveryProjectClient()
    reserve_experiment("project-id", plan.spec, client_factory=lambda: client)
    transition_experiment_to_running(
        "project-id",
        plan.spec.experiment_id,
        client_factory=lambda: client,
    )
    client.add_verified_member(plan=plan, target_id="top-target", repeat=0)

    result = execute_replay_attempt(
        plan,
        submit_trial=lambda **_kwargs: pytest.fail("recovery duplicated a top child"),
        client_factory=lambda: client,
    )

    assert result.record.status == "completed"
    assert [row.replay_exec_id for row in result.submission.results] == [
        "child-top-target-0"
    ]


def test_execute_attempt_fails_closed_for_partial_running_attempt() -> None:
    plan = _attempt_plan(repeats=1)
    client = _RecoveryProjectClient()
    reserve_experiment("project-id", plan.spec, client_factory=lambda: client)
    transition_experiment_to_running(
        "project-id",
        plan.spec.experiment_id,
        client_factory=lambda: client,
    )
    client.add_verified_member(plan=plan, target_id="first", repeat=0)

    with pytest.raises(KitaruStateError, match="cannot safely resubmit"):
        execute_replay_attempt(
            plan,
            submit_trial=lambda **_kwargs: pytest.fail(
                "recovery duplicated or replaced a child"
            ),
            client_factory=lambda: client,
        )


def test_execute_attempt_keeps_batch_plan_as_unresolved_request() -> None:
    plan = _attempt_plan(repeats=1)
    client = _ProjectClient(_base_envelope())

    def submit_trial(*, trial: Any, submission_id: str, **_kwargs: Any) -> Any:
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=None,
            at="at",
            wait=False,
            plan=ReplayPlanDocument(
                flow_overrides={"resolved_for": trial.target_execution_id}
            ),
            results=[
                ReplayResultRow(
                    original_exec_ref=trial.target_execution_id,
                    original_exec_id=trial.target_execution_id,
                    replay_exec_id=f"child-{trial.target_execution_id}",
                    status="submitted",
                    experiment=_experiment_outcome(
                        experiment_id=plan.spec.experiment_id,
                        trial=trial,
                        verified=True,
                    ),
                )
            ],
        )

    result = execute_replay_attempt(
        plan,
        submit_trial=submit_trial,
        client_factory=lambda: client,
    )

    assert result.submission.plan == ReplayPlanDocument()
    assert [
        row.replay_plan.document
        for row in result.spec.planning_rows
        if row.replay_plan is not None
    ] == [ReplayPlanDocument(), ReplayPlanDocument()]


def test_execute_attempt_preserves_unverified_children_and_fail_fast_rows() -> None:
    partial_plan = _attempt_plan(repeats=1, on_error="collect")
    partial_client = _ProjectClient(_base_envelope())

    def submit_partial(*, trial: Any, replay_plan: Any, submission_id: str) -> Any:
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=None,
            at="at",
            wait=False,
            plan=replay_plan.document,
            results=[
                ReplayResultRow(
                    original_exec_ref=trial.target_execution_id,
                    original_exec_id=trial.target_execution_id,
                    replay_exec_id=f"child-{trial.target_execution_id}",
                    status="submitted",
                    experiment=_experiment_outcome(
                        experiment_id=partial_plan.spec.experiment_id,
                        trial=trial,
                        verified=trial.target_execution_id == "first",
                        reason=(
                            None
                            if trial.target_execution_id == "first"
                            else "metadata did not reread"
                        ),
                    ),
                )
            ],
        )

    partial = execute_replay_attempt(
        partial_plan,
        submit_trial=submit_partial,
        client_factory=lambda: partial_client,
    )
    assert partial.record.status == "partial"
    assert partial.record.counts.submitted == 2
    assert partial.record.counts.verified == 1
    assert partial.record.counts.unverified == 1
    assert partial.record.unverified_children[0].child_execution_id == "child-second"

    failed_plan = _attempt_plan(repeats=1, on_error="fail")
    failed_client = _ProjectClient(_base_envelope())
    submission_calls = 0

    def submit_failure(*, replay_plan: Any, submission_id: str, **_kwargs: Any) -> Any:
        nonlocal submission_calls
        submission_calls += 1
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=None,
            at="at",
            wait=False,
            plan=replay_plan.document,
            failures=[
                ReplayFailureRow(
                    original_exec_ref="first",
                    original_exec_id="first",
                    reason="submission failed",
                )
            ],
        )

    failed = execute_replay_attempt(
        failed_plan,
        submit_trial=submit_failure,
        client_factory=lambda: failed_client,
    )
    assert submission_calls == 1
    assert failed.record.status == "failed"
    assert failed.record.counts.failed == 2
    assert failed.submission.summary.failed == 2


def test_cross_version_baseline_uses_registered_candidate_binding() -> None:
    baseline = _run("baseline", _step("at"))
    draft = preplan_replay_attempt(
        ["baseline"],
        binding=_binding("candidate-pipeline"),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key="request",
        repeats=1,
        wait=False,
        client=_RunClient({"baseline": baseline}),
        pipeline_verifier=lambda _client, binding: (
            binding.pipeline_id == "candidate-pipeline"
        ),
    )

    assert draft.candidate_pipeline_id == "candidate-pipeline"
    assert draft.planning_rows[0].target_execution_id == "baseline"
