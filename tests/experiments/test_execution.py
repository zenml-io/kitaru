"""Focused contracts for experiment persistence and replay preplanning."""

from __future__ import annotations

from typing import Any, cast

import pytest

from kitaru._experiments import (
    execute_replay_attempt,
    freeze_replay_attempt,
    preplan_replay_attempt,
)
from kitaru.replay import (
    ExperimentMemberVerification,
    ExperimentReplayContext,
    ExperimentReplayOutcome,
    ReplayFailureRow,
    ReplayPlanDocument,
    ReplayResultRow,
    ReplaySubmission,
)
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
    assert set(result.to_json()) == {"record", "submission"}

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
