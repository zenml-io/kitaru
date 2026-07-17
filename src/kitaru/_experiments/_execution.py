"""Experiment replay submission and durable outcome publication."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any, Literal

from zenml.client import Client

from kitaru._experiments._catalog import (
    finalize_experiment_outcomes,
    reserve_experiment,
    transition_experiment_to_running,
)
from kitaru._experiments._models import (
    _MAX_ISSUE_SUMMARIES,
    ExperimentCounts,
    ExperimentIssue,
    ReplayAttemptPlan,
    ReplayTrialPlan,
    _required_string,
)
from kitaru._experiments._views import (
    ExperimentReplayResult,
    ExperimentRunLookup,
)
from kitaru.replay import (
    ReplayFailureRow,
    ReplayPlanDocument,
    ReplayResultRow,
    ReplaySkippedRow,
    ReplaySubmission,
    safe_compare_url_for_executions,
)


def experiment_submission_id(experiment_id: str) -> str:
    """Return the stable transport correlation ID for one experiment attempt."""
    normalized = _required_string(experiment_id, field_name="Experiment ID")
    return f"rs-{normalized}"


def _append_catalog_issue(
    issues: list[ExperimentIssue], issue: ExperimentIssue
) -> None:
    if issue not in issues and len(issues) < _MAX_ISSUE_SUMMARIES:
        issues.append(issue)


def execute_replay_attempt(
    plan: ReplayAttemptPlan,
    *,
    submit_trial: Callable[..., ReplaySubmission],
    tag: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentReplayResult:
    """Reserve, submit, verify, and finalize one registered replay attempt."""
    spec = plan.spec
    submission_id = experiment_submission_id(spec.experiment_id)
    plan_document = ReplayPlanDocument(
        flow_overrides=deepcopy(spec.replay_inputs.flow_overrides),
        checkpoint_overrides=deepcopy(spec.replay_inputs.checkpoint_overrides),
        invocation_overrides=deepcopy(spec.replay_inputs.invocation_overrides),
        skip=list(spec.replay_inputs.skip),
    )
    reservation = reserve_experiment(
        spec.candidate_project_id,
        spec,
        client_factory=client_factory,
    )
    runs = ExperimentRunLookup(
        experiment_id=spec.experiment_id,
        project_id=spec.candidate_project_id,
        _client_factory=client_factory,
    )
    if not reservation.created:
        return ExperimentReplayResult(
            record=reservation.record,
            submission=ReplaySubmission.create(
                submission_id=submission_id,
                tag=tag,
                at=spec.at,
                wait=spec.wait,
                plan=plan_document,
            ),
            runs=runs,
        )

    transition_experiment_to_running(
        spec.candidate_project_id,
        spec.experiment_id,
        client_factory=client_factory,
    )

    results: list[ReplayResultRow] = []
    failures: list[ReplayFailureRow] = []
    skipped: list[ReplaySkippedRow] = []
    errors: list[ExperimentIssue] = []
    skips: list[ExperimentIssue] = []
    unverified: list[ExperimentIssue] = []
    compare_ids: list[str] = []
    planning_by_target = {row.target_execution_id: row for row in spec.planning_rows}
    stop_submissions = False

    for row in spec.planning_rows:
        target_id = row.target_execution_id
        replay_plan = (
            row.replay_plan.thaw(target_execution_id=target_id)
            if row.replay_plan is not None
            else None
        )
        for repeat_index in range(spec.repeats):
            trial = ReplayTrialPlan(target=row, repeat_index=repeat_index)
            if trial.disposition == "skip":
                reason = planning_by_target[target_id].reason or "Target was skipped."
                skipped.append(
                    ReplaySkippedRow(
                        original_exec_ref=target_id,
                        original_exec_id=target_id,
                        reason=reason,
                    )
                )
                _append_catalog_issue(
                    skips,
                    ExperimentIssue(
                        target_execution_id=target_id,
                        repeat_index=trial.repeat_index,
                        reason=reason,
                    ),
                )
                continue

            if stop_submissions:
                reason = "Not submitted after an earlier fail-fast replay error."
                failures.append(
                    ReplayFailureRow(
                        original_exec_ref=target_id,
                        original_exec_id=target_id,
                        reason=reason,
                    )
                )
                _append_catalog_issue(
                    errors,
                    ExperimentIssue(
                        target_execution_id=target_id,
                        repeat_index=trial.repeat_index,
                        reason=reason,
                    ),
                )
                continue

            assert replay_plan is not None
            try:
                child = submit_trial(
                    trial=trial,
                    replay_plan=replay_plan,
                    submission_id=submission_id,
                )
            except Exception as exc:
                reason = str(exc) or type(exc).__name__
                failures.append(
                    ReplayFailureRow(
                        original_exec_ref=target_id,
                        original_exec_id=target_id,
                        reason=reason,
                    )
                )
                _append_catalog_issue(
                    errors,
                    ExperimentIssue(
                        target_execution_id=target_id,
                        repeat_index=trial.repeat_index,
                        reason=reason,
                    ),
                )
                if spec.on_error == "fail":
                    stop_submissions = True
                continue

            results.extend(child.results)
            skipped.extend(child.skipped)
            failures.extend(child.failures)
            for result_row in child.results:
                compare_ids.extend(
                    [result_row.original_exec_id, result_row.replay_exec_id]
                )
                if result_row.membership_verified is not True:
                    _append_catalog_issue(
                        unverified,
                        ExperimentIssue(
                            target_execution_id=target_id,
                            repeat_index=trial.repeat_index,
                            child_execution_id=result_row.replay_exec_id,
                            reason=(
                                result_row.membership_error
                                or "Experiment membership could not be verified."
                            ),
                        ),
                    )
            for failure_row in child.failures:
                _append_catalog_issue(
                    errors,
                    ExperimentIssue(
                        target_execution_id=target_id,
                        repeat_index=trial.repeat_index,
                        reason=failure_row.reason,
                    ),
                )
            if child.failures and spec.on_error == "fail":
                stop_submissions = True

    counts = ExperimentCounts(
        target_count=spec.target_membership.count,
        intended=spec.target_membership.count * spec.repeats,
        submitted=len(results),
        verified=sum(row.membership_verified is True for row in results),
        skipped=len(skipped),
        failed=len(failures),
        unverified=sum(row.membership_verified is not True for row in results),
    )
    if counts.verified == counts.intended:
        terminal_status: Literal["completed", "partial", "failed"] = "completed"
    elif counts.verified > 0:
        terminal_status = "partial"
    else:
        terminal_status = "failed"
    record = finalize_experiment_outcomes(
        spec.candidate_project_id,
        spec.experiment_id,
        status=terminal_status,
        counts=counts,
        errors=errors,
        skips=skips,
        unverified_children=unverified,
        client_factory=client_factory,
    )
    submission = ReplaySubmission.create(
        submission_id=submission_id,
        tag=tag,
        at=spec.at,
        wait=spec.wait,
        plan=plan_document,
        results=results,
        failures=failures,
        skipped=skipped,
        compare_url=safe_compare_url_for_executions(compare_ids),
    )
    return ExperimentReplayResult(record=record, submission=submission, runs=runs)
