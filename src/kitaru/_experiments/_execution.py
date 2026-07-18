"""Experiment replay submission and durable outcome publication."""

from __future__ import annotations

from collections.abc import Callable, Mapping
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
    _TERMINAL_STATUSES,
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
from kitaru.errors import KitaruStateError
from kitaru.replay import (
    EXPERIMENT_ID_METADATA_KEY,
    EXPERIMENT_PARENT_EXECUTION_ID_METADATA_KEY,
    EXPERIMENT_REPEAT_INDEX_METADATA_KEY,
    EXPERIMENT_ROOT_EXECUTION_ID_METADATA_KEY,
    EXPERIMENT_TAG_PREFIX,
    EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY,
    ExperimentReplayContext,
    ExperimentReplayOutcome,
    ReplayFailureRow,
    ReplayPlanDocument,
    ReplayResultRow,
    ReplaySkippedRow,
    ReplaySubmission,
    persist_and_verify_experiment_membership,
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


def _recovery_binding(spec: Any, client: Any) -> Any:
    from kitaru._agent_registration import RegisteredAgentVersionBinding
    from kitaru._config._agents import (
        _complete_project_metadata,
        _parse_agent_metadata,
    )
    from kitaru._config._projects import _get_project_by_exact_selector

    project = _get_project_by_exact_selector(client, spec.candidate_project_id)
    envelope = _parse_agent_metadata(
        spec.candidate_project_id,
        _complete_project_metadata(project),
    )
    if envelope is None:
        raise KitaruStateError(
            "Experiment recovery requires initialized Agent metadata."
        )
    manifest = envelope.agent_versions.get(spec.candidate_agent_version_id)
    if manifest is None:
        raise KitaruStateError(
            "Experiment recovery could not resolve the candidate AgentVersion."
        )
    return RegisteredAgentVersionBinding(
        project_id=spec.candidate_project_id,
        manifest=manifest,
    )


def _page_items(page: Any) -> list[Any]:
    items = getattr(page, "items", None)
    if items is not None and not callable(items):
        return list(items)
    raise KitaruStateError(
        "Experiment recovery received an unexpected member-run response."
    )


def _recovered_result_status(run: Any) -> Literal["submitted", "completed", "failed"]:
    status = getattr(run, "status", None)
    value = str(getattr(status, "value", status) or "").lower()
    if value == "completed":
        return "completed"
    if value == "failed":
        return "failed"
    return "submitted"


def _has_exact_tag(run: Any, expected: str) -> bool:
    tags = getattr(run, "tags", ()) or ()
    if isinstance(tags, Mapping):
        return expected in tags
    return any(str(getattr(tag, "name", tag)) == expected for tag in tags)


def _recover_verified_results(
    *,
    spec: Any,
    runs: ExperimentRunLookup,
    client: Any,
) -> dict[tuple[str, int], ReplayResultRow]:
    binding = _recovery_binding(spec, client)
    trials = {
        (trial.target_execution_id, trial.repeat_index): trial
        for row in spec.planning_rows
        if row.disposition == "replay"
        for trial in (
            ReplayTrialPlan(target=row, repeat_index=repeat_index)
            for repeat_index in range(spec.repeats)
        )
    }
    recovered: dict[tuple[str, int], ReplayResultRow] = {}
    page_number = 1
    page_size = 50
    while True:
        page_items = _page_items(runs.list(page=page_number, size=page_size))
        for listed_run in page_items:
            run_id = str(getattr(listed_run, "id", "")).strip()
            if not run_id:
                raise KitaruStateError(
                    "Experiment recovery found a tagged child without an execution ID."
                )
            run = client.get_pipeline_run(
                name_id_or_prefix=run_id,
                allow_name_prefix_match=False,
                hydrate=True,
                project=spec.candidate_project_id,
            )
            metadata = getattr(run, "run_metadata", {}) or {}
            if not isinstance(metadata, Mapping):
                raise KitaruStateError(
                    "Experiment recovery found a child without readable metadata."
                )
            target_id = metadata.get(EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY)
            repeat_index = metadata.get(EXPERIMENT_REPEAT_INDEX_METADATA_KEY)
            if (
                not isinstance(target_id, str)
                or isinstance(repeat_index, bool)
                or not isinstance(repeat_index, int)
            ):
                raise KitaruStateError(
                    "Experiment recovery found invalid child trial identity metadata."
                )
            key = (target_id, repeat_index)
            trial = trials.get(key)
            if trial is None:
                raise KitaruStateError(
                    "Experiment recovery found a tagged child that does not match "
                    "the frozen target and repeat plan."
                )
            if key in recovered:
                raise KitaruStateError(
                    "Experiment recovery found multiple children for one frozen trial."
                )
            context = ExperimentReplayContext(
                experiment_id=spec.experiment_id,
                target_execution_id=trial.target_execution_id,
                repeat_index=trial.repeat_index,
                parent_execution_id=trial.parent_execution_id,
                root_execution_id=trial.root_execution_id,
            )
            expected_metadata = {
                EXPERIMENT_ID_METADATA_KEY: context.experiment_id,
                EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY: (
                    context.target_execution_id
                ),
                EXPERIMENT_REPEAT_INDEX_METADATA_KEY: context.repeat_index,
                EXPERIMENT_PARENT_EXECUTION_ID_METADATA_KEY: (
                    context.parent_execution_id
                ),
                EXPERIMENT_ROOT_EXECUTION_ID_METADATA_KEY: context.root_execution_id,
            }
            experiment_tag = f"{EXPERIMENT_TAG_PREFIX}{context.experiment_id}"
            if not _has_exact_tag(run, experiment_tag) or any(
                metadata.get(name) != value for name, value in expected_metadata.items()
            ):
                raise KitaruStateError(
                    "Experiment recovery found a child without exact durable "
                    "membership evidence."
                )
            verification = persist_and_verify_experiment_membership(
                replay_exec_id=run_id,
                context=context,
                binding=binding,
                client=client,
            )
            if not verification.verified:
                raise KitaruStateError(
                    "Experiment recovery could not verify an existing child: "
                    f"{verification.reason or 'unknown verification failure'}"
                )
            recovered[key] = ReplayResultRow(
                original_exec_ref=trial.target_execution_id,
                original_exec_id=trial.target_execution_id,
                replay_exec_id=run_id,
                status=_recovered_result_status(run),
                experiment=ExperimentReplayOutcome(
                    context=context,
                    verification=verification,
                ),
            )
        if len(page_items) < page_size:
            break
        page_number += 1
    return recovered


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
    if reservation.record.status in _TERMINAL_STATUSES:
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

    recovered_results: dict[tuple[str, int], ReplayResultRow] = {}
    if reservation.record.status == "pending":
        transition_experiment_to_running(
            spec.candidate_project_id,
            spec.experiment_id,
            client_factory=client_factory,
        )
    elif reservation.record.status == "running":
        recovery_client = client_factory()
        recovered_results = _recover_verified_results(
            spec=spec,
            runs=runs,
            client=recovery_client,
        )
        expected_replay_trials = (
            sum(row.disposition == "replay" for row in spec.planning_rows)
            * spec.repeats
        )
        if len(recovered_results) != expected_replay_trials:
            raise KitaruStateError(
                "The running experiment has an incomplete durable child set, so "
                "Kitaru cannot safely resubmit the missing trials. A child may "
                "have been created before its membership evidence was persisted."
            )
    else:
        raise KitaruStateError(
            "Experiment recovery does not support status "
            f"{reservation.record.status!r}."
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

            recovered = recovered_results.get((target_id, repeat_index))
            if recovered is not None:
                results.append(recovered)
                compare_ids.extend(
                    [recovered.original_exec_id, recovered.replay_exec_id]
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
