"""Pure experiment replay planning and freezing."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import NAMESPACE_URL, uuid5

from zenml.artifacts.utils import save_artifact
from zenml.client import Client

from kitaru._experiments._membership import persist_target_membership
from kitaru._experiments._models import (
    CohortAudit,
    CohortRankingRow,
    CoveragePolicy,
    ExperimentExecutable,
    ExperimentIssue,
    ExperimentPlanningError,
    ExperimentSpec,
    ForkCoverage,
    FrozenReplayPlan,
    ReplayAttemptDraft,
    ReplayAttemptPlan,
    ReplayRequestInputs,
    ReplayTrialPlan,
    TargetMembership,
    TargetPlanningRow,
    _required_string,
    _timestamp,
    experiment_request_hash,
)
from kitaru._import_contract import raise_if_imported_execution
from kitaru._run_identity import extract_run_project_identity
from kitaru.errors import KitaruBackendError, KitaruStateError, KitaruUsageError
from kitaru.replay import (
    ReplayPlan,
    build_replay_from_start_plan,
    build_replay_plan,
    replay_at_skip_reason,
    replay_at_status,
)
from kitaru.scoring import ScorerSnapshot

if TYPE_CHECKING:
    from kitaru._agent_registration import RegisteredAgentVersionBinding
    from kitaru.cohort import CohortResult


def _spec_from_draft(
    draft: ReplayAttemptDraft,
    membership: TargetMembership,
) -> ExperimentSpec:
    payload = {
        "experiment_id": draft.experiment_id,
        "name": draft.name,
        "display_name": draft.display_name,
        "suite_key": draft.suite_key,
        "idempotency_key": draft.idempotency_key,
        "created_at": draft.created_at,
        "candidate_project_id": draft.candidate_project_id,
        "candidate_agent_version_id": draft.candidate_agent_version_id,
        "candidate_pipeline_id": draft.candidate_pipeline_id,
        "executable": draft.executable,
        "target_membership": membership,
        "replay_inputs": draft.replay_inputs,
        "at": draft.at,
        "repeats": draft.repeats,
        "wait": draft.wait,
        "on_error": draft.on_error,
        "coverage": draft.coverage,
        "planning_rows": draft.planning_rows,
        "cohort_audit": draft.cohort_audit,
        "scorers": draft.scorers,
    }
    provisional = cast(Any, ExperimentSpec).model_construct(
        schema_version=1,
        kind="replay",
        request_hash="sha256:" + "0" * 64,
        **payload,
    )
    payload["request_hash"] = experiment_request_hash(provisional)
    return ExperimentSpec.model_validate(payload)


def preplan_replay_attempt(
    executions: str | CohortResult | Sequence[str],
    *,
    binding: RegisteredAgentVersionBinding,
    at: str,
    on_error: Literal["collect", "fail"],
    uncovered_policy: CoveragePolicy,
    idempotency_key: str,
    repeats: int,
    wait: bool,
    name: str | None = None,
    suite_key: str | None = None,
    acknowledge_partial_cohort: bool = False,
    flow_overrides: Mapping[str, Any] | None = None,
    checkpoint_overrides: Mapping[str, Any] | None = None,
    invocation_overrides: Mapping[str, Any] | None = None,
    skip: Sequence[str] | None = None,
    created_at: str | None = None,
    client: Any | None = None,
    pipeline_verifier: Callable[[Any, Any], Any] | None = None,
    scorers: Sequence[ScorerSnapshot] = (),
) -> ReplayAttemptDraft:
    """Hydrate and validate every target before any experiment write or child run."""
    from kitaru._agent_registration import verify_registered_pipeline
    from kitaru.cohort import CohortResult, coerce_exec_ids

    normalized_at = _required_string(at, field_name="at")
    normalized_key = _required_string(idempotency_key, field_name="Idempotency key")
    if on_error not in {"collect", "fail"}:
        raise KitaruUsageError("on_error must be explicitly 'collect' or 'fail'.")
    if uncovered_policy not in {"fail", "skip", "top"}:
        raise KitaruUsageError(
            "uncovered_policy must be explicitly 'fail', 'skip', or 'top'."
        )
    if isinstance(repeats, bool) or repeats < 1:
        raise KitaruUsageError("repeats must be >= 1.")

    cohort_audit: CohortAudit | None = None
    cohort_supplied = isinstance(executions, CohortResult)
    if cohort_supplied:
        if executions.at != normalized_at:
            raise KitaruUsageError(
                "The frozen cohort checkpoint must equal the replay checkpoint."
            )
        if executions.partial and not acknowledge_partial_cohort:
            raise KitaruUsageError(
                "A partial cohort scan requires explicit acknowledgement."
            )
        if executions.matched != len(executions.exec_ids):
            raise KitaruUsageError(
                "The frozen cohort matched count must equal its ordered ID list."
            )
        cohort_audit = CohortAudit(
            flow=executions.flow,
            at=executions.at,
            deployment=executions.deployment,
            deployment_version=executions.deployment_version,
            order_by=executions.order_by,
            scanned=executions.scanned,
            matched=executions.matched,
            partial=executions.partial,
            filtered=dict(executions.filtered),
            ranked=[
                CohortRankingRow(execution_id=execution_id, sort_value=value)
                for execution_id, value in executions.ranked
            ],
        )

    target_ids = (
        [executions] if isinstance(executions, str) else coerce_exec_ids(executions)
    )
    if not target_ids:
        raise KitaruUsageError("Pass at least one execution ID.")
    if len(target_ids) != len(set(target_ids)):
        raise KitaruUsageError("Replay attempt targets must be unique.")

    resolved_client = client or Client()
    verifier = pipeline_verifier or verify_registered_pipeline
    verifier(resolved_client, binding)

    request_inputs = ReplayRequestInputs(
        flow_overrides=deepcopy(dict(flow_overrides or {})),
        checkpoint_overrides=deepcopy(dict(checkpoint_overrides or {})),
        invocation_overrides=deepcopy(dict(invocation_overrides or {})),
        skip=[str(item) for item in (skip or [])],
    )
    cache: dict[str, Any] = {}
    rows: list[TargetPlanningRow] = []
    issues: list[ExperimentIssue] = []

    for target_ref in target_ids:
        try:
            run = _load_run(resolved_client, target_ref, cache=cache)
            target_id = str(getattr(run, "id", "")).strip()
            if not target_id:
                raise KitaruStateError(
                    f"Execution '{target_ref}' did not resolve to an ID."
                )
            _validate_native_target(run, project_id=binding.project_id)
            parent_id, root_id = _resolve_lineage(
                run,
                client=resolved_client,
                project_id=binding.project_id,
                cache=cache,
            )
            status = replay_at_status(run=run, at=normalized_at)
            covered = cohort_supplied or status == "present"

            if covered:
                plan = build_replay_plan(
                    run=run,
                    at=normalized_at,
                    flow_overrides=request_inputs.flow_overrides,
                    checkpoint_overrides=request_inputs.checkpoint_overrides,
                    invocation_overrides=request_inputs.invocation_overrides,
                    skip=request_inputs.skip,
                )
                rows.append(
                    TargetPlanningRow(
                        target_execution_id=target_id,
                        parent_execution_id=parent_id,
                        root_execution_id=root_id,
                        checkpoint_covered=True,
                        disposition="replay",
                        replay_plan=_freeze_plan(
                            plan, replay_from_start=False, resolved_at=normalized_at
                        ),
                    )
                )
                continue

            reason = replay_at_skip_reason(run=run, at=normalized_at)
            if uncovered_policy == "fail":
                issues.append(
                    ExperimentIssue(
                        target_execution_id=target_id,
                        reason=reason,
                    )
                )
            elif uncovered_policy == "skip":
                rows.append(
                    TargetPlanningRow(
                        target_execution_id=target_id,
                        parent_execution_id=parent_id,
                        root_execution_id=root_id,
                        checkpoint_covered=False,
                        disposition="skip",
                        reason=reason,
                    )
                )
            else:
                plan = build_replay_from_start_plan(
                    run=run,
                    flow_overrides=request_inputs.flow_overrides,
                    checkpoint_overrides=request_inputs.checkpoint_overrides,
                    invocation_overrides=request_inputs.invocation_overrides,
                    skip=request_inputs.skip,
                )
                rows.append(
                    TargetPlanningRow(
                        target_execution_id=target_id,
                        parent_execution_id=parent_id,
                        root_execution_id=root_id,
                        checkpoint_covered=False,
                        disposition="top",
                        reason=reason,
                        replay_plan=_freeze_plan(
                            plan, replay_from_start=True, resolved_at=None
                        ),
                    )
                )
        except Exception as exc:
            if isinstance(exc, ExperimentPlanningError):
                issues.extend(exc.issues)
            else:
                issues.append(
                    ExperimentIssue(
                        target_execution_id=str(target_ref),
                        reason=str(exc) or type(exc).__name__,
                    )
                )

    resolved_ids = [row.target_execution_id for row in rows]
    if len(resolved_ids) != len(set(resolved_ids)):
        issues.append(
            ExperimentIssue(
                reason="Multiple target selectors resolved to the same execution ID."
            )
        )
    if issues:
        raise ExperimentPlanningError(issues)

    experiment_id = _experiment_id(binding.project_id, normalized_key)
    normalized_name = (
        _required_string(name, field_name="Experiment name")
        if name is not None
        else None
    )
    resolved_suite = (
        _required_string(suite_key, field_name="Suite key")
        if suite_key is not None
        else f"suite-{experiment_id.removeprefix('exp-')}"
    )
    timestamp = created_at or datetime.now(UTC).isoformat()
    _timestamp(timestamp, field_name="created_at")
    return ReplayAttemptDraft(
        experiment_id=experiment_id,
        name=normalized_name,
        display_name=normalized_name or f"Replay {experiment_id[-8:]}",
        suite_key=resolved_suite,
        idempotency_key=normalized_key,
        created_at=timestamp,
        candidate_project_id=binding.project_id,
        candidate_agent_version_id=binding.manifest.agent_version_id,
        candidate_pipeline_id=binding.pipeline_id,
        executable=ExperimentExecutable(entrypoint=binding.manifest.entrypoint),
        replay_inputs=request_inputs,
        at=normalized_at,
        repeats=repeats,
        wait=wait,
        on_error=on_error,
        coverage=ForkCoverage(
            selected=len(rows),
            covered=sum(row.checkpoint_covered for row in rows),
            policy=uncovered_policy,
        ),
        planning_rows=rows,
        cohort_audit=cohort_audit,
        scorers=list(scorers),
    )


def thaw_replay_plan(trial: ReplayTrialPlan) -> ReplayPlan:
    """Rebuild the existing replay-engine input from one generated trial."""
    frozen = trial.replay_plan
    if frozen is None:
        raise KitaruStateError("Skipped replay trials do not have an execution plan.")
    return frozen.thaw(target_execution_id=trial.target_execution_id)


def freeze_replay_attempt(
    draft: ReplayAttemptDraft,
    *,
    client: Any | None = None,
    save_artifact_fn: Callable[..., Any] = save_artifact,
) -> ReplayAttemptPlan:
    """Publish large membership if needed and return the immutable specification."""
    membership = persist_target_membership(
        experiment_id=draft.experiment_id,
        project_id=draft.candidate_project_id,
        execution_ids=draft.target_execution_ids,
        client=client,
        save_artifact_fn=save_artifact_fn,
    )
    return ReplayAttemptPlan(spec=_spec_from_draft(draft, membership))


def _experiment_id(project_id: str, idempotency_key: str) -> str:
    value = uuid5(
        NAMESPACE_URL,
        f"kitaru-experiment:{project_id}:{idempotency_key}",
    )
    return f"exp-{value.hex}"


def _load_run(client: Any, selector: str, *, cache: dict[str, Any]) -> Any:
    if selector in cache:
        return cache[selector]
    try:
        run = client.get_pipeline_run(
            name_id_or_prefix=selector,
            allow_name_prefix_match=False,
            hydrate=True,
        )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load source execution '{selector}' for planning."
        ) from exc
    run_id = str(getattr(run, "id", "")).strip()
    cache[selector] = run
    if run_id:
        cache[run_id] = run
    return run


def _validate_native_target(run: Any, *, project_id: str) -> None:
    raise_if_imported_execution(run, "replayed")
    identity = extract_run_project_identity(run)
    if identity.project_id != project_id:
        raise KitaruUsageError(
            f"Execution '{getattr(run, 'id', '<unknown>')}' belongs to a "
            "different Agent Project."
        )
    raw_status = getattr(run, "status", None)
    status = str(getattr(raw_status, "value", raw_status) or "").lower()
    if status in {"running", "initializing", "provisioning"}:
        raise KitaruUsageError(
            f"Execution '{getattr(run, 'id', '<unknown>')}' is still running."
        )


def _resolve_lineage(
    run: Any,
    *,
    client: Any,
    project_id: str,
    cache: dict[str, Any],
) -> tuple[str | None, str]:
    target_id = str(getattr(run, "id", "")).strip()
    seen = {target_id}
    current = run
    immediate_parent: str | None = None
    root_id = target_id

    while True:
        original = getattr(current, "original_run", None)
        if original is None:
            return immediate_parent, root_id
        original_id = str(getattr(original, "id", "")).strip()
        if not original_id:
            raise KitaruStateError(
                f"Execution '{target_id}' has malformed replay lineage."
            )
        if immediate_parent is None:
            immediate_parent = original_id
        if original_id in seen:
            raise KitaruStateError(
                f"Execution '{target_id}' has a replay lineage cycle."
            )
        seen.add(original_id)
        ancestor = _load_run(client, original_id, cache=cache)
        _validate_native_target(ancestor, project_id=project_id)
        root_id = original_id
        current = ancestor


def _freeze_plan(
    plan: ReplayPlan,
    *,
    replay_from_start: bool,
    resolved_at: str | None,
) -> FrozenReplayPlan:
    return FrozenReplayPlan.freeze(
        plan,
        replay_from_start=replay_from_start,
        resolved_at=resolved_at,
    )
