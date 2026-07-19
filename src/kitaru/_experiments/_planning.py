"""Pure experiment replay planning and freezing."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import NAMESPACE_URL, uuid5

from zenml.artifacts.utils import save_artifact
from zenml.client import Client

from kitaru._experiments._limits import RegressionLimits
from kitaru._experiments._membership import (
    load_target_membership,
    persist_target_membership,
)
from kitaru._experiments._models import (
    CohortAudit,
    CohortRankingRow,
    CoveragePolicy,
    ExperimentExecutable,
    ExperimentIssue,
    ExperimentPlanningError,
    ExperimentRecord,
    ExperimentSpec,
    ForkCoverage,
    FrozenImportedReplayPlan,
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
from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.imports._pydantic_ai_replay import (
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    ImportedReplayMode,
)
from kitaru.imports._replay_evidence import ReplayReadinessStatus
from kitaru.replay import (
    ReplayPlan,
    build_replay_from_start_plan,
    build_replay_plan,
    replay_at_skip_reason,
    replay_at_status,
)
from kitaru.scoring import (
    GroundedPolicySnapshot,
    ProtectionSnapshot,
    ScorerIdentity,
    ScorerSnapshot,
    VerdictPolicy,
)

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
        "source_experiment_id": draft.source_experiment_id,
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
        "grounded_policy": draft.grounded_policy,
        "verdict_policy": draft.verdict_policy,
        "regression_limits": draft.regression_limits,
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
    grounded_policy: GroundedPolicySnapshot | None = None,
    verdict_policy: VerdictPolicy | None = None,
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
        grounded_policy=grounded_policy,
        verdict_policy=verdict_policy,
    )


def preplan_imported_replay_attempt(
    executions: str | Sequence[str],
    *,
    binding: RegisteredAgentVersionBinding,
    mode: ImportedReplayMode,
    boundary: ImportedReplayBoundary | None,
    on_error: Literal["collect", "fail"],
    idempotency_key: str,
    repeats: int,
    wait: bool,
    name: str | None = None,
    suite_key: str | None = None,
    created_at: str | None = None,
    client: Any | None = None,
    scorers: Sequence[ScorerSnapshot] = (),
    grounded_policy: GroundedPolicySnapshot | None = None,
    verdict_policy: VerdictPolicy | None = None,
) -> ReplayAttemptDraft:
    """Validate imported evidence and freeze explicit PydanticAI candidate starts."""
    from kitaru._agent_registration import (
        _registered_imported_replay_compatibility,
        verify_registered_pipeline,
    )
    from kitaru.imports._replay_loading import load_imported_replay_evidence

    normalized_key = _required_string(idempotency_key, field_name="Idempotency key")
    if on_error not in {"collect", "fail"}:
        raise KitaruUsageError("on_error must be explicitly 'collect' or 'fail'.")
    if isinstance(repeats, bool) or repeats < 1:
        raise KitaruUsageError("repeats must be >= 1.")
    target_ids = [executions] if isinstance(executions, str) else list(executions)
    target_ids = [
        _required_string(item, field_name="Imported execution ID")
        for item in target_ids
    ]
    if not target_ids:
        raise KitaruUsageError("Pass at least one imported execution ID.")
    if len(target_ids) != len(set(target_ids)):
        raise KitaruUsageError("Imported replay targets must be unique.")

    if mode is ImportedReplayMode.ROOT_INPUT:
        selected_boundary = boundary or ImportedReplayBoundary(
            kind=ImportedReplayBoundaryKind.ROOT_INPUT
        )
        if selected_boundary.kind is not ImportedReplayBoundaryKind.ROOT_INPUT:
            raise KitaruUsageError(
                "Root-input replay requires the root-input boundary."
            )
    else:
        if boundary is None or boundary.kind is ImportedReplayBoundaryKind.ROOT_INPUT:
            raise KitaruUsageError(
                "Message-history replay requires one explicit complete message or "
                "tool-result boundary."
            )
        selected_boundary = boundary

    resolved_client = client or Client()
    verify_registered_pipeline(resolved_client, binding)
    compatibility = _registered_imported_replay_compatibility(binding.manifest)
    if not compatibility.root_input_supported:
        raise KitaruUsageError(
            "The registered candidate does not support imported root-input replay."
        )
    if mode is ImportedReplayMode.MESSAGE_HISTORY:
        supported = (
            compatibility.tool_result_boundary_supported
            if selected_boundary.kind is ImportedReplayBoundaryKind.TOOL_RESULT
            else compatibility.message_history_supported
        )
        if not supported:
            raise KitaruUsageError(
                "The registered candidate does not support the requested imported "
                "replay boundary."
            )

    rows: list[TargetPlanningRow] = []
    issues: list[ExperimentIssue] = []
    for target_id in target_ids:
        try:
            evidence = load_imported_replay_evidence(target_id, client=resolved_client)
            if evidence.identity.project_id != binding.project_id:
                raise KitaruUsageError(
                    f"Imported execution {target_id!r} belongs to a different "
                    "Agent Project."
                )
            if mode is ImportedReplayMode.ROOT_INPUT:
                if (
                    evidence.readiness.root_input_candidate_rerun.status
                    is not ReplayReadinessStatus.READY
                ):
                    raise KitaruUsageError(
                        f"Imported execution '{target_id}' has no complete root input."
                    )
            else:
                from kitaru.adapters.pydantic_ai._imported_replay import (
                    prepare_imported_replay_history,
                )

                prepare_imported_replay_history(
                    evidence,
                    boundary=selected_boundary,
                )
            rows.append(
                TargetPlanningRow(
                    target_execution_id=evidence.identity.execution_id,
                    parent_execution_id=evidence.identity.execution_id,
                    root_execution_id=evidence.identity.execution_id,
                    checkpoint_covered=True,
                    disposition="imported",
                    replay_plan=FrozenImportedReplayPlan(
                        mode=mode,
                        boundary=selected_boundary,
                        evidence_identity=evidence.identity,
                    ),
                )
            )
        except Exception as exc:
            issues.append(
                ExperimentIssue(
                    target_execution_id=target_id,
                    reason=str(exc) or type(exc).__name__,
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
        display_name=normalized_name or f"Imported replay {experiment_id[-8:]}",
        suite_key=resolved_suite,
        idempotency_key=normalized_key,
        created_at=timestamp,
        candidate_project_id=binding.project_id,
        candidate_agent_version_id=binding.manifest.agent_version_id,
        candidate_pipeline_id=binding.pipeline_id,
        executable=ExperimentExecutable(entrypoint=binding.manifest.entrypoint),
        replay_inputs=ReplayRequestInputs(),
        at=selected_boundary.kind.value,
        repeats=repeats,
        wait=wait,
        on_error=on_error,
        coverage=ForkCoverage(
            selected=len(rows),
            covered=len(rows),
            policy="fail",
        ),
        planning_rows=rows,
        cohort_audit=None,
        scorers=list(scorers),
        grounded_policy=grounded_policy,
        verdict_policy=verdict_policy,
    )


def _validate_suite_rerun_request(
    source: ExperimentRecord,
    *,
    binding: RegisteredAgentVersionBinding,
    idempotency_key: str,
    repeats: int,
    limits: RegressionLimits | None,
) -> str:
    source_spec = cast(ExperimentSpec, source.spec)
    if source_spec.kind != "replay":
        raise KitaruUsageError("Suite reruns require a replay experiment source.")
    if source.status not in {"completed", "partial", "failed", "cancelled"}:
        raise KitaruUsageError("Suite reruns require a terminal source attempt.")
    if source_spec.candidate_project_id != binding.project_id:
        raise KitaruUsageError(
            "The source attempt belongs to a different Agent Project."
        )
    normalized_key = _required_string(idempotency_key, field_name="Idempotency key")
    if normalized_key == source_spec.idempotency_key:
        raise KitaruUsageError("A suite rerun requires a new idempotency key.")
    if isinstance(repeats, bool) or repeats < 1:
        raise KitaruUsageError("repeats must be >= 1.")
    trial_count = source_spec.target_membership.count * repeats
    if limits is not None and trial_count > limits.max_trials:
        raise KitaruUsageError(
            f"Suite rerun plans {trial_count} trials, exceeding "
            f"max_trials={limits.max_trials}. No experiment was created."
        )
    return normalized_key


def _suite_rerun_scoring_contract(
    source: ExperimentRecord,
    *,
    binding: RegisteredAgentVersionBinding,
    objective_scorers: Sequence[ScorerSnapshot],
    protections: Sequence[ProtectionSnapshot],
) -> tuple[list[ScorerSnapshot], VerdictPolicy | None]:
    source_spec = cast(ExperimentSpec, source.spec)
    objective_items = list(objective_scorers)
    if source_spec.verdict_policy is None and source_spec.scorers:
        raise KitaruUsageError(
            "This suite predates verdict policies but has a frozen scorer contract. "
            "Create a new scored replay suite before rerunning it."
        )
    source_objective = (
        None
        if source_spec.verdict_policy is None
        else source_spec.verdict_policy.objective
    )
    if source_objective is None:
        if objective_items:
            raise KitaruUsageError(
                "This suite has no objective scorer contract to rerun."
            )
        objective_snapshot = None
        objective_minimum = None
    else:
        if len(objective_items) != 1:
            raise KitaruUsageError(
                "This suite rerun requires exactly one current objective "
                "scorer callable."
            )
        objective_snapshot = objective_items[0]
        if ScorerIdentity.from_snapshot(objective_snapshot) != source_objective.scorer:
            raise KitaruMetadataConflictError(
                "The current objective scorer does not match the source suite revision "
                "and configuration."
            )
        objective_minimum = source_objective.minimum_mean

    protection_items = list(protections)
    actual_protections = {item.protection_id: item for item in protection_items}
    if actual_protections != binding.manifest.protections:
        raise KitaruMetadataConflictError(
            "Current protection callables do not match the registered candidate "
            "AgentVersion."
        )
    verdict_policy = VerdictPolicy.create(
        objective=objective_snapshot,
        minimum_mean=objective_minimum,
        protections=protection_items,
        imported_replay=(
            None
            if source_spec.verdict_policy is None
            else source_spec.verdict_policy.imported_replay
        ),
    )
    scorer_snapshots = [
        *([] if objective_snapshot is None else [objective_snapshot]),
        *(item.scorer for item in protection_items),
    ]
    return scorer_snapshots, verdict_policy


def validate_existing_suite_rerun(
    existing: ExperimentRecord,
    source: ExperimentRecord,
    *,
    binding: RegisteredAgentVersionBinding,
    idempotency_key: str,
    repeats: int,
    objective_scorers: Sequence[ScorerSnapshot] = (),
    protections: Sequence[ProtectionSnapshot] = (),
    limits: RegressionLimits | None = None,
) -> ReplayAttemptPlan:
    """Validate retry inputs without rebuilding already-frozen replay plans."""
    source_spec = cast(ExperimentSpec, source.spec)
    normalized_key = _validate_suite_rerun_request(
        source,
        binding=binding,
        idempotency_key=idempotency_key,
        repeats=repeats,
        limits=limits,
    )
    scorer_snapshots, verdict_policy = _suite_rerun_scoring_contract(
        source,
        binding=binding,
        objective_scorers=objective_scorers,
        protections=protections,
    )
    spec = existing.spec
    if spec.kind != "replay":
        raise KitaruMetadataConflictError(
            "The idempotency key already belongs to a non-replay attempt."
        )
    expected = {
        "source_experiment_id": source_spec.experiment_id,
        "idempotency_key": normalized_key,
        "candidate_project_id": binding.project_id,
        "candidate_agent_version_id": binding.manifest.agent_version_id,
        "candidate_pipeline_id": binding.pipeline_id,
        "executable": ExperimentExecutable(entrypoint=binding.manifest.entrypoint),
        "name": source_spec.name,
        "display_name": source_spec.display_name,
        "suite_key": source_spec.suite_key,
        "target_membership": source_spec.target_membership,
        "replay_inputs": source_spec.replay_inputs,
        "at": source_spec.at,
        "repeats": repeats,
        "wait": True,
        "on_error": source_spec.on_error,
        "coverage": source_spec.coverage,
        "planning_rows": source_spec.planning_rows,
        "cohort_audit": source_spec.cohort_audit,
        "scorers": scorer_snapshots,
        "grounded_policy": source_spec.grounded_policy,
        "verdict_policy": verdict_policy,
        "regression_limits": limits,
    }
    if any(
        getattr(spec, field_name) != value for field_name, value in expected.items()
    ):
        raise KitaruMetadataConflictError(
            "The idempotent suite rerun request conflicts with its existing "
            "immutable attempt."
        )
    return ReplayAttemptPlan(spec=spec)


def plan_suite_rerun(
    source: ExperimentRecord,
    *,
    binding: RegisteredAgentVersionBinding,
    idempotency_key: str,
    repeats: int = 1,
    objective_scorers: Sequence[ScorerSnapshot] = (),
    protections: Sequence[ProtectionSnapshot] = (),
    limits: RegressionLimits | None = None,
    created_at: str | None = None,
    client: Any | None = None,
    pipeline_verifier: Callable[[Any, Any], Any] | None = None,
) -> ReplayAttemptPlan:
    """Create a new immutable attempt from one verified replay suite source."""
    from kitaru._agent_registration import verify_registered_pipeline

    source_spec = cast(ExperimentSpec, source.spec)
    normalized_key = _validate_suite_rerun_request(
        source,
        binding=binding,
        idempotency_key=idempotency_key,
        repeats=repeats,
        limits=limits,
    )

    resolved_client = client or Client()
    verifier = pipeline_verifier or verify_registered_pipeline
    verifier(resolved_client, binding)
    target_ids = load_target_membership(
        source_spec.target_membership,
        project_id=binding.project_id,
        client=resolved_client,
    )
    row_ids = [row.target_execution_id for row in source_spec.planning_rows]
    if row_ids != target_ids:
        raise KitaruMetadataConflictError(
            "The source planning rows do not match its verified target membership."
        )
    if any(
        row.disposition in {"replay", "top"} and row.replay_plan is None
        for row in source_spec.planning_rows
    ):
        raise KitaruMetadataConflictError(
            "The source attempt is missing a frozen per-target replay plan."
        )

    scorer_snapshots, verdict_policy = _suite_rerun_scoring_contract(
        source,
        binding=binding,
        objective_scorers=objective_scorers,
        protections=protections,
    )
    timestamp = created_at or datetime.now(UTC).isoformat()
    _timestamp(timestamp, field_name="created_at")
    experiment_id = _experiment_id(binding.project_id, normalized_key)
    if experiment_id == source_spec.experiment_id:
        raise KitaruUsageError("A suite rerun must create a new experiment attempt.")

    draft = ReplayAttemptDraft(
        experiment_id=experiment_id,
        name=source_spec.name,
        display_name=source_spec.display_name,
        suite_key=source_spec.suite_key,
        source_experiment_id=source_spec.experiment_id,
        idempotency_key=normalized_key,
        created_at=timestamp,
        candidate_project_id=binding.project_id,
        candidate_agent_version_id=binding.manifest.agent_version_id,
        candidate_pipeline_id=binding.pipeline_id,
        executable=ExperimentExecutable(entrypoint=binding.manifest.entrypoint),
        replay_inputs=deepcopy(source_spec.replay_inputs),
        at=source_spec.at,
        repeats=repeats,
        wait=True,
        on_error=source_spec.on_error,
        coverage=deepcopy(source_spec.coverage),
        planning_rows=deepcopy(source_spec.planning_rows),
        cohort_audit=deepcopy(source_spec.cohort_audit),
        scorers=scorer_snapshots,
        grounded_policy=deepcopy(source_spec.grounded_policy),
        verdict_policy=verdict_policy,
        regression_limits=limits,
    )
    return ReplayAttemptPlan(
        spec=_spec_from_draft(draft, deepcopy(source_spec.target_membership))
    )


def thaw_replay_plan(trial: ReplayTrialPlan) -> ReplayPlan:
    """Rebuild the existing replay-engine input from one generated trial."""
    frozen = trial.replay_plan
    if frozen is None:
        raise KitaruStateError("Skipped replay trials do not have an execution plan.")
    if not isinstance(frozen, FrozenReplayPlan):
        raise KitaruStateError(
            "Imported replay trials execute through their registered candidate."
        )
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
