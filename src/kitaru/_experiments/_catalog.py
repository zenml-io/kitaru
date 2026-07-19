"""Project-metadata experiment catalog transitions."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

from zenml.client import Client

from kitaru._experiments._models import (
    _MAX_ISSUE_SUMMARIES,
    _TERMINAL_STATUSES,
    ExperimentCounts,
    ExperimentIssue,
    ExperimentRecord,
    ExperimentReservation,
    ExperimentSpecRecord,
    ImportedReplayMemberEvidence,
    _required_string,
    experiment_request_hash,
)
from kitaru.errors import KitaruMetadataConflictError, KitaruStateError
from kitaru.scoring import (
    ImportedReplayEvidenceSummary,
    OperationalLimitOutcome,
    ScoreAggregateReference,
    VerdictResult,
)


def validate_experiment_record_transition(
    previous: ExperimentRecord,
    desired: ExperimentRecord,
) -> None:
    """Reject replacement, regression, or non-monotonic experiment updates."""
    if desired.spec != previous.spec:
        raise KitaruMetadataConflictError(
            "Experiment metadata reconciliation cannot replace an immutable spec."
        )
    allowed: dict[str, set[str]] = {
        "pending": {"pending", "running"},
        "running": {"running"} | set(_TERMINAL_STATUSES),
        "completed": {"completed"},
        "partial": {"partial"},
        "failed": {"failed"},
        "cancelled": {"cancelled"},
    }
    if desired.status not in allowed[previous.status]:
        raise KitaruMetadataConflictError(
            f"Invalid experiment status transition: {previous.status} -> "
            f"{desired.status}."
        )
    for field_name in (
        "target_count",
        "intended",
        "submitted",
        "verified",
        "skipped",
        "failed",
        "unverified",
    ):
        if getattr(desired.counts, field_name) < getattr(previous.counts, field_name):
            raise KitaruMetadataConflictError(
                "Experiment cached counts cannot move backwards."
            )
    if desired.created_at != previous.created_at:
        raise KitaruMetadataConflictError(
            "Experiment creation timestamps are immutable."
        )
    if (
        previous.score_aggregate is not None
        and desired.score_aggregate != previous.score_aggregate
    ):
        raise KitaruMetadataConflictError(
            "Experiment score aggregate references cannot be replaced."
        )
    if (
        previous.operational_limit is not None
        and desired.operational_limit != previous.operational_limit
    ):
        raise KitaruMetadataConflictError(
            "Experiment operational limit outcomes cannot be replaced."
        )
    if (
        previous.imported_replay_members
        and desired.imported_replay_members != previous.imported_replay_members
    ):
        raise KitaruMetadataConflictError(
            "Imported replay member evidence cannot be replaced."
        )
    if (
        previous.imported_replay_evidence is not None
        and desired.imported_replay_evidence != previous.imported_replay_evidence
    ):
        raise KitaruMetadataConflictError(
            "Imported replay evidence summaries cannot be replaced."
        )
    if previous.verdict is not None and desired.verdict != previous.verdict:
        raise KitaruMetadataConflictError("Experiment verdicts cannot be replaced.")
    for old, new in (
        (previous.errors, desired.errors),
        (previous.skips, desired.skips),
        (previous.unverified_children, desired.unverified_children),
    ):
        if new[: len(old)] != old:
            raise KitaruMetadataConflictError(
                "Experiment issue summaries cannot replace retained entries."
            )


def get_experiment_by_idempotency_key(
    project_id: str,
    idempotency_key: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentRecord | None:
    """Return an existing attempt for an idempotency key without mutation."""
    from kitaru._config._agents import (
        _complete_project_metadata,
        _parse_agent_metadata,
        _validate_exact_project,
    )
    from kitaru._config._projects import _get_project_by_exact_selector

    normalized_project = _required_string(project_id, field_name="Project ID")
    normalized_key = _required_string(
        idempotency_key, field_name="Experiment idempotency key"
    )
    client = client_factory()
    try:
        project = _get_project_by_exact_selector(client, normalized_project)
        _validate_exact_project(project, project_id=normalized_project)
        metadata = _complete_project_metadata(project)
        envelope = _parse_agent_metadata(normalized_project, metadata)
    except Exception:
        return None
    if envelope is None:
        return None
    experiment_id = envelope.experiment_idempotency_index.get(normalized_key)
    if experiment_id is None:
        return None
    return envelope.experiments.get(experiment_id)


def reserve_experiment(
    project_id: str,
    spec: ExperimentSpecRecord,
    *,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentReservation:
    """Reserve one pending attempt or return the idempotent existing attempt."""
    from kitaru._config._agents import reconcile_kitaru_metadata

    if spec.request_hash != experiment_request_hash(spec):
        raise KitaruStateError("Experiment request_hash changed before reservation.")

    created = False

    def mutate(current: Any) -> Any:
        nonlocal created
        if current is None:
            raise KitaruStateError(
                "Experiment catalog writes require an initialized Agent Project."
            )
        existing_id = current.experiment_idempotency_index.get(spec.idempotency_key)
        if existing_id is not None:
            existing = current.experiments[existing_id]
            if existing.spec.request_hash != spec.request_hash:
                raise KitaruMetadataConflictError(
                    "The idempotency key already identifies a different request."
                )
            created = False
            return current

        existing = current.experiments.get(spec.experiment_id)
        if existing is not None:
            if existing.spec.request_hash != spec.request_hash:
                raise KitaruMetadataConflictError(
                    "The experiment ID already identifies a different request."
                )
            created = False
            return current

        experiments = dict(current.experiments)
        experiments[spec.experiment_id] = ExperimentRecord.pending(spec)
        index = dict(current.experiment_idempotency_index)
        index[spec.idempotency_key] = spec.experiment_id
        created = True
        return current.model_copy(
            update={
                "experiments": experiments,
                "experiment_idempotency_index": index,
            },
            deep=True,
        )

    actual = reconcile_kitaru_metadata(
        project_id,
        mutate,
        lambda envelope: (
            envelope.experiment_idempotency_index.get(spec.idempotency_key)
            == spec.experiment_id
            and envelope.experiments.get(spec.experiment_id) is not None
            and envelope.experiments[spec.experiment_id].spec.request_hash
            == spec.request_hash
        ),
        client_factory=client_factory,
    )
    return ExperimentReservation(
        record=actual.experiments[spec.experiment_id],
        created=created,
    )


def transition_experiment_to_running(
    project_id: str,
    experiment_id: str,
    *,
    at: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentRecord:
    """Idempotently move one pending experiment to running."""
    timestamp = at or datetime.now(UTC).isoformat()

    def update(record: ExperimentRecord) -> ExperimentRecord:
        if record.status == "running":
            return record
        if record.status != "pending":
            raise KitaruMetadataConflictError(
                "Only a pending experiment can transition to running."
            )
        return record.model_copy(
            update={
                "status": "running",
                "started_at": timestamp,
                "updated_at": timestamp,
            },
            deep=True,
        )

    return _update_experiment(
        project_id,
        experiment_id,
        update,
        client_factory=client_factory,
    )


def record_experiment_outcomes(
    project_id: str,
    experiment_id: str,
    *,
    counts: ExperimentCounts,
    errors: Sequence[ExperimentIssue] = (),
    skips: Sequence[ExperimentIssue] = (),
    unverified_children: Sequence[ExperimentIssue] = (),
    at: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentRecord:
    """Idempotently publish monotonic cached child membership outcomes."""
    timestamp = at or datetime.now(UTC).isoformat()

    def update(record: ExperimentRecord) -> ExperimentRecord:
        if record.status != "running":
            raise KitaruMetadataConflictError(
                "Child outcomes can only be recorded for a running experiment."
            )
        return record.model_copy(
            update={
                "counts": counts,
                "errors": _merge_issues(record.errors, errors),
                "skips": _merge_issues(record.skips, skips),
                "unverified_children": _merge_issues(
                    record.unverified_children, unverified_children
                ),
                "updated_at": timestamp,
            },
            deep=True,
        )

    return _update_experiment(
        project_id,
        experiment_id,
        update,
        client_factory=client_factory,
    )


def finalize_experiment(
    project_id: str,
    experiment_id: str,
    *,
    status: Literal["completed", "partial", "failed", "cancelled"],
    at: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentRecord:
    """Idempotently finalize one running experiment."""
    timestamp = at or datetime.now(UTC).isoformat()

    def update(record: ExperimentRecord) -> ExperimentRecord:
        if record.status == status:
            return record
        if record.status != "running":
            raise KitaruMetadataConflictError(
                "Only a running experiment can be finalized."
            )
        return record.model_copy(
            update={
                "status": status,
                "finished_at": timestamp,
                "updated_at": timestamp,
            },
            deep=True,
        )

    return _update_experiment(
        project_id,
        experiment_id,
        update,
        client_factory=client_factory,
    )


def finalize_experiment_outcomes(
    project_id: str,
    experiment_id: str,
    *,
    status: Literal["completed", "partial", "failed", "cancelled"],
    counts: ExperimentCounts,
    errors: Sequence[ExperimentIssue] = (),
    skips: Sequence[ExperimentIssue] = (),
    unverified_children: Sequence[ExperimentIssue] = (),
    imported_replay_members: Sequence[ImportedReplayMemberEvidence] = (),
    imported_replay_evidence: ImportedReplayEvidenceSummary | None = None,
    aggregate_reference: ScoreAggregateReference | None = None,
    operational_limit: OperationalLimitOutcome | None = None,
    verdict_result: VerdictResult | None = None,
    at: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentRecord:
    """Atomically publish final outcomes and the terminal catalog status."""
    timestamp = at or datetime.now(UTC).isoformat()

    def update(record: ExperimentRecord) -> ExperimentRecord:
        desired_errors = _merge_issues(record.errors, errors)
        desired_skips = _merge_issues(record.skips, skips)
        desired_unverified = _merge_issues(
            record.unverified_children, unverified_children
        )
        if record.status == status:
            if (
                record.counts == counts
                and record.errors == desired_errors
                and record.skips == desired_skips
                and record.unverified_children == desired_unverified
                and (
                    not imported_replay_members
                    or record.imported_replay_members == list(imported_replay_members)
                )
                and (
                    imported_replay_evidence is None
                    or record.imported_replay_evidence == imported_replay_evidence
                )
                and (
                    aggregate_reference is None
                    or record.score_aggregate == aggregate_reference
                )
                and (
                    operational_limit is None
                    or record.operational_limit == operational_limit
                )
                and (verdict_result is None or record.verdict == verdict_result)
            ):
                return record
            raise KitaruMetadataConflictError(
                "Terminal experiment outcomes cannot be replaced."
            )
        if record.status != "running":
            raise KitaruMetadataConflictError(
                "Only a running experiment can publish terminal outcomes."
            )
        return record.model_copy(
            update={
                "status": status,
                "counts": counts,
                "errors": desired_errors,
                "skips": desired_skips,
                "unverified_children": desired_unverified,
                "imported_replay_members": (
                    list(imported_replay_members)
                    if imported_replay_members
                    else record.imported_replay_members
                ),
                "imported_replay_evidence": (
                    imported_replay_evidence or record.imported_replay_evidence
                ),
                "score_aggregate": aggregate_reference or record.score_aggregate,
                "operational_limit": operational_limit or record.operational_limit,
                "verdict": verdict_result or record.verdict,
                "finished_at": timestamp,
                "updated_at": timestamp,
            },
            deep=True,
        )

    return _update_experiment(
        project_id,
        experiment_id,
        update,
        client_factory=client_factory,
    )


def attach_experiment_score_aggregate(
    project_id: str,
    experiment_id: str,
    *,
    aggregate_reference: ScoreAggregateReference,
    operational_limit: OperationalLimitOutcome | None = None,
    verdict_result: VerdictResult | None = None,
    client_factory: Callable[[], Any] = Client,
) -> ExperimentRecord:
    """Atomically attach immutable score evidence and its optional verdict."""

    def update(record: ExperimentRecord) -> ExperimentRecord:
        if (
            record.score_aggregate == aggregate_reference
            and record.operational_limit == operational_limit
            and record.verdict == verdict_result
        ):
            return record
        if record.score_aggregate is not None or record.verdict is not None:
            raise KitaruMetadataConflictError(
                "Experiment score evidence or verdict cannot be replaced."
            )
        if (
            record.operational_limit is not None
            and record.operational_limit != operational_limit
        ):
            raise KitaruMetadataConflictError(
                "Experiment operational limit outcomes cannot be replaced."
            )
        return record.model_copy(
            update={
                "score_aggregate": aggregate_reference,
                "operational_limit": operational_limit or record.operational_limit,
                "verdict": verdict_result,
            },
            deep=True,
        )

    return _update_experiment(
        project_id,
        experiment_id,
        update,
        client_factory=client_factory,
    )


def _update_experiment(
    project_id: str,
    experiment_id: str,
    update: Callable[[ExperimentRecord], ExperimentRecord],
    *,
    client_factory: Callable[[], Any],
) -> ExperimentRecord:
    from kitaru._config._agents import reconcile_kitaru_metadata

    normalized_id = _required_string(experiment_id, field_name="Experiment ID")

    def mutate(current: Any) -> Any:
        if current is None or normalized_id not in current.experiments:
            raise KitaruStateError(f"Unknown experiment '{normalized_id}'.")
        existing = current.experiments[normalized_id]
        desired = update(existing)
        if desired == existing:
            return current
        experiments = dict(current.experiments)
        experiments[normalized_id] = desired
        return current.model_copy(update={"experiments": experiments}, deep=True)

    actual = reconcile_kitaru_metadata(
        project_id,
        mutate,
        lambda envelope: (
            normalized_id in envelope.experiments
            and envelope.experiments[normalized_id]
            == update(envelope.experiments[normalized_id])
        ),
        client_factory=client_factory,
    )
    return actual.experiments[normalized_id]


def _merge_issues(
    existing: Sequence[ExperimentIssue],
    additions: Sequence[ExperimentIssue],
) -> list[ExperimentIssue]:
    merged = list(existing)
    for issue in additions:
        if issue not in merged and len(merged) < _MAX_ISSUE_SUMMARIES:
            merged.append(issue)
    return merged
