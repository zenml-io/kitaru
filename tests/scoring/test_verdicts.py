"""Truth-table and persistence tests for immutable experiment verdicts."""

from __future__ import annotations

from typing import Any

import pytest

from kitaru._experiments import (
    ExperimentCounts,
    ExperimentRecord,
    attach_experiment_score_aggregate,
    freeze_replay_attempt,
)
from kitaru._inspection_serialization import serialize_experiment
from kitaru.errors import KitaruMetadataConflictError
from kitaru.scoring import (
    ExperimentVerdict,
    ProtectionSnapshot,
    Score,
    ScoreAggregateReference,
    ScoreAttemptAggregate,
    ScoreObservation,
    ScoreObservationOutcome,
    ScoreObservationStatus,
    ScorerSnapshot,
    VerdictPolicy,
    evaluate_verdict,
)
from tests.experiments._helpers import _base_envelope, _draft, _ProjectClient


def _objective(_: object) -> Score:
    return Score(value=1.0)


def _protection(_: object) -> Score:
    return Score(value=1.0)


OBJECTIVE = ScorerSnapshot.from_callable(
    _objective,
    capability="pure",
    name="quality",
)
PROTECTION = ProtectionSnapshot(
    protection_id="safe-output",
    scorer=ScorerSnapshot.from_callable(
        _protection,
        capability="pure",
        name="safe-output",
    ),
)
MANIFEST_HASH = f"sha256:{'3' * 64}"


def _observation(
    snapshot: ScorerSnapshot,
    *,
    value: float | None = 1.0,
    status: ScoreObservationStatus = ScoreObservationStatus.SCORED,
) -> ScoreObservation:
    if status is ScoreObservationStatus.SCORED:
        assert value is not None
        outcome = ScoreObservationOutcome(status=status, score=Score(value=value))
    else:
        outcome = ScoreObservationOutcome(
            status=status,
            reason=f"{status.value.lower()} result",
        )
    return ScoreObservation(
        observation_id=f"obs-{snapshot.name}",
        project_id="project-id",
        execution_id="child-1",
        experiment_id="exp-placeholder",
        scorer=snapshot,
        outcome=outcome,
        completed_at="2026-07-18T10:00:00Z",
        evidence_manifest_sha256=MANIFEST_HASH,
    )


def _frozen_evidence(
    observations: list[ScoreObservation],
    *,
    objective: bool = True,
    protection: bool = True,
    complete_membership: bool = True,
) -> tuple[
    ExperimentRecord,
    ScoreAttemptAggregate,
    ScoreAggregateReference,
    Any,
]:
    policy = VerdictPolicy.create(
        objective=OBJECTIVE if objective else None,
        protections=[PROTECTION] if protection else [],
    )
    assert policy is not None
    snapshots = [
        *([OBJECTIVE] if objective else []),
        *([PROTECTION.scorer] if protection else []),
    ]
    draft = _draft().model_copy(
        update={"scorers": snapshots, "verdict_policy": policy},
        deep=True,
    )
    spec = freeze_replay_attempt(draft).spec
    selected = [
        item.model_copy(update={"experiment_id": spec.experiment_id}, deep=True)
        for item in observations
    ]
    aggregate = ScoreAttemptAggregate.create(
        experiment_id=spec.experiment_id,
        project_id="project-id",
        observations=selected,
        planned=len(snapshots) if complete_membership else 0,
    )
    reference = ScoreAggregateReference(
        artifact_version_id="aggregate-id",
        sha256=aggregate.content_hash,
    )
    pending = ExperimentRecord.pending(spec)
    if complete_membership:
        counts = ExperimentCounts(
            target_count=1,
            intended=1,
            submitted=1,
            verified=1,
        )
        status = "completed"
    else:
        counts = ExperimentCounts(
            target_count=1,
            intended=1,
            submitted=0,
            failed=1,
        )
        status = "failed"
    preview = pending.model_copy(
        update={
            "status": status,
            "started_at": "2026-07-18T09:00:00Z",
            "finished_at": "2026-07-18T10:00:00Z",
            "updated_at": "2026-07-18T10:00:00Z",
            "counts": counts,
            "score_aggregate": reference,
        },
        deep=True,
    )
    verdict = evaluate_verdict(preview, aggregate, policy)
    return preview, aggregate, reference, verdict


def test_verdict_truth_table_pass_and_trustworthy_failures() -> None:
    _, _, _, passed = _frozen_evidence(
        [_observation(OBJECTIVE), _observation(PROTECTION.scorer)]
    )
    _, _, _, objective_failed = _frozen_evidence(
        [_observation(OBJECTIVE, value=0.5), _observation(PROTECTION.scorer)]
    )
    _, _, _, protection_failed = _frozen_evidence(
        [_observation(OBJECTIVE), _observation(PROTECTION.scorer, value=0.0)]
    )

    assert passed.verdict is ExperimentVerdict.PASS
    assert passed.reason_codes == []
    assert objective_failed.verdict is ExperimentVerdict.FAIL
    assert "objective_below_threshold" in {
        item.value for item in objective_failed.reason_codes
    }
    assert protection_failed.verdict is ExperimentVerdict.FAIL
    assert "protection_below_passing_score" in {
        item.value for item in protection_failed.reason_codes
    }


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (ScoreObservationStatus.ABSTAINED, "abstained_observations"),
        (ScoreObservationStatus.BLOCKED, "blocked_observations"),
        (ScoreObservationStatus.ERROR, "error_observations"),
    ],
)
def test_non_numeric_protection_outcomes_hold(
    status: ScoreObservationStatus,
    reason: str,
) -> None:
    _, _, _, result = _frozen_evidence(
        [
            _observation(OBJECTIVE),
            _observation(PROTECTION.scorer, value=None, status=status),
        ]
    )

    assert result.verdict is ExperimentVerdict.HOLD
    assert reason in {item.value for item in result.reason_codes}


def test_hold_takes_precedence_over_a_trustworthy_failure() -> None:
    _, _, _, result = _frozen_evidence(
        [
            _observation(OBJECTIVE, value=0.5),
            _observation(
                PROTECTION.scorer,
                value=None,
                status=ScoreObservationStatus.BLOCKED,
            ),
        ]
    )

    reason_codes = {item.value for item in result.reason_codes}
    assert result.verdict is ExperimentVerdict.HOLD
    assert "objective_below_threshold" in reason_codes
    assert "blocked_observations" in reason_codes


def test_incomplete_membership_and_missing_observations_hold() -> None:
    _, _, _, incomplete = _frozen_evidence(
        [],
        complete_membership=False,
    )
    _, _, _, missing = _frozen_evidence([_observation(OBJECTIVE)])

    assert incomplete.verdict is ExperimentVerdict.HOLD
    assert incomplete.replay_completeness.complete is False
    assert missing.verdict is ExperimentVerdict.HOLD
    assert "missing_scorer_aggregate" in {item.value for item in missing.reason_codes}


def test_duplicate_and_unexpected_scorer_rows_hold() -> None:
    preview, aggregate, _, _ = _frozen_evidence(
        [_observation(OBJECTIVE), _observation(PROTECTION.scorer)]
    )
    duplicate = aggregate.model_copy(
        update={
            "scorer_aggregates": [
                *aggregate.scorer_aggregates,
                aggregate.scorer_aggregates[0],
            ]
        },
        deep=True,
    )
    policy = preview.spec.verdict_policy
    assert policy is not None
    duplicate_result = evaluate_verdict(preview, duplicate, policy)

    extra_snapshot = ScorerSnapshot.from_callable(
        _objective,
        capability="pure",
        name="diagnostic",
    )
    _, _, _, unexpected_result = _frozen_evidence(
        [
            _observation(OBJECTIVE),
            _observation(PROTECTION.scorer),
            _observation(extra_snapshot),
        ]
    )

    assert duplicate_result.verdict is ExperimentVerdict.HOLD
    assert "duplicate_scorer_aggregate" in {
        item.value for item in duplicate_result.reason_codes
    }
    assert unexpected_result.verdict is ExperimentVerdict.HOLD
    assert "unexpected_scorer_aggregate" in {
        item.value for item in unexpected_result.reason_codes
    }


def test_protection_only_attempt_can_pass() -> None:
    _, _, _, result = _frozen_evidence(
        [_observation(PROTECTION.scorer)],
        objective=False,
    )

    assert result.verdict is ExperimentVerdict.PASS
    assert result.objective is None
    assert [item.protection_id for item in result.protections] == ["safe-output"]


def test_catalog_attaches_verdict_once_and_serialization_matches() -> None:
    preview, aggregate, reference, verdict = _frozen_evidence(
        [_observation(OBJECTIVE), _observation(PROTECTION.scorer)]
    )
    terminal = ExperimentRecord.model_validate(
        preview.model_copy(
            update={"score_aggregate": None},
            deep=True,
        ).model_dump(mode="json")
    )
    envelope = _base_envelope().model_copy(
        update={
            "experiments": {terminal.spec.experiment_id: terminal},
            "experiment_idempotency_index": {
                terminal.spec.idempotency_key: terminal.spec.experiment_id
            },
        },
        deep=True,
    )
    client = _ProjectClient(envelope)
    client.raise_after_first_commit = True

    attached = attach_experiment_score_aggregate(
        "project-id",
        terminal.spec.experiment_id,
        aggregate_reference=reference,
        verdict_result=verdict,
        client_factory=lambda: client,
    )
    retried = attach_experiment_score_aggregate(
        "project-id",
        terminal.spec.experiment_id,
        aggregate_reference=reference,
        verdict_result=verdict,
        client_factory=lambda: client,
    )

    assert attached == retried
    assert attached.verdict == verdict
    assert attached.verdict is not None
    assert attached.verdict.content_hash == verdict.content_hash
    assert serialize_experiment(attached)["verdict"] == verdict.model_dump(mode="json")

    conflicting = ScoreAggregateReference(
        artifact_version_id="different-aggregate",
        sha256=aggregate.content_hash,
    )
    with pytest.raises(KitaruMetadataConflictError, match="cannot be replaced"):
        attach_experiment_score_aggregate(
            "project-id",
            terminal.spec.experiment_id,
            aggregate_reference=conflicting,
            client_factory=lambda: client,
        )
