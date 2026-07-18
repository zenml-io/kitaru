"""Focused contracts for experiment persistence and replay preplanning."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from kitaru._config._agents import (
    _agent_info_from_project_model,
)
from kitaru._experiments import (
    ArtifactTargetMembership,
    Experiment,
    ExperimentCounts,
    ExperimentRecord,
    InlineTargetMembership,
    finalize_experiment,
    freeze_replay_attempt,
    load_target_membership,
    persist_target_membership,
    record_experiment_outcomes,
    reserve_experiment,
    target_manifest_payload,
    transition_experiment_to_running,
)
from kitaru._inspection_serialization import serialize_experiment
from kitaru.errors import (
    KitaruMetadataConflictError,
    KitaruUsageError,
)
from kitaru.scoring import (
    GroundedPolicySnapshot,
    ProtectionSnapshot,
    ScorerSnapshot,
    VerdictPolicy,
)
from tests.experiments._helpers import (
    _Artifact,
    _ArtifactClient,
    _base_envelope,
    _draft,
    _plan,
    _ProjectClient,
    _stored_metadata,
)


def test_hydrated_agent_lists_newest_first_and_rejects_ambiguous_names() -> None:
    older = ExperimentRecord.pending(
        _plan(
            idempotency_key="older",
            name="Regression",
            created_at="2026-07-17T09:00:00Z",
        ).spec
    )
    newer = ExperimentRecord.pending(
        _plan(
            idempotency_key="newer",
            name="Regression",
            created_at="2026-07-17T10:00:00Z",
        ).spec
    )
    envelope = _base_envelope().model_copy(
        update={
            "experiments": {
                older.spec.experiment_id: older,
                newer.spec.experiment_id: newer,
            },
            "experiment_idempotency_index": {
                older.spec.idempotency_key: older.spec.experiment_id,
                newer.spec.idempotency_key: newer.spec.experiment_id,
            },
        },
        deep=True,
    )
    agent = _agent_info_from_project_model(
        SimpleNamespace(
            id="project-id",
            name="support-agent",
            display_name="Support Agent",
            description="Support",
            project_metadata=_stored_metadata(envelope),
        ),
        active_project_id="project-id",
    )

    assert agent is not None
    assert [record.spec.experiment_id for record in agent.list_experiments()] == [
        newer.spec.experiment_id,
        older.spec.experiment_id,
    ]
    assert agent.get_experiment(older.spec.experiment_id) == older
    assert agent.get_experiment(newer.spec.suite_key) == newer
    with pytest.raises(KitaruUsageError, match="ambiguous"):
        agent.get_experiment("Regression")


def test_exact_suite_and_attempt_selection_ignores_display_names() -> None:
    older_pending = ExperimentRecord.pending(
        _plan(
            idempotency_key="suite-older",
            name="Shared display",
            suite_key="regression-suite",
            created_at="2026-07-17T09:00:00Z",
        ).spec
    )
    older = ExperimentRecord.model_validate(
        {
            **older_pending.model_dump(mode="json"),
            "status": "completed",
            "started_at": "2026-07-17T09:01:00Z",
            "finished_at": "2026-07-17T09:02:00Z",
            "updated_at": "2026-07-17T09:02:00Z",
            "counts": {
                **older_pending.counts.model_dump(mode="json"),
                "submitted": older_pending.counts.intended,
                "verified": older_pending.counts.intended,
            },
        }
    )
    newest_terminal_pending = ExperimentRecord.pending(
        _plan(
            idempotency_key="suite-newest-terminal",
            name="Shared display",
            suite_key="regression-suite",
            created_at="2026-07-17T10:00:00Z",
        ).spec
    )
    newest_terminal = ExperimentRecord.model_validate(
        {
            **newest_terminal_pending.model_dump(mode="json"),
            "status": "completed",
            "started_at": "2026-07-17T10:01:00Z",
            "finished_at": "2026-07-17T10:02:00Z",
            "updated_at": "2026-07-17T10:02:00Z",
            "counts": {
                **newest_terminal_pending.counts.model_dump(mode="json"),
                "submitted": newest_terminal_pending.counts.intended,
                "verified": newest_terminal_pending.counts.intended,
            },
        }
    )
    newest_pending = ExperimentRecord.pending(
        _plan(
            idempotency_key="suite-running",
            name="Shared display",
            suite_key="regression-suite",
            created_at="2026-07-17T11:00:00Z",
        ).spec
    )
    envelope = _base_envelope().model_copy(
        update={
            "experiments": {
                record.spec.experiment_id: record
                for record in (older, newest_terminal, newest_pending)
            },
            "experiment_idempotency_index": {
                record.spec.idempotency_key: record.spec.experiment_id
                for record in (older, newest_terminal, newest_pending)
            },
        },
        deep=True,
    )
    agent = _agent_info_from_project_model(
        SimpleNamespace(
            id="project-id",
            name="support-agent",
            display_name="Support Agent",
            description="Support",
            project_metadata=_stored_metadata(envelope),
        ),
        active_project_id="project-id",
    )

    assert agent is not None
    assert agent.list_suite_attempts("regression-suite") == [
        newest_pending,
        newest_terminal,
        older,
    ]
    assert (
        agent.get_experiment_attempt(newest_pending.spec.experiment_id)
        == newest_pending
    )
    assert agent.resolve_experiment_source("regression-suite") == newest_terminal
    assert (
        agent.get_experiment_by_idempotency_key(newest_terminal.spec.idempotency_key)
        == newest_terminal
    )
    assert agent.resolve_experiment_source(older.spec.experiment_id) == older
    assert agent.resolve_suite_rerun_request(
        "regression-suite",
        "new-rerun-key",
    ) == (None, newest_terminal)
    with pytest.raises(KitaruUsageError, match="ambiguous"):
        agent.get_experiment("Shared display")


def test_artifact_backed_membership_is_hash_verified_for_reads() -> None:
    execution_ids = [f"run-{index}" for index in range(501)]
    _, content_hash = target_manifest_payload(execution_ids)
    membership = ArtifactTargetMembership(
        artifact_version_id="artifact-id",
        count=len(execution_ids),
        sha256=content_hash,
    )
    client = _ArtifactClient()
    artifact = _Artifact("artifact-id", "targets", execution_ids)
    client.artifacts.append(artifact)

    assert (
        load_target_membership(
            membership,
            project_id="project-id",
            client=client,
        )
        == execution_ids
    )

    artifact._value[-1] = "tampered"
    with pytest.raises(KitaruMetadataConflictError, match="SHA-256"):
        load_target_membership(
            membership,
            project_id="project-id",
            client=client,
        )


def test_frontend_experiment_serialization_is_stable_and_omits_member_runs() -> None:
    record = ExperimentRecord.pending(_plan(name="Regression").spec)
    client = _ProjectClient(_base_envelope())
    experiment = Experiment(
        record=record,
        runs=cast(Any, SimpleNamespace()),
    )

    first = serialize_experiment(experiment)
    second = serialize_experiment(
        ExperimentRecord.model_validate(record.model_dump(mode="json"))
    )

    assert first == second
    assert json.loads(json.dumps(first, sort_keys=True)) == first
    assert first["experiment_id"] == record.spec.experiment_id
    assert first["candidate_agent_version_id"] == "pipeline-id"
    assert first["coverage"] == {"selected": 1, "covered": 1, "policy": "fail"}
    assert first["target_membership"]["storage"] == "inline"
    assert "runs" not in first
    assert "spec" not in first
    assert client.list_run_calls == []


def _policy_score(_: object) -> bool:
    return True


def test_verdict_policy_round_trip_and_request_hash_sensitivity() -> None:
    objective = ScorerSnapshot.from_callable(
        _policy_score,
        capability="pure",
        name="quality",
    )
    protection = ProtectionSnapshot(
        protection_id="safe-output",
        scorer=ScorerSnapshot.from_callable(
            _policy_score,
            capability="pure",
            name="safe-output",
        ),
    )
    first_policy = VerdictPolicy.create(
        objective=objective,
        minimum_mean=0.8,
        protections=[protection],
    )
    second_policy = VerdictPolicy.create(
        objective=objective,
        minimum_mean=0.9,
        protections=[protection],
    )
    assert first_policy is not None
    assert second_policy is not None

    first = freeze_replay_attempt(
        _draft().model_copy(
            update={
                "scorers": [objective, protection.scorer],
                "verdict_policy": first_policy,
            },
            deep=True,
        )
    ).spec
    second = freeze_replay_attempt(
        _draft().model_copy(
            update={
                "scorers": [objective, protection.scorer],
                "verdict_policy": second_policy,
            },
            deep=True,
        )
    ).spec

    assert type(first).model_validate(first.model_dump(mode="json")) == first
    assert first.request_hash != second.request_hash
    assert _plan().spec.verdict_policy is None


def test_replay_grounded_policy_is_frozen_and_hashed() -> None:
    grounded_policy = GroundedPolicySnapshot(policy_id="read-only-v1")
    baseline = _plan().spec
    protected = freeze_replay_attempt(
        _draft().model_copy(update={"grounded_policy": grounded_policy}, deep=True)
    ).spec

    assert protected.grounded_policy == grounded_policy
    assert (
        type(protected).model_validate(protected.model_dump(mode="json")) == protected
    )
    assert protected.request_hash != baseline.request_hash


def test_replay_scorers_reject_duplicate_identities() -> None:
    scorer = ScorerSnapshot.from_callable(
        _policy_score,
        capability="pure",
        name="quality",
    )

    with pytest.raises(ValidationError, match="must be unique"):
        freeze_replay_attempt(
            _draft().model_copy(update={"scorers": [scorer, scorer]}, deep=True)
        )


def test_verdict_policy_rejects_unknown_and_duplicate_protections() -> None:
    objective = ScorerSnapshot.from_callable(
        _policy_score,
        capability="pure",
        name="quality",
    )
    protection = ProtectionSnapshot(
        protection_id="safe-output",
        scorer=ScorerSnapshot.from_callable(
            _policy_score,
            capability="pure",
            name="safe-output",
        ),
    )
    policy = VerdictPolicy.create(protections=[protection])
    assert policy is not None

    with pytest.raises(ValidationError, match="exactly match"):
        freeze_replay_attempt(
            _draft().model_copy(
                update={"scorers": [objective], "verdict_policy": policy},
                deep=True,
            )
        )

    with pytest.raises(ValidationError, match="protection IDs must be unique"):
        VerdictPolicy.create(protections=[protection, protection])


def test_experiment_models_round_trip_and_reject_malformed_references() -> None:
    spec = _plan().spec

    assert type(spec).model_validate(spec.model_dump(mode="json")) == spec
    assert ExperimentRecord.model_validate(
        ExperimentRecord.pending(spec).model_dump(mode="json")
    ) == ExperimentRecord.pending(spec)

    with pytest.raises(ValidationError, match="greater than 500"):
        ArtifactTargetMembership(
            artifact_version_id="artifact-id",
            count=500,
            sha256="sha256:" + "0" * 64,
        )
    with pytest.raises(ValidationError, match="equal counts"):
        type(spec).model_validate(
            {
                **spec.model_dump(mode="json"),
                "coverage": {"selected": 2, "covered": 1, "policy": "fail"},
            }
        )


def test_unnamed_display_names_are_stable_and_duplicate_names_are_independent() -> None:
    first = _plan(idempotency_key="one").spec
    retry = _plan(idempotency_key="one", created_at="2026-07-17T10:00:00Z").spec
    named_one = _plan(idempotency_key="two", name="Regression").spec
    named_two = _plan(idempotency_key="three", name="Regression").spec

    assert first.display_name == retry.display_name
    assert first.suite_key == retry.suite_key
    assert first.request_hash == retry.request_hash
    assert named_one.name == named_two.name == "Regression"
    assert named_one.suite_key != named_two.suite_key


def test_record_rejects_invalid_completion_and_count_mismatch() -> None:
    pending = ExperimentRecord.pending(_plan().spec)

    with pytest.raises(ValidationError, match="targets multiplied by repeats"):
        ExperimentRecord.model_validate(
            {
                **pending.model_dump(mode="json"),
                "counts": {
                    **pending.counts.model_dump(mode="json"),
                    "intended": 2,
                },
            }
        )
    with pytest.raises(ValidationError, match="Completed experiments require"):
        ExperimentRecord.model_validate(
            {
                **pending.model_dump(mode="json"),
                "status": "completed",
                "started_at": "2026-07-17T09:01:00Z",
                "finished_at": "2026-07-17T09:02:00Z",
                "counts": {
                    **pending.counts.model_dump(mode="json"),
                    "submitted": 1,
                },
            }
        )


def test_lost_response_reservation_is_idempotent_and_preserves_foreign_keys() -> None:
    client = _ProjectClient(_base_envelope())
    client.raise_after_first_commit = True
    spec = _plan().spec

    reservation = reserve_experiment(
        "project-id",
        spec,
        client_factory=lambda: client,
    )

    assert reservation.record.spec == spec
    assert reservation.created is False
    assert list(client.metadata["kitaru"]["experiments"]) == [spec.experiment_id]
    assert client.metadata["foreign"] == {"preserve": True}
    assert client.metadata["kitaru"]["future_key"] == {"preserve": [1, 2, 3]}


def test_conflicting_idempotency_key_fails_without_second_update() -> None:
    client = _ProjectClient(_base_envelope())
    first = _plan(flow_overrides={"temperature": 0}).spec
    conflict = _plan(flow_overrides={"temperature": 1}).spec
    reserve_experiment("project-id", first, client_factory=lambda: client)
    updates_before = len(client.update_calls)

    with pytest.raises(KitaruMetadataConflictError, match="different request"):
        reserve_experiment(
            "project-id",
            conflict,
            client_factory=lambda: client,
        )

    assert len(client.update_calls) == updates_before
    assert len(client.metadata["kitaru"]["experiments"]) == 1


def test_catalog_updates_are_monotonic_and_preserve_spec() -> None:
    client = _ProjectClient(_base_envelope())
    spec = _plan().spec
    reserve_experiment("project-id", spec, client_factory=lambda: client)

    running = transition_experiment_to_running(
        "project-id",
        spec.experiment_id,
        at="2026-07-17T09:01:00Z",
        client_factory=lambda: client,
    )
    updated = record_experiment_outcomes(
        "project-id",
        spec.experiment_id,
        counts=ExperimentCounts(
            target_count=1,
            intended=1,
            submitted=1,
            verified=1,
        ),
        at="2026-07-17T09:02:00Z",
        client_factory=lambda: client,
    )
    completed = finalize_experiment(
        "project-id",
        spec.experiment_id,
        status="completed",
        at="2026-07-17T09:03:00Z",
        client_factory=lambda: client,
    )

    assert running.spec == updated.spec == completed.spec == spec
    assert completed.status == "completed"
    with pytest.raises(KitaruMetadataConflictError, match="Only a running"):
        finalize_experiment(
            "project-id",
            spec.experiment_id,
            status="failed",
            client_factory=lambda: client,
        )


def test_inline_boundary_and_verified_artifact_manifest() -> None:
    inline = persist_target_membership(
        experiment_id="exp-small",
        project_id="project-id",
        execution_ids=[f"run-{index}" for index in range(500)],
    )
    client = _ArtifactClient()
    ids = [f"run-{index}" for index in range(501)]

    def save(**kwargs: Any) -> _Artifact:
        artifact = _Artifact("artifact-id", kwargs["name"], kwargs["data"])
        client.artifacts.append(artifact)
        return artifact

    artifact = persist_target_membership(
        experiment_id="exp-large",
        project_id="project-id",
        execution_ids=ids,
        client=client,
        save_artifact_fn=save,
    )

    assert isinstance(inline, InlineTargetMembership)
    assert inline.count == 500
    assert isinstance(artifact, ArtifactTargetMembership)
    assert artifact.count == 501
    assert client.artifacts[0].load() == ids
    assert artifact.sha256 == target_manifest_payload(ids)[1]


def test_artifact_retry_reuses_matching_manifest_and_rejects_hash_mismatch() -> None:
    client = _ArtifactClient()
    ids = [f"run-{index}" for index in range(501)]
    name = "kitaru-experiment-targets-exp-large"
    client.artifacts.append(_Artifact("artifact-id", name, ids))
    save_calls = 0

    def save(**_: Any) -> Any:
        nonlocal save_calls
        save_calls += 1
        raise AssertionError("existing manifest must be reused")

    reused = persist_target_membership(
        experiment_id="exp-large",
        project_id="project-id",
        execution_ids=ids,
        client=client,
        save_artifact_fn=save,
    )
    assert isinstance(reused, ArtifactTargetMembership)
    assert reused.artifact_version_id == "artifact-id"
    assert save_calls == 0

    with pytest.raises(KitaruMetadataConflictError, match="conflicts"):
        persist_target_membership(
            experiment_id="exp-large",
            project_id="project-id",
            execution_ids=[*ids[:-1], "different"],
            client=client,
            save_artifact_fn=save,
        )
