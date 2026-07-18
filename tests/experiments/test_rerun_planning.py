"""Immutable suite rerun planning contracts."""

from __future__ import annotations

from typing import Any

import pytest

from kitaru._agent_registration import RegisteredAgentVersionBinding
from kitaru._experiments import (
    ExperimentRecord,
    ExperimentSpec,
    freeze_replay_attempt,
    plan_suite_rerun,
    preplan_replay_attempt,
    validate_existing_suite_rerun,
)
from kitaru._experiments import _planning as planning_module
from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruUsageError,
)
from kitaru.experiments import RegressionLimits
from kitaru.scoring import (
    ProtectionSnapshot,
    Score,
    ScorerIdentity,
    VerdictPolicy,
    scorer,
    scorer_snapshot,
)
from tests.experiments._helpers import _binding, _manifest, _run, _RunClient, _step


@scorer(capability="pure", name="objective", configuration={"revision": 1})
def _objective(_: object) -> Score:
    return Score(value=1.0)


@scorer(capability="pure", name="objective", configuration={"revision": 2})
def _stale_objective(_: object) -> Score:
    return Score(value=1.0)


@scorer(capability="pure", name="old-protection")
def _old_protection(_: object) -> Score:
    return Score(value=1.0)


@scorer(capability="pure", name="current-protection")
def _current_protection(_: object) -> Score:
    return Score(value=1.0)


def _terminal_record(plan: Any) -> ExperimentRecord:
    pending = ExperimentRecord.pending(plan.spec)
    return ExperimentRecord.model_validate(
        {
            **pending.model_dump(mode="json"),
            "status": "completed",
            "started_at": "2026-07-17T09:01:00Z",
            "finished_at": "2026-07-17T09:02:00Z",
            "updated_at": "2026-07-17T09:02:00Z",
            "counts": {
                **pending.counts.model_dump(mode="json"),
                "submitted": pending.counts.intended,
                "verified": pending.counts.intended,
                "skipped": 0,
            },
        }
    )


def _source_with_frozen_settings() -> ExperimentRecord:
    covered = _run("covered", _step("at"))
    uncovered = _run("uncovered", _step("other"))
    draft = preplan_replay_attempt(
        ["covered", "uncovered"],
        binding=_binding("old-pipeline"),
        at="at",
        on_error="collect",
        uncovered_policy="top",
        idempotency_key="source-request",
        repeats=2,
        wait=True,
        name="Regression",
        suite_key="regression-suite",
        flow_overrides={"model": "old-model"},
        created_at="2026-07-17T09:00:00Z",
        client=_RunClient({"covered": covered, "uncovered": uncovered}),
        pipeline_verifier=lambda _client, _binding: None,
    )
    return _terminal_record(freeze_replay_attempt(draft))


def test_rerun_copies_frozen_selection_and_rebinds_current_candidate() -> None:
    source = _source_with_frozen_settings()
    before = source.model_dump_json()
    assert isinstance(source.spec, ExperimentSpec)
    client = _RunClient({})

    first = plan_suite_rerun(
        source,
        binding=_binding("current-pipeline"),
        idempotency_key="rerun-request",
        repeats=1,
        created_at="2026-07-18T10:00:00Z",
        client=client,
        pipeline_verifier=lambda _client, _binding: None,
    )
    retried = plan_suite_rerun(
        source,
        binding=_binding("current-pipeline"),
        idempotency_key="rerun-request",
        repeats=1,
        created_at="2026-07-18T10:00:00Z",
        client=client,
        pipeline_verifier=lambda _client, _binding: None,
    )

    assert first == retried
    assert first.spec.experiment_id != source.spec.experiment_id
    assert first.spec.source_experiment_id == source.spec.experiment_id
    assert first.spec.suite_key == source.spec.suite_key
    assert first.spec.candidate_agent_version_id == "current-pipeline"
    assert first.spec.candidate_pipeline_id == "current-pipeline"
    assert source.spec.candidate_pipeline_id == "old-pipeline"
    assert first.spec.target_membership == source.spec.target_membership
    assert first.spec.planning_rows == source.spec.planning_rows
    assert [row.target_execution_id for row in first.spec.planning_rows] == [
        "covered",
        "uncovered",
    ]
    assert first.spec.replay_inputs == source.spec.replay_inputs
    assert first.spec.coverage == source.spec.coverage
    assert first.spec.on_error == source.spec.on_error
    assert first.spec.at == source.spec.at
    assert first.spec.wait is True
    assert first.spec.repeats == 1
    assert source.model_dump_json() == before
    assert client.get_calls == []


def test_rerun_rejects_trial_overflow_before_creating_an_attempt() -> None:
    source = _source_with_frozen_settings()
    client = _RunClient({})

    with pytest.raises(KitaruUsageError, match="exceeding max_trials=3"):
        plan_suite_rerun(
            source,
            binding=_binding("current-pipeline"),
            idempotency_key="oversized-rerun",
            repeats=2,
            limits=RegressionLimits(max_trials=3),
            client=client,
            pipeline_verifier=lambda _client, _binding: None,
        )

    assert client.get_calls == []


def test_idempotent_rerun_validation_does_not_reload_or_replan_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_with_frozen_settings()
    binding = _binding("current-pipeline")
    planned = plan_suite_rerun(
        source,
        binding=binding,
        idempotency_key="idempotent-rerun",
        client=_RunClient({}),
        pipeline_verifier=lambda _client, _binding: None,
    )
    existing = ExperimentRecord.pending(planned.spec)
    monkeypatch.setattr(
        planning_module,
        "load_target_membership",
        lambda *_args, **_kwargs: pytest.fail("idempotent retry reloaded membership"),
    )

    validated = validate_existing_suite_rerun(
        existing,
        source,
        binding=binding,
        idempotency_key="idempotent-rerun",
        repeats=1,
    )

    assert validated.spec is existing.spec
    with pytest.raises(KitaruMetadataConflictError, match="conflicts"):
        validate_existing_suite_rerun(
            existing,
            source,
            binding=binding,
            idempotency_key="idempotent-rerun",
            repeats=1,
            limits=RegressionLimits(max_trials=2),
        )


def test_rerun_freezes_limits_into_idempotent_request_identity() -> None:
    source = _source_with_frozen_settings()
    client = _RunClient({})
    limits = RegressionLimits(max_trials=2, max_cost_usd=0.5)

    first = plan_suite_rerun(
        source,
        binding=_binding("current-pipeline"),
        idempotency_key="bounded-rerun",
        repeats=1,
        limits=limits,
        created_at="2026-07-18T10:00:00Z",
        client=client,
        pipeline_verifier=lambda _client, _binding: None,
    )
    changed = plan_suite_rerun(
        source,
        binding=_binding("current-pipeline"),
        idempotency_key="bounded-rerun",
        repeats=1,
        limits=RegressionLimits(max_trials=2, max_cost_usd=0.75),
        created_at="2026-07-18T10:00:00Z",
        client=client,
        pipeline_verifier=lambda _client, _binding: None,
    )

    assert first.spec.regression_limits == limits
    assert first.spec.request_hash != changed.spec.request_hash


def test_rerun_fails_before_planning_when_source_membership_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_with_frozen_settings()
    before = source.model_dump_json()

    def fail_membership(*_args: Any, **_kwargs: Any) -> list[str]:
        raise KitaruBackendError("Unable to load the experiment target manifest.")

    monkeypatch.setattr(
        planning_module,
        "load_target_membership",
        fail_membership,
    )
    with pytest.raises(KitaruBackendError, match="target manifest"):
        plan_suite_rerun(
            source,
            binding=_binding("current-pipeline"),
            idempotency_key="missing-membership",
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )

    assert source.model_dump_json() == before


def test_rerun_requires_new_idempotency_and_terminal_source() -> None:
    source = _source_with_frozen_settings()

    with pytest.raises(KitaruUsageError, match="new idempotency key"):
        plan_suite_rerun(
            source,
            binding=_binding("current-pipeline"),
            idempotency_key=source.spec.idempotency_key,
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )

    pending = ExperimentRecord.pending(source.spec)
    with pytest.raises(KitaruUsageError, match="terminal source"):
        plan_suite_rerun(
            pending,
            binding=_binding("current-pipeline"),
            idempotency_key="new-request",
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )


def test_rerun_requires_exact_objective_callable_compatibility() -> None:
    objective_snapshot = scorer_snapshot(_objective)
    draft = preplan_replay_attempt(
        "run-1",
        binding=_binding("old-pipeline"),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key="objective-source",
        repeats=1,
        wait=True,
        suite_key="objective-suite",
        created_at="2026-07-17T09:00:00Z",
        client=_RunClient({"run-1": _run("run-1", _step("at"))}),
        pipeline_verifier=lambda _client, _binding: None,
        scorers=[objective_snapshot],
        verdict_policy=VerdictPolicy.create(
            objective=objective_snapshot,
            minimum_mean=0.8,
        ),
    )
    source = _terminal_record(freeze_replay_attempt(draft))

    with pytest.raises(KitaruUsageError, match="exactly one current objective"):
        plan_suite_rerun(
            source,
            binding=_binding("current-pipeline"),
            idempotency_key="missing-objective",
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )
    with pytest.raises(KitaruMetadataConflictError, match="does not match"):
        plan_suite_rerun(
            source,
            binding=_binding("current-pipeline"),
            idempotency_key="stale-objective",
            objective_scorers=[scorer_snapshot(_stale_objective)],
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )

    rerun = plan_suite_rerun(
        source,
        binding=_binding("current-pipeline"),
        idempotency_key="matching-objective",
        objective_scorers=[scorer_snapshot(_objective)],
        client=_RunClient({}),
        pipeline_verifier=lambda _client, _binding: None,
    )
    assert rerun.spec.verdict_policy is not None
    assert rerun.spec.verdict_policy.objective is not None
    assert rerun.spec.verdict_policy.objective.minimum_mean == 0.8
    assert rerun.spec.verdict_policy.objective.scorer == ScorerIdentity.from_snapshot(
        objective_snapshot
    )


def test_legacy_scored_replay_requires_explicit_migration() -> None:
    snapshot = scorer_snapshot(_objective)
    draft = preplan_replay_attempt(
        "run-1",
        binding=_binding("old-pipeline"),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key="legacy-scored-source",
        repeats=1,
        wait=True,
        suite_key="legacy-scored-suite",
        created_at="2026-07-17T09:00:00Z",
        client=_RunClient({"run-1": _run("run-1", _step("at"))}),
        pipeline_verifier=lambda _client, _binding: None,
        scorers=[snapshot],
    )
    serialized_source = ExperimentRecord.model_validate(
        _terminal_record(freeze_replay_attempt(draft)).model_dump(mode="json")
    )

    with pytest.raises(KitaruUsageError, match="predates verdict policies"):
        plan_suite_rerun(
            serialized_source,
            binding=_binding("current-pipeline"),
            idempotency_key="legacy-scored-rerun",
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )


def test_rerun_replaces_source_protections_with_current_registered_set() -> None:
    old = ProtectionSnapshot(
        protection_id="old",
        scorer=scorer_snapshot(_old_protection),
    )
    current = ProtectionSnapshot(
        protection_id="current",
        scorer=scorer_snapshot(_current_protection),
    )
    draft = preplan_replay_attempt(
        "run-1",
        binding=_binding("old-pipeline"),
        at="at",
        on_error="fail",
        uncovered_policy="fail",
        idempotency_key="protected-source",
        repeats=1,
        wait=True,
        suite_key="protected-suite",
        created_at="2026-07-17T09:00:00Z",
        client=_RunClient({"run-1": _run("run-1", _step("at"))}),
        pipeline_verifier=lambda _client, _binding: None,
        scorers=[old.scorer],
        verdict_policy=VerdictPolicy.create(protections=[old]),
    )
    source = _terminal_record(freeze_replay_attempt(draft))
    manifest = _manifest("current-pipeline").model_copy(
        update={"protections": {"current": current}}
    )
    binding = RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=manifest,
    )

    rerun = plan_suite_rerun(
        source,
        binding=binding,
        idempotency_key="protected-rerun",
        protections=[current],
        client=_RunClient({}),
        pipeline_verifier=lambda _client, _binding: None,
    )

    assert rerun.spec.verdict_policy is not None
    assert [item.protection_id for item in rerun.spec.verdict_policy.protections] == [
        "current"
    ]
    assert rerun.spec.scorers == [current.scorer]
    with pytest.raises(KitaruMetadataConflictError, match="registered candidate"):
        plan_suite_rerun(
            source,
            binding=binding,
            idempotency_key="missing-current-protection",
            protections=[],
            client=_RunClient({}),
            pipeline_verifier=lambda _client, _binding: None,
        )
