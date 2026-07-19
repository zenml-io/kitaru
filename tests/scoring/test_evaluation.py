"""Shared score evaluation service tests."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru._experiments import (
    ExperimentCounts,
    ExperimentRecord,
    freeze_replay_attempt,
)
from kitaru.errors import KitaruMetadataConflictError, KitaruUsageError
from kitaru.scoring import (
    OBSERVATION_ARTIFACT_NAME,
    ExperimentVerdict,
    OperationalLimitFacts,
    OperationalLimitOutcome,
    OperationalLimitReason,
    OperationalLimitThresholds,
    ProtectionDeclaration,
    Score,
    VerdictPolicy,
    scorer,
)
from kitaru.scoring._contracts import canonical_json, scorer_snapshot
from kitaru.scoring._evaluation import (
    ScoreEvaluationService,
    _request_key,
    _score_experiment_id,
)
from tests.experiments._helpers import _draft


@scorer(capability="pure")
def _constant_score(_: object) -> Score:
    return Score(value=1.0)


def _constant_protection(_: object) -> Score:
    return Score(value=1.0)


def test_ungraded_request_key_preserves_the_legacy_payload() -> None:
    snapshot = scorer_snapshot(_constant_score)
    payload = {
        "target_ids": ["run-1"],
        "scorers": [
            {
                "name": snapshot.name,
                "revision": snapshot.revision,
                "configuration_hash": snapshot.configuration_hash,
            }
        ],
        "name": None,
        "suite_key": None,
        "comparative": False,
        "metadata": {},
        "grounded_policy": None,
    }

    assert _request_key(
        ["run-1"],
        [snapshot],
        name=None,
        suite_key=None,
        comparative=False,
        metadata=None,
        grounded_policy=None,
        verdict_policy=None,
    ) == _score_experiment_id("request", canonical_json(payload))


def _run(run_id: str) -> Any:
    return SimpleNamespace(
        id=run_id,
        project_id="project-id",
        status=SimpleNamespace(value="completed"),
        original_run=None,
        steps={},
        config=SimpleNamespace(parameters={}),
        run_metadata={},
    )


class _Artifact:
    def __init__(self, artifact_id: str, name: str, value: Any, **kwargs: Any) -> None:
        self.id = artifact_id
        self.name = name
        self.version = kwargs.get("version")
        self.tags = kwargs.get("tags") or []
        self.metadata = kwargs.get("user_metadata") or {}
        self._value = deepcopy(value)

    def load(self) -> Any:
        return deepcopy(self._value)


class _Client:
    def __init__(self, runs: dict[str, Any]) -> None:
        self.runs = runs
        self.artifacts: list[_Artifact] = []
        self.active_project = SimpleNamespace(id="project-id")

    def get_pipeline_run(self, *, name_id_or_prefix: str, **_: Any) -> Any:
        return self.runs[name_id_or_prefix]

    def get_artifact_version(self, *, name_id_or_prefix: str, **_: Any) -> _Artifact:
        return next(
            artifact for artifact in self.artifacts if artifact.id == name_id_or_prefix
        )

    def list_artifact_versions(
        self, *, name: str, run_metadata: list[str] | None = None, **kwargs: Any
    ) -> Any:
        expected_name = name.removeprefix("equals:")
        expected_version = kwargs.get("version")
        expected_tag = kwargs.get("tags")
        items = [
            artifact for artifact in self.artifacts if artifact.name == expected_name
        ]
        if expected_version is not None:
            items = [
                artifact for artifact in items if artifact.version == expected_version
            ]
        if expected_tag is not None:
            items = [artifact for artifact in items if expected_tag in artifact.tags]
        for entry in run_metadata or []:
            key, value = entry.split(":", 1)
            if ":" in value:
                _operator, value = value.split(":", 1)
            items = [
                artifact
                for artifact in items
                if str(artifact.metadata.get(key)) == value
            ]
        page = kwargs.get("page", 1)
        size = kwargs.get("size", len(items) or 1)
        start = (page - 1) * size
        return SimpleNamespace(items=items[start : start + size])


def test_idempotent_score_retry_reuses_large_pre_reservation_artifacts(
    monkeypatch: Any,
) -> None:
    import kitaru.scoring._evaluation as evaluation_module

    runs = {f"run-{index}": _run(f"run-{index}") for index in range(501)}
    client = _Client(runs)
    saved_names: list[str] = []
    reservations = 0
    captured_specs: list[Any] = []

    def save(**kwargs: Any) -> _Artifact:
        artifact_kwargs = dict(kwargs)
        artifact_kwargs.pop("name", None)
        artifact_kwargs.pop("data", None)
        artifact = _Artifact(
            f"artifact-{len(client.artifacts) + 1}",
            kwargs["name"],
            kwargs["data"],
            **artifact_kwargs,
        )
        client.artifacts.append(artifact)
        saved_names.append(kwargs["name"])
        return artifact

    def reserve(project_id: str, spec: Any, **_: Any) -> Any:
        nonlocal reservations
        reservations += 1
        captured_specs.append(spec)
        return SimpleNamespace(
            record=ExperimentRecord.pending(spec),
            created=reservations == 1,
        )

    monkeypatch.setattr(evaluation_module, "reserve_experiment", reserve)
    monkeypatch.setattr(
        evaluation_module,
        "transition_experiment_to_running",
        lambda *_, **__: ExperimentRecord.pending(captured_specs[-1]).model_copy(
            update={
                "status": "running",
                "started_at": captured_specs[-1].created_at,
            }
        ),
    )
    monkeypatch.setattr(
        evaluation_module,
        "finalize_experiment_outcomes",
        lambda _project_id, _experiment_id, **kwargs: kwargs["counts"],
    )

    service = ScoreEvaluationService(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    target_ids = list(runs)

    first = service.evaluate(
        target_ids,
        [_constant_score],
        idempotency_key="same-request",
    )
    second = service.evaluate(
        target_ids,
        [_constant_score],
        idempotency_key="same-request",
    )

    evidence_saves = [
        name for name in saved_names if name.startswith("kitaru-evidence-manifest-")
    ]
    target_saves = [
        name for name in saved_names if name.startswith("kitaru-experiment-targets-")
    ]
    assert len(evidence_saves) == 1
    assert len(target_saves) == 1
    assert len(first.observations) == 501
    assert len(second.observations) == 501
    assert sum(name == OBSERVATION_ARTIFACT_NAME for name in saved_names) == 501


def test_protections_run_automatically_and_freeze_policy(
    monkeypatch: Any,
) -> None:
    import kitaru.scoring._evaluation as evaluation_module

    client = _Client({"run-1": _run("run-1")})
    captured_specs: list[Any] = []

    def save(**kwargs: Any) -> _Artifact:
        artifact_kwargs = dict(kwargs)
        artifact_kwargs.pop("name", None)
        artifact_kwargs.pop("data", None)
        artifact = _Artifact(
            f"artifact-{len(client.artifacts) + 1}",
            kwargs["name"],
            kwargs["data"],
            **artifact_kwargs,
        )
        client.artifacts.append(artifact)
        return artifact

    def reserve(_project_id: str, spec: Any, **_: Any) -> Any:
        captured_specs.append(spec)
        return SimpleNamespace(record=ExperimentRecord.pending(spec), created=True)

    protection = ProtectionDeclaration.from_callable(
        _constant_protection,
        protection_id="safe-output",
        capability="pure",
    )
    monkeypatch.setattr(evaluation_module, "reserve_experiment", reserve)
    monkeypatch.setattr(
        evaluation_module,
        "transition_experiment_to_running",
        lambda *_, **__: None,
    )

    def finalize(
        _project_id: str,
        _experiment_id: str,
        **kwargs: Any,
    ) -> ExperimentRecord:
        pending = ExperimentRecord.pending(captured_specs[0])
        return pending.model_copy(
            update={
                "status": kwargs["status"],
                "started_at": pending.created_at,
                "finished_at": pending.created_at,
                "updated_at": pending.created_at,
                "counts": kwargs["counts"],
                "score_aggregate": kwargs["aggregate_reference"],
                "verdict": kwargs["verdict_result"],
            },
            deep=True,
        )

    monkeypatch.setattr(evaluation_module, "finalize_experiment_outcomes", finalize)

    result = ScoreEvaluationService(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    ).evaluate(
        ["run-1"],
        [],
        protections=[protection],
        idempotency_key="protected-score",
    )

    assert len(result.observations) == 1
    assert result.observations[0].scorer == protection.snapshot.scorer
    assert captured_specs[0].scorers == [protection.snapshot.scorer]
    assert captured_specs[0].verdict_policy is not None
    assert [
        item.protection_id for item in captured_specs[0].verdict_policy.protections
    ] == ["safe-output"]
    assert result.record.verdict is not None
    assert result.record.verdict.verdict.value == "pass"


def test_existing_attempt_rejects_an_empty_execution_set() -> None:
    service = ScoreEvaluationService(
        project_id="project-id",
        client=_Client({}),
    )

    with pytest.raises(KitaruUsageError, match="verified replay child"):
        service.evaluate_existing_attempt(
            experiment_id="experiment-id",
            executions=[],
            scorers=[_constant_score],
        )


def test_existing_bounded_attempt_with_no_children_freezes_empty_hold(
    monkeypatch: Any,
) -> None:
    import kitaru.scoring._evaluation as evaluation_module

    snapshot = scorer_snapshot(_constant_score)
    policy = VerdictPolicy.create(objective=snapshot)
    assert policy is not None
    spec = freeze_replay_attempt(
        _draft().model_copy(
            update={"scorers": [snapshot], "verdict_policy": policy},
            deep=True,
        )
    ).spec
    operational_limit = OperationalLimitOutcome.create(
        verified=True,
        stopped=True,
        reason_code=OperationalLimitReason.DURATION_LIMIT_REACHED,
        facts=OperationalLimitFacts(
            limits=OperationalLimitThresholds(
                max_trials=1,
                max_duration_seconds=1.0,
            ),
            submitted_trials=0,
            remaining_trials=1,
            incurred_cost_usd=0.0,
            incurred_tokens=0,
            duration_seconds=1.0,
            cost_complete=True,
            tokens_complete=True,
            checked_between_terminal_trials=True,
            one_trial_may_overshoot=False,
        ),
    )
    record = ExperimentRecord.pending(spec).model_copy(
        update={
            "status": "failed",
            "started_at": "2026-07-18T09:00:00Z",
            "finished_at": "2026-07-18T09:00:01Z",
            "updated_at": "2026-07-18T09:00:01Z",
            "counts": ExperimentCounts(
                target_count=1,
                intended=1,
                failed=1,
            ),
            "operational_limit": operational_limit,
        },
        deep=True,
    )
    client = _Client({})

    def save(**kwargs: Any) -> _Artifact:
        artifact_kwargs = dict(kwargs)
        artifact_kwargs.pop("name", None)
        artifact_kwargs.pop("data", None)
        artifact = _Artifact(
            f"artifact-{len(client.artifacts) + 1}",
            kwargs["name"],
            kwargs["data"],
            **artifact_kwargs,
        )
        client.artifacts.append(artifact)
        return artifact

    def attach(
        _project_id: str,
        _experiment_id: str,
        *,
        aggregate_reference: Any,
        operational_limit: OperationalLimitOutcome | None,
        verdict_result: Any,
        **_: Any,
    ) -> ExperimentRecord:
        assert operational_limit == record.operational_limit
        return record.model_copy(
            update={
                "score_aggregate": aggregate_reference,
                "verdict": verdict_result,
            },
            deep=True,
        )

    monkeypatch.setattr(evaluation_module, "attach_experiment_score_aggregate", attach)

    result = ScoreEvaluationService(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    ).evaluate_existing_attempt(
        experiment_id=spec.experiment_id,
        executions=[],
        scorers=[_constant_score],
        record=record,
    )

    assert result.observations == []
    assert result.aggregate is not None
    assert result.aggregate.planned == 0
    assert result.record.verdict is not None
    assert result.record.verdict.verdict is ExperimentVerdict.HOLD
    assert result.record.verdict.operational_limit == operational_limit


def test_interrupted_score_attempt_resumes_and_terminal_retry_is_complete(
    monkeypatch: Any,
) -> None:
    import kitaru.scoring._evaluation as evaluation_module

    runs = {"run-1": _run("run-1"), "run-2": _run("run-2")}
    client = _Client(runs)
    current_record: ExperimentRecord | None = None

    def save(**kwargs: Any) -> _Artifact:
        artifact_kwargs = dict(kwargs)
        artifact_kwargs.pop("name", None)
        artifact_kwargs.pop("data", None)
        artifact = _Artifact(
            f"artifact-{len(client.artifacts) + 1}",
            kwargs["name"],
            kwargs["data"],
            **artifact_kwargs,
        )
        client.artifacts.append(artifact)
        return artifact

    def reserve(_project_id: str, spec: Any, **_: Any) -> Any:
        nonlocal current_record
        current_record = ExperimentRecord.pending(spec)
        return SimpleNamespace(record=current_record, created=True)

    def transition(_project_id: str, _experiment_id: str, **_: Any) -> Any:
        nonlocal current_record
        assert current_record is not None
        current_record = current_record.model_copy(
            update={
                "status": "running",
                "started_at": current_record.spec.created_at,
            }
        )
        return current_record

    def finalize(_project_id: str, _experiment_id: str, **kwargs: Any) -> Any:
        nonlocal current_record
        assert current_record is not None
        current_record = current_record.model_copy(
            update={
                "status": kwargs["status"],
                "counts": kwargs["counts"],
                "errors": kwargs["errors"],
                "skips": kwargs["skips"],
                "score_aggregate": kwargs["aggregate_reference"],
                "finished_at": current_record.spec.created_at,
            }
        )
        return current_record

    monkeypatch.setattr(
        evaluation_module,
        "get_experiment_by_idempotency_key",
        lambda *_, **__: current_record,
    )
    monkeypatch.setattr(evaluation_module, "reserve_experiment", reserve)
    monkeypatch.setattr(
        evaluation_module, "transition_experiment_to_running", transition
    )
    monkeypatch.setattr(evaluation_module, "finalize_experiment_outcomes", finalize)

    service = ScoreEvaluationService(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    original_invoke = service._invoke_one
    invocation_count = 0
    interrupt = True

    def invoke_one(**kwargs: Any) -> Any:
        nonlocal invocation_count
        if interrupt and invocation_count == 1:
            raise KeyboardInterrupt
        invocation_count += 1
        return original_invoke(**kwargs)

    monkeypatch.setattr(service, "_invoke_one", invoke_one)

    with pytest.raises(KeyboardInterrupt):
        service.evaluate(
            list(runs),
            [_constant_score],
            idempotency_key="recover-request",
        )

    interrupt = False
    resumed = service.evaluate(
        list(runs),
        [_constant_score],
        idempotency_key="recover-request",
    )
    retried = service.evaluate(
        list(runs),
        [_constant_score],
        idempotency_key="recover-request",
    )

    assert invocation_count == 2
    assert len(resumed.observations) == len(retried.observations) == 2
    assert resumed.record.status == retried.record.status == "completed"
    assert retried.aggregate == resumed.aggregate
    assert retried.aggregate_reference == resumed.aggregate_reference

    with pytest.raises(KitaruMetadataConflictError, match="different score request"):
        service.evaluate(
            list(runs),
            [_constant_score],
            idempotency_key="recover-request",
            name="different",
        )


def test_interrupted_replay_scoring_reuses_evidence_and_observations(
    monkeypatch: Any,
) -> None:
    import kitaru.scoring._evaluation as evaluation_module

    snapshot = scorer_snapshot(_constant_score)
    spec = freeze_replay_attempt(
        _draft().model_copy(
            update={"repeats": 2, "scorers": [snapshot]},
            deep=True,
        )
    ).spec
    pending = ExperimentRecord.pending(spec)
    record = ExperimentRecord.model_validate(
        {
            **pending.model_dump(mode="json"),
            "status": "completed",
            "started_at": "2026-07-18T09:00:00Z",
            "finished_at": "2026-07-18T09:00:02Z",
            "updated_at": "2026-07-18T09:00:02Z",
            "counts": ExperimentCounts(
                target_count=1,
                intended=2,
                submitted=2,
                verified=2,
            ).model_dump(mode="json"),
        }
    )
    client = _Client(
        {
            "child-1": _run("child-1"),
            "child-2": _run("child-2"),
        }
    )

    def save(**kwargs: Any) -> _Artifact:
        artifact_kwargs = dict(kwargs)
        artifact_kwargs.pop("name", None)
        artifact_kwargs.pop("data", None)
        artifact = _Artifact(
            f"artifact-{len(client.artifacts) + 1}",
            kwargs["name"],
            kwargs["data"],
            **artifact_kwargs,
        )
        client.artifacts.append(artifact)
        return artifact

    def attach(
        _project_id: str,
        _experiment_id: str,
        *,
        aggregate_reference: Any,
        **_: Any,
    ) -> ExperimentRecord:
        return record.model_copy(
            update={"score_aggregate": aggregate_reference},
            deep=True,
        )

    monkeypatch.setattr(evaluation_module, "attach_experiment_score_aggregate", attach)
    service = ScoreEvaluationService(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    original_invoke = service._invoke_one
    invocation_count = 0
    interrupt = True

    def invoke_one(**kwargs: Any) -> Any:
        nonlocal invocation_count
        if interrupt and invocation_count == 1:
            raise KeyboardInterrupt
        invocation_count += 1
        return original_invoke(**kwargs)

    monkeypatch.setattr(service, "_invoke_one", invoke_one)

    with pytest.raises(KeyboardInterrupt):
        service.evaluate_existing_attempt(
            experiment_id=spec.experiment_id,
            executions=["child-1", "child-2"],
            scorers=[_constant_score],
            record=record,
        )

    interrupt = False
    resumed = service.evaluate_existing_attempt(
        experiment_id=spec.experiment_id,
        executions=["child-1", "child-2"],
        scorers=[_constant_score],
        record=record,
    )

    assert invocation_count == 2
    assert len(resumed.observations) == 2
    assert (
        len(
            [
                artifact
                for artifact in client.artifacts
                if artifact.name == OBSERVATION_ARTIFACT_NAME
            ]
        )
        == 2
    )
    assert (
        len(
            {
                observation.evidence_manifest_sha256
                for observation in resumed.observations
            }
        )
        == 1
    )


def test_observation_identity_is_scoped_to_the_score_attempt(
    monkeypatch: Any,
) -> None:
    import kitaru.scoring._evaluation as evaluation_module

    client = _Client({"run-1": _run("run-1")})

    def save(**kwargs: Any) -> _Artifact:
        artifact_kwargs = dict(kwargs)
        artifact_kwargs.pop("name", None)
        artifact_kwargs.pop("data", None)
        artifact = _Artifact(
            f"artifact-{len(client.artifacts) + 1}",
            kwargs["name"],
            kwargs["data"],
            **artifact_kwargs,
        )
        client.artifacts.append(artifact)
        return artifact

    def reserve(_project_id: str, spec: Any, **_: Any) -> Any:
        return SimpleNamespace(record=ExperimentRecord.pending(spec), created=True)

    monkeypatch.setattr(evaluation_module, "reserve_experiment", reserve)
    monkeypatch.setattr(
        evaluation_module,
        "transition_experiment_to_running",
        lambda *_, **__: None,
    )
    monkeypatch.setattr(
        evaluation_module,
        "finalize_experiment_outcomes",
        lambda _project_id, _experiment_id, **kwargs: kwargs["counts"],
    )
    service = ScoreEvaluationService(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )

    service.evaluate(["run-1"], [_constant_score], idempotency_key="attempt-1")
    service.evaluate(["run-1"], [_constant_score], idempotency_key="attempt-2")

    observation_versions = {
        artifact.version
        for artifact in client.artifacts
        if artifact.name == OBSERVATION_ARTIFACT_NAME
    }
    assert len(observation_versions) == 2
