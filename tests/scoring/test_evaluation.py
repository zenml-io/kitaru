"""Shared score evaluation service tests."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru._experiments import ExperimentRecord
from kitaru.errors import KitaruMetadataConflictError, KitaruUsageError
from kitaru.scoring import OBSERVATION_ARTIFACT_NAME, Score, scorer
from kitaru.scoring._evaluation import ScoreEvaluationService


@scorer(capability="pure")
def _constant_score(_: object) -> Score:
    return Score(value=1.0)


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
