"""Descriptive score aggregate tests."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.scoring import (
    Score,
    ScoreObservation,
    ScoreObservationOutcome,
    ScoreObservationStatus,
    ScorerSnapshot,
)
from kitaru.scoring._aggregates import (
    AGGREGATE_ARTIFACT_NAME,
    AGGREGATE_ARTIFACT_TAG,
    ScoreAttemptAggregate,
    persist_score_aggregate,
)


def _scorer(_: object) -> Score:
    return Score(value=1.0)


SNAPSHOT = ScorerSnapshot.from_callable(_scorer, capability="pure")
MANIFEST_HASH = f"sha256:{'2' * 64}"


def _observation(
    observation_id: str,
    value: float | None,
    *,
    status: ScoreObservationStatus = ScoreObservationStatus.SCORED,
    execution_id: str | None = None,
    original_id: str | None = None,
) -> ScoreObservation:
    outcome = (
        ScoreObservationOutcome(
            status=ScoreObservationStatus.SCORED, score=Score(value=value)
        )
        if value is not None
        else ScoreObservationOutcome(status=status, reason="not scored")
    )
    return ScoreObservation(
        observation_id=observation_id,
        project_id="project-id",
        execution_id=execution_id or f"run-{observation_id}",
        experiment_id="exp-score",
        scorer=SNAPSHOT,
        outcome=outcome,
        completed_at=f"2026-07-18T10:00:0{observation_id[-1]}Z",
        evidence_manifest_sha256=MANIFEST_HASH,
        comparative_original_execution_id=original_id,
    )


def test_score_attempt_aggregate_freezes_selected_observation_ids() -> None:
    first = _observation("obs-1", 0.25)
    second = _observation("obs-2", 0.75)

    aggregate = ScoreAttemptAggregate.create(
        experiment_id="exp-score",
        project_id="project-id",
        observations=[first, second],
    )
    later = ScoreAttemptAggregate.create(
        experiment_id="exp-score",
        project_id="project-id",
        observations=[
            first,
            second,
            _observation("obs-3", None, status=ScoreObservationStatus.ERROR),
        ],
    )

    assert aggregate.observation_ids == ["obs-1", "obs-2"]
    assert aggregate.scored == 2
    assert aggregate.error == 0
    assert aggregate.scorer_aggregates[0].mean == 0.5
    assert aggregate.scorer_aggregates[0].spread == 0.5
    assert later.observation_ids == ["obs-1", "obs-2", "obs-3"]
    assert later.content_hash != aggregate.content_hash


def test_score_attempt_aggregate_calculates_available_paired_deltas() -> None:
    original = _observation("obs-1", 0.4, execution_id="original-run")
    candidate = _observation(
        "obs-2",
        0.9,
        execution_id="candidate-run",
        original_id="original-run",
    )

    aggregate = ScoreAttemptAggregate.create(
        experiment_id="exp-score",
        project_id="project-id",
        observations=[original, candidate],
    )

    scorer_aggregate = aggregate.scorer_aggregates[0]
    assert scorer_aggregate.paired_delta_count == 1
    assert scorer_aggregate.paired_delta_mean == 0.5
    assert scorer_aggregate.paired_delta_minimum == 0.5
    assert scorer_aggregate.paired_delta_maximum == 0.5


class _Artifact:
    def __init__(self, artifact_id: str, data: dict[str, Any]) -> None:
        self.id = artifact_id
        self.name = AGGREGATE_ARTIFACT_NAME
        self.data = deepcopy(data)

    def load(self) -> dict[str, Any]:
        return deepcopy(self.data)


class _Client:
    active_project = SimpleNamespace(id="project-id")

    def __init__(self) -> None:
        self.artifacts: list[_Artifact] = []
        self.list_calls = 0

    def list_artifact_versions(self, **kwargs: Any) -> Any:
        self.list_calls += 1
        assert kwargs["version"] == "exp-score"
        return SimpleNamespace(items=list(self.artifacts))

    def get_artifact_version(self, **kwargs: Any) -> _Artifact:
        artifact_id = kwargs["name_id_or_prefix"]
        return next(item for item in self.artifacts if item.id == artifact_id)


def test_score_attempt_aggregate_rejects_missing_or_duplicate_observation_ids() -> None:
    missing = _observation("obs-1", 1.0).model_copy(update={"observation_id": None})

    with pytest.raises(ValueError, match="Score observation ID"):
        ScoreAttemptAggregate.create(
            experiment_id="exp-score",
            project_id="project-id",
            observations=[missing],
        )
    with pytest.raises(ValueError, match="observation IDs must be unique"):
        ScoreAttemptAggregate.create(
            experiment_id="exp-score",
            project_id="project-id",
            observations=[
                _observation("obs-1", 1.0),
                _observation("obs-1", 0.5),
            ],
        )


def test_persist_score_aggregate_writes_kitaru_artifact_shape() -> None:
    saved: list[dict[str, Any]] = []
    client = _Client()

    def save(**kwargs: Any) -> Any:
        saved.append(deepcopy(kwargs))
        artifact = _Artifact("aggregate-1", kwargs["data"])
        client.artifacts.append(artifact)
        return artifact

    aggregate = ScoreAttemptAggregate.create(
        experiment_id="exp-score",
        project_id="project-id",
        observations=[_observation("obs-1", 1.0)],
    )
    reference = persist_score_aggregate(
        aggregate,
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )

    assert reference.artifact_version_id == "aggregate-1"
    assert saved[0]["name"] == AGGREGATE_ARTIFACT_NAME
    assert saved[0]["version"] == "exp-score"
    assert saved[0]["tags"] == [AGGREGATE_ARTIFACT_TAG]
    assert saved[0]["data"]["content_hash"] == aggregate.content_hash
    assert saved[0]["user_metadata"]["kitaru_experiment_id"] == "exp-score"
    assert client.list_calls == 0


def test_persist_score_aggregate_recovers_an_ambiguous_save() -> None:
    client = _Client()
    aggregate = ScoreAttemptAggregate.create(
        experiment_id="exp-score",
        project_id="project-id",
        observations=[_observation("obs-1", 1.0)],
    )

    def save(**kwargs: Any) -> Any:
        client.artifacts.append(_Artifact("aggregate-1", kwargs["data"]))
        raise RuntimeError("response lost after commit")

    reference = persist_score_aggregate(
        aggregate,
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )

    assert reference.artifact_version_id == "aggregate-1"
    assert reference.sha256 == aggregate.content_hash
    assert len(client.artifacts) == 1
    assert client.list_calls == 1
