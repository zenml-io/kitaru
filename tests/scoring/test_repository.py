"""Append-only score observation repository tests."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.errors import KitaruMetadataConflictError, KitaruStateError
from kitaru.scoring import (
    OBSERVATION_ARTIFACT_NAME,
    OBSERVATION_ARTIFACT_TAG,
    ObservationQuery,
    Score,
    ScoreObservation,
    ScoreObservationOutcome,
    ScoreObservationRepository,
    ScoreObservationStatus,
    ScorerSnapshot,
)


def _scorer(_: object) -> Score:
    return Score(value=1.0)


SNAPSHOT = ScorerSnapshot.from_callable(_scorer, capability="pure")
MANIFEST_HASH = f"sha256:{'1' * 64}"


class _Artifact:
    def __init__(
        self,
        artifact_id: str,
        name: str,
        value: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        version: str | None = None,
    ) -> None:
        self.id = artifact_id
        self.name = name
        self._value = deepcopy(value)
        self.metadata = deepcopy(metadata or {})
        self.version = version

    def load(self) -> dict[str, Any]:
        return deepcopy(self._value)


class _Client:
    def __init__(self) -> None:
        self.active_project = SimpleNamespace(id="project-id")
        self.artifacts: list[_Artifact] = []
        self.list_calls: list[dict[str, Any]] = []
        self.project_metadata: dict[str, Any] = {"kitaru": {"experiments": {}}}

    def list_artifact_versions(self, **kwargs: Any) -> Any:
        self.list_calls.append(deepcopy(kwargs))
        filters = kwargs.get("run_metadata") or []
        items = [
            artifact
            for artifact in self.artifacts
            if artifact.name == OBSERVATION_ARTIFACT_NAME
        ]
        version = kwargs.get("version")
        if version is not None:
            items = [artifact for artifact in items if artifact.version == version]
        for entry in filters:
            key, raw = entry.split(":", 1)
            if ":" in raw:
                _operator, value = raw.split(":", 1)
            else:
                value = raw
            items = [
                artifact
                for artifact in items
                if str(artifact.metadata.get(key)) == value
            ]
        return SimpleNamespace(items=items)


def _observation(
    execution_id: str,
    *,
    completed_at: str,
    status: ScoreObservationStatus = ScoreObservationStatus.SCORED,
    valid: bool = True,
    supersedes: str | None = None,
    project_id: str = "project-id",
) -> ScoreObservation:
    outcome = (
        ScoreObservationOutcome(
            status=ScoreObservationStatus.SCORED, score=Score(value=0.75), valid=valid
        )
        if status == ScoreObservationStatus.SCORED
        else ScoreObservationOutcome(
            status=status,
            reason="not available",
            valid=valid,
        )
    )
    return ScoreObservation(
        project_id=project_id,
        execution_id=execution_id,
        experiment_id="exp-score",
        scorer=SNAPSHOT,
        outcome=outcome,
        completed_at=completed_at,
        evidence_manifest_sha256=MANIFEST_HASH,
        comparative_original_execution_id="original-run",
        source_observation_ids=["source-1"],
        supersedes_observation_id=supersedes,
        explanation="stored explanation",
    )


def test_append_preserves_every_observation_and_does_not_touch_project_metadata() -> (
    None
):
    client = _Client()
    before = deepcopy(client.project_metadata)

    def save(**kwargs: Any) -> _Artifact:
        artifact_id = f"obs-{len(client.artifacts) + 1}"
        value = deepcopy(kwargs["data"])
        artifact = _Artifact(
            artifact_id,
            kwargs["name"],
            value,
            metadata=kwargs["user_metadata"],
        )
        client.artifacts.append(artifact)
        return artifact

    repo = ScoreObservationRepository(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    first = repo.append(_observation("run-1", completed_at="2026-07-18T10:00:00Z"))
    second = repo.append(_observation("run-1", completed_at="2026-07-18T10:00:00Z"))

    assert first.observation_id == "obs-1"
    assert second.observation_id == "obs-2"
    assert len(client.artifacts) == 2
    assert client.project_metadata == before
    assert client.artifacts[0]._value["observation_id"] is None
    assert client.artifacts[0].metadata["kitaru_execution_id"] == "run-1"
    assert (
        client.artifacts[0]._value["comparative_original_execution_id"]
        == "original-run"
    )
    assert client.artifacts[0]._value["source_observation_ids"] == ["source-1"]


def test_append_once_recovers_the_winner_after_a_concurrent_create() -> None:
    client = _Client()
    save_calls = 0

    def save(**kwargs: Any) -> _Artifact:
        nonlocal save_calls
        save_calls += 1
        artifact = _Artifact(
            "obs-winner",
            kwargs["name"],
            kwargs["data"],
            metadata=kwargs["user_metadata"],
            version=kwargs["version"],
        )
        client.artifacts.append(artifact)
        raise RuntimeError("version was created by the competing writer")

    repo = ScoreObservationRepository(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    observation = _observation("run-1", completed_at="2026-07-18T10:00:00Z")

    recovered = repo.append_once(observation, idempotency_key="matrix-cell")
    retried = repo.append_once(observation, idempotency_key="matrix-cell")

    assert recovered.observation_id == retried.observation_id == "obs-winner"
    assert save_calls == 1

    competing_result = observation.model_copy(
        update={"completed_at": "2026-07-18T10:01:00Z"}
    )
    assert (
        repo.append_once(competing_result, idempotency_key="matrix-cell") == recovered
    )

    conflicting = observation.model_copy(update={"execution_id": "run-2"})
    with pytest.raises(KitaruMetadataConflictError, match="conflicts"):
        repo.append_once(conflicting, idempotency_key="matrix-cell")


def test_query_filters_and_orders_timestamp_ties_by_observation_id() -> None:
    client = _Client()

    def save(**kwargs: Any) -> _Artifact:
        artifact_id = f"obs-{len(client.artifacts) + 1}"
        value = deepcopy(kwargs["data"])
        artifact = _Artifact(
            artifact_id,
            kwargs["name"],
            value,
            metadata=kwargs["user_metadata"],
        )
        client.artifacts.append(artifact)
        return artifact

    repo = ScoreObservationRepository(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    repo.append(_observation("run-2", completed_at="2026-07-18T10:00:01Z"))
    repo.append(_observation("run-1", completed_at="2026-07-18T10:00:00Z"))
    repo.append(
        _observation(
            "run-1",
            completed_at="2026-07-18T10:00:00Z",
            status=ScoreObservationStatus.ABSTAINED,
        )
    )

    matches = repo.list(ObservationQuery(execution_id="run-1"))

    assert [item.observation_id for item in matches] == ["obs-2", "obs-3"]
    assert [item.status for item in matches] == ["SCORED", "ABSTAINED"]
    assert client.list_calls[-1]["name"] == f"equals:{OBSERVATION_ARTIFACT_NAME}"
    assert client.list_calls[-1]["tags"] == OBSERVATION_ARTIFACT_TAG
    assert "kitaru_execution_id:run-1" in client.list_calls[-1]["run_metadata"]


def test_projection_can_exclude_invalid_and_superseded_rows() -> None:
    client = _Client()

    def save(**kwargs: Any) -> _Artifact:
        artifact_id = f"obs-{len(client.artifacts) + 1}"
        value = deepcopy(kwargs["data"])
        artifact = _Artifact(
            artifact_id,
            kwargs["name"],
            value,
            metadata=kwargs["user_metadata"],
        )
        client.artifacts.append(artifact)
        return artifact

    repo = ScoreObservationRepository(
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    first = repo.append(_observation("run-1", completed_at="2026-07-18T10:00:00Z"))
    repo.append(
        _observation(
            "run-1",
            completed_at="2026-07-18T10:01:00Z",
            supersedes=first.observation_id,
        )
    )
    repo.append(
        _observation(
            "run-1",
            completed_at="2026-07-18T10:02:00Z",
            valid=False,
        )
    )

    visible = repo.list(
        ObservationQuery(execution_id="run-1", include_superseded=False)
    )

    assert [item.observation_id for item in visible] == ["obs-2"]


def test_repository_rejects_cross_project_writes_and_payloads() -> None:
    client = _Client()
    repo = ScoreObservationRepository(
        project_id="project-id", client=client, save_artifact_fn=lambda **_: None
    )

    with pytest.raises(KitaruStateError, match="across projects"):
        repo.append(
            _observation(
                "run-1",
                completed_at="2026-07-18T10:00:00Z",
                project_id="other-project",
            )
        )

    foreign = _observation(
        "run-1",
        completed_at="2026-07-18T10:00:00Z",
        project_id="other-project",
    ).model_dump(mode="json")
    artifact = _Artifact("obs-foreign", OBSERVATION_ARTIFACT_NAME, foreign)
    with pytest.raises(KitaruMetadataConflictError, match="different project"):
        repo._load_observation(artifact)
