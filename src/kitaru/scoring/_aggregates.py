"""Immutable descriptive aggregate artifacts for score attempts."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactSaveType, ArtifactType

from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruStateError,
)
from kitaru.scoring._contracts import (
    ScoreObservation,
    ScoreObservationStatus,
    ScorerAggregate,
    require_string,
    sha256_json,
    validate_sha256,
)

AGGREGATE_ARTIFACT_NAME = "kitaru-score-aggregates"
AGGREGATE_ARTIFACT_TAG = "kitaru-score-aggregate-v1"
AGGREGATE_SCHEMA_VERSION = 1


class ScoreAggregateReference(BaseModel):
    """Immutable artifact reference for one frozen score-attempt aggregate."""

    schema_version: Literal[1] = 1
    artifact_version_id: str
    sha256: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("artifact_version_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return require_string(value, field_name="Aggregate artifact version ID")

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)


class ScoreAttemptAggregate(BaseModel):
    """Frozen as-of descriptive aggregate for a score-producing attempt."""

    schema_version: Literal[1] = 1
    experiment_id: str
    project_id: str
    observation_ids: list[str]
    scorer_aggregates: list[ScorerAggregate]
    planned: int = Field(ge=0)
    scored: int = Field(ge=0)
    abstained: int = Field(ge=0)
    blocked: int = Field(ge=0)
    error: int = Field(ge=0)
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("experiment_id", "project_id")
    @classmethod
    def _validate_strings(cls, value: str) -> str:
        return require_string(value, field_name="Score aggregate field")

    @field_validator("observation_ids")
    @classmethod
    def _validate_observation_ids(cls, value: list[str]) -> list[str]:
        normalized = [
            require_string(item, field_name="Score observation ID") for item in value
        ]
        if len(normalized) != len(set(normalized)):
            raise ValueError("Score aggregate observation IDs must be unique.")
        return normalized

    @field_validator("content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_content_hash(self) -> ScoreAttemptAggregate:
        expected = sha256_json(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("Score aggregate content_hash does not match payload.")
        return self

    @classmethod
    def create(
        cls,
        *,
        experiment_id: str,
        project_id: str,
        observations: Sequence[ScoreObservation],
        planned: int | None = None,
    ) -> ScoreAttemptAggregate:
        """Build immutable descriptive aggregates from selected observations."""
        normalized_project = require_string(project_id, field_name="Project ID")
        normalized_experiment = require_string(
            experiment_id, field_name="Experiment ID"
        )
        selected = list(observations)
        if any(item.project_id != normalized_project for item in selected):
            raise KitaruStateError(
                "Score aggregate observations must stay in one project."
            )
        if any(item.experiment_id != normalized_experiment for item in selected):
            raise KitaruStateError(
                "Score aggregate observations must belong to one attempt."
            )
        scorer_aggregates = _per_scorer_aggregates(selected)
        observation_ids = [item.observation_id or "" for item in selected]
        aggregate_planned = planned if planned is not None else len(selected)
        scored = sum(item.status is ScoreObservationStatus.SCORED for item in selected)
        abstained = sum(
            item.status is ScoreObservationStatus.ABSTAINED for item in selected
        )
        blocked = sum(
            item.status is ScoreObservationStatus.BLOCKED for item in selected
        )
        error = sum(item.status is ScoreObservationStatus.ERROR for item in selected)
        hash_payload = {
            "schema_version": AGGREGATE_SCHEMA_VERSION,
            "experiment_id": normalized_experiment,
            "project_id": normalized_project,
            "observation_ids": observation_ids,
            "scorer_aggregates": [
                item.model_dump(mode="json") for item in scorer_aggregates
            ],
            "planned": aggregate_planned,
            "scored": scored,
            "abstained": abstained,
            "blocked": blocked,
            "error": error,
        }
        return cls(
            experiment_id=normalized_experiment,
            project_id=normalized_project,
            observation_ids=observation_ids,
            scorer_aggregates=scorer_aggregates,
            planned=aggregate_planned,
            scored=scored,
            abstained=abstained,
            blocked=blocked,
            error=error,
            content_hash=sha256_json(hash_payload),
        )


def load_score_aggregate(
    reference: ScoreAggregateReference,
    *,
    project_id: str,
    client: Any,
) -> ScoreAttemptAggregate:
    """Load and hash-verify an immutable score aggregate artifact."""
    try:
        artifact = client.get_artifact_version(
            name_id_or_prefix=reference.artifact_version_id,
            project=project_id,
            hydrate=True,
        )
        loaded = artifact.load()
    except Exception as exc:
        raise KitaruBackendError("Unable to load the score aggregate.") from exc
    aggregate = ScoreAttemptAggregate.model_validate(loaded)
    if aggregate.project_id != project_id or aggregate.content_hash != reference.sha256:
        raise KitaruMetadataConflictError("Score aggregate project or hash mismatch.")
    return aggregate


def persist_score_aggregate(
    aggregate: ScoreAttemptAggregate,
    *,
    project_id: str,
    client: Any | None = None,
    save_artifact_fn: Callable[..., Any] = save_artifact,
) -> ScoreAggregateReference:
    """Persist a score aggregate as an immutable manual artifact version."""
    if aggregate.project_id != project_id:
        raise KitaruStateError("Score aggregates cannot be written across projects.")
    resolved_client = client or Client()
    active_project_id = str(
        getattr(getattr(resolved_client, "active_project", None), "id", "")
    ).strip()
    if active_project_id != project_id:
        raise KitaruStateError(
            "Score aggregates require the Agent Project to be active."
        )

    try:
        artifact = save_artifact_fn(
            data=aggregate.model_dump(mode="json"),
            name=AGGREGATE_ARTIFACT_NAME,
            version=aggregate.experiment_id,
            artifact_type=ArtifactType.DATA,
            save_type=ArtifactSaveType.MANUAL,
            has_custom_name=True,
            tags=[AGGREGATE_ARTIFACT_TAG],
            extract_metadata=False,
            include_visualizations=False,
            user_metadata={
                "kitaru_project_id": project_id,
                "kitaru_experiment_id": aggregate.experiment_id,
                "kitaru_score_aggregate_sha256": aggregate.content_hash,
                "kitaru_score_schema_version": AGGREGATE_SCHEMA_VERSION,
            },
        )
    except Exception as exc:
        artifact = _find_score_aggregate(
            resolved_client,
            experiment_id=aggregate.experiment_id,
            project_id=project_id,
        )
        if artifact is None:
            raise KitaruBackendError("Unable to save the score aggregate.") from exc

    artifact_id = str(getattr(artifact, "id", "")).strip()
    if not artifact_id:
        raise KitaruStateError("The score aggregate has no artifact-version ID.")
    try:
        loaded = resolved_client.get_artifact_version(
            name_id_or_prefix=artifact_id,
            project=project_id,
            hydrate=True,
        ).load()
    except Exception as exc:
        raise KitaruBackendError("Unable to verify the score aggregate.") from exc
    loaded_aggregate = ScoreAttemptAggregate.model_validate(loaded)
    if loaded_aggregate != aggregate:
        raise KitaruMetadataConflictError(
            "The existing score aggregate conflicts with this idempotent attempt."
        )
    return ScoreAggregateReference(
        artifact_version_id=artifact_id, sha256=aggregate.content_hash
    )


def _find_score_aggregate(
    client: Any,
    *,
    experiment_id: str,
    project_id: str,
) -> Any | None:
    try:
        page = client.list_artifact_versions(
            name=f"equals:{AGGREGATE_ARTIFACT_NAME}",
            version=experiment_id,
            project=project_id,
            tags=AGGREGATE_ARTIFACT_TAG,
            hydrate=True,
            size=2,
        )
    except Exception as exc:
        raise KitaruBackendError("Unable to resolve the score aggregate.") from exc
    items = list(getattr(page, "items", page))
    exact = [
        item
        for item in items
        if str(getattr(item, "name", "")) == AGGREGATE_ARTIFACT_NAME
    ]
    if len(exact) > 1:
        raise KitaruMetadataConflictError(
            "Multiple artifact versions match the immutable score aggregate."
        )
    return exact[0] if exact else None


def _per_scorer_aggregates(
    observations: Sequence[ScoreObservation],
) -> list[ScorerAggregate]:
    groups: dict[tuple[str, str, str], list[ScoreObservation]] = {}
    for observation in observations:
        key = (
            observation.scorer.name,
            observation.scorer.revision,
            observation.scorer.configuration_hash,
        )
        groups.setdefault(key, []).append(observation)
    aggregates: list[ScorerAggregate] = []
    for (name, revision, config_hash), group in sorted(groups.items()):
        values = [
            item.outcome.score.value for item in group if item.outcome.score is not None
        ]
        scores_by_execution = {
            item.execution_id: item.outcome.score.value
            for item in group
            if item.outcome.score is not None
        }
        deltas = [
            item.outcome.score.value
            - scores_by_execution[item.comparative_original_execution_id]
            for item in group
            if item.outcome.score is not None
            and item.comparative_original_execution_id in scores_by_execution
        ]
        minimum = min(values) if values else None
        maximum = max(values) if values else None
        delta_minimum = min(deltas) if deltas else None
        delta_maximum = max(deltas) if deltas else None
        aggregates.append(
            ScorerAggregate(
                scorer_name=name,
                scorer_revision=revision,
                scorer_configuration_hash=config_hash,
                planned=len(group),
                denominator=len(group),
                scored=len(values),
                abstained=sum(
                    item.status is ScoreObservationStatus.ABSTAINED for item in group
                ),
                blocked=sum(
                    item.status is ScoreObservationStatus.BLOCKED for item in group
                ),
                error=sum(
                    item.status is ScoreObservationStatus.ERROR for item in group
                ),
                mean=(sum(values) / len(values)) if values else None,
                minimum=minimum,
                maximum=maximum,
                spread=(maximum - minimum)
                if minimum is not None and maximum is not None
                else None,
                paired_delta_count=len(deltas),
                paired_delta_mean=(sum(deltas) / len(deltas)) if deltas else None,
                paired_delta_minimum=delta_minimum,
                paired_delta_maximum=delta_maximum,
            )
        )
    return aggregates
