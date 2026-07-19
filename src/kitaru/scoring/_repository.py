"""Append-only score observation persistence."""

from __future__ import annotations

import builtins
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactSaveType, ArtifactType

from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.scoring._contracts import (
    ScoreObservation,
    ScoreObservationStatus,
    require_string,
)

OBSERVATION_ARTIFACT_NAME = "kitaru-score-observations"
OBSERVATION_ARTIFACT_TAG = "kitaru-score-observation-v1"
OBSERVATION_SCHEMA_VERSION = 1
_DEFAULT_OBSERVATION_SCAN_LIMIT = 5000


@dataclass(frozen=True)
class ObservationQuery:
    """Filter options for score observation history."""

    execution_id: str | None = None
    experiment_id: str | None = None
    scorer_name: str | None = None
    scorer_revision: str | None = None
    scorer_configuration_hash: str | None = None
    status: ScoreObservationStatus | None = None
    valid: bool | None = None
    completed_at_gte: str | None = None
    completed_at_lt: str | None = None
    include_superseded: bool = True

    def __post_init__(self) -> None:
        """Normalize enum-like public inputs once at the query boundary."""
        if self.status is not None and not isinstance(
            self.status, ScoreObservationStatus
        ):
            object.__setattr__(self, "status", ScoreObservationStatus(str(self.status)))


class ScoreObservationRepository:
    """Append and query immutable score observations in ZenML artifacts."""

    def __init__(
        self,
        *,
        project_id: str,
        client: Any | None = None,
        save_artifact_fn: Callable[..., Any] = save_artifact,
    ) -> None:
        self.project_id = require_string(project_id, field_name="Project ID")
        self._client = client
        self._save_artifact = save_artifact_fn

    @property
    def client(self) -> Any:
        """Return the repository client, creating the default ZenML client lazily."""
        if self._client is None:
            self._client = Client()
        return self._client

    def append(self, observation: ScoreObservation) -> ScoreObservation:
        """Persist one new observation and return it with its artifact-version ID."""
        payload, metadata = self._prepare_write(observation)
        try:
            artifact = self._save_artifact(
                data=payload,
                name=OBSERVATION_ARTIFACT_NAME,
                artifact_type=ArtifactType.DATA,
                save_type=ArtifactSaveType.MANUAL,
                has_custom_name=True,
                tags=[OBSERVATION_ARTIFACT_TAG],
                extract_metadata=False,
                include_visualizations=False,
                user_metadata=metadata,
            )
        except Exception as exc:
            raise KitaruBackendError("Unable to save the score observation.") from exc
        return self._with_artifact_id(observation, artifact)

    def append_once(
        self,
        observation: ScoreObservation,
        *,
        idempotency_key: str,
    ) -> ScoreObservation:
        """Create or recover one immutable observation for a matrix cell."""
        payload, metadata = self._prepare_write(observation)
        normalized_key = require_string(
            idempotency_key, field_name="Observation idempotency key"
        )
        version = uuid5(
            NAMESPACE_URL,
            f"kitaru-score-observation:{self.project_id}:{normalized_key}",
        ).hex
        artifact = self._find_idempotent_artifact(version)
        if artifact is None:
            try:
                artifact = self._save_artifact(
                    data=payload,
                    name=OBSERVATION_ARTIFACT_NAME,
                    version=version,
                    artifact_type=ArtifactType.DATA,
                    save_type=ArtifactSaveType.MANUAL,
                    has_custom_name=True,
                    tags=[OBSERVATION_ARTIFACT_TAG],
                    extract_metadata=False,
                    include_visualizations=False,
                    user_metadata=metadata,
                )
            except Exception as exc:
                artifact = self._find_idempotent_artifact(version)
                if artifact is None:
                    raise KitaruBackendError(
                        "Unable to save the score observation."
                    ) from exc
        persisted = self._load_observation(artifact)
        if _observation_cell_identity(persisted) != _observation_cell_identity(
            observation
        ):
            raise KitaruMetadataConflictError(
                "The existing idempotent score observation conflicts with this cell."
            )
        return persisted

    def _prepare_write(
        self, observation: ScoreObservation
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if observation.project_id != self.project_id:
            raise KitaruStateError(
                "Score observations cannot be written across projects."
            )
        active_project_id = str(
            getattr(getattr(self.client, "active_project", None), "id", "")
        ).strip()
        if active_project_id and active_project_id != self.project_id:
            raise KitaruStateError(
                "Score observations require the Agent Project to be active."
            )
        payload = observation.model_copy(update={"observation_id": None}).model_dump(
            mode="json"
        )
        return payload, _observation_metadata(observation)

    def _with_artifact_id(
        self, observation: ScoreObservation, artifact: Any
    ) -> ScoreObservation:
        observation_id = str(getattr(artifact, "id", "")).strip()
        if not observation_id:
            raise KitaruStateError("The score observation has no artifact-version ID.")
        return observation.model_copy(
            update={"observation_id": observation_id}, deep=True
        )

    def _find_idempotent_artifact(self, version: str) -> Any | None:
        try:
            page = self.client.list_artifact_versions(
                name=f"equals:{OBSERVATION_ARTIFACT_NAME}",
                version=version,
                project=self.project_id,
                tags=OBSERVATION_ARTIFACT_TAG,
                hydrate=True,
                size=2,
            )
        except Exception as exc:
            raise KitaruBackendError(
                "Unable to resolve the idempotent score observation."
            ) from exc
        items = _page_items(page)
        if len(items) > 1:
            raise KitaruMetadataConflictError(
                "Multiple artifacts match one idempotent score observation."
            )
        return items[0] if items else None

    def list(
        self,
        query: ObservationQuery | None = None,
        *,
        page: int = 1,
        size: int = 50,
        scan_limit: int = _DEFAULT_OBSERVATION_SCAN_LIMIT,
    ) -> builtins.list[ScoreObservation]:
        """Load matching observations in deterministic completed_at, UUID order."""
        if page < 1 or size < 1:
            raise KitaruStateError("Observation query page and size must be positive.")
        normalized = query or ObservationQuery()
        if normalized.include_superseded:
            artifacts = self._artifact_page(
                normalized,
                page=page,
                size=size,
                hydrate=True,
            )
            observations = [self._load_observation(item) for item in artifacts]
            return _sort_observations(observations)

        projected = self._projected_history(normalized, scan_limit=scan_limit)
        start = (page - 1) * size
        return projected[start : start + size]

    def latest_valid(
        self,
        query: ObservationQuery | None = None,
        *,
        scan_limit: int = _DEFAULT_OBSERVATION_SCAN_LIMIT,
    ) -> ScoreObservation | None:
        """Return the latest valid scored observation in one bounded projection."""
        base = query or ObservationQuery()
        narrowed = ObservationQuery(
            execution_id=base.execution_id,
            experiment_id=base.experiment_id,
            scorer_name=base.scorer_name,
            scorer_revision=base.scorer_revision,
            scorer_configuration_hash=base.scorer_configuration_hash,
            status=ScoreObservationStatus.SCORED,
            valid=True,
            completed_at_gte=base.completed_at_gte,
            completed_at_lt=base.completed_at_lt,
            include_superseded=False,
        )
        projected = self._projected_history(narrowed, scan_limit=scan_limit)
        return projected[-1] if projected else None

    def matching_execution_ids(
        self,
        query: ObservationQuery,
        *,
        minimum: float | None = None,
        maximum: float | None = None,
        cap: int,
        scan_limit: int = _DEFAULT_OBSERVATION_SCAN_LIMIT,
    ) -> set[str]:
        """Return execution IDs using observation metadata without payload loads."""
        if cap < 1:
            raise KitaruUsageError("Score filter candidate_cap must be >= 1.")
        filters = _metadata_filters(self.project_id, query)
        if minimum is not None:
            filters.append(f"kitaru_score_value:gte:{minimum}")
        if maximum is not None:
            filters.append(f"kitaru_score_value:lte:{maximum}")
        selected: set[str] = set()
        artifacts = self._all_artifacts(
            filters=filters,
            hydrate=False,
            scan_limit=scan_limit,
        )
        for artifact in artifacts:
            metadata = _artifact_metadata(artifact)
            execution_id = str(metadata.get("kitaru_execution_id") or "").strip()
            if not execution_id:
                continue
            if not _metadata_score_matches(metadata, minimum=minimum, maximum=maximum):
                continue
            selected.add(execution_id)
            if len(selected) > cap:
                raise KitaruUsageError(
                    "Score filter matched too many executions. Narrow the score "
                    "filter or raise candidate_cap."
                )
        return selected

    def _projected_history(
        self,
        query: ObservationQuery,
        *,
        scan_limit: int,
    ) -> builtins.list[ScoreObservation]:
        if scan_limit < 1:
            raise KitaruUsageError("Observation scan_limit must be >= 1.")
        filters = _metadata_filters(self.project_id, query)
        artifacts = self._all_artifacts(
            filters=filters,
            hydrate=True,
            scan_limit=scan_limit,
        )
        observations = [self._load_observation(item) for item in artifacts]
        selected = _apply_supersession_view(observations, query)
        return _sort_observations(selected)

    def _artifact_page(
        self,
        query: ObservationQuery,
        *,
        page: int,
        size: int,
        hydrate: bool,
    ) -> builtins.list[Any]:
        filters = _metadata_filters(self.project_id, query)
        try:
            artifact_page = self.client.list_artifact_versions(
                name=f"equals:{OBSERVATION_ARTIFACT_NAME}",
                project=self.project_id,
                tags=OBSERVATION_ARTIFACT_TAG,
                run_metadata=filters,
                hydrate=hydrate,
                sort_by="asc:created",
                page=page,
                size=size,
            )
        except Exception as exc:
            raise KitaruBackendError("Unable to query score observations.") from exc
        return _page_items(artifact_page)

    def _all_artifacts(
        self,
        *,
        filters: Sequence[str],
        hydrate: bool,
        scan_limit: int,
    ) -> builtins.list[Any]:
        page = 1
        page_size = min(100, scan_limit)
        artifacts: list[Any] = []
        while True:
            try:
                artifact_page = self.client.list_artifact_versions(
                    name=f"equals:{OBSERVATION_ARTIFACT_NAME}",
                    project=self.project_id,
                    tags=OBSERVATION_ARTIFACT_TAG,
                    run_metadata=list(filters),
                    hydrate=hydrate,
                    sort_by="asc:created",
                    page=page,
                    size=page_size,
                )
            except Exception as exc:
                raise KitaruBackendError("Unable to query score observations.") from exc
            items = _page_items(artifact_page)
            artifacts.extend(items)
            if len(items) < page_size:
                return artifacts
            if len(artifacts) >= scan_limit:
                raise KitaruUsageError(
                    "Score observation history exceeded the bounded scan limit. "
                    "Narrow the observation filters."
                )
            page += 1

    def _load_observation(self, artifact: Any) -> ScoreObservation:
        artifact_id = str(getattr(artifact, "id", "")).strip()
        if not artifact_id:
            raise KitaruMetadataConflictError("Score observation artifact has no ID.")
        try:
            loaded = artifact.load()
        except Exception as exc:
            raise KitaruBackendError("Unable to load a score observation.") from exc
        observation = ScoreObservation.model_validate(loaded).model_copy(
            update={"observation_id": artifact_id},
            deep=True,
        )
        if observation.project_id != self.project_id:
            raise KitaruMetadataConflictError(
                "Score observation payload belongs to a different project."
            )
        return observation


def _observation_metadata(observation: ScoreObservation) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "kitaru_project_id": observation.project_id,
        "kitaru_execution_id": observation.execution_id,
        "kitaru_experiment_id": observation.experiment_id,
        "kitaru_scorer_name": observation.scorer.name,
        "kitaru_scorer_revision": observation.scorer.revision,
        "kitaru_scorer_config_hash": observation.scorer.configuration_hash,
        "kitaru_score_status": observation.status.value,
        "kitaru_score_valid": str(observation.valid).lower(),
        "kitaru_score_completed_at": observation.completed_at,
        "kitaru_score_schema_version": OBSERVATION_SCHEMA_VERSION,
    }
    if observation.outcome.score is not None:
        metadata["kitaru_score_value"] = observation.outcome.score.value
    return metadata


def _observation_cell_identity(observation: ScoreObservation) -> tuple[Any, ...]:
    return (
        observation.project_id,
        observation.experiment_id,
        observation.execution_id,
        observation.scorer,
        observation.evidence_manifest_sha256,
        observation.comparative_original_execution_id,
    )


def _metadata_filters(project_id: str, query: ObservationQuery) -> list[str]:
    filters = [f"kitaru_project_id:{project_id}"]
    for key, value in (
        ("kitaru_execution_id", query.execution_id),
        ("kitaru_experiment_id", query.experiment_id),
        ("kitaru_scorer_name", query.scorer_name),
        ("kitaru_scorer_revision", query.scorer_revision),
        ("kitaru_scorer_config_hash", query.scorer_configuration_hash),
        ("kitaru_score_status", query.status.value if query.status else None),
    ):
        if value is not None:
            filters.append(f"{key}:{value}")
    if query.valid is not None:
        filters.append(f"kitaru_score_valid:{str(query.valid).lower()}")
    if query.completed_at_gte is not None:
        filters.append(f"kitaru_score_completed_at:gte:{query.completed_at_gte}")
    if query.completed_at_lt is not None:
        filters.append(f"kitaru_score_completed_at:lt:{query.completed_at_lt}")
    filters.append(f"kitaru_score_schema_version:{OBSERVATION_SCHEMA_VERSION}")
    return filters


def _page_items(page: Any) -> list[Any]:
    items = getattr(page, "items", page)
    if callable(items):
        items = items()
    return list(items)


def _apply_supersession_view(
    observations: Sequence[ScoreObservation],
    query: ObservationQuery,
) -> list[ScoreObservation]:
    if query.include_superseded:
        return list(observations)
    superseded_ids = {
        observation.supersedes_observation_id
        for observation in observations
        if observation.supersedes_observation_id
    }
    return [
        observation
        for observation in observations
        if observation.observation_id not in superseded_ids and observation.valid
    ]


def _sort_observations(
    observations: Sequence[ScoreObservation],
) -> list[ScoreObservation]:
    return sorted(
        observations,
        key=lambda item: (item.completed_at, item.observation_id or ""),
    )


def _artifact_metadata(artifact: Any) -> dict[str, Any]:
    raw = (
        getattr(artifact, "metadata", None)
        or getattr(artifact, "run_metadata", None)
        or {}
    )
    if not isinstance(raw, dict):
        return {}
    metadata: dict[str, Any] = {}
    for key, value in raw.items():
        metadata[str(key)] = getattr(value, "value", value)
    return metadata


def _metadata_score_matches(
    metadata: dict[str, Any],
    *,
    minimum: float | None,
    maximum: float | None,
) -> bool:
    if minimum is None and maximum is None:
        return True
    raw_value = metadata.get("kitaru_score_value")
    if raw_value is None:
        return False
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return False
    if minimum is not None and value < minimum:
        return False
    return not (maximum is not None and value > maximum)
