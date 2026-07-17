"""Read-only experiment views exposed to SDK users."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from zenml.client import Client

from kitaru._experiments._models import ExperimentRecord, ExperimentSpec
from kitaru.errors import KitaruUsageError
from kitaru.replay import EXPERIMENT_TAG_PREFIX, ReplaySubmission


@dataclass(frozen=True)
class ExperimentRunLookup:
    """Lazy paginated member-run lookup backed by ZenML native tags."""

    experiment_id: str
    project_id: str
    _client_factory: Callable[[], Any] = field(
        default=Client,
        repr=False,
        compare=False,
    )

    def list(self, *, page: int = 1, size: int = 50) -> Any:
        """Return one lightweight native ZenML run page for this experiment."""
        if isinstance(page, bool) or page < 1:
            raise KitaruUsageError("page must be >= 1.")
        if isinstance(size, bool) or size < 1:
            raise KitaruUsageError("size must be >= 1.")
        return self._client_factory().list_pipeline_runs(
            sort_by="asc:created",
            page=page,
            size=size,
            project=self.project_id,
            hydrate=False,
            tags=[f"{EXPERIMENT_TAG_PREFIX}{self.experiment_id}"],
        )


@dataclass(frozen=True)
class Experiment:
    """Read-only experiment detail backed by Project metadata and native runs."""

    record: ExperimentRecord
    runs: ExperimentRunLookup

    @property
    def spec(self) -> ExperimentSpec:
        """Return the immutable submitted specification."""
        return self.record.spec

    @property
    def experiment_id(self) -> str:
        """Return the durable attempt ID."""
        return self.spec.experiment_id

    def to_json(self) -> dict[str, Any]:
        """Serialize the frontend contract without fetching member runs."""
        from kitaru._inspection_serialization import serialize_experiment

        return serialize_experiment(self)


@dataclass(frozen=True)
class ExperimentReplayResult:
    """Typed durable experiment result with the existing replay projection."""

    record: ExperimentRecord
    submission: ReplaySubmission
    runs: ExperimentRunLookup

    @property
    def spec(self) -> ExperimentSpec:
        """Return the immutable specification for this attempt."""
        return self.record.spec

    def to_json(self) -> dict[str, Any]:
        """Serialize durable state and replay details without fetching member runs."""
        from kitaru._inspection_serialization import (
            serialize_experiment_replay_result,
        )

        return serialize_experiment_replay_result(self)
