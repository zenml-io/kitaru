"""Read-only experiment views exposed to SDK users."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

from zenml.client import Client
from zenml.models import PipelineRunResponse
from zenml.models.v2.base.page import Page

from kitaru._experiments._models import (
    ExperimentRecord,
    ExperimentSpec,
    ExperimentSpecRecord,
)
from kitaru.errors import KitaruUsageError
from kitaru.replay import EXPERIMENT_TAG_PREFIX, ReplaySubmission
from kitaru.scoring import (
    ExperimentVerdict,
    ScoreAttemptAggregate,
    VerdictResult,
)


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

    def list(self, *, page: int = 1, size: int = 50) -> Page[PipelineRunResponse]:
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
    def spec(self) -> ExperimentSpecRecord:
        """Return the immutable submitted specification."""
        return self.record.spec

    @property
    def experiment_id(self) -> str:
        """Return the durable attempt ID."""
        return self.spec.experiment_id

    @property
    def verdict(self) -> VerdictResult | None:
        """Return the immutable PASS, FAIL, or HOLD result when graded."""
        return self.record.verdict

    @property
    def score_aggregate(self) -> ScoreAttemptAggregate | None:
        """Load the immutable score aggregate when one is attached."""
        if self.record.score_aggregate is None:
            return None
        from kitaru.scoring import load_score_aggregate

        return load_score_aggregate(
            self.record.score_aggregate,
            project_id=self.runs.project_id,
            client=self.runs._client_factory(),
        )

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
        return cast(ExperimentSpec, self.record.spec)

    @property
    def verdict(self) -> VerdictResult | None:
        """Return the immutable PASS, FAIL, or HOLD result when graded."""
        return self.record.verdict

    def regression_summary(self) -> dict[str, Any]:
        """Return the bounded CI-facing verdict and diagnostic facts."""
        verdict = self.record.verdict
        operational_limit = self.record.operational_limit
        one_trial_may_overshoot = (
            operational_limit is not None
            and operational_limit.facts.one_trial_may_overshoot
        )
        return {
            "suite_key": self.spec.suite_key,
            "attempt_id": self.spec.experiment_id,
            "verdict": None if verdict is None else verdict.verdict.value,
            "objective": (
                None
                if verdict is None or verdict.objective is None
                else verdict.objective.model_dump(mode="json")
            ),
            "failed_protections": (
                []
                if verdict is None
                else [
                    fact.model_dump(mode="json")
                    for fact in verdict.protections
                    if fact.passed is False
                ]
            ),
            "incomplete_counts": self.record.counts.model_dump(mode="json"),
            "operational_limit": (
                None
                if operational_limit is None
                else operational_limit.model_dump(mode="json")
            ),
            "compare_url": self.submission.compare_url,
            "limit_note": (
                "Cost and token limits are checked between terminal trials; one "
                "trial may cross a ceiling before further submissions stop."
                if one_trial_may_overshoot
                else None
            ),
        }

    def assert_pass(self) -> None:
        """Raise an ``AssertionError`` unless this attempt has a PASS verdict."""
        verdict = self.record.verdict
        if verdict is not None and verdict.verdict == ExperimentVerdict.PASS:
            return
        details = self.regression_summary()
        raise AssertionError(
            "Regression suite did not pass.\n"
            + json.dumps(details, indent=2, sort_keys=True)
        )

    def to_json(self) -> dict[str, Any]:
        """Serialize durable state and replay details without fetching member runs."""
        from kitaru._inspection_serialization import (
            serialize_experiment_replay_result,
        )

        return serialize_experiment_replay_result(self)
