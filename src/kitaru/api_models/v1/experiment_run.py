#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Experiment run API models."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    FiniteFloat,
    OwnedResponseModel,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    check_baseline_evaluation_fields,
)


class ExperimentRunStatus(StrEnum):
    """Experiment run status."""

    RUNNING = "running"
    CANCELING = "canceling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class ExperimentRunProgress(ResponseModel):
    """Experiment run progress."""

    pending: int = Field(description="Replays pending.")
    evaluating: int = Field(description="Replays evaluating.")
    completed: int = Field(description="Replays completed.")
    failed: int = Field(description="Replays failed.")
    canceled: int = Field(description="Replays canceled.")
    total: int = Field(description="Total replays in the run.")


class ExperimentRunCreateRequest(RequestModel):
    """Experiment run create request."""

    cohort_version_id: uuid.UUID = Field(
        description="Cohort version whose sessions are replayed."
    )
    agent_version_id: uuid.UUID = Field(description="Agent version to replay with.")
    evaluate_baselines: bool = Field(
        default=False,
        deprecated="Use baseline_evaluation_mode instead.",
        description="Whether to also score each baseline session.",
    )
    baseline_evaluation_mode: BaselineEvaluationMode | None = Field(
        default=None, description="How to score each baseline session."
    )

    @model_validator(mode="after")
    def _evaluate_baselines_xor_mode(self) -> Self:
        """Forbid setting both evaluate_baselines and baseline_evaluation_mode.

        Raises:
            ValueError: Both fields were explicitly set.

        Returns:
            The validated request.
        """
        check_baseline_evaluation_fields(self.model_fields_set)
        return self


class ExperimentRunListParams(FilterableListParams):
    """Experiment run list params."""


class ExperimentRunJobsListParams(FilterableListParams):
    """Experiment run jobs list params."""


class ExperimentRunResponse(OwnedResponseModel):
    """Experiment run response."""

    id: uuid.UUID = Field(description="Experiment run id.")
    experiment_id: uuid.UUID = Field(description="Experiment this run belongs to.")
    number: int = Field(description="Run number within the experiment.")
    status: ExperimentRunStatus = Field(description="Run status.")
    cohort_version_id: uuid.UUID = Field(
        description="Cohort version whose sessions are replayed."
    )
    agent_version_id: uuid.UUID = Field(description="Agent version to replay with.")
    evaluate_baselines: bool = Field(
        deprecated="Use baseline_evaluation_mode instead.",
        description="Whether baseline sessions are also scored.",
    )
    baseline_evaluation_mode: BaselineEvaluationMode = Field(
        description="How baseline sessions are scored."
    )
    started_at: datetime | None = Field(
        default=None, description="Time the run started."
    )
    ended_at: datetime | None = Field(default=None, description="Time the run settled.")
    error: str | None = Field(default=None, description="Error from a failed run.")
    progress: ExperimentRunProgress = Field(description="Replay counts by status.")


class EvaluationValue(ResponseModel):
    """Evaluation value."""

    score: FiniteFloat | bool | None = Field(
        default=None, description="Numeric or boolean score."
    )
    value: str | None = Field(default=None, description="Label or string value.")
    passed: bool | None = Field(default=None, description="Pass or fail verdict.")
    min_score: FiniteFloat | None = Field(
        default=None, description="Lower bound of the score scale."
    )
    max_score: FiniteFloat | None = Field(
        default=None, description="Upper bound of the score scale."
    )
    target_score: FiniteFloat | None = Field(default=None, description="Score to beat.")


class ReplayEvaluationValues(ResponseModel):
    """Replay evaluation values."""

    replay_id: uuid.UUID = Field(description="Replay id.")
    baseline: EvaluationValue | None = Field(
        default=None, description="Value from the baseline session."
    )
    result: EvaluationValue | None = Field(
        default=None, description="Value from the result session."
    )


class EvaluationStats(ResponseModel):
    """Evaluation stats."""

    count: int = Field(description="Number of aggregated evaluations.")
    mean: float | None = Field(
        default=None,
        description="Mean score of float evaluations, share of true results of "
        "bool evaluations, null for other data types.",
    )
    min: float | None = Field(
        default=None,
        description="Lowest score of float and bool evaluations, null for "
        "other data types.",
    )
    max: float | None = Field(
        default=None,
        description="Highest score of float and bool evaluations, null for "
        "other data types.",
    )
    pass_rate: float | None = Field(
        default=None,
        description="Share of passed evaluations among those carrying a passed "
        "flag, null when none do.",
    )
    value_counts: dict[str, int] | None = Field(
        default=None,
        description="Occurrences per value, only for categorical evaluations.",
    )
    min_score: float | None = Field(
        default=None,
        description="Lower bound of the score scale shared by every aggregated "
        "evaluation, null when they differ or one lacks it.",
    )
    max_score: float | None = Field(
        default=None,
        description="Upper bound of the score scale shared by every aggregated "
        "evaluation, null when they differ or one lacks it.",
    )
    target_score: float | None = Field(
        default=None,
        description="Score to beat shared by every aggregated evaluation, null "
        "when they differ or one lacks it.",
    )


class EvaluationAggregateResponse(ResponseModel):
    """Evaluation aggregate response."""

    name: str = Field(description="Evaluation name.")
    evaluator_version_id: uuid.UUID | None = Field(
        default=None, description="Evaluator version that produced the group."
    )
    evaluator_name: str | None = Field(
        default=None, description="Name of the evaluator that produced the group."
    )
    evaluator_version: int | None = Field(
        default=None, description="Version of the evaluator that produced the group."
    )
    data_type: EvaluationDataType = Field(description="Evaluation data type.")
    baseline: EvaluationStats = Field(description="Stats over the baseline sessions.")
    result: EvaluationStats = Field(description="Stats over the result sessions.")
    replays: list[ReplayEvaluationValues] = Field(
        description="Evaluation values of the 50 most recent replays, oldest first."
    )
