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
"""UI API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import FiniteFloat, RequestModel, ResponseModel
from kitaru.api_models.v1.evaluation import EvaluationDataType, EvaluationResponse
from kitaru.api_models.v1.session import SessionDetailResponse, SessionResponse


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


class SessionWithEvaluationsResponse(ResponseModel):
    """Session with evaluations response."""

    session: SessionResponse = Field(description="Session.")
    evaluations: list[EvaluationResponse] = Field(
        description="Every evaluation of the session, newest first."
    )


class SessionDetailWithEvaluationsResponse(ResponseModel):
    """Session detail with evaluations response."""

    session: SessionDetailResponse = Field(description="Session.")
    evaluations: list[EvaluationResponse] = Field(
        description="Every evaluation of the session, newest first."
    )


class SampleDataCreateRequest(RequestModel):
    """Sample data create request."""

    agent_name: str | None = Field(
        default=None, description="Agent name, None uses the sample data's agent name."
    )


class SampleDataResponse(ResponseModel):
    """Sample data response."""

    agent_id: uuid.UUID = Field(description="Agent the sample data was seeded under.")
