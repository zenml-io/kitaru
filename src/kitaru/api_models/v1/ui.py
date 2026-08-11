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

from pydantic import Field

from kitaru.api_models.v1.base import ResponseModel
from kitaru.api_models.v1.evaluation import EvaluationDataType, EvaluationResponse
from kitaru.api_models.v1.session import SessionResponse


class EvaluationAggregateResponse(ResponseModel):
    """Evaluation aggregate response."""

    name: str = Field(description="Evaluation name.")
    data_type: EvaluationDataType = Field(description="Evaluation data type.")
    count: int = Field(description="Number of aggregated evaluations.")
    average: float | None = Field(
        default=None,
        description="Mean score of float evaluations, share of true results of "
        "bool evaluations, null for other data types.",
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


class SessionWithEvaluationsResponse(ResponseModel):
    """Session with evaluations response."""

    session: SessionResponse = Field(description="Session.")
    evaluations: list[EvaluationResponse] = Field(
        description="Every evaluation of the session, newest first."
    )
