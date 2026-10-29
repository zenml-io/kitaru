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

from kitaru.api_models.v1.base import RequestModel, ResponseModel
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.experiment_run import (
    EvaluationAggregateResponse as EvaluationAggregateResponse,
)
from kitaru.api_models.v1.experiment_run import (
    EvaluationStats as EvaluationStats,
)
from kitaru.api_models.v1.experiment_run import (
    EvaluationValue as EvaluationValue,
)
from kitaru.api_models.v1.experiment_run import (
    ReplayEvaluationValues as ReplayEvaluationValues,
)
from kitaru.api_models.v1.session import SessionDetailResponse, SessionResponse


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
