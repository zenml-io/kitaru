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

from pydantic import Field

from kitaru.api_models.v1.base import (
    ListParams,
    OwnedResponseModel,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.job import JobStatus


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

    cohort_id: uuid.UUID = Field(description="Cohort whose sessions are replayed.")
    agent_version_id: uuid.UUID = Field(description="Agent version to replay with.")
    evaluate_baselines: bool = Field(
        default=False, description="Whether to also score each baseline session."
    )


class ExperimentRunListParams(ListParams):
    """Experiment run list params."""

    experiment_id: uuid.UUID | None = Field(
        default=None, description="Filter on experiment."
    )
    status: ExperimentRunStatus | None = Field(
        default=None, description="Filter on run status."
    )
    tag: str | None = Field(default=None, description="Filter on tag name.")


class ExperimentRunJobsListParams(ListParams):
    """Experiment run jobs list params."""

    status: JobStatus | None = Field(default=None, description="Filter on job status.")


class ExperimentRunResponse(OwnedResponseModel):
    """Experiment run response."""

    id: uuid.UUID = Field(description="Experiment run id.")
    experiment_id: uuid.UUID = Field(description="Experiment this run belongs to.")
    number: int = Field(description="Run number within the experiment.")
    status: ExperimentRunStatus = Field(description="Run status.")
    cohort_id: uuid.UUID = Field(description="Cohort whose sessions are replayed.")
    agent_version_id: uuid.UUID = Field(description="Agent version to replay with.")
    evaluate_baselines: bool = Field(
        description="Whether baseline sessions are also scored."
    )
    started_at: datetime | None = Field(
        default=None, description="Time the run started."
    )
    ended_at: datetime | None = Field(default=None, description="Time the run settled.")
    error: str | None = Field(default=None, description="Error from a failed run.")
    progress: ExperimentRunProgress = Field(description="Replay counts by status.")
