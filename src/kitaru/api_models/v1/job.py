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
"""Job API models."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import ListParams, OwnedResponseModel
from kitaru.api_models.v1.task import TaskKind, TaskStatus


class JobStatus(StrEnum):
    """Job status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class JobResponse(OwnedResponseModel):
    """Job response."""

    id: uuid.UUID = Field(description="Job id.")
    status: JobStatus = Field(description="Job status.")
    cancel_requested_at: datetime | None = Field(
        default=None, description="Time cancellation was requested."
    )
    started_at: datetime | None = Field(
        default=None, description="Time the job started."
    )
    ended_at: datetime | None = Field(default=None, description="Time the job settled.")
    error: str | None = Field(
        default=None, description="First counted task failure's error."
    )


class JobListParams(ListParams):
    """Job list params."""

    status: JobStatus | None = Field(default=None, description="Filter on job status.")


class JobTasksListParams(ListParams):
    """Job tasks list params."""

    kind: TaskKind | None = Field(default=None, description="Filter on task kind.")
    status: TaskStatus | None = Field(
        default=None, description="Filter on task status."
    )
