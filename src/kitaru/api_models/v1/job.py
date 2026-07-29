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


class JobListParams(ListParams):
    """Job list params."""

    status: JobStatus | None = Field(default=None, description="Filter on status.")


class JobTasksListParams(ListParams):
    """Job task list params."""

    kind: TaskKind | None = Field(default=None, description="Filter on task kind.")
    status: TaskStatus | None = Field(
        default=None, description="Filter on task status."
    )


class JobResponse(OwnedResponseModel):
    """Job response."""

    id: uuid.UUID = Field(description="Job id.")
    status: JobStatus = Field(description="Job status.")
    cancel_requested_at: datetime | None = Field(
        description="Cancellation request time."
    )
    started_at: datetime | None = Field(description="Start time.")
    ended_at: datetime | None = Field(description="End time.")
    error: str | None = Field(description="First counted task error.")
