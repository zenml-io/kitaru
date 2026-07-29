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
    """Replay counts for an experiment run."""

    pending: int = Field(description="Pending replay count.")
    evaluating: int = Field(description="Evaluating replay count.")
    completed: int = Field(description="Completed replay count.")
    failed: int = Field(description="Failed replay count.")
    canceled: int = Field(description="Canceled replay count.")
    total: int = Field(description="Total replay count.")


class ExperimentRunCreateRequest(RequestModel):
    """Experiment run create request."""

    cohort_id: uuid.UUID = Field(description="Cohort id.")
    agent_version_id: uuid.UUID = Field(description="Agent version id.")
    evaluate_baselines: bool = Field(
        description="Whether to evaluate baseline sessions."
    )


class ExperimentRunListParams(ListParams):
    """Experiment run list params."""

    experiment_id: uuid.UUID | None = Field(
        default=None, description="Filter on experiment id."
    )
    status: ExperimentRunStatus | None = Field(
        default=None, description="Filter on status."
    )
    tag: str | None = Field(default=None, description="Filter on tag name.")


class ExperimentRunJobsListParams(ListParams):
    """Experiment run job list params."""

    status: JobStatus | None = Field(default=None, description="Filter on job status.")


class ExperimentRunResponse(OwnedResponseModel):
    """Experiment run response."""

    id: uuid.UUID = Field(description="Experiment run id.")
    experiment_id: uuid.UUID = Field(description="Experiment id.")
    number: int = Field(description="Run number.")
    status: ExperimentRunStatus = Field(description="Run status.")
    cohort_id: uuid.UUID = Field(description="Cohort id.")
    agent_version_id: uuid.UUID = Field(description="Agent version id.")
    evaluate_baselines: bool = Field(description="Whether baselines are evaluated.")
    started_at: datetime | None = Field(description="Start time.")
    ended_at: datetime | None = Field(description="End time.")
    error: str | None = Field(description="Failure detail.")
    progress: ExperimentRunProgress = Field(description="Replay progress.")
