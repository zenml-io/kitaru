"""Job DTO conversions."""

import uuid

from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.server.application.models.job import JobFilter, JobTasksFilter
from kitaru.server.domain.job import Job


def job_to_response(job: Job) -> JobResponse:
    """Convert a job entity to its response."""
    assert job.created is not None
    assert job.updated is not None
    return JobResponse(
        id=job.id,
        owner_id=job.owner_id,
        status=job.status,
        cancel_requested_at=job.cancel_requested_at,
        started_at=job.started_at,
        ended_at=job.ended_at,
        error=job.error,
        created=job.created,
        updated=job.updated,
    )


def job_list_params_to_filter(params: JobListParams) -> JobFilter:
    """Convert job list query parameters."""
    return JobFilter(**params.model_dump(mode="python"))


def job_tasks_params_to_filter(
    job_id: uuid.UUID, params: JobTasksListParams
) -> JobTasksFilter:
    """Convert job task list query parameters."""
    return JobTasksFilter(job_id=job_id, **params.model_dump(mode="python"))
