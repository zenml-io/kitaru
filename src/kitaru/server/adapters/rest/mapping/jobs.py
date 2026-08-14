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
"""Job DTO conversions."""

from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.job import JobFilter
from kitaru.server.application.models.task import JobTasksFilter
from kitaru.server.domain.job import Job


def job_to_response(job: Job) -> JobResponse:
    """Convert a job entity to its response DTO.

    Args:
        job: Stored job.

    Returns:
        Job response.
    """
    assert job.created is not None
    assert job.updated is not None
    return JobResponse(
        id=job.id,
        owner_id=job.owner_id,
        kind=job.kind,
        status=job.status,
        provisional=job.provisional,
        cancel_requested_at=job.cancel_requested_at,
        started_at=job.started_at,
        ended_at=job.ended_at,
        error=job.error,
        created=job.created,
        updated=job.updated,
    )


def job_list_params_to_filter(params: JobListParams) -> JobFilter:
    """Convert job list params to the application filter.

    Args:
        params: Job list params.

    Returns:
        Job filter.
    """
    return JobFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def job_tasks_list_params_to_filter(params: JobTasksListParams) -> JobTasksFilter:
    """Convert job task list params to the application filter.

    Args:
        params: Job tasks list params.

    Returns:
        Task filter without the owning job, which the service pins.
    """
    return JobTasksFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
