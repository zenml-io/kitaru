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
"""Experiment run routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.jobs import (
    JobResponse,
    JobStatus,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_service,
)
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    experiment_run_to_response,
    run_status_to_domain,
)
from kitaru.server.adapters.rest.mapping.jobs import (
    job_status_to_domain,
    job_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunJobsFilter,
)
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)

router = APIRouter()


@router.get("")
async def list_experiment_runs(
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    experiment_id: uuid.UUID | None = None,
    status: ExperimentRunStatus | None = None,
    tag: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ExperimentRunResponse]:
    """List experiment runs.

    Clients observe HTTP 200 on success, 404 when no experiment has the
    filtered experiment id, and 422 on invalid filter or pagination
    parameters.

    Args:
        service: Experiment run service.
        actor: Caller context.
        experiment_id: Filter on experiment id.
        status: Filter on run status.
        tag: Filter on attached tag name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of experiment runs.
    """
    run_filter = ExperimentRunFilter(
        experiment_id=experiment_id,
        status=run_status_to_domain(status),
        tag=tag,
        page=page,
        page_size=page_size,
    )
    runs, total = await service.list_runs(run_filter, actor=actor)
    return Page[ExperimentRunResponse](
        items=[experiment_run_to_response(run, progress) for run, progress in runs],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{run_id}")
async def get_experiment_run(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Get an experiment run by id.

    Clients observe HTTP 200 on success and 404 when no experiment run
    has this id.

    Args:
        run_id: Id of the experiment run.
        service: Experiment run service.
        actor: Caller context.

    Returns:
        Stored experiment run.
    """
    run, progress = await service.get_run(run_id, actor=actor)
    return experiment_run_to_response(run, progress)


@router.delete("/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_experiment_run(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a terminal experiment run, including its jobs.

    Deletes each job's config when nothing else references it.

    Clients observe HTTP 204 on success, 404 when no experiment run has
    this id, and 409 when the run is not terminal.

    Args:
        run_id: Id of the experiment run.
        service: Experiment run service.
        actor: Caller context.
    """
    await service.delete_run(run_id, actor=actor)


@router.get("/{run_id}/jobs")
async def list_experiment_run_jobs(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    status: JobStatus | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[JobResponse]:
    """List the jobs of an experiment run.

    Clients observe HTTP 200 on success, 404 when no experiment run has
    this id, and 422 on invalid filter or pagination parameters.

    Args:
        run_id: Id of the experiment run.
        service: Experiment run service.
        actor: Caller context.
        status: Filter on job status.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of jobs.
    """
    jobs_filter = ExperimentRunJobsFilter(
        status=job_status_to_domain(status), page=page, page_size=page_size
    )
    jobs, total = await service.list_run_jobs(run_id, jobs_filter, actor=actor)
    return Page[JobResponse](
        items=[job_to_response(job, config) for job, config in jobs],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/{run_id}/cancel")
async def cancel_experiment_run(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Cancel an experiment run.

    Pending and claimed jobs are canceled immediately, running ones
    drain through the heartbeat path. The run lands on canceled right away
    when no running job remains.

    Clients observe HTTP 200 on success, 404 when no experiment run has
    this id, and 409 when the run is already terminal.

    Args:
        run_id: Id of the experiment run.
        service: Experiment run service.
        actor: Caller context.

    Returns:
        Updated experiment run.
    """
    run, progress = await service.cancel_run(run_id, actor=actor)
    return experiment_run_to_response(run, progress)
