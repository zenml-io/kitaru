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
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.job import JobResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_service,
)
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    experiment_run_jobs_list_params_to_filter,
    experiment_run_list_params_to_filter,
    experiment_run_to_response,
)
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.api.run_cancellation import RunCanceler, get_run_canceler
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_experiment_runs(
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ExperimentRunListParams, Query()],
) -> Page[ExperimentRunResponse]:
    """List experiment runs.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Experiment run service.
        actor: Caller context.
        params: Experiment run list params.

    Returns:
        Page of experiment runs.
    """
    run_filter = experiment_run_list_params_to_filter(params)
    pairs, next_cursor = await service.list_runs(run_filter, actor=actor)
    return Page[ExperimentRunResponse](
        items=[experiment_run_to_response(run, counts) for run, counts in pairs],
        next_cursor=next_cursor,
    )


@router.get("/{experiment_run_id}")
async def get_experiment_run(
    experiment_run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Get an experiment run by id.

    Clients observe HTTP 200 on success and 404 when no run has this id.

    Args:
        experiment_run_id: Id of the run.
        service: Experiment run service.
        actor: Caller context.

    Returns:
        Stored experiment run.
    """
    run, counts = await service.get_run(experiment_run_id, actor=actor)
    return experiment_run_to_response(run, counts)


@router.delete("/{experiment_run_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_experiment_run(
    experiment_run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an experiment run and its jobs.

    Clients observe HTTP 204 on success and 404 when no run has this id.

    Args:
        experiment_run_id: Id of the run.
        service: Experiment run service.
        actor: Caller context.
    """
    await service.delete_run(experiment_run_id, actor=actor)


@router.get("/{experiment_run_id}/jobs")
async def list_experiment_run_jobs(
    experiment_run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ExperimentRunJobsListParams, Query()],
) -> Page[JobResponse]:
    """List the jobs backing an experiment run's replays.

    Clients observe HTTP 200 on success and 404 when no run has this id.

    Args:
        experiment_run_id: Id of the run.
        service: Experiment run service.
        actor: Caller context.
        params: Experiment run jobs list params.

    Returns:
        Page of jobs.
    """
    job_filter = experiment_run_jobs_list_params_to_filter(params)
    jobs, next_cursor = await service.list_run_jobs(
        experiment_run_id, job_filter, actor=actor
    )
    return Page[JobResponse](
        items=[job_to_response(job) for job in jobs], next_cursor=next_cursor
    )


@router.post("/{experiment_run_id}/cancel")
async def cancel_experiment_run(
    experiment_run_id: uuid.UUID,
    actor: Annotated[AuthContext, Depends(authorize)],
    cancel: Annotated[RunCanceler, Depends(get_run_canceler)],
) -> ExperimentRunResponse:
    """Request cancellation of a running experiment run.

    Clients observe HTTP 200 on success, 404 when no run has this id, and
    409 when the run is not running.

    Args:
        experiment_run_id: Id of the run.
        actor: Caller context.
        cancel: Run cancellation flow, committed across its own transactions.

    Returns:
        Run carrying the cancel request.
    """
    run, counts = await cancel(experiment_run_id, actor)
    return experiment_run_to_response(run, counts)
