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
    experiment_run_jobs_params_to_filter,
    experiment_run_list_params_to_filter,
    experiment_run_to_response,
)
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
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
    """List experiment runs; clients observe 200 or 422."""
    items, cursor = await service.list_runs(
        experiment_run_list_params_to_filter(params), actor=actor
    )
    return Page[ExperimentRunResponse](
        items=[experiment_run_to_response(item, progress) for item, progress in items],
        next_cursor=cursor,
    )


@router.get("/{run_id}")
async def get_experiment_run(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Get an experiment run; clients observe 200 or 404."""
    run, progress = await service.get_run(run_id, actor=actor)
    return experiment_run_to_response(run, progress)


@router.get("/{run_id}/jobs")
async def list_experiment_run_jobs(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ExperimentRunJobsListParams, Query()],
) -> Page[JobResponse]:
    """List run jobs; clients observe 200, 404, or 422."""
    items, cursor = await service.list_run_jobs(
        run_id, experiment_run_jobs_params_to_filter(run_id, params), actor=actor
    )
    return Page[JobResponse](
        items=[job_to_response(item) for item in items], next_cursor=cursor
    )


@router.post("/{run_id}/cancel")
async def cancel_experiment_run(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Cancel an experiment run; clients observe 200 or 404."""
    run, progress = await service.cancel_run(run_id, actor=actor)
    return experiment_run_to_response(run, progress)


@router.delete("/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_experiment_run(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an experiment run; clients observe 204, 404, or 409."""
    await service.delete_run(run_id, actor=actor)
