"""Experiment routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentListParams,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_service,
)
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    experiment_run_to_response,
)
from kitaru.server.adapters.rest.mapping.experiments import (
    experiment_create_to_command,
    experiment_list_params_to_filter,
    experiment_to_response,
    experiment_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.experiment_service import ExperimentService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_experiment(
    body: ExperimentCreateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Create an experiment; clients observe 201, 409, or 422."""
    experiment, config = await service.create_experiment(
        experiment_create_to_command(body), actor=actor
    )
    return experiment_to_response(experiment, config)


@router.get("")
async def list_experiments(
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ExperimentListParams, Query()],
) -> Page[ExperimentResponse]:
    """List experiments; clients observe 200 or 422."""
    items, cursor = await service.list_experiments(
        experiment_list_params_to_filter(params), actor=actor
    )
    return Page[ExperimentResponse](
        items=[experiment_to_response(item, config) for item, config in items],
        next_cursor=cursor,
    )


@router.get("/{experiment_id}")
async def get_experiment(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Get an experiment; clients observe 200 or 404."""
    item, config = await service.get_experiment(experiment_id, actor=actor)
    return experiment_to_response(item, config)


@router.patch("/{experiment_id}")
async def update_experiment(
    experiment_id: uuid.UUID,
    body: ExperimentUpdateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Update an experiment; clients observe 200, 404, 409, or 422."""
    item, config = await service.update_experiment(
        experiment_id, experiment_update_to_command(body), actor=actor
    )
    return experiment_to_response(item, config)


@router.delete("/{experiment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_experiment(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an experiment; clients observe 204, 404, or 409."""
    await service.delete_experiment(experiment_id, actor=actor)


@router.post("/{experiment_id}/runs", status_code=status.HTTP_201_CREATED)
async def start_experiment_run(
    experiment_id: uuid.UUID,
    body: ExperimentRunCreateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Start an experiment run; clients observe 201, 404, or 422."""
    run, progress = await service.start_run(
        experiment_id,
        cohort_id=body.cohort_id,
        agent_version_id=body.agent_version_id,
        evaluate_baselines=body.evaluate_baselines,
        actor=actor,
    )
    return experiment_run_to_response(run, progress)
