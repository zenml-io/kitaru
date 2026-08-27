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
from kitaru.server.adapters.rest.dependencies import authorize, get_experiment_service
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    experiment_run_create_to_command,
    experiment_run_to_response,
)
from kitaru.server.adapters.rest.mapping.experiments import (
    experiment_create_to_command,
    experiment_list_params_to_filter,
    experiment_to_response,
    experiment_update_to_command,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.experiment_service import ExperimentService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent
async def create_experiment(
    body: ExperimentCreateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Create an experiment.

    Clients observe HTTP 201 on success, 404 when the agent does not exist
    or an evaluator config names an unknown evaluator or version, 409 when
    the name is already registered, and 422 on invalid input, including a
    duplicate resolved evaluator version.

    Args:
        body: Experiment create request.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Created experiment.
    """
    command = experiment_create_to_command(body)
    experiment, config = await service.create_experiment(command, actor=actor)
    return experiment_to_response(experiment, config)


@router.get("")
async def list_experiments(
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ExperimentListParams, Query()],
) -> Page[ExperimentResponse]:
    """List experiments.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Experiment service.
        actor: Caller context.
        params: Experiment list params.

    Returns:
        Page of experiments.
    """
    experiment_filter = experiment_list_params_to_filter(params)
    pairs, next_cursor = await service.list_experiments(experiment_filter, actor=actor)
    return Page[ExperimentResponse](
        items=[
            experiment_to_response(experiment, config) for experiment, config in pairs
        ],
        next_cursor=next_cursor,
    )


@router.get("/{experiment_id}")
async def get_experiment(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Get an experiment by id.

    Clients observe HTTP 200 on success and 404 when no experiment has this
    id.

    Args:
        experiment_id: Id of the experiment.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Stored experiment.
    """
    experiment, config = await service.get_experiment(experiment_id, actor=actor)
    return experiment_to_response(experiment, config)


@router.patch("/{experiment_id}")
async def update_experiment(
    experiment_id: uuid.UUID,
    body: ExperimentUpdateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Update an experiment.

    Clients observe HTTP 200 on success, 404 when no experiment has this id
    or a new evaluator config names an unknown evaluator or version, and 422
    on invalid input, including an attempt to clear the name, the tool
    policy, or every evaluator.

    Args:
        experiment_id: Id of the experiment.
        body: Experiment update request.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Updated experiment.
    """
    command = experiment_update_to_command(body)
    experiment, config = await service.update_experiment(
        experiment_id, command, actor=actor
    )
    return experiment_to_response(experiment, config)


@router.delete("/{experiment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_experiment(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an experiment.

    Clients observe HTTP 204 on success and 404 when no experiment has this
    id.

    Args:
        experiment_id: Id of the experiment.
        service: Experiment service.
        actor: Caller context.
    """
    await service.delete_experiment(experiment_id, actor=actor)


@router.post("/{experiment_id}/runs", status_code=status.HTTP_201_CREATED)
@idempotent
async def start_run(
    experiment_id: uuid.UUID,
    body: ExperimentRunCreateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Start an experiment run, fanning out one replay per cohort version session.

    Clients observe HTTP 201 on success, 404 when the experiment, the
    cohort version, or the resolved agent version does not exist, 409 when
    evaluate_baselines is set and a cohort version session is not finished,
    and 422 when the cohort version has no sessions, the cohort version or
    agent version belongs to another agent, or the resolved agent version
    has no run spec.

    Args:
        experiment_id: Id of the experiment.
        body: Experiment run create request.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Created run.
    """
    command = experiment_run_create_to_command(body)
    run, counts = await service.start_run(experiment_id, command, actor=actor)
    return experiment_run_to_response(run, counts)
