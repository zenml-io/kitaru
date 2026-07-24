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
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.experiments import (
    ExperimentCreateRequest,
    ExperimentResponse,
    ExperimentUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_service,
    get_experiment_service,
)
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    execution_target_to_domain,
    experiment_run_to_response,
)
from kitaru.server.adapters.rest.mapping.experiments import (
    experiment_create_to_command,
    experiment_to_response,
    experiment_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import ExperimentRunFilter
from kitaru.server.application.models.experiments import ExperimentFilter
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import (
    ExperimentService,
)

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_experiment(
    body: ExperimentCreateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Create an experiment over a cohort with an inline replay config.

    The tool policy defaults to passthrough when omitted.

    Clients observe HTTP 201 on success, 404 when no cohort has the
    referenced id, 409 when the name is already registered, and 422 on
    invalid input.

    Args:
        body: Experiment create request.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Created experiment.
    """
    experiment, config = await service.create_experiment(
        experiment_create_to_command(body), actor=actor
    )
    return experiment_to_response(experiment, config)


@router.get("")
async def list_experiments(
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    name: str | None = None,
    tag: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ExperimentResponse]:
    """List experiments.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Experiment service.
        actor: Caller context.
        name: Filter on experiment name.
        tag: Filter on attached tag name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of experiments.
    """
    experiment_filter = ExperimentFilter(
        name=name, tag=tag, page=page, page_size=page_size
    )
    experiments, total = await service.list_experiments(experiment_filter, actor=actor)
    return Page[ExperimentResponse](
        items=[
            experiment_to_response(experiment, config)
            for experiment, config in experiments
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{experiment_id}")
async def get_experiment(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentResponse:
    """Get an experiment by id.

    Clients observe HTTP 200 on success and 404 when no experiment has
    this id.

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

    Name and description update on any experiment. Cohort and config
    changes are rejected once a run exists.

    Clients observe HTTP 200 on success, 404 when no experiment or
    referenced cohort has this id, 409 when the new name is already
    registered or a cohort or config change hits an experiment with runs,
    and 422 on invalid input.

    Args:
        experiment_id: Id of the experiment.
        body: Experiment update request.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Updated experiment.
    """
    experiment, config = await service.update_experiment(
        experiment_id, experiment_update_to_command(body), actor=actor
    )
    return experiment_to_response(experiment, config)


@router.delete("/{experiment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_experiment(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an experiment, including its tag links.

    Clients observe HTTP 204 on success, 404 when no experiment has this
    id, and 409 while the experiment has runs.

    Args:
        experiment_id: Id of the experiment.
        service: Experiment service.
        actor: Caller context.
    """
    await service.delete_experiment(experiment_id, actor=actor)


@router.post("/{experiment_id}/runs", status_code=status.HTTP_201_CREATED)
async def create_experiment_run(
    experiment_id: uuid.UUID,
    body: ExperimentRunCreateRequest,
    service: Annotated[ExperimentService, Depends(get_experiment_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ExperimentRunResponse:
    """Start an experiment run.

    Creates the run plus one pending job per cohort session.

    Clients observe HTTP 201 on success, 404 when no experiment or agent
    version has the referenced id, 409 when no runnable agent version
    resolves or an on demand run resolves to a version without an image,
    and 422 when the version belongs to another agent.

    Args:
        experiment_id: Id of the experiment.
        body: Experiment run create request.
        service: Experiment service.
        actor: Caller context.

    Returns:
        Created experiment run.
    """
    run, progress = await service.start_run(
        experiment_id,
        agent_version_id=body.agent_version_id,
        score_baselines=body.score_baselines,
        actor=actor,
        execution_target=execution_target_to_domain(body.execution_target),
    )
    return experiment_run_to_response(run, progress)


@router.get("/{experiment_id}/runs")
async def list_experiment_runs(
    experiment_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ExperimentRunResponse]:
    """List the runs of an experiment.

    Clients observe HTTP 200 on success, 404 when no experiment has this
    id, and 422 on invalid pagination parameters.

    Args:
        experiment_id: Id of the experiment.
        service: Experiment run service.
        actor: Caller context.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of experiment runs.
    """
    run_filter = ExperimentRunFilter(
        experiment_id=experiment_id, page=page, page_size=page_size
    )
    runs, total = await service.list_runs(run_filter, actor=actor)
    return Page[ExperimentRunResponse](
        items=[experiment_run_to_response(run, progress) for run, progress in runs],
        total=total,
        page=page,
        page_size=page_size,
    )
