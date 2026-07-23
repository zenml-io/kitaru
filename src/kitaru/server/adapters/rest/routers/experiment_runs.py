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

from fastapi import APIRouter, Depends, Query

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment_runs import ExperimentRunResponse
from kitaru.api_models.v1.replays import ReplayResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_service,
)
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    experiment_run_to_response,
)
from kitaru.server.adapters.rest.mapping.replays import replay_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment_runs import (
    ExperimentRunFilter,
    ExperimentRunReplaysFilter,
)
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)

router = APIRouter()


@router.get("")
async def list_experiment_runs(
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    tag: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ExperimentRunResponse]:
    """List experiment runs.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Experiment run service.
        actor: Caller context.
        tag: Filter on attached tag name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of experiment runs.
    """
    run_filter = ExperimentRunFilter(tag=tag, page=page, page_size=page_size)
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


@router.get("/{run_id}/replays")
async def list_experiment_run_replays(
    run_id: uuid.UUID,
    service: Annotated[ExperimentRunService, Depends(get_experiment_run_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ReplayResponse]:
    """List the replays of an experiment run.

    Clients observe HTTP 200 on success, 404 when no experiment run has
    this id, and 422 on invalid pagination parameters.

    Args:
        run_id: Id of the experiment run.
        service: Experiment run service.
        actor: Caller context.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of replays.
    """
    replays_filter = ExperimentRunReplaysFilter(page=page, page_size=page_size)
    replays, total = await service.list_run_replays(run_id, replays_filter, actor=actor)
    return Page[ReplayResponse](
        items=[replay_to_response(replay, config) for replay, config in replays],
        total=total,
        page=page,
        page_size=page_size,
    )
