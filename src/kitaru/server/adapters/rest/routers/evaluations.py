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
"""Evaluation routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationListParams, EvaluationResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_evaluation_service
from kitaru.server.adapters.rest.mapping.evaluations import (
    evaluation_list_params_to_filter,
    evaluation_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.evaluation_service import EvaluationService

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_evaluations(
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[EvaluationListParams, Query()],
) -> Page[EvaluationResponse]:
    """List evaluations.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Evaluation service.
        actor: Caller context.
        params: Evaluation list params.

    Returns:
        Page of evaluations.
    """
    evaluation_filter = evaluation_list_params_to_filter(params)
    items, next_cursor = await service.list_evaluations(evaluation_filter, actor=actor)
    return Page[EvaluationResponse](
        items=[evaluation_to_response(item) for item in items],
        next_cursor=next_cursor,
    )


@router.get("/{evaluation_id}")
async def get_evaluation(
    evaluation_id: uuid.UUID,
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluationResponse:
    """Get an evaluation by id.

    Clients observe HTTP 200 on success and 404 when no evaluation has this
    id.

    Args:
        evaluation_id: Id of the evaluation.
        service: Evaluation service.
        actor: Caller context.

    Returns:
        Stored evaluation.
    """
    item = await service.get_evaluation(evaluation_id, actor=actor)
    return evaluation_to_response(item)
