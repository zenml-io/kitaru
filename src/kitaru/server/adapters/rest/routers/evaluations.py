"""Evaluation routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationListParams,
    EvaluationResponse,
)
from kitaru.api_models.v1.job import JobResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluation_service,
    get_job_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import (
    evaluation_list_params_to_filter,
    evaluation_to_response,
)
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.job import EvaluationBatchCreate
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.job_service import JobService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_evaluations(
    body: EvaluationBatchCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Create an evaluation job; clients observe 201, 404, or 422."""
    command = EvaluationBatchCreate.model_validate(body.model_dump(mode="python"))
    return job_to_response(await service.create_evaluations(command, actor=actor))


@router.get("")
async def list_evaluations(
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[EvaluationListParams, Query()],
) -> Page[EvaluationResponse]:
    """List evaluations; clients observe 200 or 422."""
    items, cursor = await service.list_evaluations(
        evaluation_list_params_to_filter(params), actor=actor
    )
    return Page[EvaluationResponse](
        items=[
            evaluation_to_response(item, name, version) for item, name, version in items
        ],
        next_cursor=cursor,
    )


@router.get("/{evaluation_id}")
async def get_evaluation(
    evaluation_id: uuid.UUID,
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluationResponse:
    """Get an evaluation; clients observe 200 or 404."""
    item, name, version = await service.get_evaluation(evaluation_id, actor=actor)
    return evaluation_to_response(item, name, version)
