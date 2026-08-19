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
"""Annotation routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.annotation import (
    AnnotationCreateRequest,
    AnnotationListParams,
    AnnotationResponse,
    AnnotationUpdateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.dependencies import authorize, get_annotation_service
from kitaru.server.adapters.rest.mapping.annotations import (
    annotation_list_params_to_filter,
    annotation_to_response,
    annotation_update_to_value,
    investigation_answer_create_to_command,
    manual_annotation_create_to_command,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.annotation_service import AnnotationService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent
async def create_annotation(
    body: AnnotationCreateRequest,
    service: Annotated[AnnotationService, Depends(get_annotation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnnotationResponse:
    """Create a manual annotation, or answer an investigation session.

    A body carrying session_id creates a manual annotation. A body carrying
    investigation_session_id answers an investigation session, moving a
    pending investigation to in_progress on its first answer.

    Clients observe HTTP 201 on success, 404 when the session or the
    investigation session does not exist, and 422 when the selector names a
    node outside the session.

    Args:
        body: Manual annotation or investigation answer create request.
        service: Annotation service.
        actor: Caller context.

    Returns:
        Created annotation.
    """
    if isinstance(body, ManualAnnotationCreateRequest):
        command = manual_annotation_create_to_command(body)
        annotation = await service.create_manual_annotation(command, actor=actor)
    else:
        answer_command = investigation_answer_create_to_command(body)
        annotation = await service.create_investigation_answer(
            answer_command, actor=actor
        )
    return annotation_to_response(annotation)


@router.get("")
async def list_annotations(
    service: Annotated[AnnotationService, Depends(get_annotation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[AnnotationListParams, Query()],
) -> Page[AnnotationResponse]:
    """List annotations.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Annotation service.
        actor: Caller context.
        params: Annotation list params.

    Returns:
        Page of annotations.
    """
    annotation_filter = annotation_list_params_to_filter(params)
    items, next_cursor = await service.list_annotations(annotation_filter, actor=actor)
    return Page[AnnotationResponse](
        items=[annotation_to_response(item) for item in items],
        next_cursor=next_cursor,
    )


@router.get("/{annotation_id}")
async def get_annotation(
    annotation_id: uuid.UUID,
    service: Annotated[AnnotationService, Depends(get_annotation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnnotationResponse:
    """Get an annotation by id.

    Clients observe HTTP 200 on success and 404 when no annotation has this
    id.

    Args:
        annotation_id: Id of the annotation.
        service: Annotation service.
        actor: Caller context.

    Returns:
        Stored annotation.
    """
    annotation = await service.get_annotation(annotation_id, actor=actor)
    return annotation_to_response(annotation)


@router.patch("/{annotation_id}")
async def update_annotation(
    annotation_id: uuid.UUID,
    body: AnnotationUpdateRequest,
    service: Annotated[AnnotationService, Depends(get_annotation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnnotationResponse:
    """Set a new value on an annotation.

    Clients observe HTTP 200 on success and 404 when no annotation has this
    id.

    Args:
        annotation_id: Id of the annotation.
        body: Annotation update request.
        service: Annotation service.
        actor: Caller context.

    Returns:
        Updated annotation.
    """
    value = annotation_update_to_value(body)
    annotation = await service.update_annotation(annotation_id, value, actor=actor)
    return annotation_to_response(annotation)


@router.delete("/{annotation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_annotation(
    annotation_id: uuid.UUID,
    service: Annotated[AnnotationService, Depends(get_annotation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an annotation.

    Clients observe HTTP 204 on success and 404 when no annotation has this
    id.

    Args:
        annotation_id: Id of the annotation.
        service: Annotation service.
        actor: Caller context.
    """
    await service.delete_annotation(annotation_id, actor=actor)
