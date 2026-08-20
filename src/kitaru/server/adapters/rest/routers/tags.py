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
"""Tag routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagLinkResponse,
    TagListParams,
    TagResourceType,
    TagResponse,
    TagUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import authorize, get_tag_service
from kitaru.server.adapters.rest.mapping.tags import (
    tag_link_to_response,
    tag_list_params_to_filter,
    tag_to_response,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.tag_service import TagService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent
async def create_tag(
    body: TagCreateRequest,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TagResponse:
    """Create a tag.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input.

    Args:
        body: Tag create request.
        service: Tag service.
        actor: Caller context.

    Returns:
        Created tag.
    """
    tag = await service.create_tag(name=body.name, actor=actor)
    return tag_to_response(tag)


@router.get("")
async def list_tags(
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[TagListParams, Query()],
) -> Page[TagResponse]:
    """List tags.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Tag service.
        actor: Caller context.
        params: Tag list params.

    Returns:
        Page of tags.
    """
    tag_filter = tag_list_params_to_filter(params)
    tags, next_cursor = await service.list_tags(tag_filter, actor=actor)
    return Page[TagResponse](
        items=[tag_to_response(tag) for tag in tags], next_cursor=next_cursor
    )


@router.patch("/{tag_id}")
async def update_tag(
    tag_id: uuid.UUID,
    body: TagUpdateRequest,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TagResponse:
    """Update a tag.

    Clients observe HTTP 200 on success, 404 when no tag has this id, 409
    when the new name is already registered, and 422 on invalid input.

    Args:
        tag_id: Id of the tag.
        body: Tag update request.
        service: Tag service.
        actor: Caller context.

    Returns:
        Updated tag.
    """
    tag = await service.update_tag(tag_id, name=body.name, actor=actor)
    return tag_to_response(tag)


@router.delete("/{tag_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tag(
    tag_id: uuid.UUID,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a tag.

    Deleting a tag cascades its links. Clients observe HTTP 204 on success
    and 404 when no tag has this id.

    Args:
        tag_id: Id of the tag.
        service: Tag service.
        actor: Caller context.
    """
    await service.delete_tag(tag_id, actor=actor)


@router.post("/{tag_id}/links", status_code=status.HTTP_201_CREATED)
async def create_tag_link(
    tag_id: uuid.UUID,
    body: TagLinkCreateRequest,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TagLinkResponse:
    """Link a tag to a resource.

    Clients observe HTTP 201 on success, 404 when no tag has this id, 409
    when the link is already registered, and 422 on invalid input.

    Args:
        tag_id: Id of the tag.
        body: Tag link create request.
        service: Tag service.
        actor: Caller context.

    Returns:
        Created tag link.
    """
    link = await service.create_tag_link(
        tag_id, body.resource_type, body.resource_id, actor=actor
    )
    return tag_link_to_response(link)


@router.delete(
    "/{tag_id}/links/{resource_type}/{resource_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_tag_link(
    tag_id: uuid.UUID,
    resource_type: TagResourceType,
    resource_id: uuid.UUID,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Unlink a tag from a resource.

    Clients observe HTTP 204 on success and 404 when no link matches the
    tag and resource.

    Args:
        tag_id: Id of the tag.
        resource_type: Kind of the linked resource.
        resource_id: Id of the linked resource.
        service: Tag service.
        actor: Caller context.
    """
    await service.delete_tag_link(tag_id, resource_type, resource_id, actor=actor)
