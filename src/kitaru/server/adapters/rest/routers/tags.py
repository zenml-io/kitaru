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
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_tag_service
from kitaru.server.adapters.rest.mapping.tags import (
    tag_link_to_response,
    tag_list_params_to_filter,
    tag_resource_type_to_domain,
    tag_to_response,
    tag_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.tag_service import TagService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_tag(
    body: TagCreateRequest,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TagResponse:
    """Create a tag; clients observe 201, 409, or 422."""
    return tag_to_response(await service.create_tag(body.name, actor=actor))


@router.get("")
async def list_tags(
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[TagListParams, Query()],
) -> Page[TagResponse]:
    """List tags; clients observe 200 or 422."""
    items, cursor = await service.list_tags(
        tag_list_params_to_filter(params), actor=actor
    )
    return Page[TagResponse](
        items=[tag_to_response(item) for item in items], next_cursor=cursor
    )


@router.patch("/{tag_id}")
async def update_tag(
    tag_id: uuid.UUID,
    body: TagUpdateRequest,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TagResponse:
    """Update a tag; clients observe 200, 404, or 409."""
    return tag_to_response(
        await service.update_tag(tag_id, tag_update_to_command(body), actor=actor)
    )


@router.delete("/{tag_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tag(
    tag_id: uuid.UUID,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a tag; clients observe 204 or 404."""
    await service.delete_tag(tag_id, actor=actor)


@router.post("/{tag_id}/links", status_code=status.HTTP_201_CREATED)
async def create_tag_link(
    tag_id: uuid.UUID,
    body: TagLinkCreateRequest,
    service: Annotated[TagService, Depends(get_tag_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TagLinkResponse:
    """Create a tag link; clients observe 201, 404, or 409."""
    link = await service.create_tag_link(
        tag_id,
        tag_resource_type_to_domain(body.resource_type),
        body.resource_id,
        actor=actor,
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
    """Delete a tag link; clients observe 204 or 404."""
    await service.delete_tag_link(
        tag_id,
        tag_resource_type_to_domain(resource_type),
        resource_id,
        actor=actor,
    )
