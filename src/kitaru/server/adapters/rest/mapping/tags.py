"""Tag DTO conversions."""

from kitaru.api_models.v1.tag import (
    TagLinkResponse,
    TagListParams,
    TagResponse,
    TagUpdateRequest,
)
from kitaru.api_models.v1.tag import (
    TagResourceType as TagResourceTypeDTO,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.application.models.tag import TagFilter, TagUpdate
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType


def tag_to_response(tag: Tag) -> TagResponse:
    """Convert a tag entity to its response."""
    assert tag.created is not None
    assert tag.updated is not None
    return TagResponse(
        id=tag.id,
        owner_id=tag.owner_id,
        name=tag.name,
        created=tag.created,
        updated=tag.updated,
    )


def tag_link_to_response(link: TagLink) -> TagLinkResponse:
    """Convert a tag link entity to its response."""
    assert link.created is not None
    assert link.updated is not None
    return TagLinkResponse(
        id=link.id,
        tag_id=link.tag_id,
        resource_type=link.resource_type,
        resource_id=link.resource_id,
        created=link.created,
        updated=link.updated,
    )


def tag_resource_type_to_domain(
    resource_type: TagResourceTypeDTO,
) -> TagResourceType:
    """Convert an API tag resource type to its domain counterpart."""
    return TagResourceType(resource_type.value)


def tag_list_params_to_filter(params: TagListParams) -> TagFilter:
    """Convert tag list query parameters."""
    return TagFilter(**params.model_dump(mode="python"))


def tag_update_to_command(body: TagUpdateRequest) -> TagUpdate:
    """Convert a tag PATCH body while preserving unset fields."""
    return to_partial(TagUpdate, body)
