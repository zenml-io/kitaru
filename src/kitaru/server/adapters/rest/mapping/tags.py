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
"""Tag DTO conversions."""

import kitaru.api_models.v1.tags as tag_models
from kitaru.api_models.v1.tags import TagLinkResponse, TagResponse
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType


def resource_type_to_domain(
    resource_type: tag_models.TagResourceType,
) -> TagResourceType:
    """Convert a resource type DTO to its domain enum.

    Args:
        resource_type: Resource type from the API.

    Returns:
        Domain resource type.
    """
    return TagResourceType(resource_type.value)


def tag_to_response(tag: Tag) -> TagResponse:
    """Convert a tag entity to its response DTO.

    Args:
        tag: Stored tag.

    Returns:
        Tag response.
    """
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
    """Convert a tag link entity to its response DTO.

    Args:
        link: Stored tag link.

    Returns:
        Tag link response.
    """
    assert link.created is not None
    assert link.updated is not None
    return TagLinkResponse(
        id=link.id,
        tag_id=link.tag_id,
        resource_type=tag_models.TagResourceType(link.resource_type.value),
        resource_id=link.resource_id,
        created=link.created,
        updated=link.updated,
    )
