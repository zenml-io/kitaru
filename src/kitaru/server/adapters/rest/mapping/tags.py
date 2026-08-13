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
"""Tag and tag link DTO conversions."""

from kitaru.api_models.v1.tag import TagLinkResponse, TagListParams, TagResponse
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.tag import Tag, TagLink


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
        resource_type=link.resource_type,
        resource_id=link.resource_id,
        created=link.created,
        updated=link.updated,
    )


def tag_list_params_to_filter(params: TagListParams) -> TagFilter:
    """Convert tag list params to the application filter.

    Args:
        params: Tag list params.

    Returns:
        Tag filter.
    """
    return TagFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
