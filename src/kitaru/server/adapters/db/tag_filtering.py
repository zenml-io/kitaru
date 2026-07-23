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
"""Shared tag link filtering."""

import uuid

from sqlalchemy import Select, select
from sqlmodel import col

from kitaru.server.adapters.db.schemas.tag import TagLinkSchema, TagSchema
from kitaru.server.domain.tag import TagResourceType


def tagged_resource_ids(
    tag_name: str, resource_type: TagResourceType
) -> Select[tuple[uuid.UUID]]:
    """Build a select of the resource ids a tag name is attached to.

    Args:
        tag_name: Name of the tag.
        resource_type: Type of the linked resources.

    Returns:
        Select usable as an ``IN`` subquery.
    """
    return (
        select(col(TagLinkSchema.resource_id))
        .join(TagSchema, col(TagLinkSchema.tag_id) == col(TagSchema.id))
        .where(
            col(TagSchema.name) == tag_name,
            col(TagLinkSchema.resource_type) == resource_type.value,
        )
    )
