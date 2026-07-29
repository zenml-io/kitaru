#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Tag and tag-link use cases."""

import uuid

from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.tag import TagFilter, TagUpdate
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType


class TagService:
    """Tag and tag-link use cases."""

    def __init__(self, repository: TagRepository) -> None:
        self._repository = repository

    async def create_tag(self, name: str, actor: AuthContext) -> Tag:
        """Create a tag."""
        return await self._repository.create(Tag(owner_id=actor.account.id, name=name))

    async def list_tags(
        self, tag_filter: TagFilter, actor: AuthContext
    ) -> tuple[list[Tag], str | None]:
        """List tags."""
        _ = actor
        return await self._repository.query(tag_filter)

    async def update_tag(
        self,
        tag_id: uuid.UUID,
        command: TagUpdate,
        actor: AuthContext,
    ) -> Tag:
        """Update a tag name."""
        _ = actor
        tag = await self._repository.get(tag_id)
        if "name" in command.model_fields_set:
            if command.name is None:
                raise ValidationError("Tag name cannot be null")
            tag.update_name(command.name)
        return await self._repository.update(tag)

    async def delete_tag(self, tag_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a tag and its links."""
        _ = actor
        await self._repository.get(tag_id)
        await self._repository.delete(tag_id)

    async def create_tag_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
        actor: AuthContext,
    ) -> TagLink:
        """Attach a tag to a resource."""
        _ = actor
        await self._repository.get(tag_id)
        return await self._repository.create_link(
            TagLink(
                tag_id=tag_id,
                resource_type=resource_type,
                resource_id=resource_id,
            )
        )

    async def delete_tag_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
        actor: AuthContext,
    ) -> None:
        """Detach a tag from a resource."""
        _ = actor
        await self._repository.get(tag_id)
        await self._repository.delete_link(tag_id, resource_type, resource_id)
