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
"""Tag use cases."""

import uuid

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.tag import Tag, TagLink


class TagService:
    """Tag use cases."""

    def __init__(self, repository: TagRepository) -> None:
        """Initialize the service.

        Args:
            repository: Tag repository.
        """
        self._repository = repository

    async def create_tag(self, name: str, actor: AuthContext) -> Tag:
        """Create a tag owned by the caller.

        Args:
            name: Tag name.
            actor: Caller context.

        Raises:
            DuplicateTagName: The tag name is already registered.

        Returns:
            Created tag.
        """
        tag = Tag(owner_id=actor.account.id, name=name)
        return await self._repository.create(tag)

    async def get_tag(self, tag_id: uuid.UUID, actor: AuthContext) -> Tag:
        """Get a tag by id.

        Args:
            tag_id: Id of the tag.
            actor: Caller context.

        Raises:
            TagNotFound: No tag has this id.

        Returns:
            Stored tag.
        """
        _ = actor
        return await self._repository.get(tag_id)

    async def list_tags(
        self, tag_filter: TagFilter, actor: AuthContext
    ) -> tuple[list[Tag], str | None]:
        """List tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching tags and the next cursor.
        """
        _ = actor
        return await self._repository.query(tag_filter)

    async def update_tag(self, tag_id: uuid.UUID, name: str, actor: AuthContext) -> Tag:
        """Rename a tag.

        Args:
            tag_id: Id of the tag.
            name: New tag name.
            actor: Caller context.

        Raises:
            TagNotFound: No tag has this id.
            DuplicateTagName: The tag name is already registered.

        Returns:
            Updated tag.
        """
        tag = await self.get_tag(tag_id, actor=actor)
        tag.update_name(name)
        return await self._repository.update(tag)

    async def delete_tag(self, tag_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a tag and its links.

        Args:
            tag_id: Id of the tag.
            actor: Caller context.

        Raises:
            TagNotFound: No tag has this id.
        """
        await self.get_tag(tag_id, actor=actor)
        await self._repository.delete(tag_id)

    async def create_tag_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
        actor: AuthContext,
    ) -> TagLink:
        """Link a tag to a resource.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of resource to tag.
            resource_id: Id of the resource to tag.
            actor: Caller context.

        Raises:
            TagNotFound: No tag has this id.
            SessionNotFound: No session has the resource id.
            CohortNotFound: No cohort has the resource id.
            CohortVersionIdNotFound: No cohort version has the resource id.
            AgentVersionNotFound: No agent version has the resource id.
            ExperimentNotFound: No experiment has the resource id.
            ExperimentRunNotFound: No experiment run has the resource id.
            DuplicateTagLink: The tag is already linked to the resource.

        Returns:
            Created tag link.
        """
        _ = actor
        link = TagLink(
            tag_id=tag_id, resource_type=resource_type, resource_id=resource_id
        )
        return await self._repository.create_link(link)

    async def delete_tag_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
        actor: AuthContext,
    ) -> None:
        """Unlink a tag from a resource.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of resource to untag.
            resource_id: Id of the resource to untag.
            actor: Caller context.

        Raises:
            TagLinkNotFound: No link matches the tag and resource.
        """
        _ = actor
        await self._repository.delete_link(tag_id, resource_type, resource_id)
