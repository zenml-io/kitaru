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
"""Tests for tag use cases."""

import uuid

import pytest

from conftest import FakeTagRepository, create_tag
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    TagLinkNotFound,
    TagNotFound,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def repository() -> FakeTagRepository:
    """Provide a fake tag repository."""
    return FakeTagRepository()


@pytest.fixture
def service(repository: FakeTagRepository) -> TagService:
    """Provide a tag service backed by the fake repository."""
    return TagService(repository=repository)


async def test_create_tag(service: TagService) -> None:
    """Create a tag owned by the caller."""
    tag = await service.create_tag(name="prod", actor=ACTOR)
    assert tag.name == "prod"
    assert tag.owner_id == ACTOR.account.id
    assert tag.created is not None
    assert tag.updated is not None


async def test_create_tag_duplicate_name(service: TagService) -> None:
    """Reject a second tag with the same name."""
    await service.create_tag(name="prod", actor=ACTOR)
    with pytest.raises(DuplicateTagName, match="Tag name 'prod' is already registered"):
        await service.create_tag(name="prod", actor=ACTOR)


async def test_list_tags(service: TagService) -> None:
    """List tags newest-first with filters."""
    for name in ["prod", "staging", "canary"]:
        await service.create_tag(name=name, actor=ACTOR)

    tags, next_cursor = await service.list_tags(TagFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [tag.name for tag in tags] == ["canary", "staging", "prod"]

    tags, next_cursor = await service.list_tags(
        TagFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="staging")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert tags[0].name == "staging"


async def test_update_tag(service: TagService) -> None:
    """Rename a tag."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    updated = await service.update_tag(created.id, name="production", actor=ACTOR)
    assert updated.name == "production"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_tag_not_found(service: TagService) -> None:
    """Raise for an unknown tag id."""
    with pytest.raises(TagNotFound):
        await service.update_tag(uuid.uuid4(), name="production", actor=ACTOR)


async def test_update_tag_duplicate_name(service: TagService) -> None:
    """Reject renaming a tag to a registered name."""
    await service.create_tag(name="prod", actor=ACTOR)
    staging = await service.create_tag(name="staging", actor=ACTOR)
    with pytest.raises(DuplicateTagName, match="Tag name 'prod' is already registered"):
        await service.update_tag(staging.id, name="prod", actor=ACTOR)


async def test_delete_tag(service: TagService) -> None:
    """Delete a tag."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    await service.delete_tag(created.id, actor=ACTOR)
    with pytest.raises(TagNotFound):
        await service.get_tag(created.id, actor=ACTOR)


async def test_delete_tag_not_found(service: TagService) -> None:
    """Raise for an unknown tag id."""
    with pytest.raises(TagNotFound):
        await service.delete_tag(uuid.uuid4(), actor=ACTOR)


async def test_delete_tag_cascades_links(
    service: TagService, repository: FakeTagRepository
) -> None:
    """Delete a tag's links along with the tag."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
    )
    await service.delete_tag(created.id, actor=ACTOR)
    with pytest.raises(TagLinkNotFound):
        await service.delete_tag_link(
            created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
        )


async def test_create_tag_link(service: TagService) -> None:
    """Link a tag to a resource."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    link = await service.create_tag_link(
        created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
    )
    assert link.tag_id == created.id
    assert link.resource_type == TagResourceType.SESSION
    assert link.resource_id == resource_id
    assert link.created is not None
    assert link.updated is not None


async def test_create_tag_link_missing_tag(service: TagService) -> None:
    """Raise when linking a resource to an unknown tag."""
    with pytest.raises(TagNotFound):
        await service.create_tag_link(
            uuid.uuid4(), TagResourceType.SESSION, uuid.uuid4(), actor=ACTOR
        )


async def test_create_tag_link_duplicate(service: TagService) -> None:
    """Reject linking the same tag and resource twice."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
    )
    with pytest.raises(DuplicateTagLink):
        await service.create_tag_link(
            created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
        )


async def test_create_tag_link_distinct_resource_types(service: TagService) -> None:
    """Allow the same resource id under different resource types."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
    )
    link = await service.create_tag_link(
        created.id, TagResourceType.COHORT, resource_id, actor=ACTOR
    )
    assert link.resource_type == TagResourceType.COHORT


async def test_delete_tag_link(service: TagService) -> None:
    """Delete a tag link by type and id."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
    )
    await service.delete_tag_link(
        created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
    )
    with pytest.raises(TagLinkNotFound):
        await service.delete_tag_link(
            created.id, TagResourceType.SESSION, resource_id, actor=ACTOR
        )


async def test_delete_tag_link_not_found(
    service: TagService, repository: FakeTagRepository
) -> None:
    """Raise when deleting a link that does not exist."""
    created = await create_tag(repository, ACTOR.account.id)
    with pytest.raises(TagLinkNotFound):
        await service.delete_tag_link(
            created.id, TagResourceType.SESSION, uuid.uuid4(), actor=ACTOR
        )
