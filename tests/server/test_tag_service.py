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

from conftest import FakeTagRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.tags import TagFilter
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    TagLinkNotFound,
    TagNotFound,
    TagResourceType,
)

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
    """List tags with filters and pagination."""
    for name in ["prod", "staging", "flaky"]:
        await service.create_tag(name=name, actor=ACTOR)

    tags, total = await service.list_tags(TagFilter(), actor=ACTOR)
    assert total == 3
    assert [tag.name for tag in tags] == ["prod", "staging", "flaky"]

    tags, total = await service.list_tags(TagFilter(name="staging"), actor=ACTOR)
    assert total == 1
    assert tags[0].name == "staging"

    tags, total = await service.list_tags(TagFilter(page=2, page_size=2), actor=ACTOR)
    assert total == 3
    assert [tag.name for tag in tags] == ["flaky"]


async def test_delete_tag(service: TagService) -> None:
    """Delete a stored tag."""
    created = await service.create_tag(name="prod", actor=ACTOR)
    await service.delete_tag(created.id, actor=ACTOR)
    tags, total = await service.list_tags(TagFilter(), actor=ACTOR)
    assert total == 0
    assert tags == []


async def test_delete_tag_not_found(service: TagService) -> None:
    """Raise for an unknown tag id."""
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await service.delete_tag(missing_id, actor=ACTOR)


async def test_create_tag_link(service: TagService) -> None:
    """Attach a tag to a resource."""
    tag = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    link = await service.create_tag_link(
        tag.id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
        actor=ACTOR,
    )
    assert link.tag_id == tag.id
    assert link.resource_type is TagResourceType.SESSION
    assert link.resource_id == resource_id
    assert link.created is not None
    assert link.updated is not None


async def test_create_tag_link_tag_not_found(service: TagService) -> None:
    """Raise for an unknown tag id."""
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await service.create_tag_link(
            missing_id,
            resource_type=TagResourceType.SESSION,
            resource_id=uuid.uuid4(),
            actor=ACTOR,
        )


async def test_create_tag_link_duplicate(service: TagService) -> None:
    """Reject a second link to the same resource."""
    tag = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        tag.id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
        actor=ACTOR,
    )
    with pytest.raises(
        DuplicateTagLink,
        match=f"Tag {tag.id} is already attached to session {resource_id}",
    ):
        await service.create_tag_link(
            tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
            actor=ACTOR,
        )


async def test_create_tag_link_same_resource_id_other_type(
    service: TagService,
) -> None:
    """Attach a tag to resources of different types sharing an id."""
    tag = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        tag.id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
        actor=ACTOR,
    )
    link = await service.create_tag_link(
        tag.id,
        resource_type=TagResourceType.COHORT,
        resource_id=resource_id,
        actor=ACTOR,
    )
    assert link.resource_type is TagResourceType.COHORT


async def test_delete_tag_link(service: TagService) -> None:
    """Detach a tag from a resource."""
    tag = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    await service.create_tag_link(
        tag.id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
        actor=ACTOR,
    )
    await service.delete_tag_link(
        tag.id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
        actor=ACTOR,
    )
    with pytest.raises(TagLinkNotFound):
        await service.delete_tag_link(
            tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
            actor=ACTOR,
        )


async def test_delete_tag_link_not_found(service: TagService) -> None:
    """Raise for an unknown tag link."""
    tag = await service.create_tag(name="prod", actor=ACTOR)
    resource_id = uuid.uuid4()
    with pytest.raises(
        TagLinkNotFound,
        match=f"Tag {tag.id} is not attached to session {resource_id}",
    ):
        await service.delete_tag_link(
            tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
            actor=ACTOR,
        )
