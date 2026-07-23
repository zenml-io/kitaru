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
"""Contract tests for tag repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeTagRepository, pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import (
    SQLTagRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.tags import TagFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
    TagResourceType,
)

Setup = tuple[TagRepository, uuid.UUID, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each tag repository implementation plus two owner ids."""
    if request.param == "fake":
        yield FakeTagRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning accounts first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        other_owner = await accounts.create(Account(name="other-owner"))
        yield SQLTagRepository(session), owner.id, other_owner.id


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new tag with both timestamps set."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    assert tag.name == "prod"
    assert tag.owner_id == owner_id
    assert tag.created is not None
    assert tag.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second tag with the same name."""
    repository, owner_id, other_owner_id = setup
    await repository.create(Tag(owner_id=owner_id, name="prod"))
    with pytest.raises(DuplicateTagName, match="Tag name 'prod' is already registered"):
        await repository.create(Tag(owner_id=other_owner_id, name="prod"))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id, _ = setup
    await repository.create(Tag(owner_id=owner_id, name="prod"))
    with pytest.raises(DuplicateTagName):
        await repository.create(Tag(owner_id=owner_id, name="prod"))
    tag = await repository.create(Tag(owner_id=owner_id, name="staging"))
    assert tag.name == "staging"


async def test_get(setup: Setup) -> None:
    """Load a stored tag by id."""
    repository, owner_id, _ = setup
    created = await repository.create(Tag(owner_id=owner_id, name="prod"))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown tag id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query tags with filters and pagination."""
    repository, owner_id, other_owner_id = setup
    prod = await repository.create(Tag(owner_id=owner_id, name="prod"))
    await repository.create(Tag(owner_id=owner_id, name="staging"))
    flaky = await repository.create(Tag(owner_id=other_owner_id, name="flaky"))

    tags, total = await repository.query(TagFilter())
    assert total == 3
    assert [tag.name for tag in tags] == ["prod", "staging", "flaky"]

    tags, total = await repository.query(TagFilter(name="prod"))
    assert total == 1
    assert tags[0] == prod

    tags, total = await repository.query(TagFilter(owner_id=other_owner_id))
    assert total == 1
    assert tags[0] == flaky

    tags, total = await repository.query(TagFilter(page=2, page_size=2))
    assert total == 3
    assert [tag.name for tag in tags] == ["flaky"]

    tags, total = await repository.query(TagFilter(name="missing"))
    assert total == 0
    assert tags == []


async def test_delete(setup: Setup) -> None:
    """Delete a stored tag."""
    repository, owner_id, _ = setup
    created = await repository.create(Tag(owner_id=owner_id, name="prod"))
    await repository.delete(created.id)
    with pytest.raises(TagNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown tag id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_removes_links(setup: Setup) -> None:
    """Remove the links of a tag when deleting it."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = uuid.uuid4()
    await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    await repository.delete(tag.id)
    with pytest.raises(TagLinkNotFound):
        await repository.delete_link(tag.id, TagResourceType.SESSION, resource_id)


async def test_create_link_sets_timestamps(setup: Setup) -> None:
    """Store a new tag link with both timestamps set."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = uuid.uuid4()
    link = await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    assert link.tag_id == tag.id
    assert link.resource_type is TagResourceType.SESSION
    assert link.resource_id == resource_id
    assert link.created is not None
    assert link.updated is not None


async def test_create_link_duplicate(setup: Setup) -> None:
    """Reject a second link to the same resource."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = uuid.uuid4()
    await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    with pytest.raises(
        DuplicateTagLink,
        match=f"Tag {tag.id} is already attached to session {resource_id}",
    ):
        await repository.create_link(
            TagLink(
                tag_id=tag.id,
                resource_type=TagResourceType.SESSION,
                resource_id=resource_id,
            )
        )


async def test_create_link_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate link failure."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = uuid.uuid4()
    link = TagLink(
        tag_id=tag.id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
    )
    await repository.create_link(link)
    with pytest.raises(DuplicateTagLink):
        await repository.create_link(link.model_copy(update={"id": uuid.uuid4()}))
    other = await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=resource_id,
        )
    )
    assert other.resource_type is TagResourceType.COHORT


async def test_delete_link(setup: Setup) -> None:
    """Delete a stored tag link."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = uuid.uuid4()
    await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    await repository.delete_link(tag.id, TagResourceType.SESSION, resource_id)
    with pytest.raises(TagLinkNotFound):
        await repository.delete_link(tag.id, TagResourceType.SESSION, resource_id)


async def test_delete_link_not_found(setup: Setup) -> None:
    """Raise for an unknown tag link."""
    repository, owner_id, _ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = uuid.uuid4()
    with pytest.raises(
        TagLinkNotFound,
        match=f"Tag {tag.id} is not attached to session {resource_id}",
    ):
        await repository.delete_link(tag.id, TagResourceType.SESSION, resource_id)
