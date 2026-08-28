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

import itertools
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from conftest import FakeTagRepository, pg_session_with_engine, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.session import Session
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
)
from kitaru.server.filtering import FilterCondition

MakeResourceId = Callable[[uuid.UUID | None], Awaitable[uuid.UUID]]

Setup = tuple[
    TagRepository,
    uuid.UUID,
    MakeResourceId,
    MakeResourceId,
    MakeResourceId,
    MakeResourceId,
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each tag repository implementation and its collaborators.

    Yields the repository, an owner id, and a factory per typed resource
    column that returns a real row's id on postgres, forcing a given id
    when one is passed.
    """
    if request.param == "fake":

        async def make_id(id_: uuid.UUID | None) -> uuid.UUID:
            return id_ if id_ is not None else uuid.uuid4()

        yield FakeTagRepository(), uuid.uuid4(), make_id, make_id, make_id, make_id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        cohorts_repository = SQLCohortRepository(session)
        cohort_versions_repository = SQLCohortVersionRepository(session)
        agent_versions_repository = SQLAgentVersionRepository(session)
        sessions_repository = SQLSessionRepository(session, engine)
        session_numbers = itertools.count(1)
        cohort_names = itertools.count(1)

        async def make_session_id(id_: uuid.UUID | None) -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    id=id_ if id_ is not None else uuid.uuid4(),
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=next(session_numbers),
                    origin=SessionOrigin.RECORDED,
                )
            )
            return created.id

        async def make_cohort_id(id_: uuid.UUID | None) -> uuid.UUID:
            created = await cohorts_repository.create(
                Cohort(
                    id=id_ if id_ is not None else uuid.uuid4(),
                    owner_id=owner.id,
                    name=f"cohort-{next(cohort_names)}",
                    agent_id=agent.id,
                )
            )
            return created.id

        async def make_cohort_version_id(id_: uuid.UUID | None) -> uuid.UUID:
            cohort_id = await make_cohort_id(None)
            created = await cohort_versions_repository.create(
                CohortVersion(
                    id=id_ if id_ is not None else uuid.uuid4(),
                    owner_id=owner.id,
                    cohort_id=cohort_id,
                    session_count=0,
                ),
                [],
            )
            return created.id

        async def make_agent_version_id(id_: uuid.UUID | None) -> uuid.UUID:
            created = await agent_versions_repository.create(
                AgentVersion(
                    id=id_ if id_ is not None else uuid.uuid4(),
                    owner_id=owner.id,
                    agent_id=agent.id,
                )
            )
            return created.id

        yield (
            SQLTagRepository(session),
            owner.id,
            make_session_id,
            make_cohort_id,
            make_cohort_version_id,
            make_agent_version_id,
        )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new tag with both timestamps set."""
    repository, owner_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    assert tag.name == "prod"
    assert tag.owner_id == owner_id
    assert tag.created is not None
    assert tag.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second tag with the same name."""
    repository, owner_id, *_ = setup
    await repository.create(Tag(owner_id=owner_id, name="prod"))
    with pytest.raises(DuplicateTagName, match="Tag name 'prod' is already registered"):
        await repository.create(Tag(owner_id=owner_id, name="prod"))


async def test_get(setup: Setup) -> None:
    """Load a stored tag by id."""
    repository, owner_id, *_ = setup
    created = await repository.create(Tag(owner_id=owner_id, name="prod"))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown tag id."""
    repository, *_ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query tags newest-first with filters."""
    repository, owner_id, *_ = setup
    prod = await repository.create(Tag(owner_id=owner_id, name="prod"))
    await repository.create(Tag(owner_id=owner_id, name="staging"))
    await repository.create(Tag(owner_id=owner_id, name="canary"))

    tags, next_cursor = await repository.query(TagFilter())
    assert next_cursor is None
    assert [tag.name for tag in tags] == ["canary", "staging", "prod"]

    tags, next_cursor = await repository.query(
        TagFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="prod")
        )
    )
    assert next_cursor is None
    assert tags[0] == prod

    tags, next_cursor = await repository.query(
        TagFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="missing")
        )
    )
    assert next_cursor is None
    assert tags == []


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, *_ = setup
    created = [
        await repository.create(Tag(owner_id=owner_id, name=f"tag-{i}"))
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Tag] = []
    cursor = None
    while True:
        tags, next_cursor = await repository.query(TagFilter(cursor=cursor, size=2))
        collected.extend(tags)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({tag.id for tag in collected}) == 5


async def test_query_invalid_cursor(setup: Setup) -> None:
    """Raise for a cursor string that fails to decode."""
    repository, *_ = setup
    with pytest.raises(ValidationError):
        await repository.query(TagFilter(cursor="not-a-valid-cursor"))


async def test_update(setup: Setup) -> None:
    """Rename a tag and renew the updated timestamp."""
    repository, owner_id, *_ = setup
    created = await repository.create(Tag(owner_id=owner_id, name="prod"))
    created.update_name("production")
    updated = await repository.update(created)
    assert updated.name == "production"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown tag id."""
    repository, owner_id, *_ = setup
    tag = Tag(owner_id=owner_id, name="prod")
    with pytest.raises(TagNotFound, match=f"Tag {tag.id} was not found"):
        await repository.update(tag)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a tag to a registered name."""
    repository, owner_id, *_ = setup
    await repository.create(Tag(owner_id=owner_id, name="prod"))
    staging = await repository.create(Tag(owner_id=owner_id, name="staging"))
    staging.update_name("prod")
    with pytest.raises(DuplicateTagName, match="Tag name 'prod' is already registered"):
        await repository.update(staging)


async def test_delete(setup: Setup) -> None:
    """Delete a stored tag."""
    repository, owner_id, *_ = setup
    created = await repository.create(Tag(owner_id=owner_id, name="prod"))
    await repository.delete(created.id)
    with pytest.raises(TagNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown tag id."""
    repository, *_ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_create_link(setup: Setup) -> None:
    """Store a new tag link with both timestamps set."""
    repository, owner_id, make_session_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = await make_session_id(None)
    link = await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    assert link.tag_id == tag.id
    assert link.resource_type == TagResourceType.SESSION
    assert link.resource_id == resource_id
    assert link.created is not None
    assert link.updated is not None


async def test_create_link_missing_tag(setup: Setup) -> None:
    """Raise when linking a resource to an unknown tag."""
    repository, *_ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(TagNotFound, match=f"Tag {missing_id} was not found"):
        await repository.create_link(
            TagLink(
                tag_id=missing_id,
                resource_type=TagResourceType.SESSION,
                resource_id=uuid.uuid4(),
            )
        )


async def test_create_link_duplicate(setup: Setup) -> None:
    """Reject linking the same tag and resource twice."""
    repository, owner_id, make_session_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = await make_session_id(None)
    await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    with pytest.raises(DuplicateTagLink):
        await repository.create_link(
            TagLink(
                tag_id=tag.id,
                resource_type=TagResourceType.SESSION,
                resource_id=resource_id,
            )
        )


async def test_create_link_distinct_resource_types(setup: Setup) -> None:
    """Allow the same resource id under different resource types."""
    repository, owner_id, make_session_id, make_cohort_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = await make_session_id(None)
    await make_cohort_id(resource_id)
    await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.SESSION,
            resource_id=resource_id,
        )
    )
    link = await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT,
            resource_id=resource_id,
        )
    )
    assert link.resource_type == TagResourceType.COHORT


async def test_create_link_distinct_version_resource_types(setup: Setup) -> None:
    """Allow the same resource id under different resource types."""
    (
        repository,
        owner_id,
        _,
        _,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = await make_cohort_version_id(None)
    await make_agent_version_id(resource_id)
    await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT_VERSION,
            resource_id=resource_id,
        )
    )
    link = await repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.AGENT_VERSION,
            resource_id=resource_id,
        )
    )
    assert link.resource_type == TagResourceType.AGENT_VERSION


async def test_delete_link(setup: Setup) -> None:
    """Delete a tag link by tag and resource."""
    repository, owner_id, make_session_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = await make_session_id(None)
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
    """Raise when deleting a link that does not exist."""
    repository, owner_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    with pytest.raises(TagLinkNotFound):
        await repository.delete_link(tag.id, TagResourceType.SESSION, uuid.uuid4())


async def test_delete_tag_cascades_links(setup: Setup) -> None:
    """Deleting a tag also removes its links."""
    repository, owner_id, make_session_id, *_ = setup
    tag = await repository.create(Tag(owner_id=owner_id, name="prod"))
    resource_id = await make_session_id(None)
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


async def test_delete_session_cascades_links() -> None:
    """Deleting a session removes tag links pointing at it.

    Postgres-only: the fake tag repository has no coupling to session
    deletion, so it cannot mirror a cross-resource cascade.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        sessions_repository = SQLSessionRepository(session, engine)
        created_session = await sessions_repository.create(
            Session(
                owner_id=owner.id,
                agent_id=agent.id,
                number=1,
                origin=SessionOrigin.RECORDED,
            )
        )
        tags_repository = SQLTagRepository(session)
        tag = await tags_repository.create(Tag(owner_id=owner.id, name="prod"))
        await tags_repository.create_link(
            TagLink(
                tag_id=tag.id,
                resource_type=TagResourceType.SESSION,
                resource_id=created_session.id,
            )
        )

        await sessions_repository.delete(created_session.id)

        with pytest.raises(TagLinkNotFound):
            await tags_repository.delete_link(
                tag.id, TagResourceType.SESSION, created_session.id
            )
