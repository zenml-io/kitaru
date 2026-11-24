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
"""Session tag read and alias contracts across repository implementations."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import event

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.session import Session
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    NotExpression,
    OrExpression,
)
from kitaru_test_support import FakeSessionRepository, FakeTagRepository

Setup = tuple[SessionRepository, TagRepository, uuid.UUID, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide sessions and tags sharing one backend."""
    if request.param == "fake":
        tags = FakeTagRepository()
        yield FakeSessionRepository(tags=tags), tags, uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        yield (
            SQLSessionRepository(session, engine),
            SQLTagRepository(session),
            owner.id,
            agent.id,
        )


async def test_tags_follow_links_renames_and_unlinks(setup: Setup) -> None:
    """Every read and write response includes current linked tags only."""
    repository, tags, owner_id, agent_id = setup
    first, second = [
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=number,
                origin=SessionOrigin.RECORDED,
            )
        )
        for number in (1, 2)
    ]
    assert first.tags == second.tags == []
    tag = await tags.create(Tag(owner_id=owner_id, name="prod"))
    unrelated = await tags.create(Tag(owner_id=owner_id, name="private"))
    for tag_id, session_id in ((tag.id, first.id), (unrelated.id, second.id)):
        await tags.create_link(
            TagLink(
                tag_id=tag_id,
                resource_type=TagResourceType.SESSION,
                resource_id=session_id,
            )
        )
    for include_payloads in (False, True):
        assert (await repository.get(first.id, include_payloads)).tags == [tag]
        loaded = await repository.get_many([first.id, second.id], include_payloads)
        assert loaded[first.id].tags == [tag]
        assert loaded[second.id].tags == [unrelated]
    tag.update_name("production")
    renamed = await tags.update(tag)
    loaded_first = await repository.get(first.id, False)
    assert loaded_first.tags == [renamed]
    loaded_first.update_name("renamed session")
    assert (await repository.update(loaded_first)).tags == [renamed]
    await tags.delete_link(tag.id, TagResourceType.SESSION, first.id)
    assert (await repository.get(first.id, False)).tags == []
    await tags.delete(unrelated.id)
    assert (await repository.get(second.id, False)).tags == []


async def test_alias_pagination_nested_and_exclusion(setup: Setup) -> None:
    """Tag alias and canonical names share cursors and boolean semantics."""
    repository, tags, owner_id, agent_id = setup
    sessions = [
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=number,
                origin=SessionOrigin.RECORDED,
            )
        )
        for number in range(1, 5)
    ]
    prod = await tags.create(Tag(owner_id=owner_id, name="prod"))
    excluded = await tags.create(Tag(owner_id=owner_id, name="excluded"))
    also = await tags.create(Tag(owner_id=owner_id, name="also"))
    await tags.create_link(
        TagLink(
            tag_id=also.id,
            resource_type=TagResourceType.SESSION,
            resource_id=sessions[0].id,
        )
    )
    for session in sessions[:3]:
        await tags.create_link(
            TagLink(
                tag_id=prod.id,
                resource_type=TagResourceType.SESSION,
                resource_id=session.id,
            )
        )
    await tags.create_link(
        TagLink(
            tag_id=excluded.id,
            resource_type=TagResourceType.SESSION,
            resource_id=sessions[2].id,
        )
    )

    def expression(field: str) -> AndExpression:
        return AndExpression(
            operands=(
                OrExpression(
                    operands=(
                        FilterCondition(
                            field=field, op=FilterOp.IN, value=["prod", "also"]
                        ),
                        FilterCondition(field="name", op=FilterOp.EQ, value="absent"),
                    )
                ),
                NotExpression(
                    operand=FilterCondition(
                        field=field, op=FilterOp.EQ, value="excluded"
                    )
                ),
            )
        )

    canonical = SessionFilter(expression=expression("tag"), size=1, sort="created:asc")
    alias = SessionFilter(expression=expression("tags"), size=1, sort="created:asc")
    assert alias.compute_filter_hash() == canonical.compute_filter_hash()
    page, cursor = await repository.query(canonical, False)
    assert [s.id for s in page] == [sessions[0].id]
    assert page[0].tags == [prod, also]
    assert cursor is not None
    page, cursor = await repository.query(
        SessionFilter(
            expression=expression("tags"), size=1, sort="created:asc", cursor=cursor
        ),
        False,
    )
    assert [s.id for s in page] == [sessions[1].id]
    assert page[0].tags == [prod]
    assert cursor is None
    page, _ = await repository.query(
        SessionFilter(
            expression=NotExpression(
                operand=FilterCondition(field="tags", op=FilterOp.EQ, value="prod")
            )
        ),
        False,
    )
    assert [s.id for s in page] == [sessions[3].id]
    assert page[0].tags == []


async def test_page_tags_use_one_batch_without_payload_reads() -> None:
    """A whole page reads tags in one query without joining session payloads."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        repository = SQLSessionRepository(session, engine)
        tags = SQLTagRepository(session)
        tag = await tags.create(Tag(owner_id=owner.id, name="prod"))
        for number in range(1, 5):
            stored = await repository.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=number,
                    origin=SessionOrigin.RECORDED,
                )
            )
            await tags.create_link(
                TagLink(
                    tag_id=tag.id,
                    resource_type=TagResourceType.SESSION,
                    resource_id=stored.id,
                )
            )
        statements: list[str] = []

        def record(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", record)
        try:
            page, cursor = await repository.query(SessionFilter(size=3), False)
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", record)
        assert len(page) == 3
        assert cursor is not None
        assert all(s.tags == [tag] for s in page)
        selects = [
            sql for sql in statements if sql.lstrip().upper().startswith("SELECT")
        ]
        assert len(selects) == 2
        assert "session.inputs," not in selects[0]
        assert "session.outputs," not in selects[0]
        assert "tag_link" in selects[1]


@pytest.mark.parametrize("field", ["tag", "tags"])
@pytest.mark.parametrize("operator", [FilterOp.NE, FilterOp.CONTAINS, FilterOp.IS_NULL])
def test_tag_alias_rejects_unsupported_operators(
    field: str, operator: FilterOp
) -> None:
    """Keep the same operator restrictions for canonical and alias fields."""
    with pytest.raises(ValidationError, match="not supported"):
        SessionFilter(
            expression=FilterCondition(field=field, op=operator, value="prod")
        )
