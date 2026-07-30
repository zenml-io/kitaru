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
"""Contract tests for session node repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest

from conftest import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    create_session,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.session_node_repository import (
    SQLSessionNodeRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.cohort_version_repository import (
    CohortVersionRepository,
)
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.models.session_node import SessionNodeFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.session import Session
from kitaru.server.domain.session_node import SessionNode

Setup = tuple[SessionNodeRepository, uuid.UUID, Callable[[], Awaitable[uuid.UUID]]]
ScopedSetup = tuple[
    SessionNodeRepository,
    CohortRepository,
    CohortVersionRepository,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[uuid.UUID], Awaitable[uuid.UUID]],
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each session node repository implementation, a session id to
    attach nodes to, and a factory for further session ids."""
    if request.param == "fake":
        sessions = FakeSessionRepository()
        owner_id = uuid.uuid4()

        async def make_session_id() -> uuid.UUID:
            created = await create_session(sessions, owner_id, agent_id=uuid.uuid4())
            return created.id

        session_id = await make_session_id()
        yield FakeSessionNodeRepository(), session_id, make_session_id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        sessions_repository = SQLSessionRepository(session)

        async def make_session_id() -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id, agent_id=agent.id, origin=SessionOrigin.RECORDED
                )
            )
            return created.id

        session_id = await make_session_id()
        yield SQLSessionNodeRepository(session), session_id, make_session_id


@pytest.fixture(params=["fake", "postgres"])
async def scoped_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[ScopedSetup, None]:
    """Provide a session node repository wired to sessions and cohort versions
    sharing its backend, a cohort repository, a cohort version repository, an
    owner id, a factory for agent ids, and a factory for session ids on a
    given agent."""
    if request.param == "fake":
        sessions = FakeSessionRepository()
        cohorts = FakeCohortRepository()
        cohort_versions = FakeCohortVersionRepository(
            cohorts=cohorts, sessions=sessions
        )
        owner_id = uuid.uuid4()

        async def make_agent_id() -> uuid.UUID:
            return uuid.uuid4()

        async def make_session_id(agent_id: uuid.UUID) -> uuid.UUID:
            created = await create_session(sessions, owner_id, agent_id=agent_id)
            return created.id

        yield (
            FakeSessionNodeRepository(
                sessions=sessions, cohort_versions=cohort_versions
            ),
            cohorts,
            cohort_versions,
            owner_id,
            make_agent_id,
            make_session_id,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        sessions_repository = SQLSessionRepository(session)

        async def make_agent_id() -> uuid.UUID:
            created = await agents.create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        async def make_session_id(agent_id: uuid.UUID) -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id, agent_id=agent_id, origin=SessionOrigin.RECORDED
                )
            )
            return created.id

        yield (
            SQLSessionNodeRepository(session),
            SQLCohortRepository(session),
            SQLCohortVersionRepository(session),
            owner.id,
            make_agent_id,
            make_session_id,
        )


def _node(index: int, **overrides: Any) -> SessionNode:
    values: dict[str, Any] = {
        "session_id": uuid.uuid4(),
        "index": index,
        "node_type": NodeType.LLM_CALL,
        "name": "call",
        "status": NodeStatus.COMPLETED,
    }
    values.update(overrides)
    return SessionNode(**values)


async def test_get_by_indexes_empty_when_none_stored(setup: Setup) -> None:
    """Return no rows for indexes that are not stored."""
    repository, session_id, _ = setup
    result = await repository.get_by_indexes(session_id, [0, 1])
    assert result == {}


async def test_get_by_indexes_bulk_fetch(setup: Setup) -> None:
    """Bulk-load stored nodes keyed by index, missing indexes omitted."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [_node(0, session_id=session_id), _node(1, session_id=session_id)],
    )
    result = await repository.get_by_indexes(session_id, [0, 1, 2])
    assert set(result.keys()) == {0, 1}


async def test_upsert_batch_inserts_new_rows(setup: Setup) -> None:
    """Insert new rows preserving batch order and the given ids."""
    repository, session_id, _ = setup
    nodes = [_node(0, session_id=session_id), _node(1, session_id=session_id)]
    stored = await repository.upsert_batch(session_id, nodes)
    assert [node.id for node in stored] == [nodes[0].id, nodes[1].id]
    assert stored[0].created is not None
    assert stored[0].updated is not None


async def test_upsert_batch_replaces_existing_row_preserving_id(
    setup: Setup,
) -> None:
    """Replace an existing index whole, preserving the row id."""
    repository, session_id, _ = setup
    first = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id, name="first")]
    )
    replaced = await repository.upsert_batch(
        session_id,
        [
            SessionNode(
                id=first[0].id,
                session_id=session_id,
                index=0,
                node_type=NodeType.LLM_CALL,
                name="second",
                status=NodeStatus.COMPLETED,
            )
        ],
    )
    assert replaced[0].id == first[0].id
    assert replaced[0].name == "second"

    loaded = await repository.get_by_indexes(session_id, [0])
    assert loaded[0].name == "second"


async def test_upsert_batch_replace_clears_omitted_fields(setup: Setup) -> None:
    """Clear fields the replacing row omits."""
    repository, session_id, _ = setup
    first = await repository.upsert_batch(
        session_id,
        [_node(0, session_id=session_id, error="boom", tool_name="unused")],
    )
    replaced = await repository.upsert_batch(
        session_id,
        [
            SessionNode(
                id=first[0].id,
                session_id=session_id,
                index=0,
                node_type=NodeType.LLM_CALL,
                name="call",
                status=NodeStatus.COMPLETED,
            )
        ],
    )
    assert replaced[0].error is None
    assert replaced[0].tool_name is None


async def test_query_ordered_by_index_ascending(setup: Setup) -> None:
    """Order nodes by index ascending regardless of insertion order."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [_node(index, session_id=session_id) for index in (2, 0, 1)],
    )
    nodes, next_cursor = await repository.query(
        SessionNodeFilter(session_id=session_id)
    )
    assert next_cursor is None
    assert [node.index for node in nodes] == [0, 1, 2]


async def test_query_walks_pages_by_index(setup: Setup) -> None:
    """Walk every page via next_cursor in index order without gaps."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [_node(index, session_id=session_id) for index in range(5)],
    )

    collected: list[int] = []
    cursor = None
    while True:
        nodes, next_cursor = await repository.query(
            SessionNodeFilter(session_id=session_id, cursor=cursor, size=2)
        )
        collected.extend(node.index for node in nodes)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == [0, 1, 2, 3, 4]


async def test_query_include_payloads_false_nulls_heavy_columns(
    setup: Setup,
) -> None:
    """Null inputs, outputs, and attributes when include_payloads is unset."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                inputs={"q": "hi"},
                outputs={"a": "there"},
                attributes={"k": 1},
            )
        ],
    )
    nodes, _ = await repository.query(
        SessionNodeFilter(session_id=session_id, include_payloads=False)
    )
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes is None
    assert nodes[0].metadata == {}


async def test_query_include_payloads_true_populates_heavy_columns(
    setup: Setup,
) -> None:
    """Populate inputs, outputs, and attributes when requested."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                inputs={"q": "hi"},
                outputs={"a": "there"},
                attributes={"k": 1},
            )
        ],
    )
    nodes, _ = await repository.query(
        SessionNodeFilter(session_id=session_id, include_payloads=True)
    )
    assert nodes[0].inputs == {"q": "hi"}
    assert nodes[0].outputs == {"a": "there"}
    assert nodes[0].attributes == {"k": 1}


async def test_query_scoped_to_session(setup: Setup) -> None:
    """List only the nodes of the requested session."""
    repository, session_id, make_session_id = setup
    other_session_id = await make_session_id()
    await repository.upsert_batch(session_id, [_node(0, session_id=session_id)])
    await repository.upsert_batch(
        other_session_id, [_node(0, session_id=other_session_id)]
    )
    nodes, _ = await repository.query(SessionNodeFilter(session_id=session_id))
    assert len(nodes) == 1
    assert nodes[0].session_id == session_id


async def test_get_index_by_id_returns_every_node_index(setup: Setup) -> None:
    """Map every node id in a session to its index."""
    repository, session_id, _ = setup
    nodes = [_node(0, session_id=session_id), _node(1, session_id=session_id)]
    stored = await repository.upsert_batch(session_id, nodes)

    index_by_id = await repository.get_index_by_id(session_id)

    assert index_by_id == {stored[0].id: 0, stored[1].id: 1}


async def test_find_latest_by_cache_key_in_agent_scopes_to_agent(
    scoped_setup: ScopedSetup,
) -> None:
    """Match only cache-key hits recorded under the requested agent."""
    repository, _, _, _, make_agent_id, make_session_id = scoped_setup
    matching_agent_id = await make_agent_id()
    other_agent_id = await make_agent_id()
    matching_session_id = await make_session_id(matching_agent_id)
    other_session_id = await make_session_id(other_agent_id)
    cache_key = "b" * 64
    await repository.upsert_batch(
        matching_session_id,
        [
            _node(
                0,
                session_id=matching_session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"temperature": 18},
            )
        ],
    )
    await repository.upsert_batch(
        other_session_id,
        [
            _node(
                0,
                session_id=other_session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"temperature": 99},
            )
        ],
    )

    found = await repository.find_latest_by_cache_key_in_agent(
        matching_agent_id, cache_key
    )

    assert found is not None
    assert found.outputs == {"temperature": 18}


async def test_find_latest_by_cache_key_in_cohort_version_scopes_to_cohort_version(
    scoped_setup: ScopedSetup,
) -> None:
    """Match only cache-key hits within the requested cohort version's sessions."""
    repository, cohorts, cohort_versions, owner_id, make_agent_id, make_session_id = (
        scoped_setup
    )
    agent_id = await make_agent_id()
    matching_session_id = await make_session_id(agent_id)
    other_session_id = await make_session_id(agent_id)
    cache_key = "c" * 64
    await repository.upsert_batch(
        matching_session_id,
        [
            _node(
                0,
                session_id=matching_session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"temperature": 18},
            )
        ],
    )
    await repository.upsert_batch(
        other_session_id,
        [
            _node(
                0,
                session_id=other_session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"temperature": 99},
            )
        ],
    )
    cohort = await cohorts.create(
        Cohort(owner_id=owner_id, name="matching-cohort", agent_id=agent_id)
    )
    cohort_version = await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=1),
        [matching_session_id],
    )

    found = await repository.find_latest_by_cache_key_in_cohort_version(
        cohort_version.id, cache_key
    )

    assert found is not None
    assert found.outputs == {"temperature": 18}
