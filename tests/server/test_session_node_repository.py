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

import itertools
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from conftest import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    create_session,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
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
from kitaru.server.domain.payload import Payload
from kitaru.server.domain.session import Session
from kitaru.server.domain.session_node import (
    DuplicateSessionNodeExternalId,
    PendingLinkKind,
    PendingParentLink,
    SessionNode,
)
from kitaru.server.filtering import (
    FilterCondition,
    FilterExpression,
    NotExpression,
    OrExpression,
)

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
    """Provide each session node repository implementation and its collaborators.

    Yields the repository, a session id to attach nodes to, and a factory for
    further session ids.
    """
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
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        sessions_repository = SQLSessionRepository(session, engine)
        session_numbers = itertools.count(1)

        async def make_session_id() -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=next(session_numbers),
                    origin=SessionOrigin.RECORDED,
                )
            )
            return created.id

        session_id = await make_session_id()
        yield SQLSessionNodeRepository(session), session_id, make_session_id


@pytest.fixture(params=["fake", "postgres"])
async def scoped_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[ScopedSetup, None]:
    """Provide a cohort-scoped session node repository and its collaborators.

    Yields a session node repository wired to sessions and cohort versions
    sharing its backend, a cohort repository, a cohort version repository, an
    owner id, a factory for agent ids, and a factory for session ids on a
    given agent.
    """
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
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        sessions_repository = SQLSessionRepository(session, engine)
        session_numbers: dict[uuid.UUID, Iterator[int]] = {}

        async def make_agent_id() -> uuid.UUID:
            created = await agents.create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        async def make_session_id(agent_id: uuid.UUID) -> uuid.UUID:
            numbers = session_numbers.setdefault(agent_id, itertools.count(1))
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent_id,
                    number=next(numbers),
                    origin=SessionOrigin.RECORDED,
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


def _start(position: int) -> datetime:
    """Build the start time of a node at a position.

    Args:
        position: Position of the node within its session.

    Returns:
        Start time.
    """
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=position)


def _external_id(position: int) -> str:
    """Build the external id of a node at a position.

    Args:
        position: Position of the node within its session.

    Returns:
        External id.
    """
    return f"n{position}"


def _pending_link(
    session_id: uuid.UUID,
    child_id: uuid.UUID,
    parent_external_id: str,
    kind: PendingLinkKind = PendingLinkKind.PRIMARY,
) -> PendingParentLink:
    """Build a pending parent link.

    Args:
        session_id: Id of the owning session.
        child_id: Id of the referencing node.
        parent_external_id: External id the node references.
        kind: Reference kind.

    Returns:
        Pending parent link.
    """
    return PendingParentLink(
        session_id=session_id,
        parent_external_id=parent_external_id,
        child_id=child_id,
        kind=kind,
    )


def _node(position: int, **overrides: Any) -> SessionNode:
    values: dict[str, Any] = {
        "session_id": uuid.uuid4(),
        "external_id": _external_id(position),
        "started_at": _start(position),
        "node_type": NodeType.LLM_CALL,
        "name": "call",
        "status": NodeStatus.COMPLETED,
    }
    values.update(overrides)
    for field in ("reasoning", "inputs", "outputs", "attributes"):
        value = values.get(field)
        if value is None or isinstance(value, Payload):
            continue
        values[field] = (
            Payload.from_text(value)
            if field == "reasoning"
            else Payload.from_json(value)
        )
    return SessionNode(**values)


def _by_id(nodes: list[SessionNode]) -> list[str]:
    """List the external ids of nodes in ascending id order.

    Args:
        nodes: Nodes to order.

    Returns:
        External ids in id order.
    """
    return [node.external_id for node in sorted(nodes, key=lambda node: node.id)]


async def _walk_pages(
    repository: SessionNodeRepository, session_id: uuid.UUID, size: int
) -> list[str]:
    """Collect the external ids of every page of a session, following the cursor.

    Args:
        repository: Repository under test.
        session_id: Id of the session to read.
        size: Page size.

    Returns:
        External ids in page order.
    """
    collected: list[str] = []
    cursor = None
    while True:
        nodes, next_cursor = await repository.query(
            SessionNodeFilter(session_id=session_id, cursor=cursor, size=size)
        )
        collected.extend(node.external_id for node in nodes)
        if next_cursor is None:
            return collected
        cursor = next_cursor


async def test_get_by_external_ids_empty_when_none_stored(setup: Setup) -> None:
    """Return no rows for external ids that are not stored."""
    repository, session_id, _ = setup
    result = await repository.get_by_external_ids(
        session_id, ["n0", "n1"], include_payloads=True
    )
    assert result == {}


async def test_get_by_external_ids_bulk_fetch(setup: Setup) -> None:
    """Bulk-load stored nodes keyed by external id, missing ids omitted."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [_node(0, session_id=session_id), _node(1, session_id=session_id)],
    )
    result = await repository.get_by_external_ids(
        session_id, ["n0", "n1", "n2"], include_payloads=True
    )
    assert set(result.keys()) == {"n0", "n1"}


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
    """Replace an existing external id whole, preserving the row id."""
    repository, session_id, _ = setup
    first = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id, name="first")]
    )
    replaced = await repository.upsert_batch(
        session_id,
        [_node(0, id=first[0].id, session_id=session_id, name="second")],
    )
    assert replaced[0].id == first[0].id
    assert replaced[0].name == "second"

    loaded = await repository.get_by_external_ids(
        session_id, ["n0"], include_payloads=True
    )
    assert loaded["n0"].name == "second"


async def test_upsert_batch_rejects_an_external_id_held_by_another_node(
    setup: Setup,
) -> None:
    """Translate the session external id constraint into a domain conflict."""
    repository, session_id, _ = setup
    await repository.upsert_batch(session_id, [_node(0, session_id=session_id)])

    with pytest.raises(DuplicateSessionNodeExternalId):
        await repository.upsert_batch(
            session_id, [_node(1, session_id=session_id, external_id="n0")]
        )


async def test_upsert_batch_replace_clears_omitted_fields(setup: Setup) -> None:
    """Clear fields the replacing row omits."""
    repository, session_id, _ = setup
    first = await repository.upsert_batch(
        session_id,
        [_node(0, session_id=session_id, error="boom", tool_name="unused")],
    )
    replaced = await repository.upsert_batch(
        session_id,
        [_node(0, id=first[0].id, session_id=session_id)],
    )
    assert replaced[0].error is None
    assert replaced[0].tool_name is None


async def test_query_ordered_by_start_ascending(setup: Setup) -> None:
    """Order nodes by start ascending regardless of insertion order."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [_node(position, session_id=session_id) for position in (2, 0, 1)],
    )
    nodes, next_cursor = await repository.query(
        SessionNodeFilter(session_id=session_id)
    )
    assert next_cursor is None
    assert [node.external_id for node in nodes] == ["n0", "n1", "n2"]


async def test_query_breaks_start_ties_by_id(setup: Setup) -> None:
    """Order nodes sharing one start by ascending id."""
    repository, session_id, _ = setup
    tied = [
        _node(position, session_id=session_id, started_at=_start(0))
        for position in range(3)
    ]
    await repository.upsert_batch(session_id, tied)
    nodes, _ = await repository.query(SessionNodeFilter(session_id=session_id))
    assert [node.id for node in nodes] == sorted(node.id for node in tied)


async def test_query_walks_pages_by_start(setup: Setup) -> None:
    """Walk every page via next_cursor in start order without gaps."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [_node(position, session_id=session_id) for position in range(5)],
    )

    collected = await _walk_pages(repository, session_id, size=2)

    assert collected == ["n0", "n1", "n2", "n3", "n4"]


async def test_query_orders_untimed_nodes_last(setup: Setup) -> None:
    """Sort a node without a start time after every timed node."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [
            _node(2, session_id=session_id, started_at=None),
            _node(1, session_id=session_id),
            _node(0, session_id=session_id),
        ],
    )
    nodes, next_cursor = await repository.query(
        SessionNodeFilter(session_id=session_id)
    )
    assert next_cursor is None
    assert [node.external_id for node in nodes] == ["n0", "n1", "n2"]


async def test_query_walks_pages_into_the_untimed_tail(setup: Setup) -> None:
    """Walk from the last timed node into the untimed tail without gaps."""
    repository, session_id, _ = setup
    timed = [_node(position, session_id=session_id) for position in range(2)]
    untimed = [
        _node(position, session_id=session_id, started_at=None)
        for position in range(2, 5)
    ]
    await repository.upsert_batch(session_id, timed + untimed)

    collected = await _walk_pages(repository, session_id, size=2)

    assert collected == ["n0", "n1", *_by_id(untimed)]


async def test_query_walks_pages_within_the_untimed_tail(setup: Setup) -> None:
    """Walk nodes that all lack a start time page by page in id order."""
    repository, session_id, _ = setup
    untimed = [
        _node(position, session_id=session_id, started_at=None) for position in range(3)
    ]
    await repository.upsert_batch(session_id, untimed)

    collected = await _walk_pages(repository, session_id, size=1)

    assert collected == _by_id(untimed)


async def test_list_all_orders_untimed_nodes_last(setup: Setup) -> None:
    """Read a node without a start time after every timed node."""
    repository, session_id, _ = setup
    await repository.upsert_batch(
        session_id,
        [
            _node(2, session_id=session_id, started_at=None),
            _node(0, session_id=session_id),
            _node(1, session_id=session_id),
        ],
    )
    nodes = await repository.list_all(session_id, include_payloads=False)
    assert [node.external_id for node in nodes] == ["n0", "n1", "n2"]


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (None, [0, 1, 2, 3, 4, 5]),
        (FilterCondition(field="node_type", op=FilterOp.EQ, value="llm_call"), [1, 5]),
        (
            FilterCondition(
                field="node_type", op=FilterOp.IN, value=["llm_call", "tool_call"]
            ),
            [1, 3, 5],
        ),
        (FilterCondition(field="node_type", op=FilterOp.NE, value="span"), [1, 3, 5]),
        (FilterCondition(field="node_type", op=FilterOp.EQ, value="subagent_call"), []),
        (
            OrExpression(
                operands=(
                    FilterCondition(
                        field="node_type", op=FilterOp.EQ, value="tool_call"
                    ),
                    NotExpression(
                        operand=FilterCondition(
                            field="node_type",
                            op=FilterOp.IN,
                            value=["span", "tool_call"],
                        )
                    ),
                )
            ),
            [1, 3, 5],
        ),
    ],
)
async def test_query_filters_node_types_before_pagination(
    setup: Setup, expression: FilterExpression | None, expected: list[int]
) -> None:
    """Fill pages with matching types while retaining the original node order."""
    repository, session_id, make_session_id = setup
    types = [
        NodeType.SPAN,
        NodeType.LLM_CALL,
        NodeType.SPAN,
        NodeType.TOOL_CALL,
        NodeType.SPAN,
        NodeType.LLM_CALL,
    ]
    await repository.upsert_batch(
        session_id,
        [
            _node(position, session_id=session_id, node_type=kind)
            for position, kind in enumerate(types)
        ],
    )
    other_session_id = await make_session_id()
    await repository.upsert_batch(
        other_session_id,
        [_node(0, session_id=other_session_id, node_type=NodeType.LLM_CALL)],
    )
    collected: list[str] = []
    cursor = None
    for _ in range(len(types) + 1):
        nodes, cursor = await repository.query(
            SessionNodeFilter(
                session_id=session_id, expression=expression, size=2, cursor=cursor
            )
        )
        collected.extend(node.external_id for node in nodes)
        if cursor is None:
            break
        assert len(nodes) == 2
    else:
        pytest.fail("Filtered pagination did not terminate")
    assert collected == [_external_id(position) for position in expected]


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
    assert nodes[0].inputs is not None
    assert nodes[0].inputs.value == {"q": "hi"}
    assert nodes[0].outputs is not None
    assert nodes[0].outputs.value == {"a": "there"}
    assert nodes[0].attributes is not None
    assert nodes[0].attributes.value == {"k": 1}


async def test_get_by_external_ids_include_payloads_false_nulls_heavy_columns(
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
    loaded = await repository.get_by_external_ids(
        session_id, ["n0"], include_payloads=False
    )
    assert loaded["n0"].inputs is None
    assert loaded["n0"].outputs is None
    assert loaded["n0"].attributes is None
    assert loaded["n0"].metadata == {}


async def test_get_by_external_ids_include_payloads_true_populates_heavy_columns(
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
    loaded = await repository.get_by_external_ids(
        session_id, ["n0"], include_payloads=True
    )
    assert loaded["n0"].inputs is not None
    assert loaded["n0"].inputs.value == {"q": "hi"}
    assert loaded["n0"].outputs is not None
    assert loaded["n0"].outputs.value == {"a": "there"}
    assert loaded["n0"].attributes is not None
    assert loaded["n0"].attributes.value == {"k": 1}


async def test_upsert_batch_replace_keeps_payloads_of_deferred_reload(
    setup: Setup,
) -> None:
    """Store the replacing payloads when the existing row loads them deferred."""
    repository, session_id, _ = setup
    first = await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                inputs={"q": "old"},
                outputs={"a": "old"},
                attributes={"k": 0},
            )
        ],
    )
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                id=first[0].id,
                inputs={"q": "new"},
                outputs={"a": "new"},
                attributes={"k": 1},
            )
        ],
    )

    loaded = await repository.get_by_external_ids(
        session_id, ["n0"], include_payloads=True
    )
    assert loaded["n0"].inputs is not None
    assert loaded["n0"].inputs.value == {"q": "new"}
    assert loaded["n0"].outputs is not None
    assert loaded["n0"].outputs.value == {"a": "new"}
    assert loaded["n0"].attributes is not None
    assert loaded["n0"].attributes.value == {"k": 1}


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


async def test_link_pending_parents_links_a_stored_child(setup: Setup) -> None:
    """Link a stored child whose parent external id a later batch carries."""
    repository, session_id, _ = setup
    stored_child = await repository.upsert_batch(
        session_id,
        [_node(1, session_id=session_id, parent_external_id="n0")],
    )
    await repository.replace_pending_links(
        [stored_child[0].id],
        [_pending_link(session_id, stored_child[0].id, "n0")],
    )
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )

    relinked = await repository.link_pending_parents(session_id, stored_parent)

    assert [node.id for node in relinked] == [stored_child[0].id]
    assert relinked[0].parent_id == stored_parent[0].id


async def test_link_pending_parents_keeps_the_child_start(setup: Setup) -> None:
    """Leave the start time of a linked child untouched."""
    repository, session_id, _ = setup
    stored_child = await repository.upsert_batch(
        session_id,
        [
            _node(
                9,
                session_id=session_id,
                external_id="n1",
                parent_external_id="n0",
            )
        ],
    )
    await repository.replace_pending_links(
        [stored_child[0].id],
        [_pending_link(session_id, stored_child[0].id, "n0")],
    )
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )

    relinked = await repository.link_pending_parents(session_id, stored_parent)

    assert relinked[0].started_at == _start(9)


async def test_link_pending_parents_links_a_secondary_reference(
    setup: Setup,
) -> None:
    """Append the resolved parent to the secondary parents of a child."""
    repository, session_id, _ = setup
    stored_child = await repository.upsert_batch(
        session_id,
        [_node(1, session_id=session_id, secondary_parent_external_ids=["n0"])],
    )
    await repository.replace_pending_links(
        [stored_child[0].id],
        [
            _pending_link(
                session_id, stored_child[0].id, "n0", PendingLinkKind.SECONDARY
            )
        ],
    )
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )

    relinked = await repository.link_pending_parents(session_id, stored_parent)

    assert [node.id for node in relinked] == [stored_child[0].id]
    assert relinked[0].parent_id is None
    assert relinked[0].secondary_parent_ids == [stored_parent[0].id]


async def test_link_pending_parents_drops_the_resolved_links(setup: Setup) -> None:
    """Leave no pending link behind for a reference that resolved."""
    repository, session_id, _ = setup
    stored_child = await repository.upsert_batch(
        session_id,
        [
            _node(
                1,
                session_id=session_id,
                parent_external_id="n0",
                secondary_parent_external_ids=["n0"],
            )
        ],
    )
    await repository.replace_pending_links(
        [stored_child[0].id],
        [
            _pending_link(session_id, stored_child[0].id, "n0"),
            _pending_link(
                session_id, stored_child[0].id, "n0", PendingLinkKind.SECONDARY
            ),
        ],
    )
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )
    linked = await repository.link_pending_parents(session_id, stored_parent)
    assert linked[0].parent_id == stored_parent[0].id
    assert linked[0].secondary_parent_ids == [stored_parent[0].id]

    assert await repository.link_pending_parents(session_id, stored_parent) == []

    loaded = await repository.get_by_external_ids(
        session_id, ["n1"], include_payloads=False
    )
    assert loaded["n1"].secondary_parent_ids == [stored_parent[0].id]


async def test_replace_pending_links_drops_the_previous_links(setup: Setup) -> None:
    """Keep only the links of the latest write for a child."""
    repository, session_id, _ = setup
    stored_child = await repository.upsert_batch(
        session_id,
        [_node(2, session_id=session_id, parent_external_id="n0")],
    )
    await repository.replace_pending_links(
        [stored_child[0].id],
        [_pending_link(session_id, stored_child[0].id, "n0")],
    )
    await repository.replace_pending_links(
        [stored_child[0].id],
        [_pending_link(session_id, stored_child[0].id, "n1")],
    )
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )

    assert await repository.link_pending_parents(session_id, stored_parent) == []


async def test_link_pending_parents_leaves_a_linked_child_alone(setup: Setup) -> None:
    """Report no link when every reference of the session already resolves."""
    repository, session_id, _ = setup
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )
    await repository.upsert_batch(
        session_id,
        [
            _node(
                1,
                session_id=session_id,
                parent_id=stored_parent[0].id,
                parent_external_id="n0",
            )
        ],
    )

    assert await repository.link_pending_parents(session_id, stored_parent) == []


async def test_link_pending_parents_scoped_to_session(setup: Setup) -> None:
    """Leave a pending child of another session unlinked."""
    repository, session_id, make_session_id = setup
    other_session_id = await make_session_id()
    other_child = await repository.upsert_batch(
        other_session_id,
        [_node(1, session_id=other_session_id, parent_external_id="n0")],
    )
    await repository.replace_pending_links(
        [other_child[0].id],
        [_pending_link(other_session_id, other_child[0].id, "n0")],
    )
    stored_parent = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )

    await repository.link_pending_parents(session_id, stored_parent)

    loaded = await repository.get_by_external_ids(
        other_session_id, ["n1"], include_payloads=False
    )
    assert loaded["n1"].id == other_child[0].id
    assert loaded["n1"].parent_id is None


async def test_exists_in_session_matches_only_the_owning_session(
    setup: Setup,
) -> None:
    """Report a node only for the session that holds it."""
    repository, session_id, make_session_id = setup
    other_session_id = await make_session_id()
    stored = await repository.upsert_batch(
        session_id, [_node(0, session_id=session_id)]
    )

    assert await repository.exists_in_session(session_id, stored[0].id)
    assert not await repository.exists_in_session(other_session_id, stored[0].id)
    assert not await repository.exists_in_session(session_id, uuid.uuid4())


async def test_find_nth_by_cache_key_in_session_walks_position_order(
    setup: Setup,
) -> None:
    """Resolve each occurrence in position order, missing past the last match."""
    repository, session_id, make_session_id = setup
    other_session_id = await make_session_id()
    cache_key = "d" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                position,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"ticket": ticket},
            )
            for position, ticket in enumerate(["a", "b", "c"])
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
                outputs={"ticket": "elsewhere"},
            )
        ],
    )

    found = [
        await repository.find_nth_by_cache_key_in_session(
            session_id, cache_key, occurrence
        )
        for occurrence in range(3)
    ]
    assert [
        node.outputs.value for node in found if node is not None and node.outputs
    ] == [
        {"ticket": "a"},
        {"ticket": "b"},
        {"ticket": "c"},
    ]
    assert (
        await repository.find_nth_by_cache_key_in_session(session_id, cache_key, 3)
        is None
    )


async def test_find_nth_by_cache_key_in_session_matches_a_failed_node(
    setup: Setup,
) -> None:
    """Count a failed tool call as a finished candidate."""
    repository, session_id, _ = setup
    cache_key = "e" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.FAILED,
                error="boom",
            )
        ],
    )
    found = await repository.find_nth_by_cache_key_in_session(session_id, cache_key, 0)
    assert found is not None
    assert found.status == NodeStatus.FAILED
    assert found.error == "boom"


async def test_find_nth_by_cache_key_in_session_skips_an_in_progress_node(
    setup: Setup,
) -> None:
    """Exclude an in-progress tool call from the occurrence count."""
    repository, session_id, _ = setup
    cache_key = "f" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.IN_PROGRESS,
            ),
            _node(
                1,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"ticket": "b"},
            ),
        ],
    )
    found = await repository.find_nth_by_cache_key_in_session(session_id, cache_key, 0)
    assert found is not None
    assert found.outputs is not None
    assert found.outputs.value == {"ticket": "b"}


async def test_find_nth_by_cache_key_in_session_counts_only_finished_nodes(
    setup: Setup,
) -> None:
    """Count failed and completed calls while excluding in-progress calls."""
    repository, session_id, _ = setup
    cache_key = "h" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.FAILED,
                error="boom",
            ),
            _node(
                1,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.IN_PROGRESS,
            ),
            _node(
                2,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"ticket": "a"},
            ),
            _node(
                3,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                outputs={"ticket": "b"},
            ),
        ],
    )

    found = [
        await repository.find_nth_by_cache_key_in_session(
            session_id, cache_key, occurrence
        )
        for occurrence in range(4)
    ]

    assert [node.status if node is not None else None for node in found] == [
        NodeStatus.FAILED,
        NodeStatus.COMPLETED,
        NodeStatus.COMPLETED,
        None,
    ]
    assert [
        node.outputs.value if node is not None and node.outputs is not None else None
        for node in found
    ] == [
        None,
        {"ticket": "a"},
        {"ticket": "b"},
        None,
    ]


async def test_find_latest_by_cache_key_in_session_skips_a_failed_node(
    setup: Setup,
) -> None:
    """Only completed tool calls are candidates for the newest match."""
    repository, session_id, _ = setup
    cache_key = "g" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.FAILED,
            )
        ],
    )
    found = await repository.find_latest_by_cache_key_in_session(session_id, cache_key)
    assert found is None


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
    assert found.outputs is not None
    assert found.outputs.value == {"temperature": 18}


async def test_find_latest_by_cache_key_in_agent_skips_a_failed_node(
    scoped_setup: ScopedSetup,
) -> None:
    """Only completed tool calls are candidates for the newest match."""
    repository, _, _, _, make_agent_id, make_session_id = scoped_setup
    agent_id = await make_agent_id()
    session_id = await make_session_id(agent_id)
    cache_key = "q" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.FAILED,
            )
        ],
    )

    found = await repository.find_latest_by_cache_key_in_agent(agent_id, cache_key)

    assert found is None


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
    assert found.outputs is not None
    assert found.outputs.value == {"temperature": 18}


async def test_find_latest_by_cache_key_in_cohort_version_skips_a_failed_node(
    scoped_setup: ScopedSetup,
) -> None:
    """Only completed tool calls are candidates for the newest match."""
    repository, cohorts, cohort_versions, owner_id, make_agent_id, make_session_id = (
        scoped_setup
    )
    agent_id = await make_agent_id()
    session_id = await make_session_id(agent_id)
    cache_key = "r" * 64
    await repository.upsert_batch(
        session_id,
        [
            _node(
                0,
                session_id=session_id,
                node_type=NodeType.TOOL_CALL,
                cache_key=cache_key,
                status=NodeStatus.FAILED,
            )
        ],
    )
    cohort = await cohorts.create(
        Cohort(owner_id=owner_id, name="failed-cohort", agent_id=agent_id)
    )
    cohort_version = await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=1),
        [session_id],
    )

    found = await repository.find_latest_by_cache_key_in_cohort_version(
        cohort_version.id, cache_key
    )

    assert found is None
