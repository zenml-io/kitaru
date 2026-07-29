"""PostgreSQL coverage for bounded v2 lookup queries."""

import pytest

from conftest import pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionNodeRepository,
    SQLSessionRepository,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import Session, SessionOrigin
from kitaru.server.domain.session_node import NodeStatus, NodeType, SessionNode


async def test_blob_metadata_read_omits_content() -> None:
    """Load content bytes only through the content-specific repository method."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        account = await SQLAccountRepository(session).create(Account(name="blob-owner"))
        repository = SQLBlobRepository(session)
        stored, created = await repository.create(
            Blob(
                owner_id=account.id,
                sha256="a" * 64,
                size=7,
                media_type="text/plain",
                data=b"payload",
            )
        )

        metadata = await repository.get(stored.id)
        content = await repository.get_content(stored.id)

        assert created is True
        assert metadata.data is None
        assert content.data == b"payload"


async def test_agent_history_filters_in_database() -> None:
    """Find tool history for one agent without materializing session ids."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        account = await SQLAccountRepository(session).create(
            Account(name="history-owner")
        )
        agents = SQLAgentRepository(session)
        matching_agent = await agents.create(
            Agent(owner_id=account.id, name="matching-agent")
        )
        other_agent = await agents.create(
            Agent(owner_id=account.id, name="other-agent")
        )
        sessions = SQLSessionRepository(session)
        matching_session = await sessions.create(
            Session(
                owner_id=account.id,
                agent_id=matching_agent.id,
                origin=SessionOrigin.RECORDED,
            )
        )
        other_session = await sessions.create(
            Session(
                owner_id=account.id,
                agent_id=other_agent.id,
                origin=SessionOrigin.RECORDED,
            )
        )
        cache_key = "b" * 64
        nodes = SQLSessionNodeRepository(session)
        await nodes.upsert_many(
            [
                SessionNode(
                    session_id=matching_session.id,
                    index=0,
                    node_type=NodeType.TOOL_CALL,
                    name="weather",
                    status=NodeStatus.COMPLETED,
                    cache_key=cache_key,
                    outputs={"temperature": 18},
                )
            ]
        )
        await nodes.upsert_many(
            [
                SessionNode(
                    session_id=other_session.id,
                    index=0,
                    node_type=NodeType.TOOL_CALL,
                    name="weather",
                    status=NodeStatus.COMPLETED,
                    cache_key=cache_key,
                    outputs={"temperature": 99},
                )
            ]
        )
        cohort = await SQLCohortRepository(session).create(
            Cohort(
                owner_id=account.id,
                name="matching-cohort",
                agent_id=matching_agent.id,
            ),
            [matching_session.id],
        )

        found_for_agent = await nodes.find_tool_result(
            cache_key, agent_id=matching_agent.id
        )
        found_for_cohort = await nodes.find_tool_result(cache_key, cohort_id=cohort.id)

        assert found_for_agent is not None
        assert found_for_agent.outputs == {"temperature": 18}
        assert found_for_cohort is not None
        assert found_for_cohort.outputs == {"temperature": 18}
