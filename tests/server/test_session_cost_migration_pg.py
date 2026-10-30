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
"""Repair incomplete historical session costs against PostgreSQL."""

from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import (
    add_missing_orm_columns,
    db_settings,
    drop_test_database,
    postgres_available,
)
from kitaru.api_models.v1.session import SessionOrigin, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.session_node_repository import (
    SQLSessionNodeRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.session import Session
from kitaru.server.domain.session_node import SessionNode

COST_COMPLETENESS_REVISION = "022_session_cost_completeness"
PREVIOUS_REVISION = "021_node_external_id"


async def test_upgrade_repairs_costs_and_preserves_other_session_data() -> None:
    """Null incomplete totals without changing known costs or other session fields."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        await alembic.upgrade(PREVIOUS_REVISION)
        # The current repositories write and read every ORM column, including
        # those later revisions add.
        await add_missing_orm_columns(engine, "session", "session_node")
        factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        cases: list[
            tuple[
                str,
                Decimal | None,
                list[tuple[NodeType, Decimal | None]],
                Decimal | None,
            ]
        ] = [
            ("all-unknown", Decimal(0), [(NodeType.LLM_CALL, None)], None),
            (
                "mixed",
                Decimal("1.25"),
                [(NodeType.LLM_CALL, Decimal("1.25")), (NodeType.LLM_CALL, None)],
                None,
            ),
            ("true-zero", Decimal(0), [(NodeType.LLM_CALL, Decimal(0))], Decimal(0)),
            (
                "all-priced",
                Decimal("2.00"),
                [
                    (NodeType.LLM_CALL, Decimal("1.25")),
                    (NodeType.LLM_CALL, Decimal("0.75")),
                ],
                Decimal("2.00"),
            ),
            (
                "unpriced-non-llm",
                Decimal("1.25"),
                [
                    (NodeType.LLM_CALL, Decimal("1.25")),
                    (NodeType.TOOL_CALL, None),
                    (NodeType.SPAN, None),
                ],
                Decimal("1.25"),
            ),
            ("no-nodes", Decimal(0), [], Decimal(0)),
            ("already-unknown", None, [(NodeType.LLM_CALL, None)], None),
        ]
        originals: list[tuple[Session, Decimal | None]] = []
        async with factory() as db_session:
            owner = await SQLAccountRepository(db_session).create(Account(name="owner"))
            agent = await SQLAgentRepository(db_session).create(
                Agent(owner_id=owner.id, name="assistant")
            )
            sessions = SQLSessionRepository(db_session, engine)
            nodes = SQLSessionNodeRepository(db_session)
            for number, (name, old_cost, node_costs, expected_cost) in enumerate(
                cases, start=1
            ):
                session = await sessions.create(
                    Session(
                        owner_id=owner.id,
                        agent_id=agent.id,
                        number=number,
                        name=name,
                        origin=SessionOrigin.IMPORTED,
                        cost=old_cost,
                        tokens=TokenUsage(
                            input_tokens=30,
                            output_tokens=15,
                            cached_input_tokens=5,
                            reasoning_tokens=3,
                        ),
                        llm_call_count=sum(
                            node_type == NodeType.LLM_CALL
                            for node_type, _ in node_costs
                        ),
                        tool_call_count=sum(
                            node_type == NodeType.TOOL_CALL
                            for node_type, _ in node_costs
                        ),
                    )
                )
                await nodes.upsert_batch(
                    session.id,
                    [
                        SessionNode(
                            session_id=session.id,
                            external_id=f"node-{index}",
                            node_type=node_type,
                            name="call",
                            status=NodeStatus.COMPLETED,
                            cost=node_cost,
                        )
                        for index, (node_type, node_cost) in enumerate(node_costs)
                    ],
                )
                originals.append((session, expected_cost))
            await db_session.commit()

        for action, revision in [
            (alembic.upgrade, COST_COMPLETENESS_REVISION),
            (alembic.downgrade, PREVIOUS_REVISION),
            (alembic.upgrade, COST_COMPLETENESS_REVISION),
        ]:
            await action(revision)
            assert await alembic.current_revisions() == [revision]
            async with factory() as db_session:
                sessions = SQLSessionRepository(db_session, engine)
                for original, expected_cost in originals:
                    actual = await sessions.get(original.id, include_payloads=True)
                    assert actual.cost == expected_cost, original.name
                    assert actual.model_dump(exclude={"cost"}) == original.model_dump(
                        exclude={"cost"}
                    ), original.name
    finally:
        await engine.dispose()
        await drop_test_database(settings)
