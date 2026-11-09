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
"""Repair incomplete historical session token totals against PostgreSQL."""

from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, drop_test_database, postgres_available
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

TOKEN_COMPLETENESS_REVISION = "024_token_completeness"
PREVIOUS_REVISION = "023_insight_task_id_index"


async def test_upgrade_repairs_tokens_and_preserves_other_session_data() -> None:
    """Repair each token field and retain corrected values across downgrade."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        await alembic.upgrade(PREVIOUS_REVISION)
        factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        measured = TokenUsage(
            input_tokens=30,
            output_tokens=15,
            cached_input_tokens=5,
            reasoning_tokens=3,
        )
        zero = TokenUsage(
            input_tokens=0,
            output_tokens=0,
            cached_input_tokens=0,
            reasoning_tokens=0,
        )
        cases: list[
            tuple[
                str,
                TokenUsage | None,
                list[tuple[NodeType, TokenUsage | None]],
                TokenUsage | None,
            ]
        ] = [
            ("all-unknown", zero, [(NodeType.LLM_CALL, None)], None),
            (
                "mixed",
                measured,
                [(NodeType.LLM_CALL, measured), (NodeType.LLM_CALL, None)],
                None,
            ),
            ("known-zero", measured, [(NodeType.LLM_CALL, zero)], zero),
            (
                "all-known",
                zero,
                [(NodeType.LLM_CALL, measured), (NodeType.LLM_CALL, measured)],
                TokenUsage(
                    input_tokens=60,
                    output_tokens=30,
                    cached_input_tokens=10,
                    reasoning_tokens=6,
                ),
            ),
            (
                "unmeasured-non-llm",
                zero,
                [
                    (NodeType.LLM_CALL, measured),
                    (NodeType.TOOL_CALL, None),
                    (NodeType.SPAN, None),
                ],
                measured,
            ),
            (
                "measured-non-llm",
                zero,
                [
                    (NodeType.TOOL_CALL, TokenUsage(input_tokens=7)),
                    (NodeType.SPAN, TokenUsage(input_tokens=2, output_tokens=4)),
                    (NodeType.TOOL_CALL, None),
                ],
                TokenUsage(input_tokens=9, output_tokens=4),
            ),
            (
                "llm-and-non-llm-measurements",
                zero,
                [
                    (NodeType.LLM_CALL, measured),
                    (NodeType.SPAN, TokenUsage(input_tokens=7)),
                ],
                TokenUsage(
                    input_tokens=37,
                    output_tokens=15,
                    cached_input_tokens=5,
                    reasoning_tokens=3,
                ),
            ),
            ("no-measurements", measured, [(NodeType.TOOL_CALL, None)], None),
            ("no-nodes-legacy-totals", measured, [], None),
            ("no-nodes-legacy-zero", zero, [], None),
            ("already-unknown", None, [(NodeType.LLM_CALL, None)], None),
            ("missing-legacy-totals", None, [(NodeType.LLM_CALL, measured)], measured),
        ]
        for field in TokenUsage.model_fields:
            cases.append(
                (
                    f"unknown-{field}",
                    zero,
                    [
                        (NodeType.LLM_CALL, measured),
                        (NodeType.LLM_CALL, measured.model_copy(update={field: None})),
                        (NodeType.SPAN, measured),
                    ],
                    TokenUsage(
                        input_tokens=90,
                        output_tokens=45,
                        cached_input_tokens=15,
                        reasoning_tokens=9,
                    ).model_copy(update={field: None}),
                )
            )
        originals: list[tuple[Session, TokenUsage | None]] = []
        async with factory() as db_session:
            owner = await SQLAccountRepository(db_session).create(Account(name="owner"))
            agent = await SQLAgentRepository(db_session).create(
                Agent(owner_id=owner.id, name="assistant")
            )
            sessions = SQLSessionRepository(db_session, engine)
            nodes = SQLSessionNodeRepository(db_session)
            for number, (name, old_tokens, node_tokens, expected) in enumerate(
                cases, start=1
            ):
                session = await sessions.create(
                    Session(
                        owner_id=owner.id,
                        agent_id=agent.id,
                        number=number,
                        name=name,
                        origin=SessionOrigin.IMPORTED,
                        cost=Decimal("1.25"),
                        tokens=old_tokens,
                        llm_call_count=sum(
                            node_type == NodeType.LLM_CALL
                            for node_type, _ in node_tokens
                        ),
                        tool_call_count=sum(
                            node_type == NodeType.TOOL_CALL
                            for node_type, _ in node_tokens
                        ),
                        metadata={"historical": name},
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
                            tokens=tokens,
                        )
                        for index, (node_type, tokens) in enumerate(node_tokens)
                    ],
                )
                originals.append((session, expected))
            await db_session.commit()

        for action, revision in [
            (alembic.upgrade, TOKEN_COMPLETENESS_REVISION),
            (alembic.downgrade, PREVIOUS_REVISION),
            (alembic.upgrade, TOKEN_COMPLETENESS_REVISION),
        ]:
            await action(revision)
            assert await alembic.current_revisions() == [revision]
            async with factory() as db_session:
                sessions = SQLSessionRepository(db_session, engine)
                for original, expected in originals:
                    actual = await sessions.get(original.id, include_payloads=True)
                    assert actual.tokens == expected, original.name
                    assert actual.model_dump(exclude={"tokens"}) == original.model_dump(
                        exclude={"tokens"}
                    ), original.name
    finally:
        await engine.dispose()
        await drop_test_database(settings)
