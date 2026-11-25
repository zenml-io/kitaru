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
"""PostgreSQL backfill and downgrade tests for migration 028."""

import json
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.api_models.v1.insight import (
    BinnedArtifact,
    CategoricalArtifact,
    EvidenceArtifact,
    StatGridArtifact,
    TextArtifact,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent

STRUCTURED_REPORTS_REVISION = "028_structured_insight_reports"
PREVIOUS_REVISION = "027_session_group"


async def test_upgrade_groups_invocation_siblings_and_downgrade_restores_rows() -> None:
    """Group invocation siblings into one structured report and cleanly downgrade."""
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
        invocation_id = uuid.uuid4()
        session_a = uuid.uuid4()
        node_a = uuid.uuid4()
        grouped_first_id = uuid.uuid4()
        grouped_second_id = uuid.uuid4()
        standalone_binned_id = uuid.uuid4()
        standalone_single_cat_id = uuid.uuid4()

        async with factory() as session:
            owner = await SQLAccountRepository(session).create(Account(name="owner"))
            agent = await SQLAgentRepository(session).create(
                Agent(owner_id=owner.id, name="assistant")
            )
            insert_sql = text(
                "INSERT INTO insight (id, owner_id, agent_id, invocation_id, name, "
                "title, description, type, data, metadata, created, updated) VALUES "
                "(:id, :owner_id, :agent_id, :invocation_id, :name, :title, "
                ":description, :type, CAST(:data AS jsonb), CAST(:metadata AS jsonb), "
                "now(), now())"
            )
            # Sibling with position=1 inserted before position=0 to verify sorting.
            await session.execute(
                insert_sql,
                {
                    "id": grouped_second_id,
                    "owner_id": owner.id,
                    "agent_id": agent.id,
                    "invocation_id": invocation_id,
                    "name": "tool-errors",
                    "title": "Tool errors concentrate in search",
                    "description": "Search timed out in 9 sessions.",
                    "type": "categorical",
                    "data": json.dumps(
                        {
                            "type": "categorical",
                            "unit": "sessions",
                            "values": [
                                {"label": f"tool-{i}", "value": 10 - i}
                                for i in range(10)
                            ],
                        }
                    ),
                    "metadata": json.dumps(
                        {
                            "kitaru.insights/v1": {
                                "position": 1,
                                "contributing_session_ids": [str(session_a)],
                            }
                        }
                    ),
                },
            )
            await session.execute(
                insert_sql,
                {
                    "id": grouped_first_id,
                    "owner_id": owner.id,
                    "agent_id": agent.id,
                    "invocation_id": invocation_id,
                    "name": "policy-skip",
                    "title": "Refunds skip the policy check",
                    "description": "31 sessions skipped check_refund_policy.",
                    "type": "text",
                    "data": json.dumps(
                        {"type": "text", "content": "Detailed narrative."}
                    ),
                    "metadata": json.dumps(
                        {
                            "kitaru.insights/v1": {
                                "position": 0,
                                "check_first": "Confirm order edits happened first.",
                                "cta_label": "Investigate refunds",
                                "investigation_prompt": "Inspect refund sessions.",
                                "contributing_session_ids": [str(session_a)],
                                "evidence": [
                                    {
                                        "session_id": str(session_a),
                                        "node_id": str(node_a),
                                        "signal": "issue_refund",
                                    }
                                ],
                                "coverage": {
                                    "sessions_analyzed": 214,
                                    "sessions_available": 250,
                                    "caveats": ["stopped at 250"],
                                },
                                "context": {"source_import": {"provider": "Langfuse"}},
                            }
                        }
                    ),
                },
            )
            # Standalone binned insight (no invocation_id).
            await session.execute(
                insert_sql,
                {
                    "id": standalone_binned_id,
                    "owner_id": owner.id,
                    "agent_id": agent.id,
                    "invocation_id": None,
                    "name": "latency",
                    "title": "Latency spread",
                    "description": None,
                    "type": "binned",
                    "data": json.dumps(
                        {
                            "type": "binned",
                            "unit": "seconds",
                            "bins": [
                                {"lower_bound": None, "upper_bound": 5, "count": 10},
                                {"lower_bound": 5, "upper_bound": None, "count": 3},
                            ],
                        }
                    ),
                    "metadata": json.dumps({}),
                },
            )
            # Standalone 1-item categorical insight -> promoted to StatGridArtifact.
            await session.execute(
                insert_sql,
                {
                    "id": standalone_single_cat_id,
                    "owner_id": owner.id,
                    "agent_id": agent.id,
                    "invocation_id": None,
                    "name": "only-one",
                    "title": "Single category",
                    "description": "Only one value observed.",
                    "type": "categorical",
                    "data": json.dumps(
                        {
                            "type": "categorical",
                            "unit": "sessions",
                            "values": [{"label": "failed", "value": 4}],
                        }
                    ),
                    "metadata": json.dumps({}),
                },
            )
            await session.commit()

        await alembic.upgrade(STRUCTURED_REPORTS_REVISION)
        assert await alembic.current_revisions() == [STRUCTURED_REPORTS_REVISION]

        async with factory() as session:
            repo = SQLInsightRepository(session)
            # Grouped pair collapsed into the position=0 row with 2 ordered findings.
            report = await repo.get(grouped_first_id)
            assert report.status == "new"
            assert report.source_provider == "Langfuse"
            assert report.summary == "31 sessions skipped check_refund_policy."
            assert report.coverage is not None
            assert report.coverage.sessions_analyzed == 214
            assert report.coverage.sessions_available == 250
            assert report.coverage.caveats == ["stopped at 250"]
            assert report.highlight is not None
            assert report.highlight.finding_id == "policy-skip"
            assert [finding.id for finding in report.findings] == [
                "policy-skip",
                "tool-errors",
            ]
            first_finding, second_finding = report.findings
            assert isinstance(first_finding.artifacts[0], TextArtifact)
            assert isinstance(first_finding.artifacts[1], EvidenceArtifact)
            assert first_finding.check_first == "Confirm order edits happened first."
            assert first_finding.action is not None
            assert first_finding.action.label == "Investigate refunds"
            # 10 legacy categories folded to 8 (top 7 + "Other") to satisfy Zuri's cap.
            assert isinstance(second_finding.artifacts[0], CategoricalArtifact)
            assert len(second_finding.artifacts[0].values) == 8
            assert second_finding.artifacts[0].values[-1].label == "Other"

            # Standalone binned row preserved as BinnedArtifact.
            binned = await repo.get(standalone_binned_id)
            assert len(binned.findings) == 1
            assert isinstance(binned.findings[0].artifacts[0], BinnedArtifact)

            # Single-item legacy categorical promoted to StatGridArtifact.
            single_cat = await repo.get(standalone_single_cat_id)
            assert isinstance(single_cat.findings[0].artifacts[0], StatGridArtifact)

            count = await session.scalar(text("SELECT count(*) FROM insight"))
            assert count == 3

        await alembic.downgrade(PREVIOUS_REVISION)
        assert await alembic.current_revisions() == [PREVIOUS_REVISION]
        async with engine.connect() as connection:
            downgraded = {
                row.id: (row.type, row.description)
                for row in (
                    await connection.execute(
                        text("SELECT id, type, description FROM insight")
                    )
                ).all()
            }
        assert set(downgraded) == {
            grouped_first_id,
            standalone_binned_id,
            standalone_single_cat_id,
        }
        assert downgraded[grouped_first_id] == (
            "text",
            "31 sessions skipped check_refund_policy.",
        )
        assert downgraded[standalone_binned_id] == ("binned", None)
        assert downgraded[standalone_single_cat_id] == (
            "text",
            "Only one value observed.",
        )
    finally:
        await engine.dispose()
        await drop_test_database(settings)
