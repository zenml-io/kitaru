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
"""Trace import schema migration tests."""

import pytest
from sqlalchemy import text

from conftest import db_settings, postgres_available
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService


async def test_upgrade_existing_004_database() -> None:
    """Upgrade the prior release schema through the trace import migration."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings, force_drop=True)
    database = DatabaseService(settings)
    alembic = Alembic(database.engine)
    try:
        await alembic.upgrade("004_add_record_replay")
        assert await alembic.current_revisions() == ["004_add_record_replay"]

        await alembic.upgrade()

        assert await alembic.current_revisions() == ["005_add_trace_import_jobs"]
        async with database.engine.connect() as connection:
            import_job_exists = await connection.scalar(
                text("SELECT to_regclass('public.import_job')")
            )
            columns = set(
                (
                    await connection.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_name = 'session'"
                        )
                    )
                ).scalars()
            )
            constraints = set(
                (
                    await connection.execute(
                        text(
                            "SELECT conname FROM pg_constraint "
                            "WHERE conrelid = 'session'::regclass"
                        )
                    )
                ).scalars()
            )
        assert import_job_exists == "import_job"
        assert {
            "source_instance",
            "source_revision",
            "source_digest",
            "source_metadata",
            "replay_readiness",
            "normalization_warnings",
            "import_job_id",
            "supersedes_session_id",
        }.issubset(columns)
        assert "uq_session_import_revision" in constraints
        assert "uq_session_import_digest" in constraints
    finally:
        await database.cleanup()
