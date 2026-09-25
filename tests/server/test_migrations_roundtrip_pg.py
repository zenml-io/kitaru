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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Every reversible migration survives an upgrade, downgrade, upgrade cycle."""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.server.database.migrations.alembic import Alembic
from kitaru.server.database.service import DatabaseService

# These revisions refuse to downgrade on purpose because the older schema
# cannot hold the data they allow.
IRREVERSIBLE_REVISIONS = {"006_deletion_rules", "010_evaluation_provenance"}


async def test_each_revision_downgrades_and_upgrades_again() -> None:
    """Step up one revision at a time, reversing and reapplying each step."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        alembic = Alembic(engine)
        revisions = list(reversed(list(alembic.script_directory.walk_revisions())))
        for revision in revisions:
            await alembic.upgrade(revision.revision)
            if revision.revision in IRREVERSIBLE_REVISIONS:
                with pytest.raises(RuntimeError, match="cannot be downgraded"):
                    await alembic.downgrade(str(revision.down_revision))
                continue
            await alembic.downgrade(str(revision.down_revision or "base"))
            expected = [revision.down_revision] if revision.down_revision else []
            assert await alembic.current_revisions() == expected
            await alembic.upgrade(revision.revision)
            assert await alembic.current_revisions() == [revision.revision]

        assert revisions[-1].revision == alembic.script_directory.get_current_head()
    finally:
        await engine.dispose()
        await drop_test_database(settings)
