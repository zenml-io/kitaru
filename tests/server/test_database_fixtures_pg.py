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
"""Repository database fixtures isolate committed rows and schema changes."""

import pytest
from sqlalchemy import text

from conftest import pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.domain.account import Account


async def test_repository_databases_isolate_committed_rows_and_schema() -> None:
    """Mutating a copied database must not affect concurrent or later copies."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as first:
        await SQLAccountRepository(first).create(Account(name="first"))
        await first.execute(text("CREATE TABLE isolation_probe (id integer)"))
        await first.commit()
        first_database = await first.scalar(text("SELECT current_database()"))

        async with pg_session() as second:
            assert (
                await second.scalar(text("SELECT current_database()")) != first_database
            )
            assert await second.scalar(text("SELECT count(*) FROM account")) == 0
            assert (
                await second.scalar(text("SELECT to_regclass('isolation_probe')"))
                is None
            )

    async with pg_session() as later:
        assert await later.scalar(text("SELECT count(*) FROM account")) == 0
        assert await later.scalar(text("SELECT to_regclass('isolation_probe')")) is None
