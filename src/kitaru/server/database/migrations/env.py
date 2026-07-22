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
"""Alembic CLI entrypoint (async)."""

import asyncio
from typing import cast

from alembic import context
from alembic.runtime.environment import EnvironmentContext

from kitaru.server.database import DatabaseService


async def run_async_migrations() -> None:
    """Apply Alembic migrations using service database settings.

    Ensures the application database exists, then runs the migration environment
    to the revision requested by the invoking Alembic CLI command.
    """
    from kitaru.server.database.migrations.alembic import Alembic

    database = DatabaseService()
    try:
        await DatabaseService.create_db(database.settings)
        alembic = Alembic(
            engine=database.engine,
            context=cast(EnvironmentContext, context),
        )
        await alembic.run_migrations(fn=None)
    finally:
        await database.cleanup()


def run_migrations() -> None:
    """Entry point for the Alembic CLI subprocess.

    Runs ``run_async_migrations`` on the event loop.
    """
    asyncio.run(run_async_migrations())


run_migrations()
