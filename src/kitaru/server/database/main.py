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
"""Database migration process entrypoint."""

import asyncio
import logging

from kitaru.server.config import get_settings
from kitaru.server.database.service import DatabaseService


async def run() -> None:
    """Run database creation and migrations."""
    logging.basicConfig(level=get_settings().LOG_LEVEL)
    database = DatabaseService()
    try:
        await database.create_db_and_tables()
    finally:
        await database.cleanup()


def main() -> None:
    """Run the database migration process from a console entrypoint."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
