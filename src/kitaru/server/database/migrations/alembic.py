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
"""Alembic wrapper configured for the Kitaru PostgreSQL database."""

import logging
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import sqlalchemy
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext, NameFilterType
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.schema import MetaData, SchemaItem
from sqlmodel import SQLModel

# Register all ORM tables on SQLModel.metadata for autogenerate.
import kitaru.server.adapters.db.schemas  # noqa: F401

logger = logging.getLogger(__name__)

exclude_tables: list[str] = []

_RevIdType = str | Sequence[str]


def include_object(
    schema: SchemaItem,
    name: str | None,
    type_: NameFilterType,
    *args: Any,
    **kwargs: Any,
) -> bool:
    """Decide whether schema autogenerate should consider a database object.

    Tables listed in ``exclude_tables`` are omitted from autogenerate output.

    Args:
        schema: Candidate schema object.
        name: Object name when applicable.
        type_: Object kind label (for example ``table``).
        *args: Additional positional arguments from Alembic.
        **kwargs: Additional keyword arguments from Alembic.

    Returns:
        ``False`` when the object is excluded, otherwise ``True``.
    """
    return not (type_ == "table" and name in exclude_tables)


class Alembic:
    """Run Alembic migrations against the service async database engine.

    Callers use this wrapper for startup migration, CLI upgrades, and revision
    inspection. The default metadata is SQLModel's registered table set.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        metadata: MetaData = SQLModel.metadata,
        context: EnvironmentContext | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Alembic wrapper.

        Args:
            engine: The SQLAlchemy engine to use.
            metadata: The SQLAlchemy metadata to use.
            context: The Alembic environment context to use. If not set, a new
                context is created pointing to the Kitaru migrations
                directory.
            **kwargs: Additional keyword arguments to pass to the Alembic
                environment context.
        """
        self.engine = engine
        self.metadata = metadata
        self.context_kwargs = kwargs

        migrations_dir = Path(__file__).parent
        self.config = Config()
        self.config.set_main_option("path_separator", "os")
        self.config.set_main_option("script_location", str(migrations_dir))
        self.config.set_main_option(
            "version_locations", str(migrations_dir / "versions")
        )

        self.script_directory = ScriptDirectory.from_config(self.config)
        if context is None:
            self.environment_context = EnvironmentContext(
                self.config, self.script_directory
            )
        else:
            self.environment_context = context

    async def db_is_empty(self) -> bool:
        """Report whether the database has never been migrated by Alembic.

        Returns:
            ``True`` when no tables besides ``alembic_version`` exist.
        """
        # Reading the current revisions already creates an empty
        # alembic_version table, so it does not count as application schema.
        async with self.engine.connect() as connection:
            table_names = await connection.run_sync(
                lambda conn: sqlalchemy.inspect(conn).get_table_names()
            )
        return set(table_names) <= {"alembic_version"}

    async def run_migrations(
        self,
        fn: Callable[[_RevIdType, MigrationContext], list[Any]] | None,
    ) -> None:
        """Execute migrations online using the bound engine.

        Args:
            fn: Optional Alembic callback for stamp, upgrade, or revision queries.
        """
        fn_context_args: dict[Any, Any] = {}
        if fn is not None:
            fn_context_args["fn"] = fn

        def do_run_migrations(conn: Connection) -> None:
            self.environment_context.configure(
                connection=conn,
                target_metadata=self.metadata,
                include_object=include_object,
                compare_type=True,
                render_as_batch=True,
                **fn_context_args,
                **self.context_kwargs,
            )
            with self.environment_context.begin_transaction():
                self.environment_context.run_migrations()

        async with self.engine.begin() as connection:
            await connection.run_sync(do_run_migrations)

    async def current_revisions(self) -> list[str]:
        """Return revision ids currently recorded in the database.

        Returns:
            Revision strings stamped on the database (empty when never migrated).
        """
        current_revisions: list[str] = []

        def do_get_current_rev(rev: _RevIdType, context: Any) -> list[Any]:
            nonlocal current_revisions
            for revision in self.script_directory.get_all_current(rev):  # ty: ignore[invalid-argument-type]
                if revision is None:
                    continue
                current_revisions.append(revision.revision)
            return []

        await self.run_migrations(do_get_current_rev)
        return current_revisions

    async def stamp(self, revision: str) -> None:
        """Record a revision on the database without running upgrade scripts.

        Args:
            revision: Target revision id (for example ``head``).
        """

        def do_stamp(rev: _RevIdType, context: Any) -> list[Any]:
            return self.script_directory._stamp_revs(
                revision,
                rev,  # ty: ignore[invalid-argument-type]
            )

        await self.run_migrations(do_stamp)

    async def upgrade(self, revision: str = "heads") -> None:
        """Apply pending upgrade scripts up to the target revision.

        Args:
            revision: Target revision id or ``heads`` for the latest scripts.
        """

        def do_upgrade(rev: _RevIdType, context: Any) -> list[Any]:
            return self.script_directory._upgrade_revs(
                revision,
                rev,  # ty: ignore[invalid-argument-type]
            )

        await self.run_migrations(do_upgrade)

    async def downgrade(self, revision: str) -> None:
        """Revert the database to a previous version.

        Args:
            revision: String revision target.
        """

        def do_downgrade(rev: _RevIdType, context: Any) -> list[Any]:
            return self.script_directory._downgrade_revs(
                revision,
                rev,  # ty: ignore[invalid-argument-type]
            )

        await self.run_migrations(do_downgrade)

    async def migrate_database(self) -> None:
        """Bring the database schema to the latest revision.

        Raises:
            RuntimeError: More than one revision is stamped (ambiguous state).
        """
        #
        # We need to account for 2 distinct cases here:
        #
        # 1. the database is completely empty (not initialized). We don't need
        # to involve alembic here, we can just create the tables using SQLModel
        # and then only stamp the database with the latest revision.
        #
        # 2. the database is not empty and has been migrated with alembic
        # before. This is the most common case. We just need to upgrade to the
        # latest revision using alembic.
        #
        revisions = await self.current_revisions()
        if len(revisions) >= 1:
            if len(revisions) > 1:
                raise RuntimeError(
                    "The database has multiple migration revisions present."
                )
            # Case 2: the database has been migrated with alembic before. Just
            # upgrade to the latest revision.
            logger.info(
                f"Migrating database from revision {revisions[0]} to head revision"
            )
            await self.upgrade()
        elif await self.db_is_empty():
            # Case 1: the database is empty. We can just create the
            # tables from scratch with SQLModel and then stamp the database
            # with the latest revision.
            logger.info("Creating database tables")
            async with self.engine.begin() as conn:
                await conn.run_sync(SQLModel.metadata.create_all)
            await self.stamp("head")
