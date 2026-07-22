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
"""Database service and migration bootstrap."""

import logging
from collections.abc import AsyncGenerator

import sqlalchemy
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.sql import text

from kitaru.server.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Maintenance database used for CREATE and DROP DATABASE statements.
_DEFAULT_DATABASE = "postgres"


class DatabaseService:
    """Manage the Kitaru database engine and schema lifecycle."""

    def __init__(self, settings: Settings | None = None) -> None:
        """Create a database service and initialize its async engine.

        Args:
            settings: Optional settings override for database connections.
        """
        self.settings = settings or get_settings()
        logger.info("Initializing async database engine.")
        self.engine = create_async_engine(
            self.generate_database_uri(self.settings), echo=False
        )

    async def cleanup(self) -> None:
        """Release database engine resources."""
        await self.engine.dispose()

    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Provide one async database session for the current scope.

        Yields:
            Request- or task-scoped session bound to the service engine.
        """
        session_factory = async_sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as session:
            yield session

    async def create_db_and_tables(self) -> None:
        """Prepare the application database and schema for process startup."""
        from kitaru.server.database.migrations.alembic import Alembic

        await self.create_db(self.settings)
        await Alembic(self.engine).migrate_database()

    @classmethod
    async def create_db(
        cls, settings: Settings | None = None, force_drop: bool = False
    ) -> None:
        """Ensure the PostgreSQL database exists.

        Args:
            settings: Optional settings override for host and database name.
            force_drop: Drop and recreate the database when true.
        """
        resolved = settings or get_settings()
        database_name = cls.application_database_name(resolved)
        engine = create_async_engine(
            cls.generate_database_uri(resolved, use_default_db=True),
            echo=False,
        )
        try:
            async with engine.execution_options(
                isolation_level="AUTOCOMMIT"
            ).begin() as conn:
                if force_drop:
                    logger.info(
                        "Force dropping database %s if it exists.",
                        database_name,
                    )
                    await conn.execute(
                        text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
                    )
                try:
                    await conn.execute(text(f'CREATE DATABASE "{database_name}"'))
                except ProgrammingError:
                    logger.info(
                        "Database %s already exists, skipping creation.",
                        database_name,
                    )
        finally:
            await engine.dispose()

    @staticmethod
    def application_database_name(settings: Settings) -> str:
        """Return the PostgreSQL database name used for application data.

        Args:
            settings: Service settings supplying connection parameters.

        Returns:
            Database name for DDL and application connections.
        """
        if settings.DATABASE_URL:
            database = sqlalchemy.engine.url.make_url(settings.DATABASE_URL).database
            if database:
                return database
        return settings.DB_NAME

    @classmethod
    def generate_database_uri(
        cls, settings: Settings, use_default_db: bool = False
    ) -> sqlalchemy.engine.url.URL:
        """Build the PostgreSQL connection URL used by the service.

        Args:
            settings: Service settings supplying connection parameters.
            use_default_db: Connect to the maintenance database instead of the
                application database.

        Raises:
            RuntimeError: Neither host nor full database URL is configured.

        Returns:
            SQLAlchemy URL using the ``postgresql+asyncpg`` driver.
        """
        driver = "postgresql+asyncpg"
        db_name = (
            _DEFAULT_DATABASE
            if use_default_db
            else cls.application_database_name(settings)
        )

        if settings.DATABASE_URL:
            url = sqlalchemy.engine.url.make_url(settings.DATABASE_URL)
            if use_default_db:
                url = url.set(database=_DEFAULT_DATABASE)
            return url

        if settings.DB_HOST:
            return sqlalchemy.engine.url.URL.create(
                drivername=driver,
                username=settings.DB_USER,
                password=settings.DB_PWD,
                database=db_name,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
            )

        raise RuntimeError(
            "Either KITARU_SERVER_DB_HOST or KITARU_SERVER_DATABASE_URL must be set."
        )
