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
import ssl
from collections.abc import AsyncGenerator, Awaitable, Callable
from importlib import import_module
from typing import Any, Protocol, cast

import asyncpg
import sqlalchemy
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.sql import text

from kitaru.server.config import (
    DatabaseAuthMethod,
    DatabaseSSLMode,
    Settings,
    get_settings,
)

logger = logging.getLogger(__name__)

# Maintenance database used for CREATE and DROP DATABASE statements.
_DEFAULT_DATABASE = "postgres"


class _RDSTokenClient(Protocol):
    """Subset of the boto3 RDS client used for database authentication."""

    def generate_db_auth_token(
        self,
        *,
        DBHostname: str,
        Port: int,
        DBUsername: str,
        Region: str,
    ) -> str:
        """Generate an RDS IAM database authentication token."""


class _Boto3Module(Protocol):
    """Subset of boto3 used to construct an RDS client."""

    def client(self, service_name: str, *, region_name: str) -> _RDSTokenClient:
        """Create a service client."""


class DatabaseTokenProvider(Protocol):
    """Generate short-lived database authentication tokens."""

    def generate_token(
        self,
        *,
        host: str,
        port: int,
        username: str,
        region: str,
    ) -> str:
        """Generate a token for one database connection."""


class AWSIAMDatabaseTokenProvider:
    """Generate RDS IAM tokens using the process AWS identity."""

    def generate_token(
        self,
        *,
        host: str,
        port: int,
        username: str,
        region: str,
    ) -> str:
        """Generate a token for one database connection."""
        boto3 = cast(_Boto3Module, import_module("boto3"))
        client = boto3.client("rds", region_name=region)
        return client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=username,
            Region=region,
        )


class DatabaseService:
    """Manage the Kitaru database engine and schema lifecycle."""

    def __init__(
        self,
        settings: Settings | None = None,
        token_provider: DatabaseTokenProvider | None = None,
    ) -> None:
        """Create a database service and initialize its async engine.

        Args:
            settings: Optional settings override for database connections.
            token_provider: Optional IAM token provider override.
        """
        self.settings = settings or get_settings()
        self.token_provider = token_provider or AWSIAMDatabaseTokenProvider()
        logger.info("Initializing async database engine.")
        self.engine = self.create_engine(self.settings)

    def create_engine(
        self,
        settings: Settings,
        use_default_db: bool = False,
    ) -> AsyncEngine:
        """Create an async engine for the configured authentication method.

        Args:
            settings: Database connection settings.
            use_default_db: Connect to the maintenance database when true.

        Returns:
            Configured SQLAlchemy async engine.
        """
        engine_options: dict[str, Any] = {}
        if settings.DB_AUTH_METHOD is DatabaseAuthMethod.AWS_IAM:
            engine_options["async_creator"] = self._iam_connection_factory(
                settings,
                use_default_db=use_default_db,
            )
        elif settings.DB_SSL_MODE is DatabaseSSLMode.VERIFY_FULL:
            engine_options["connect_args"] = {"ssl": ssl.create_default_context()}
        return create_async_engine(
            self.generate_database_uri(settings, use_default_db=use_default_db),
            echo=False,
            **engine_options,
        )

    def _iam_connection_factory(
        self,
        settings: Settings,
        *,
        use_default_db: bool = False,
    ) -> Callable[[], Awaitable[asyncpg.Connection]]:
        """Create an asyncpg factory that refreshes IAM tokens per connection."""
        assert settings.DB_HOST is not None
        assert settings.DB_AWS_REGION is not None
        host = settings.DB_HOST
        region = settings.DB_AWS_REGION
        database = (
            _DEFAULT_DATABASE
            if use_default_db
            else self.application_database_name(settings)
        )
        ssl_context = ssl.create_default_context()

        async def connect() -> asyncpg.Connection:
            token = self.token_provider.generate_token(
                host=host,
                port=settings.DB_PORT,
                username=settings.DB_USER,
                region=region,
            )
            return await asyncpg.connect(
                host=host,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=token,
                database=database,
                ssl=ssl_context,
            )

        return connect

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

        if self.settings.CREATE_DB_IF_MISSING:
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
        service = cls(resolved)
        engine = service.create_engine(resolved, use_default_db=True)
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
                except (IntegrityError, ProgrammingError):
                    # Postgres reports a losing concurrent CREATE as a unique
                    # violation on pg_database rather than a duplicate database.
                    logger.info(
                        "Database %s already exists, skipping creation.",
                        database_name,
                    )
        finally:
            await engine.dispose()
            await service.cleanup()

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
                password=(
                    settings.DB_PWD
                    if settings.DB_AUTH_METHOD is DatabaseAuthMethod.PASSWORD
                    else None
                ),
                database=db_name,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
            )

        raise RuntimeError(
            "Either KITARU_SERVER_DB_HOST or KITARU_SERVER_DATABASE_URL must be set."
        )
