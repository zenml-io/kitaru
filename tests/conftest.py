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
"""Shared test helpers and in-memory fakes."""

import asyncio
import os
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneClient,
    ControlPlaneError,
    ServerAuthorization,
)
from kitaru.server.adapters.db.orm.base import Base
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
    encode_api_key,
    generate_secret,
    hash_secret,
)
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretNotFound,
)

TEST_DB_PREFIX = "kitaru_test"


def db_settings(**overrides: Any) -> APISettings:
    """Build API settings pointing at the local test database.

    Args:
        **overrides: Additional settings values.

    Returns:
        Settings for the test database.
    """
    return APISettings(
        DB_HOST=os.environ.get("KITARU_TEST_DB_HOST", "localhost"),
        DB_PORT=int(os.environ.get("KITARU_TEST_DB_PORT", "5433")),
        # A database per caller. Tests drop their database on teardown, and a
        # shared name would let one test drop the database another is using.
        DB_NAME=f"{TEST_DB_PREFIX}_{uuid.uuid4().hex[:12]}",
        SECRET_ENCRYPTION_KEY="test-encryption-key",
        **overrides,
    )


def local_settings(use_db: bool = False, **overrides: Any) -> APISettings:
    """Build API settings for the local auth scheme.

    Args:
        use_db: Whether to point at the local test database.
        **overrides: Additional settings values.

    Returns:
        Settings for local authentication.
    """
    values: dict[str, Any] = {
        "AUTH_SCHEME": AuthScheme.LOCAL,
        "JWT_SIGNING_KEY": "test-signing-key-0123456789abcdef",
        **overrides,
    }
    if use_db:
        return db_settings(**values)
    return APISettings(
        DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key", **values
    )


def control_plane_settings(use_db: bool = False, **overrides: Any) -> APISettings:
    """Build API settings for the control plane auth scheme.

    Args:
        use_db: Whether to point at the local test database.
        **overrides: Additional settings values.

    Returns:
        Settings for control plane authentication.
    """
    values: dict[str, Any] = {
        "AUTH_SCHEME": AuthScheme.CONTROL_PLANE,
        "JWT_SIGNING_KEY": "test-signing-key-0123456789abcdef",
        "CONTROL_PLANE_API_URL": "https://control-plane.example.com",
        "SERVER_ID": uuid.uuid4(),
        **overrides,
    }
    if use_db:
        return db_settings(**values)
    return APISettings(
        DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key", **values
    )


_postgres_available: bool | None = None


async def postgres_available() -> bool:
    """Report whether the local test database accepts connections.

    Returns:
        ``True`` when a connection succeeds.
    """
    global _postgres_available
    if _postgres_available is not None:
        return _postgres_available
    engine = create_async_engine(
        DatabaseService.generate_database_uri(db_settings(), use_default_db=True)
    )
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        _postgres_available = True
    except Exception:
        _postgres_available = False
    finally:
        await engine.dispose()
    return _postgres_available


async def _drop_stale_test_databases() -> None:
    engine = create_async_engine(
        DatabaseService.generate_database_uri(db_settings(), use_default_db=True)
    )
    try:
        async with engine.execution_options(
            isolation_level="AUTOCOMMIT"
        ).begin() as connection:
            names = (
                await connection.execute(
                    text("SELECT datname FROM pg_database WHERE datname LIKE :prefix"),
                    {"prefix": f"{TEST_DB_PREFIX}%"},
                )
            ).scalars()
            for name in list(names):
                await connection.execute(
                    text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
                )
    except OSError:
        # PostgreSQL is unreachable. The tests that need it skip themselves.
        pass
    finally:
        await engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def reap_stale_test_databases() -> None:
    """Drop test databases left behind by a run that was killed."""
    asyncio.run(_drop_stale_test_databases())


async def drop_test_database(settings: APISettings) -> None:
    """Drop the database a test created.

    Args:
        settings: Settings naming the database to drop.
    """
    database_name = DatabaseService.application_database_name(settings)
    engine = create_async_engine(
        DatabaseService.generate_database_uri(settings, use_default_db=True)
    )
    try:
        async with engine.execution_options(
            isolation_level="AUTOCOMMIT"
        ).begin() as connection:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            )
    finally:
        await engine.dispose()


@asynccontextmanager
async def lifespan_client(
    settings: APISettings,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Run the app through its lifespan on a fresh test database.

    Args:
        settings: API server settings.

    Yields:
        HTTP client routed to the app.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    await DatabaseService.create_db(settings)
    try:
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                yield client
    finally:
        await drop_test_database(settings)


@asynccontextmanager
async def pg_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a session on a fresh test database with all tables created.

    Yields:
        Session bound to the test database engine.
    """
    settings = db_settings()
    await DatabaseService.create_db(settings)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as session:
            yield session
    finally:
        await engine.dispose()
        await drop_test_database(settings)


def asgi_api_client(app: FastAPI) -> KitaruAPIClient:
    """Build an SDK client routed to the app instead of the network.

    Args:
        app: Application to route requests to.

    Returns:
        Client wired to an ASGI transport.
    """
    client = KitaruAPIClient(base_url="http://test")
    client._http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers=client._http.headers,
    )
    return client


def _renewed_timestamp(previous: datetime | None) -> datetime:
    """Return an update time strictly after the stored updated timestamp.

    Args:
        previous: Stored updated timestamp.

    Returns:
        Update time.
    """
    now = datetime.now(UTC)
    if previous is not None and now <= previous:
        now = previous + timedelta(microseconds=1)
    return now


class FakePasswordHasher:
    """Reversible in-memory password hasher."""

    @property
    def dummy_hash(self) -> str:
        """Fake-scheme hash matching no password.

        Returns:
            Hash for timing-uniform verification when no stored hash exists.
        """
        return "hashed:cf3a4a1bc2f04ee5b2e2f36b93d1c05f"

    def hash(self, password: str) -> str:
        """Hash a password.

        Args:
            password: Plaintext password.

        Returns:
            Marked plaintext standing in for a hash.
        """
        return f"hashed:{password}"

    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against a fake hash.

        Args:
            password: Plaintext password.
            password_hash: Stored fake hash.

        Returns:
            ``True`` when the password matches the hash.
        """
        return password_hash == f"hashed:{password}"


class FakeControlPlaneClient(ControlPlaneClient):
    """Fake control plane API client returning a scripted authorization."""

    def __init__(
        self,
        authorization: ServerAuthorization | None = None,
        error: ControlPlaneError | None = None,
    ) -> None:
        """Create a fake control plane API client.

        Args:
            authorization: Authorization result returned by authorize_server.
            error: Error raised by authorize_server instead of returning.
        """
        self.authorization = authorization
        self.error = error
        self.received_credential: str | None = None
        self.received_server_id: uuid.UUID | None = None

    async def authorize_server(
        self, credential: str, server_id: uuid.UUID
    ) -> ServerAuthorization:
        """Record the call and return the scripted authorization.

        Args:
            credential: Bearer token supplied by the caller.
            server_id: Server instance this API represents.

        Raises:
            ControlPlaneError: The fake was configured to raise.

        Returns:
            Scripted authorization result.
        """
        self.received_credential = credential
        self.received_server_id = server_id
        if self.error is not None:
            raise self.error
        assert self.authorization is not None
        return self.authorization

    async def close(self) -> None:
        """Close the fake client, which holds no connections."""


class FakeAccountRepository:
    """In-memory account repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._accounts: dict[uuid.UUID, Account] = {}

    def _check_duplicate_name(self, account: Account) -> None:
        for other in self._accounts.values():
            if (
                other.id != account.id
                and other.name == account.name
                and other.is_service_account == account.is_service_account
            ):
                raise DuplicateAccountName(account.name)

    async def create(self, account: Account) -> Account:
        """Persist a new account.

        Args:
            account: Account to store.

        Raises:
            DuplicateAccountName: The account name is already registered.

        Returns:
            Stored account with timestamps set.
        """
        self._check_duplicate_name(account)
        now = datetime.now(UTC)
        stored = account.model_copy(update={"created": now, "updated": now})
        self._accounts[stored.id] = stored
        return stored.model_copy()

    async def get(self, account_id: uuid.UUID) -> Account:
        """Load an account by id.

        Args:
            account_id: Id of the account.

        Raises:
            AccountNotFound: No account has this id.

        Returns:
            Stored account.
        """
        account = self._accounts.get(account_id)
        if account is None:
            raise AccountNotFound(account_id)
        return account.model_copy()

    async def get_by_name(self, name: str, is_service_account: bool = False) -> Account:
        """Load an account by name.

        Args:
            name: Name of the account.
            is_service_account: Whether to look up a service account.

        Raises:
            AccountNotFound: No account has this name.

        Returns:
            Stored account.
        """
        for account in self._accounts.values():
            if (
                account.name == name
                and account.is_service_account == is_service_account
            ):
                return account.model_copy()
        raise AccountNotFound(name)

    async def get_by_external_id(
        self, external_id: uuid.UUID, is_service_account: bool = False
    ) -> Account:
        """Load an account by external id.

        Args:
            external_id: External id of the account.
            is_service_account: Whether to look up a service account.

        Raises:
            AccountNotFound: No account has this external id.

        Returns:
            Stored account.
        """
        for account in self._accounts.values():
            if (
                account.external_id == external_id
                and account.is_service_account == is_service_account
            ):
                return account.model_copy()
        raise AccountNotFound(external_id)

    async def query(self, account_filter: AccountFilter) -> tuple[list[Account], int]:
        """Query accounts matching a filter.

        Args:
            account_filter: Filter and pagination parameters.

        Returns:
            Page of matching accounts and the total match count.
        """
        accounts = sorted(self._accounts.values(), key=lambda account: account.id.int)
        if account_filter.name is not None:
            accounts = [
                account for account in accounts if account.name == account_filter.name
            ]
        if account_filter.active is not None:
            accounts = [
                account
                for account in accounts
                if account.active == account_filter.active
            ]
        total = len(accounts)
        start = (account_filter.page - 1) * account_filter.page_size
        page = accounts[start : start + account_filter.page_size]
        return [account.model_copy() for account in page], total

    async def update(self, account: Account) -> Account:
        """Persist changes to an existing account.

        Args:
            account: Account with modified fields.

        Raises:
            AccountNotFound: No account has this id.
            DuplicateAccountName: The account name is already registered.

        Returns:
            Stored account with the updated timestamp renewed.
        """
        stored = self._accounts.get(account.id)
        if stored is None:
            raise AccountNotFound(account.id)
        self._check_duplicate_name(account)
        now = _renewed_timestamp(stored.updated)
        updated = account.model_copy(update={"created": stored.created, "updated": now})
        self._accounts[account.id] = updated
        return updated.model_copy()


class FakeApiKeyRepository:
    """In-memory API key repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._api_keys: dict[uuid.UUID, ApiKey] = {}

    def _check_duplicate_name(self, api_key: ApiKey) -> None:
        for other in self._api_keys.values():
            if other.id != api_key.id and other.name == api_key.name:
                raise DuplicateApiKeyName(api_key.name)

    async def create(self, api_key: ApiKey) -> ApiKey:
        """Persist a new API key.

        Args:
            api_key: API key to store.

        Raises:
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with timestamps set.
        """
        self._check_duplicate_name(api_key)
        now = datetime.now(UTC)
        stored = api_key.model_copy(update={"created": now, "updated": now})
        self._api_keys[stored.id] = stored
        return stored.model_copy()

    async def get(self, api_key_id: uuid.UUID) -> ApiKey:
        """Load an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            ApiKeyNotFound: No API key has this id.

        Returns:
            Stored API key.
        """
        api_key = self._api_keys.get(api_key_id)
        if api_key is None:
            raise ApiKeyNotFound(api_key_id)
        return api_key.model_copy()

    async def query(self, api_key_filter: ApiKeyFilter) -> tuple[list[ApiKey], int]:
        """Query API keys matching a filter.

        Args:
            api_key_filter: Filter and pagination parameters.

        Returns:
            Page of matching API keys and the total match count.
        """
        api_keys = sorted(self._api_keys.values(), key=lambda api_key: api_key.id.int)
        if api_key_filter.name is not None:
            api_keys = [
                api_key for api_key in api_keys if api_key.name == api_key_filter.name
            ]
        if api_key_filter.owner_id is not None:
            api_keys = [
                api_key
                for api_key in api_keys
                if api_key.owner_id == api_key_filter.owner_id
            ]
        total = len(api_keys)
        start = (api_key_filter.page - 1) * api_key_filter.page_size
        page = api_keys[start : start + api_key_filter.page_size]
        return [api_key.model_copy() for api_key in page], total

    async def update(self, api_key: ApiKey) -> ApiKey:
        """Persist changes to an existing API key.

        Args:
            api_key: API key with modified fields.

        Raises:
            ApiKeyNotFound: No API key has this id.
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with the updated timestamp renewed.
        """
        stored = self._api_keys.get(api_key.id)
        if stored is None:
            raise ApiKeyNotFound(api_key.id)
        self._check_duplicate_name(api_key)
        now = _renewed_timestamp(stored.updated)
        updated = api_key.model_copy(update={"created": stored.created, "updated": now})
        self._api_keys[api_key.id] = updated
        return updated.model_copy()

    async def delete(self, api_key_id: uuid.UUID) -> None:
        """Delete an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            ApiKeyNotFound: No API key has this id.
        """
        if api_key_id not in self._api_keys:
            raise ApiKeyNotFound(api_key_id)
        del self._api_keys[api_key_id]


async def create_api_key(
    repository: FakeApiKeyRepository,
    owner_id: uuid.UUID,
    name: str = "ci",
    active: bool = True,
) -> tuple[ApiKey, str]:
    """Store an API key in the fake repository.

    Args:
        repository: Fake API key repository.
        owner_id: Id of the owning account.
        name: API key name.
        active: Active state.

    Returns:
        Stored API key and the encoded plaintext key.
    """
    secret = generate_secret()
    api_key = await repository.create(
        ApiKey(
            owner_id=owner_id, name=name, key_hash=hash_secret(secret), active=active
        )
    )
    return api_key, encode_api_key(api_key.id, secret)


class FakeSecretRepository:
    """In-memory secret repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._secrets: dict[uuid.UUID, Secret] = {}

    def _check_duplicate_name(self, secret: Secret) -> None:
        for other in self._secrets.values():
            if other.id != secret.id and other.name == secret.name:
                raise DuplicateSecretName(secret.name)

    async def create(self, secret: Secret) -> Secret:
        """Persist a new secret.

        Args:
            secret: Secret to store.

        Raises:
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Stored secret with timestamps set.
        """
        self._check_duplicate_name(secret)
        now = datetime.now(UTC)
        stored = secret.model_copy(update={"created": now, "updated": now})
        self._secrets[stored.id] = stored
        return stored.model_copy()

    async def get(self, secret_id: uuid.UUID) -> Secret:
        """Load a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.

        Returns:
            Stored secret.
        """
        secret = self._secrets.get(secret_id)
        if secret is None:
            raise SecretNotFound(secret_id)
        return secret.model_copy()

    async def query(self, secret_filter: SecretFilter) -> tuple[list[Secret], int]:
        """Query secrets matching a filter.

        Args:
            secret_filter: Filter and pagination parameters.

        Returns:
            Page of matching secrets and the total match count.
        """
        secrets = sorted(self._secrets.values(), key=lambda secret: secret.id.int)
        if secret_filter.name is not None:
            secrets = [
                secret for secret in secrets if secret.name == secret_filter.name
            ]
        if secret_filter.owner_id is not None:
            secrets = [
                secret
                for secret in secrets
                if secret.owner_id == secret_filter.owner_id
            ]
        if secret_filter.internal is not None:
            secrets = [
                secret
                for secret in secrets
                if secret.internal == secret_filter.internal
            ]
        total = len(secrets)
        start = (secret_filter.page - 1) * secret_filter.page_size
        page = secrets[start : start + secret_filter.page_size]
        return [secret.model_copy() for secret in page], total

    async def update(self, secret: Secret) -> Secret:
        """Persist changes to an existing secret.

        Args:
            secret: Secret with modified fields.

        Raises:
            SecretNotFound: No secret has this id.
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Stored secret with the updated timestamp renewed.
        """
        stored = self._secrets.get(secret.id)
        if stored is None:
            raise SecretNotFound(secret.id)
        self._check_duplicate_name(secret)
        now = _renewed_timestamp(stored.updated)
        updated = secret.model_copy(update={"created": stored.created, "updated": now})
        self._secrets[secret.id] = updated
        return updated.model_copy()

    async def delete(self, secret_id: uuid.UUID) -> None:
        """Delete a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.
        """
        if secret_id not in self._secrets:
            raise SecretNotFound(secret_id)
        del self._secrets[secret_id]


async def create_secret(
    repository: FakeSecretRepository,
    owner_id: uuid.UUID,
    name: str = "db",
    internal: bool = False,
    values: dict[str, SecretStr] | None = None,
) -> Secret:
    """Store a secret in the fake repository.

    Args:
        repository: Fake secret repository.
        owner_id: Id of the owning account.
        name: Secret name.
        internal: Internal state.
        values: Secret values.

    Returns:
        Stored secret.
    """
    if values is None:
        values = {"password": SecretStr("hunter2")}
    return await repository.create(
        Secret(owner_id=owner_id, name=name, internal=internal, values=values)
    )
