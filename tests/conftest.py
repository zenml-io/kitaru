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
import hashlib
import os
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, TypeVar

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from kitaru.api_models.v1.info import AuthScheme
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.client_id import ENV_CLIENT_ID
from kitaru.client.credential_store import CredentialStore
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneClient,
    ControlPlaneError,
    ControlPlaneUser,
)
from kitaru.server.adapters.db.orm.base import Base
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.application.models.plugin import PluginFilter, PluginVersionFilter
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.base import ListFilter
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
)
from kitaru.server.domain.blob import Blob, BlobInUse, BlobNotFound
from kitaru.server.domain.device import Device, DeviceNotFound, DeviceStatus
from kitaru.server.domain.keys import generate_secret, hash_secret
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginSource,
    PluginVersion,
    PluginVersionNotFound,
    ScriptPluginSource,
)
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretNotFound,
)
from kitaru.transport import RetryTransport

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
        # The timestamp lets the stale-database reaper age-gate its drops.
        DB_NAME=f"{TEST_DB_PREFIX}_{int(datetime.now(UTC).timestamp())}_{uuid.uuid4().hex[:12]}",
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


STALE_TEST_DB_AGE = timedelta(hours=1)


def _is_stale_test_database(name: str, now: datetime) -> bool:
    """Report whether a test database is old enough to reap.

    Args:
        name: Database name.
        now: Current time.

    Returns:
        ``True`` when the database is older than the stale age.
    """
    segments = name.removeprefix(f"{TEST_DB_PREFIX}_").split("_")
    try:
        created = datetime.fromtimestamp(int(segments[0]), tz=UTC)
    except (ValueError, OverflowError):
        # A name without a timestamp predates the age gate.
        return True
    return now - created > STALE_TEST_DB_AGE


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
            now = datetime.now(UTC)
            for name in list(names):
                if not _is_stale_test_database(name, now):
                    continue
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
    """Drop test databases left behind by a run that was killed.

    The age gate keeps a concurrent run's live databases out of the reap.
    """
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
async def pg_session_with_engine() -> AsyncGenerator[
    tuple[AsyncSession, AsyncEngine], None
]:
    """Provide a session and its engine on a fresh test database.

    Yields:
        Session bound to the test database engine, and the engine itself.
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
            yield session, engine
    finally:
        await engine.dispose()
        await drop_test_database(settings)


@asynccontextmanager
async def pg_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a session on a fresh test database with all tables created.

    Yields:
        Session bound to the test database engine.
    """
    async with pg_session_with_engine() as (session, _):
        yield session


@pytest.fixture
def credential_store(tmp_path: Path) -> CredentialStore:
    """Provide a credential store backed by a file under tmp_path."""
    return CredentialStore(path=tmp_path / "credentials.json")


@pytest.fixture
def isolated_config_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the client config directory at a fresh temporary directory."""
    monkeypatch.delenv(ENV_CLIENT_ID, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))


def asgi_api_client(
    app: FastAPI, credential_store: CredentialStore | None = None
) -> KitaruAPIClient:
    """Build an SDK client routed to the app instead of the network.

    Args:
        app: Application to route requests to.
        credential_store: Store holding the credentials the client
            authenticates with.

    Returns:
        Client wired to an ASGI transport.
    """
    client = KitaruAPIClient(base_url="http://test", credential_store=credential_store)
    client._http = httpx.AsyncClient(
        transport=RetryTransport(httpx.ASGITransport(app=app)),
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
    """Fake control plane API client returning a scripted user."""

    def __init__(
        self,
        user: ControlPlaneUser | None = None,
        error: ControlPlaneError | None = None,
    ) -> None:
        """Create a fake control plane API client.

        Args:
            user: User returned by authorize_user.
            error: Error raised by authorize_user instead of returning.
        """
        self.user = user
        self.error = error
        self.received_credentials: list[str] = []
        self.received_server_id: uuid.UUID | None = None

    async def authorize_user(
        self, credential: str, server_id: uuid.UUID
    ) -> ControlPlaneUser:
        """Record the call and return the scripted user.

        Args:
            credential: Bearer token supplied by the caller.
            server_id: Server instance this API represents.

        Raises:
            ControlPlaneError: The fake was configured to raise.

        Returns:
            Scripted user.
        """
        self.received_credentials.append(credential)
        self.received_server_id = server_id
        if self.error is not None:
            raise self.error
        assert self.user is not None
        return self.user

    async def close(self) -> None:
        """Close the fake client, which holds no connections."""


class _HasId(Protocol):
    """Protocol for domain objects with a stable id."""

    id: uuid.UUID


ListItemT = TypeVar("ListItemT", bound=_HasId)


def _paginate_fake(
    items: list[ListItemT], list_filter: ListFilter
) -> tuple[list[ListItemT], str | None]:
    """Apply cursor pagination to an in-memory list of domain objects.

    Args:
        items: Candidate items already filtered by non-pagination fields.
        list_filter: List filter carrying the cursor, size, and sort.

    Returns:
        Page of matching items and the next cursor.
    """
    _, _, direction = list_filter.sort.partition(":")
    descending = direction == "desc"
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    ordered = sorted(items, key=lambda item: item.id, reverse=descending)

    if cursor is not None:
        last_id = uuid.UUID(cursor.id)
        ordered = [
            item
            for item in ordered
            if (item.id < last_id if descending else item.id > last_id)
        ]

    page = ordered[: list_filter.size + 1]
    next_cursor = None
    if len(page) > list_filter.size:
        page = page[: list_filter.size]
        last_item = page[-1]
        next_cursor = encode_cursor(list_filter.sort, str(last_item.id), filter_hash)
    return page, next_cursor


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

    async def query(
        self, account_filter: AccountFilter
    ) -> tuple[list[Account], str | None]:
        """Query accounts matching a filter.

        Args:
            account_filter: Filter and pagination parameters.

        Returns:
            Page of matching accounts and the next cursor.
        """
        accounts = list(self._accounts.values())
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
        page, next_cursor = _paginate_fake(accounts, account_filter)
        return [account.model_copy() for account in page], next_cursor

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

    async def query(
        self, api_key_filter: ApiKeyFilter
    ) -> tuple[list[ApiKey], str | None]:
        """Query API keys matching a filter.

        Args:
            api_key_filter: Filter and pagination parameters.

        Returns:
            Page of matching API keys and the next cursor.
        """
        api_keys = list(self._api_keys.values())
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
        page, next_cursor = _paginate_fake(api_keys, api_key_filter)
        return [api_key.model_copy() for api_key in page], next_cursor

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


class FakeDeviceRepository:
    """In-memory device repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._devices: dict[uuid.UUID, Device] = {}

    async def create(self, device: Device) -> Device:
        """Persist a new device.

        Args:
            device: Device to store.

        Returns:
            Stored device with timestamps set.
        """
        now = datetime.now(UTC)
        stored = device.model_copy(update={"created": now, "updated": now})
        self._devices[stored.id] = stored
        return stored.model_copy()

    async def get(self, device_id: uuid.UUID) -> Device:
        """Load a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device.
        """
        device = self._devices.get(device_id)
        if device is None:
            raise DeviceNotFound(device_id)
        return device.model_copy()

    async def query(
        self, device_filter: DeviceFilter
    ) -> tuple[list[Device], str | None]:
        """Query devices matching a filter.

        Args:
            device_filter: Filter and pagination parameters.

        Returns:
            Page of matching devices and the next cursor.
        """
        devices = list(self._devices.values())
        if device_filter.account_id is not None:
            devices = [
                device
                for device in devices
                if device.account_id == device_filter.account_id
            ]
        if device_filter.status is not None:
            devices = [
                device for device in devices if device.status == device_filter.status
            ]
        page, next_cursor = _paginate_fake(devices, device_filter)
        return [device.model_copy() for device in page], next_cursor

    async def update(self, device: Device) -> Device:
        """Persist changes to an existing device.

        Args:
            device: Device with modified fields.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device with the updated timestamp renewed.
        """
        stored = self._devices.get(device.id)
        if stored is None:
            raise DeviceNotFound(device.id)
        now = _renewed_timestamp(stored.updated)
        updated = device.model_copy(update={"created": stored.created, "updated": now})
        self._devices[device.id] = updated
        return updated.model_copy()

    async def delete(self, device_id: uuid.UUID) -> None:
        """Delete a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.
        """
        if device_id not in self._devices:
            raise DeviceNotFound(device_id)
        del self._devices[device_id]

    async def record_failed_attempt(self, device: Device) -> None:
        """Persist a failed code check in its own transaction.

        Args:
            device: Device whose attempt counter and locked state changed.
        """
        stored = self._devices.get(device.id)
        if stored is None:
            raise DeviceNotFound(device.id)
        now = _renewed_timestamp(stored.updated)
        updated = stored.model_copy(
            update={
                "failed_auth_attempts": device.failed_auth_attempts,
                "locked": device.locked,
                "updated": now,
            }
        )
        self._devices[device.id] = updated

    async def delete_expired(self, now: datetime) -> int:
        """Delete every device past its expiry.

        Args:
            now: Current time.

        Returns:
            Number of deleted devices.
        """
        expired = [
            device_id
            for device_id, device in self._devices.items()
            if device.is_expired(now)
        ]
        for device_id in expired:
            del self._devices[device_id]
        return len(expired)


async def create_device(
    repository: FakeDeviceRepository,
    account_id: uuid.UUID | None = None,
    status: DeviceStatus = DeviceStatus.PENDING,
    trusted: bool = False,
    locked: bool = False,
    expires: datetime | None = None,
    user_code: str = "user-code",
    device_code: str = "device-code",
) -> tuple[Device, str, str]:
    """Store a device in the fake repository.

    Args:
        repository: Fake device repository.
        account_id: Id of the approving account, unset for a pending device.
        status: Device status.
        trusted: Trusted state.
        locked: Locked state.
        expires: Expiry time.
        user_code: Plaintext user code to hash and store.
        device_code: Plaintext device code to hash and store.

    Returns:
        Stored device and the plaintext user code and device code.
    """
    device = await repository.create(
        Device(
            account_id=account_id,
            user_code_hash=hash_secret(user_code),
            device_code_hash=hash_secret(device_code),
            status=status,
            trusted=trusted,
            locked=locked,
            expires=expires,
        )
    )
    return device, user_code, device_code


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

    async def query(
        self, secret_filter: SecretFilter
    ) -> tuple[list[Secret], str | None]:
        """Query secrets matching a filter.

        Args:
            secret_filter: Filter and pagination parameters.

        Returns:
            Page of matching secrets and the next cursor.
        """
        secrets = list(self._secrets.values())
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
        page, next_cursor = _paginate_fake(secrets, secret_filter)
        return [secret.model_copy() for secret in page], next_cursor

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


class FakeBlobRepository:
    """In-memory blob repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._blobs: dict[uuid.UUID, Blob] = {}
        self._referenced: set[uuid.UUID] = set()

    async def create(self, blob: Blob) -> tuple[Blob, bool]:
        """Persist a new blob, deduping a concurrent identical upload.

        Args:
            blob: Blob to store.

        Returns:
            Stored blob and whether this call created it. A dedup hit
            returns the existing row with its content left unloaded.
        """
        for other in self._blobs.values():
            if other.sha256 == blob.sha256:
                return other.model_copy(update={"data": b""}), False
        now = datetime.now(UTC)
        stored = blob.model_copy(update={"created": now})
        self._blobs[stored.id] = stored
        return stored.model_copy(), True

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id, content included.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        blob = self._blobs.get(blob_id)
        if blob is None:
            raise BlobNotFound(blob_id)
        return blob.model_copy()

    async def delete(self, blob_id: uuid.UUID) -> None:
        """Delete a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.
            BlobInUse: The blob is referenced by a plugin version.
        """
        if blob_id not in self._blobs:
            raise BlobNotFound(blob_id)
        if blob_id in self._referenced:
            raise BlobInUse(blob_id)
        del self._blobs[blob_id]

    def mark_referenced(self, blob_id: uuid.UUID) -> None:
        """Mark a blob as referenced by a plugin version, mirroring the FK restrict.

        Args:
            blob_id: Id of the referenced blob.
        """
        self._referenced.add(blob_id)


async def create_blob(
    repository: FakeBlobRepository,
    owner_id: uuid.UUID,
    content: bytes = b"blob-content",
    media_type: str = "application/octet-stream",
) -> Blob:
    """Store a blob in the fake repository.

    Args:
        repository: Fake blob repository.
        owner_id: Id of the owning account.
        content: Blob content.
        media_type: Content media type.

    Returns:
        Stored blob.
    """
    sha256 = hashlib.sha256(content).hexdigest()
    blob, _ = await repository.create(
        Blob(
            owner_id=owner_id,
            sha256=sha256,
            size=len(content),
            media_type=media_type,
            data=content,
        )
    )
    return blob


class FakePluginRepository:
    """In-memory plugin and plugin version repository."""

    def __init__(self, blob_repository: FakeBlobRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            blob_repository: Blob repository, marked when a script version
                references one of its blobs, mirroring the FK restrict.
        """
        self._plugins: dict[uuid.UUID, Plugin] = {}
        self._versions: dict[uuid.UUID, PluginVersion] = {}
        self._blob_repository = blob_repository

    def _check_duplicate_name(self, plugin: Plugin) -> None:
        for other in self._plugins.values():
            if (
                other.id != plugin.id
                and other.kind == plugin.kind
                and other.name == plugin.name
            ):
                raise DuplicatePluginName(plugin.kind, plugin.name)

    async def create(self, plugin: Plugin) -> Plugin:
        """Persist a new plugin.

        Args:
            plugin: Plugin to store.

        Raises:
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with timestamps set.
        """
        self._check_duplicate_name(plugin)
        now = datetime.now(UTC)
        stored = plugin.model_copy(update={"created": now, "updated": now})
        self._plugins[stored.id] = stored
        return stored.model_copy()

    async def get(self, plugin_id: uuid.UUID) -> Plugin:
        """Load a plugin by id.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.

        Returns:
            Stored plugin.
        """
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise PluginNotFound(plugin_id)
        return plugin.model_copy()

    async def query(
        self, plugin_filter: PluginFilter
    ) -> tuple[list[Plugin], str | None]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the next cursor.
        """
        plugins = [
            plugin
            for plugin in self._plugins.values()
            if plugin.kind == plugin_filter.kind
        ]
        if plugin_filter.name is not None:
            plugins = [
                plugin for plugin in plugins if plugin.name == plugin_filter.name
            ]
        if plugin_filter.provider is not None:
            plugins = [
                plugin
                for plugin in plugins
                if plugin.provider == plugin_filter.provider
            ]
        page, next_cursor = _paginate_fake(plugins, plugin_filter)
        return [plugin.model_copy() for plugin in page], next_cursor

    async def update(self, plugin: Plugin) -> Plugin:
        """Persist changes to an existing plugin.

        Args:
            plugin: Plugin with modified fields.

        Raises:
            PluginNotFound: No plugin has this id.
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with the updated timestamp renewed.
        """
        stored = self._plugins.get(plugin.id)
        if stored is None:
            raise PluginNotFound(plugin.id)
        self._check_duplicate_name(plugin)
        now = _renewed_timestamp(stored.updated)
        updated = plugin.model_copy(update={"created": stored.created, "updated": now})
        self._plugins[plugin.id] = updated
        return updated.model_copy()

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, cascading its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
        """
        if plugin_id not in self._plugins:
            raise PluginNotFound(plugin_id)
        del self._plugins[plugin_id]
        stale_ids = [
            version_id
            for version_id, version in self._versions.items()
            if version.plugin_id == plugin_id
        ]
        for version_id in stale_ids:
            del self._versions[version_id]

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        source: PluginSource,
        display_version: str | None,
    ) -> PluginVersion:
        """Persist a new plugin version with a server-assigned version number.

        Args:
            plugin_id: Id of the plugin.
            source: Plugin code source.
            display_version: Human-readable designator.

        Raises:
            PluginNotFound: No plugin has this id.
            DuplicatePluginVersion: The version number is already registered.

        Returns:
            Stored plugin version with timestamps set.
        """
        stored_plugin = self._plugins.get(plugin_id)
        if stored_plugin is None:
            raise PluginNotFound(plugin_id)
        version_number = stored_plugin.latest_version + 1
        if any(
            version.plugin_id == plugin_id and version.version == version_number
            for version in self._versions.values()
        ):
            raise DuplicatePluginVersion(plugin_id, version_number)
        self._plugins[plugin_id] = stored_plugin.model_copy(
            update={"latest_version": version_number}
        )
        now = datetime.now(UTC)
        version = PluginVersion(
            plugin_id=plugin_id,
            version=version_number,
            display_version=display_version,
            source=source,
            created=now,
            updated=now,
        )
        self._versions[version.id] = version
        if isinstance(source, ScriptPluginSource) and self._blob_repository is not None:
            self._blob_repository.mark_referenced(source.blob_id)
        return version.model_copy()

    async def get_version(self, plugin_id: uuid.UUID, version: int) -> PluginVersion:
        """Load a plugin version by plugin id and version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.

        Raises:
            PluginVersionNotFound: No version with this number exists for
                this plugin.

        Returns:
            Stored plugin version.
        """
        for stored in self._versions.values():
            if stored.plugin_id == plugin_id and stored.version == version:
                return stored.model_copy()
        raise PluginVersionNotFound(plugin_id, version)

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], str | None]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the next cursor.
        """
        versions = [
            version
            for version in self._versions.values()
            if version.plugin_id == version_filter.plugin_id
        ]
        page, next_cursor = _paginate_fake(versions, version_filter)
        return [version.model_copy() for version in page], next_cursor

    async def update_version(self, plugin_version: PluginVersion) -> PluginVersion:
        """Persist changes to an existing plugin version.

        Args:
            plugin_version: Plugin version with modified fields.

        Raises:
            PluginVersionNotFound: No version has this id.

        Returns:
            Stored plugin version with the updated timestamp renewed.
        """
        stored = self._versions.get(plugin_version.id)
        if stored is None:
            raise PluginVersionNotFound(
                plugin_version.plugin_id, plugin_version.version
            )
        now = _renewed_timestamp(stored.updated)
        updated = plugin_version.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._versions[plugin_version.id] = updated
        return updated.model_copy()


async def create_plugin(
    repository: FakePluginRepository,
    owner_id: uuid.UUID,
    kind: PluginKind,
    name: str = "plugin",
    description: str | None = None,
    provider: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Plugin:
    """Store a plugin in the fake repository.

    Args:
        repository: Fake plugin repository.
        owner_id: Id of the owning account.
        kind: Plugin kind.
        name: Plugin name.
        description: Plugin description.
        provider: Source system, evaluators must leave this unset.
        metadata: Arbitrary metadata.

    Returns:
        Stored plugin.
    """
    return await repository.create(
        Plugin(
            owner_id=owner_id,
            kind=kind,
            name=name,
            description=description,
            provider=provider,
            metadata=metadata or {},
        )
    )
