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
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.api_models.v1.task import WorkerScope
from kitaru.api_models.v1.worker import WorkerRuntime
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
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.application.models.worker import WorkerFilter
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
from kitaru.server.domain.device import Device, DeviceNotFound, DeviceStatus
from kitaru.server.domain.keys import generate_secret, hash_secret
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretNotFound,
)
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
)
from kitaru.server.domain.worker import Worker, WorkerNotFound
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


class FakeTagRepository:
    """In-memory tag repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._tags: dict[uuid.UUID, Tag] = {}
        self._links: dict[uuid.UUID, TagLink] = {}

    def _check_duplicate_name(self, tag: Tag) -> None:
        for other in self._tags.values():
            if other.id != tag.id and other.name == tag.name:
                raise DuplicateTagName(tag.name)

    def _check_duplicate_link(self, link: TagLink) -> None:
        for other in self._links.values():
            if (
                other.id != link.id
                and other.tag_id == link.tag_id
                and other.resource_type == link.resource_type
                and other.resource_id == link.resource_id
            ):
                raise DuplicateTagLink(
                    link.tag_id, link.resource_type, link.resource_id
                )

    async def create(self, tag: Tag) -> Tag:
        """Persist a new tag.

        Args:
            tag: Tag to store.

        Raises:
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with timestamps set.
        """
        self._check_duplicate_name(tag)
        now = datetime.now(UTC)
        stored = tag.model_copy(update={"created": now, "updated": now})
        self._tags[stored.id] = stored
        return stored.model_copy()

    async def get(self, tag_id: uuid.UUID) -> Tag:
        """Load a tag by id.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.

        Returns:
            Stored tag.
        """
        tag = self._tags.get(tag_id)
        if tag is None:
            raise TagNotFound(tag_id)
        return tag.model_copy()

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], str | None]:
        """Query tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.

        Returns:
            Page of matching tags and the next cursor.
        """
        tags = list(self._tags.values())
        if tag_filter.name is not None:
            tags = [tag for tag in tags if tag.name == tag_filter.name]
        page, next_cursor = _paginate_fake(tags, tag_filter)
        return [tag.model_copy() for tag in page], next_cursor

    async def update(self, tag: Tag) -> Tag:
        """Persist changes to an existing tag.

        Args:
            tag: Tag with modified fields.

        Raises:
            TagNotFound: No tag has this id.
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with the updated timestamp renewed.
        """
        stored = self._tags.get(tag.id)
        if stored is None:
            raise TagNotFound(tag.id)
        self._check_duplicate_name(tag)
        now = _renewed_timestamp(stored.updated)
        updated = tag.model_copy(update={"created": stored.created, "updated": now})
        self._tags[tag.id] = updated
        return updated.model_copy()

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag and its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.
        """
        if tag_id not in self._tags:
            raise TagNotFound(tag_id)
        del self._tags[tag_id]
        cascaded = [
            link_id for link_id, link in self._links.items() if link.tag_id == tag_id
        ]
        for link_id in cascaded:
            del self._links[link_id]

    async def create_link(self, link: TagLink) -> TagLink:
        """Persist a new tag link.

        Args:
            link: Tag link to store.

        Raises:
            TagNotFound: No tag has the link's tag id.
            DuplicateTagLink: The tag is already linked to the resource.

        Returns:
            Stored tag link with timestamps set.
        """
        if link.tag_id not in self._tags:
            raise TagNotFound(link.tag_id)
        self._check_duplicate_link(link)
        now = datetime.now(UTC)
        stored = link.model_copy(update={"created": now, "updated": now})
        self._links[stored.id] = stored
        return stored.model_copy()

    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Delete a tag link by tag and resource.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.

        Raises:
            TagLinkNotFound: No link matches the tag and resource.
        """
        for link_id, link in self._links.items():
            if (
                link.tag_id == tag_id
                and link.resource_type == resource_type
                and link.resource_id == resource_id
            ):
                del self._links[link_id]
                return
        raise TagLinkNotFound(tag_id, resource_type, resource_id)


async def create_tag(
    repository: FakeTagRepository, owner_id: uuid.UUID, name: str = "smoke-test"
) -> Tag:
    """Store a tag in the fake repository.

    Args:
        repository: Fake tag repository.
        owner_id: Id of the owning account.
        name: Tag name.

    Returns:
        Stored tag.
    """
    return await repository.create(Tag(owner_id=owner_id, name=name))


class FakeWorkerRepository:
    """In-memory worker repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._workers: dict[uuid.UUID, Worker] = {}

    async def register(self, worker: Worker) -> Worker:
        """Persist a worker, refreshing an existing row with the same name.

        Args:
            worker: Worker to store or refresh.

        Returns:
            Stored worker with its id, created, and updated timestamp set.
        """
        for stored in self._workers.values():
            if stored.name == worker.name:
                refreshed = stored.model_copy()
                refreshed.refresh(
                    worker.scope, worker.runtime, worker.metadata, worker.last_seen_at
                )
                refreshed = refreshed.model_copy(
                    update={"updated": _renewed_timestamp(stored.updated)}
                )
                self._workers[stored.id] = refreshed
                return refreshed.model_copy()
        now = datetime.now(UTC)
        stored = worker.model_copy(update={"created": now, "updated": now})
        self._workers[stored.id] = stored
        return stored.model_copy()

    async def get(self, worker_id: uuid.UUID) -> Worker:
        """Load a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Stored worker.
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            raise WorkerNotFound(worker_id)
        return worker.model_copy()

    async def query(
        self, worker_filter: WorkerFilter
    ) -> tuple[list[Worker], str | None]:
        """Query workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.

        Returns:
            Page of matching workers and the next cursor.
        """
        workers = list(self._workers.values())
        if worker_filter.name is not None:
            workers = [
                worker for worker in workers if worker.name == worker_filter.name
            ]
        if worker_filter.seen_after is not None:
            workers = [
                worker
                for worker in workers
                if worker.last_seen_at >= worker_filter.seen_after
            ]
        page, next_cursor = _paginate_fake(workers, worker_filter)
        return [worker.model_copy() for worker in page], next_cursor

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        if worker_id not in self._workers:
            raise WorkerNotFound(worker_id)
        del self._workers[worker_id]


async def create_worker(
    repository: FakeWorkerRepository,
    owner_id: uuid.UUID,
    name: str = "worker-1",
    scope: WorkerScope | None = None,
    runtime: WorkerRuntime | None = None,
    metadata: dict[str, str] | None = None,
    last_seen_at: datetime | None = None,
) -> Worker:
    """Store a worker in the fake repository.

    Args:
        repository: Fake worker repository.
        owner_id: Id of the owning account.
        name: Worker name.
        scope: Claim scope the worker reports.
        runtime: Runtime the worker reports.
        metadata: Arbitrary metadata.
        last_seen_at: Time of the worker's last heartbeat.

    Returns:
        Stored worker.
    """
    return await repository.register(
        Worker(
            owner_id=owner_id,
            name=name,
            scope=scope if scope is not None else WorkerScope(),
            runtime=runtime if runtime is not None else WorkerRuntime(platform="bare"),
            metadata=metadata if metadata is not None else {},
            last_seen_at=last_seen_at
            if last_seen_at is not None
            else datetime.now(UTC),
        )
    )
