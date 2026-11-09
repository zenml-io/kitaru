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
"""Shared pytest hooks, fixtures, and database and client test helpers."""

import asyncio
import base64
import hashlib
import os
import sys
import uuid
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from hypothesis import settings
from hypothesis.database import DirectoryBasedExampleDatabase
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from kitaru.analytics.source import AnalyticsSource
from kitaru.api_models.v1.info import AuthScheme
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.db.orm.base import Base
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru.transport import RetryTransport
from kitaru_test_support.accounts import FakeAccountRepository, FakeApiKeyRepository
from kitaru_test_support.common import FakePasswordHasher

_HYPOTHESIS_DB = DirectoryBasedExampleDatabase(".hypothesis/examples")
settings.register_profile("dev", max_examples=100, database=_HYPOTHESIS_DB)
# Hypothesis rejects derandomize=True together with a database (derandomize
# replaces the database-driven search with a fixed pseudo-random seed). PR
# runs need that determinism, so "ci" keeps the database off; only the
# nightly run caches examples for replay across runs.
settings.register_profile(
    "ci", max_examples=50, derandomize=True, deadline=None, database=None
)
settings.register_profile(
    "nightly", max_examples=2000, deadline=None, database=_HYPOTHESIS_DB
)
settings.load_profile(
    os.environ.get("HYPOTHESIS_PROFILE", "ci" if os.environ.get("CI") else "dev")
)

# Why: test modules import shared helpers with a bare `from conftest import ...`.
# A subdirectory conftest module would shadow that name on sys.path in subset
# runs, so register this module under the bare name and keep this file the
# only conftest in the tree.
sys.modules.setdefault("conftest", sys.modules[__name__])

# Settings built without an explicit ANALYTICS_OPT_IN read this environment
# variable, so no test server posts analytics to the real endpoint.
os.environ["KITARU_SERVER_ANALYTICS_OPT_IN"] = "false"

TEST_DB_PREFIX = "kitaru_test"


def imported_node(
    name: str, children: list[ImportedNode] | None = None
) -> ImportedNode:
    """Build an imported node for parser and ingest tests.

    Args:
        name: Node name.
        children: Child nodes.

    Returns:
        Imported node.
    """
    return ImportedNode(
        external_id=name,
        node_type=NodeType.LLM_CALL,
        name=name,
        status=NodeStatus.COMPLETED,
        inputs=None,
        outputs=None,
        attributes=None,
        children=children or [],
    )


def imported_session(
    external_id: str, nodes: list[ImportedNode] | None = None
) -> ImportedSession:
    """Build an imported session for parser and ingest tests.

    Args:
        external_id: Session external id.
        nodes: Top-level nodes.

    Returns:
        Imported session.
    """
    return ImportedSession(
        status=SessionStatus.COMPLETED,
        name=external_id,
        inputs=None,
        outputs=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=external_id,
        metadata={},
        nodes=nodes or [],
    )


@pytest.fixture
def complete_mastra_memory_replay_inputs() -> dict[str, Any]:
    """Build a structurally complete v3 input for server finalization tests."""
    media_type = "text/plain"
    content = b"recorded file"
    reference = hashlib.sha256(media_type.encode() + b"\0" + content).hexdigest()
    return {
        "mastra_memory_replay": {
            "version": 3,
            "complete": True,
            "reasons": [],
            "invocationId": "test-invocation",
            "rawInput": "hello",
            "initialSnapshot": {
                "threadId": "thread-1",
                "resourceId": "resource-1",
                "thread": None,
                "resource": None,
                "messages": [],
                "records": [],
            },
            "configuration": {"memoryConfig": {}},
            "requestContext": {},
            "files": [
                {
                    "url": f"kitaru-file://sha256/{reference}",
                    "mediaType": media_type,
                    "base64": base64.b64encode(content).decode("ascii"),
                    "length": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            ],
            "omTape": [],
            "turnStartedAt": "2026-01-01T00:00:00.000Z",
            "keyOrder": {"permutations": "", "sha256": "0" * 64},
        }
    }


@pytest.fixture(autouse=True)
def worker_api_env(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Set the API environment worker tests re-assert into every process."""
    if "tests/worker" in str(request.node.path):
        monkeypatch.setenv("KITARU_API_URL", "https://api.example.com")
        monkeypatch.setenv("KITARU_API_KEY", "test-key")


def db_settings(**overrides: Any) -> APISettings:
    """Build API settings pointing at the local test database.

    Args:
        **overrides: Additional settings values.

    Returns:
        Settings for the test database.
    """
    values: dict[str, Any] = {
        "DB_HOST": os.environ.get("KITARU_TEST_DB_HOST", "localhost"),
        "DB_PORT": int(os.environ.get("KITARU_TEST_DB_PORT", "5433")),
        # A database per caller. Tests drop their database on teardown, and a
        # shared name would let one test drop the database another is using.
        # The timestamp lets the stale-database reaper age-gate its drops.
        "DB_NAME": (
            f"{TEST_DB_PREFIX}_{int(datetime.now(UTC).timestamp())}_"
            f"{uuid.uuid4().hex[:12]}"
        ),
        "SECRET_ENCRYPTION_KEY": "test-encryption-key",
        "JWT_SIGNING_KEY": "test-signing-key-0123456789abcdef",
        **overrides,
    }
    return APISettings(**values)


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


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Select a stable file-based CI shard when explicitly configured."""
    index_value = os.environ.get("KITARU_TEST_SHARD_INDEX", "")
    count_value = os.environ.get("KITARU_TEST_SHARD_COUNT", "")
    if not index_value and not count_value:
        return
    try:
        index = int(index_value)
        count = int(count_value)
    except ValueError as error:
        raise pytest.UsageError(
            "KITARU_TEST_SHARD_INDEX and KITARU_TEST_SHARD_COUNT must both be integers"
        ) from error
    if count < 1 or not 0 <= index < count:
        raise pytest.UsageError(
            "KITARU_TEST_SHARD_COUNT must be positive and "
            "KITARU_TEST_SHARD_INDEX must be in [0, count)"
        )

    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        path = item.path.relative_to(config.rootpath).as_posix()
        shard = int(hashlib.sha256(path.encode()).hexdigest(), 16) % count
        (selected if shard == index else deselected).append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


def pytest_sessionstart() -> None:
    """Require PostgreSQL before collection when explicitly enabled."""
    if os.environ.get("KITARU_TEST_REQUIRE_POSTGRES") != "1":
        return
    if not asyncio.run(postgres_available()):
        raise pytest.UsageError(
            "KITARU_TEST_REQUIRE_POSTGRES=1 but PostgreSQL is not reachable. "
            "Start PostgreSQL and check KITARU_TEST_DB_HOST and KITARU_TEST_DB_PORT."
        )


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


_repository_template_name: str | None = None


@pytest.fixture(scope="session", autouse=True)
def repository_database_template(
    reap_stale_test_databases: None,
) -> Generator[None, None, None]:
    """Create one empty repository schema to copy into isolated test databases."""
    global _repository_template_name
    if not asyncio.run(postgres_available()):
        yield
        return
    template_settings = db_settings()

    async def create_template() -> None:
        await DatabaseService.create_db(template_settings)
        engine = create_async_engine(
            DatabaseService.generate_database_uri(template_settings)
        )
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
        finally:
            # PostgreSQL can only copy a template with no open connections.
            await engine.dispose()

    try:
        asyncio.run(create_template())
        _repository_template_name = DatabaseService.application_database_name(
            template_settings
        )
        yield
    finally:
        _repository_template_name = None
        asyncio.run(drop_test_database(template_settings))


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
    mutate_app: Callable[[FastAPI], None] | None = None,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Run the app through its lifespan on a fresh test database.

    Args:
        settings: API server settings.
        mutate_app: Optional hook to mutate the app before its lifespan
            starts.

    Yields:
        HTTP client routed to the app.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    await DatabaseService.create_db(settings)
    try:
        app = create_app(settings)
        if mutate_app is not None:
            mutate_app(app)
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
    if _repository_template_name is None:
        await DatabaseService.create_db(settings)
    else:
        database_name = DatabaseService.application_database_name(settings)
        admin_engine = create_async_engine(
            DatabaseService.generate_database_uri(settings, use_default_db=True)
        )
        try:
            async with admin_engine.execution_options(
                isolation_level="AUTOCOMMIT"
            ).begin() as connection:
                await connection.execute(
                    text(
                        f'CREATE DATABASE "{database_name}" '
                        f'TEMPLATE "{_repository_template_name}"'
                    )
                )
        finally:
            await admin_engine.dispose()
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        if _repository_template_name is None:
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


def base_asgi_scope(**overrides: Any) -> dict[str, Any]:
    """Build a minimal ASGI HTTP scope for driving a request by hand.

    Args:
        **overrides: Additional scope values.

    Returns:
        Scope dict with sensible defaults for local test requests.
    """
    values: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "headers": [],
        "query_string": b"",
        "server": ("test", 80),
        "client": ("test", 123),
        "root_path": "",
        **overrides,
    }
    return values


def stub_auth_session() -> AsyncMock:
    """Stand in for the auth session in apps that override authentication.

    Returns:
        Session double accepting the commit issued after authentication.
    """
    return AsyncMock()


@pytest.fixture
def credential_store(tmp_path: Path) -> CredentialStore:
    """Provide a credential store backed by a file under tmp_path."""
    return CredentialStore(path=tmp_path / "credentials.json")


@pytest.fixture(autouse=True)
def isolated_client_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Isolate client construction from the ambient environment and config."""
    for name in (
        "KITARU_API_URL",
        "KITARU_API_TOKEN",
        "KITARU_API_KEY",
        "KITARU_CLIENT_ID",
        "KITARU_CONFIG_DIR",
        "KITARU_DISABLE_CREDENTIALS_CACHE",
        "KITARU_DISABLE_CLIENT_ANALYTICS",
        "DO_NOT_TRACK",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))


class RequestRecordingTransport(httpx.AsyncBaseTransport):
    """Transport wrapper recording every request it saw."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        """Wrap a transport and start with no recorded requests.

        Args:
            transport: Transport to delegate every request to.
        """
        self._transport = transport
        self.requests: list[httpx.Request] = []

    @property
    def paths(self) -> list[str]:
        """Paths of the recorded requests.

        Returns:
            Paths of the recorded requests.
        """
        return [request.url.path for request in self.requests]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Record the request and delegate to the wrapped transport.

        Args:
            request: Outgoing HTTP request.

        Returns:
            Response from the wrapped transport.
        """
        self.requests.append(request)
        return await self._transport.handle_async_request(request)


def asgi_api_client(
    app: FastAPI,
    credential_store: CredentialStore | None = None,
    api_key: str | None = None,
    analytics_source: AnalyticsSource = AnalyticsSource.PYTHON,
) -> KitaruAPIClient:
    """Build an SDK client routed to the app instead of the network.

    Args:
        app: Application to route requests to.
        credential_store: Store holding the credentials the client
            authenticates with.
        api_key: Bearer token sent with every request.
        analytics_source: Client sending the requests.

    Returns:
        Client wired to an ASGI transport.
    """
    client = KitaruAPIClient(
        base_url="http://test",
        credential_store=credential_store,
        api_key=api_key,
        analytics_source=analytics_source,
    )
    client._http = httpx.AsyncClient(
        transport=RetryTransport(httpx.ASGITransport(app=app)),
        base_url="http://test",
        headers=client._http.headers,
    )
    return client


def recording_asgi_api_client(
    app: FastAPI,
    credential_store: CredentialStore | None = None,
) -> tuple[KitaruAPIClient, RequestRecordingTransport]:
    """Build an SDK client routed to the app, recording every request.

    Args:
        app: Application to route requests to.
        credential_store: Store holding the credentials the client
            authenticates with.

    Returns:
        Client wired to an ASGI transport, and the transport recording
        requests.
    """
    client = KitaruAPIClient(base_url="http://test", credential_store=credential_store)
    recorder = RequestRecordingTransport(httpx.ASGITransport(app=app))
    client._http = httpx.AsyncClient(
        transport=RetryTransport(recorder),
        base_url="http://test",
        headers=client._http.headers,
    )
    return client, recorder


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
async def account(account_repository: FakeAccountRepository) -> Account:
    """Provide a stored account test resources are owned by."""
    return await account_repository.create(Account(name="ann"))


@pytest.fixture
def auth_service(account_repository: FakeAccountRepository) -> AuthService:
    """Provide an authentication service backed by the fake account repository."""
    return AuthService(
        settings=local_settings(),
        account_repository=account_repository,
        api_key_repository=FakeApiKeyRepository(),
        password_hasher=FakePasswordHasher(),
    )
