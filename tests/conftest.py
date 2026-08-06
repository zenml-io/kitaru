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
import sys
import uuid
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple, Protocol, TypeVar

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

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.info import AuthScheme
from kitaru.api_models.v1.investigation import InvestigationSessionStatus
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionOrigin, TokenUsage
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.api_models.v1.worker import WorkerRuntime, WorkerScope
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneClient,
    ControlPlaneError,
    ControlPlaneUser,
)
from kitaru.server.adapters.db.orm.base import Base
from kitaru.server.api.app import create_app
from kitaru.server.api.composition import register_subscribers
from kitaru.server.api.config import APISettings
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.application.models.annotation import AnnotationFilter
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.auth import (
    GrantKind,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.cohort import CohortFilter, CohortVersionFilter
from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.application.models.investigation import (
    InvestigationFilter,
    InvestigationSessionFilter,
)
from kitaru.server.application.models.job import JobFilter
from kitaru.server.application.models.plugin import PluginFilter, PluginVersionFilter
from kitaru.server.application.models.replay import ReplayFilter, ReplayStatusCounts
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.models.session_node import SessionNodeFilter
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.application.models.task import TaskFilter, TaskPolicy
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.base import ListFilter
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)
from kitaru.server.domain.agent import (
    Agent,
    AgentInUse,
    AgentNotFound,
    DuplicateAgentName,
)
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.annotation import (
    Annotation,
    AnnotationNotFound,
)
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
    encode_api_key,
)
from kitaru.server.domain.blob import Blob, BlobInUse, BlobNotFound
from kitaru.server.domain.cohort import Cohort, CohortNotFound, DuplicateCohortName
from kitaru.server.domain.cohort_version import (
    CohortVersion,
    CohortVersionIdNotFound,
    CohortVersionInUse,
    CohortVersionNotFound,
)
from kitaru.server.domain.device import Device, DeviceNotFound, DeviceStatus
from kitaru.server.domain.evaluation import Evaluation, EvaluationNotFound
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentInUse,
    ExperimentNotFound,
)
from kitaru.server.domain.experiment_run import (
    DuplicateExperimentRunNumber,
    ExperimentRun,
    ExperimentRunNotFound,
)
from kitaru.server.domain.investigation import (
    Investigation,
    InvestigationNotFound,
    InvestigationSession,
    InvestigationSessionNotFound,
)
from kitaru.server.domain.job import Job, JobNotFound
from kitaru.server.domain.keys import generate_secret, hash_secret
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginInUse,
    PluginKind,
    PluginNotFound,
    PluginSource,
    PluginVersion,
    PluginVersionIdNotFound,
    PluginVersionNotFound,
    ScriptPluginSource,
)
from kitaru.server.domain.replay import (
    DuplicateReplayForBaseline,
    Replay,
    ReplayAlreadyExistsForJob,
    ReplayNotFound,
)
from kitaru.server.domain.replay_config import (
    ReplayConfig,
    ReplayConfigInUse,
    ReplayConfigNotFound,
)
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretNotFound,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionInUse,
    SessionNotFound,
    SessionRollups,
)
from kitaru.server.domain.session_node import SessionNode
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
)
from kitaru.server.domain.task import (
    AgentTask,
    DuplicateEvaluationTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskNotFound,
)
from kitaru.server.domain.worker import Worker, WorkerNotFound
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    FilterExpression,
    NotExpression,
    OrExpression,
)
from kitaru.transport import RetryTransport

# Why: test modules import shared fakes with a bare `from conftest import ...`.
# A subdirectory conftest module would shadow that name on sys.path in subset
# runs, so register this module under the bare name and keep this file the
# only conftest in the tree.
sys.modules.setdefault("conftest", sys.modules[__name__])

TEST_DB_PREFIX = "kitaru_test"


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
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))


def asgi_api_client(
    app: FastAPI,
    credential_store: CredentialStore | None = None,
    api_key: str | None = None,
) -> KitaruAPIClient:
    """Build an SDK client routed to the app instead of the network.

    Args:
        app: Application to route requests to.
        credential_store: Store holding the credentials the client
            authenticates with.
        api_key: Bearer token sent with every request.

    Returns:
        Client wired to an ASGI transport.
    """
    client = KitaruAPIClient(
        base_url="http://test",
        credential_store=credential_store,
        api_key=api_key,
    )
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


def _evaluate_filter_expression(
    item: Any,
    expression: FilterExpression,
    resolvers: Mapping[str, Callable[[Any, FilterCondition], bool]] | None = None,
) -> bool:
    """Evaluate a filter expression against a domain object.

    Args:
        item: Domain object to evaluate the expression against.
        expression: Filter expression.
        resolvers: Condition evaluators keyed by field name, for fields that
            are not attributes of the object.

    Returns:
        Whether the object matches the expression.
    """
    if isinstance(expression, AndExpression):
        return all(
            _evaluate_filter_expression(item, operand, resolvers)
            for operand in expression.operands
        )
    if isinstance(expression, OrExpression):
        return any(
            _evaluate_filter_expression(item, operand, resolvers)
            for operand in expression.operands
        )
    if isinstance(expression, NotExpression):
        return not _evaluate_filter_expression(item, expression.operand, resolvers)
    assert isinstance(expression, FilterCondition)
    if resolvers is not None and expression.field in resolvers:
        return resolvers[expression.field](item, expression)
    value = getattr(item, expression.field)
    if expression.op is FilterOp.IS_NULL:
        return value is None
    if value is None:
        # Comparisons with a null column value never match, mirroring SQL.
        return False
    match expression.op:
        case FilterOp.EQ:
            return value == expression.value
        case FilterOp.NE:
            return value != expression.value
        case FilterOp.LT:
            return value < expression.value
        case FilterOp.LE:
            return value <= expression.value
        case FilterOp.GT:
            return value > expression.value
        case FilterOp.GE:
            return value >= expression.value
        case FilterOp.IN:
            return value in expression.value
        case FilterOp.STARTSWITH:
            return value.startswith(expression.value)
        case FilterOp.ENDSWITH:
            return value.endswith(expression.value)
        case FilterOp.CONTAINS:
            return expression.value in value


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
        if account_filter.expression is not None:
            accounts = [
                account
                for account in accounts
                if _evaluate_filter_expression(account, account_filter.expression)
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

    async def get(self, api_key_id: uuid.UUID, exclusive: bool = False) -> ApiKey:
        """Load an API key by id.

        Args:
            api_key_id: Id of the API key.
            exclusive: Ignored, the fake has no concurrent callers to lock
                against.

        Raises:
            ApiKeyNotFound: No API key has this id.

        Returns:
            Stored API key.
        """
        _ = exclusive
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
        if api_key_filter.owner_id is not None:
            api_keys = [
                api_key
                for api_key in api_keys
                if api_key.owner_id == api_key_filter.owner_id
            ]
        if api_key_filter.expression is not None:
            api_keys = [
                api_key
                for api_key in api_keys
                if _evaluate_filter_expression(api_key, api_key_filter.expression)
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


def mint_worker_token(
    auth_service: AuthService, worker_id: uuid.UUID, account: Account
) -> str:
    """Mint a worker token for a worker registered under the given account.

    Args:
        auth_service: Authentication service backing the app.
        worker_id: Id of the worker the token is scoped to.
        account: Account the worker registered under.

    Returns:
        Encoded worker bearer token.
    """
    return auth_service.issue_worker_token(
        worker_id=worker_id, account_id=account.id
    ).token


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
        if device_filter.expression is not None:
            devices = [
                device
                for device in devices
                if _evaluate_filter_expression(device, device_filter.expression)
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
        if secret_filter.expression is not None:
            secrets = [
                secret
                for secret in secrets
                if _evaluate_filter_expression(secret, secret_filter.expression)
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


class FakeAgentRepository:
    """In-memory agent repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._agents: dict[uuid.UUID, Agent] = {}
        self._agent_versions: FakeAgentVersionRepository | None = None

    def _check_duplicate_name(self, agent: Agent) -> None:
        for other in self._agents.values():
            if other.id != agent.id and other.name == agent.name:
                raise DuplicateAgentName(agent.name)

    def increment_latest_version(self, agent_id: uuid.UUID) -> int:
        """Bump and return the agent's version counter.

        Mirrors the SQL repository's ``UPDATE ... RETURNING`` bump.

        Args:
            agent_id: Id of the agent to bump.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            New version number.
        """
        agent = self._agents.get(agent_id)
        if agent is None:
            raise AgentNotFound(agent_id)
        agent.latest_version += 1
        return agent.latest_version

    async def create(self, agent: Agent) -> Agent:
        """Persist a new agent.

        Args:
            agent: Agent to store.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with timestamps set.
        """
        self._check_duplicate_name(agent)
        now = datetime.now(UTC)
        stored = agent.model_copy(update={"created": now, "updated": now})
        self._agents[stored.id] = stored
        return stored.model_copy()

    async def get(self, agent_id: uuid.UUID) -> Agent:
        """Load an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Stored agent.
        """
        agent = self._agents.get(agent_id)
        if agent is None:
            raise AgentNotFound(agent_id)
        return agent.model_copy()

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], str | None]:
        """Query agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.

        Returns:
            Page of matching agents and the next cursor.
        """
        agents = list(self._agents.values())
        if agent_filter.expression is not None:
            agents = [
                agent
                for agent in agents
                if _evaluate_filter_expression(agent, agent_filter.expression)
            ]
        page, next_cursor = _paginate_fake(agents, agent_filter)
        return [agent.model_copy() for agent in page], next_cursor

    async def update(self, agent: Agent) -> Agent:
        """Persist changes to an existing agent.

        Args:
            agent: Agent with modified fields.

        Raises:
            AgentNotFound: No agent has this id.
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with the updated timestamp renewed.
        """
        stored = self._agents.get(agent.id)
        if stored is None:
            raise AgentNotFound(agent.id)
        self._check_duplicate_name(agent)
        now = _renewed_timestamp(stored.updated)
        updated = agent.model_copy(update={"created": stored.created, "updated": now})
        self._agents[agent.id] = updated
        return updated.model_copy()

    async def delete(self, agent_id: uuid.UUID) -> None:
        """Delete an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.
            AgentInUse: The agent has versions and cannot be deleted.
        """
        if agent_id not in self._agents:
            raise AgentNotFound(agent_id)
        if self._agent_versions is not None and any(
            version.agent_id == agent_id
            for version in self._agent_versions._versions.values()
        ):
            raise AgentInUse(agent_id)
        del self._agents[agent_id]


async def create_agent(
    repository: FakeAgentRepository,
    owner_id: uuid.UUID,
    name: str = "assistant",
    description: str | None = None,
) -> Agent:
    """Store an agent in the fake repository.

    Args:
        repository: Fake agent repository.
        owner_id: Id of the owning account.
        name: Agent name.
        description: Agent description.

    Returns:
        Stored agent.
    """
    return await repository.create(
        Agent(owner_id=owner_id, name=name, description=description)
    )


class FakeAgentVersionRepository:
    """In-memory agent version repository."""

    def __init__(
        self, agents: FakeAgentRepository, tags: "FakeTagRepository | None" = None
    ) -> None:
        """Initialize the repository.

        Args:
            agents: Fake agent repository sharing the version counter. Also
                wired back onto the agent repository so its delete can check
                for versions.
            tags: Fake tag repository, consulted by the ``tag`` filter.
        """
        self._agents = agents
        self._agents._agent_versions = self
        self._versions: dict[uuid.UUID, AgentVersion] = {}
        self._tags = tags

    async def create(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        Args:
            agent_version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the given agent id.

        Returns:
            Stored agent version with its assigned version number and
            timestamps set.
        """
        version_number = self._agents.increment_latest_version(agent_version.agent_id)
        now = datetime.now(UTC)
        stored = agent_version.model_copy(
            update={"version": version_number, "created": now, "updated": now}
        )
        self._versions[stored.id] = stored
        return stored.model_copy()

    async def get(self, agent_version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        agent_version = self._versions.get(agent_version_id)
        if agent_version is None:
            raise AgentVersionNotFound(agent_version_id)
        return agent_version.model_copy()

    async def get_agent_id(self, agent_version_id: uuid.UUID) -> uuid.UUID:
        """Load the id of the agent a version belongs to.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Id of the owning agent.
        """
        agent_version = self._versions.get(agent_version_id)
        if agent_version is None:
            raise AgentVersionNotFound(agent_version_id)
        return agent_version.agent_id

    def _agent_version_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of agent versions linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of agent versions linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id
            and link.resource_type == TagResourceType.AGENT_VERSION
        }

    async def query(
        self, agent_version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], str | None]:
        """Query agent versions matching a filter.

        Args:
            agent_version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the next cursor.
        """
        versions = list(self._versions.values())
        if agent_version_filter.agent_id is not None:
            versions = [
                version
                for version in versions
                if version.agent_id == agent_version_filter.agent_id
            ]
        if agent_version_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            versions = [
                version
                for version in versions
                if _evaluate_filter_expression(
                    version, agent_version_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(versions, agent_version_filter)
        return [version.model_copy() for version in page], next_cursor

    def _evaluate_tag_condition(
        self, agent_version: AgentVersion, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against an agent version.

        Args:
            agent_version: Agent version to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the agent version has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            agent_version.id in self._agent_version_ids_tagged(name) for name in names
        )

    async def update(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Args:
            agent_version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        stored = self._versions.get(agent_version.id)
        if stored is None:
            raise AgentVersionNotFound(agent_version.id)
        now = _renewed_timestamp(stored.updated)
        updated = agent_version.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._versions[agent_version.id] = updated
        return updated.model_copy()

    async def delete(self, agent_version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
        """
        if agent_version_id not in self._versions:
            raise AgentVersionNotFound(agent_version_id)
        del self._versions[agent_version_id]


async def create_agent_version(
    repository: FakeAgentVersionRepository,
    agent_id: uuid.UUID,
    owner_id: uuid.UUID,
    display_version: str | None = None,
    description: str | None = None,
    run_spec: RunSpec | None = None,
    capabilities: AgentCapabilities | None = None,
) -> AgentVersion:
    """Store an agent version in the fake repository.

    Args:
        repository: Fake agent version repository.
        agent_id: Id of the agent this version belongs to.
        owner_id: Id of the owning account.
        display_version: Human-readable designator.
        description: Version description.
        run_spec: Run spec.
        capabilities: Agent capabilities, empty when omitted.

    Returns:
        Stored agent version.
    """
    return await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            display_version=display_version,
            description=description,
            run_spec=run_spec,
            capabilities=capabilities
            if capabilities is not None
            else AgentCapabilities(),
        )
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
        if tag_filter.expression is not None:
            tags = [
                tag
                for tag in tags
                if _evaluate_filter_expression(tag, tag_filter.expression)
            ]
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

    def has_link(
        self,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
        tag_name: str,
    ) -> bool:
        """Report whether a resource carries a tag, mirroring the tag EXISTS join.

        Args:
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.
            tag_name: Tag name to look up.

        Returns:
            Whether a link ties the resource to a tag with this name.
        """
        tag_ids = {tag.id for tag in self._tags.values() if tag.name == tag_name}
        return any(
            link.resource_type == resource_type
            and link.resource_id == resource_id
            and link.tag_id in tag_ids
            for link in self._links.values()
        )


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


class FakeSessionRepository:
    """In-memory session repository."""

    def __init__(
        self,
        tags: FakeTagRepository | None = None,
        evaluations: "FakeEvaluationRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            tags: Fake tag repository, consulted by the ``tag`` filter.
            evaluations: Fake evaluation repository, consulted by the
                ``has_evaluation`` filter. Defaults to an empty, unwired
                repository so the filter always matches something sensible.
        """
        self._sessions: dict[uuid.UUID, Session] = {}
        self._tags = tags
        self._evaluations = (
            evaluations if evaluations is not None else FakeEvaluationRepository()
        )
        self._cohort_membership_counts: dict[uuid.UUID, int] = {}
        self._cohort_versions: FakeCohortVersionRepository | None = None
        self._session_numbers: dict[uuid.UUID, int] = {}

    def _mark_cohort_member(self, session_id: uuid.UUID) -> None:
        """Record that one more cohort version references this session.

        Mirrors the SQL repository's restricting foreign key from
        ``cohort_version_session.session_id``.

        Args:
            session_id: Id of the session.
        """
        self._cohort_membership_counts[session_id] = (
            self._cohort_membership_counts.get(session_id, 0) + 1
        )

    def _unmark_cohort_member(self, session_id: uuid.UUID) -> None:
        """Record that one fewer cohort version references this session.

        Args:
            session_id: Id of the session.
        """
        self._cohort_membership_counts[session_id] -= 1

    def _check_duplicate_external_id(self, session: Session) -> None:
        if session.imported_from is None or session.external_id is None:
            return
        for other in self._sessions.values():
            if (
                other.id != session.id
                and other.imported_from == session.imported_from
                and other.external_id == session.external_id
            ):
                raise DuplicateSessionExternalId(
                    session.imported_from, session.external_id
                )

    async def allocate_session_number(self, agent_id: uuid.UUID) -> int:
        """Bump the agent's session counter and return the new value.

        Mirrors the SQL repository's ``UPDATE ... RETURNING`` bump.

        Args:
            agent_id: Id of the agent to bump.

        Returns:
            New session number.
        """
        self._session_numbers[agent_id] = self._session_numbers.get(agent_id, 0) + 1
        return self._session_numbers[agent_id]

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        self._check_duplicate_external_id(session)
        now = datetime.now(UTC)
        stored = session.model_copy(update={"created": now, "updated": now})
        self._sessions[stored.id] = stored
        return stored.model_copy()

    async def get(self, session_id: uuid.UUID, exclusive: bool = False) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.
            exclusive: Ignored, the fake has no concurrent callers to lock
                against.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        _ = exclusive
        session = self._sessions.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return session.model_copy()

    def _session_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of sessions linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of sessions linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id and link.resource_type == TagResourceType.SESSION
        }

    def _session_ids_in_cohort_version(
        self, cohort_version_id: uuid.UUID
    ) -> set[uuid.UUID]:
        """Resolve the ids of sessions that are members of a cohort version.

        Args:
            cohort_version_id: Id of the cohort version.

        Returns:
            Ids of member sessions.
        """
        if self._cohort_versions is None:
            return set()
        return set(self._cohort_versions._members.get(cohort_version_id, []))

    async def query(
        self, session_filter: SessionFilter
    ) -> tuple[list[Session], str | None]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching sessions and the next cursor.
        """
        sessions = list(self._sessions.values())
        if session_filter.expression is not None:
            resolvers = {
                "tag": self._evaluate_tag_condition,
                "cohort_version_id": self._evaluate_cohort_version_condition,
                "has_evaluation": self._evaluate_has_evaluation_condition,
            }
            sessions = [
                s
                for s in sessions
                if _evaluate_filter_expression(s, session_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(sessions, session_filter)
        return [s.model_copy() for s in page], next_cursor

    def _evaluate_tag_condition(
        self, session: Session, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against a session.

        Args:
            session: Session to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the session has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(session.id in self._session_ids_tagged(name) for name in names)

    def _evaluate_cohort_version_condition(
        self, session: Session, condition: FilterCondition
    ) -> bool:
        """Evaluate a cohort version condition against a session.

        Args:
            session: Session to evaluate.
            condition: Validated cohort version condition.

        Returns:
            Whether the session belongs to a matching cohort version.
        """
        ids = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            session.id in self._session_ids_in_cohort_version(version_id)
            for version_id in ids
        )

    def _evaluate_has_evaluation_condition(
        self, session: Session, condition: FilterCondition
    ) -> bool:
        """Evaluate a has-evaluation condition against a session.

        Args:
            session: Session to evaluate.
            condition: Validated has-evaluation condition.

        Returns:
            Whether the session's evaluation state matches the condition.
        """
        expected = (
            condition.value if condition.op is FilterOp.EQ else not condition.value
        )
        return self._evaluations.has_evaluation(session.id) == expected

    async def get_many(
        self, session_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Session]:
        """Bulk-load sessions by id, keyed by id, missing ids omitted.

        Args:
            session_ids: Ids of the sessions to load.

        Returns:
            Stored sessions keyed by id.
        """
        return {
            session_id: self._sessions[session_id].model_copy()
            for session_id in session_ids
            if session_id in self._sessions
        }

    async def update(self, session: Session) -> Session:
        """Persist changes to an existing session.

        Args:
            session: Session with modified fields.

        Raises:
            SessionNotFound: No session has this id.
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Stored session with the updated timestamp renewed.
        """
        stored = self._sessions.get(session.id)
        if stored is None:
            raise SessionNotFound(session.id)
        self._check_duplicate_external_id(session)
        now = _renewed_timestamp(stored.updated)
        updated = session.model_copy(update={"created": stored.created, "updated": now})
        self._sessions[session.id] = updated
        return updated.model_copy()

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session belongs to a cohort version and cannot
                be deleted.
        """
        if session_id not in self._sessions:
            raise SessionNotFound(session_id)
        if self._cohort_membership_counts.get(session_id, 0) > 0:
            raise SessionInUse(session_id)
        del self._sessions[session_id]

    async def apply_rollups(
        self, session_id: uuid.UUID, deltas: SessionRollups
    ) -> None:
        """Apply rollup deltas to a session's cost, tokens, and call counts.

        Args:
            session_id: Id of the session.
            deltas: Rollup deltas to add.

        Raises:
            SessionNotFound: No session has this id.
        """
        stored = self._sessions.get(session_id)
        if stored is None:
            raise SessionNotFound(session_id)
        tokens = stored.tokens if stored.tokens is not None else TokenUsage()
        new_tokens = TokenUsage(
            input_tokens=(tokens.input_tokens or 0) + deltas.input_tokens,
            output_tokens=(tokens.output_tokens or 0) + deltas.output_tokens,
            cached_input_tokens=(tokens.cached_input_tokens or 0)
            + deltas.cached_input_tokens,
            reasoning_tokens=(tokens.reasoning_tokens or 0) + deltas.reasoning_tokens,
        )
        now = _renewed_timestamp(stored.updated)
        updated = stored.model_copy(
            update={
                "cost": (stored.cost if stored.cost is not None else Decimal(0))
                + deltas.cost,
                "tokens": new_tokens,
                "llm_call_count": stored.llm_call_count + deltas.llm_call_count,
                "tool_call_count": stored.tool_call_count + deltas.tool_call_count,
                "updated": now,
            }
        )
        self._sessions[session_id] = updated


async def create_session(
    repository: FakeSessionRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    **overrides: Any,
) -> Session:
    """Store a session in the fake repository.

    Args:
        repository: Fake session repository.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the session belongs to.
        **overrides: Additional session fields.

    Returns:
        Stored session.
    """
    values: dict[str, Any] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "origin": SessionOrigin.RECORDED,
    }
    values.update(overrides)
    if "number" not in values:
        values["number"] = await repository.allocate_session_number(values["agent_id"])
    return await repository.create(Session(**values))


def _paginate_fake_by_index(
    items: list[ListItemT],
    list_filter: ListFilter,
    index: Callable[[ListItemT], int],
) -> tuple[list[ListItemT], str | None]:
    """Apply index-ascending cursor pagination to an in-memory list.

    Args:
        items: Candidate domain objects, already scoped by the caller.
        list_filter: Filter carrying the cursor and size.
        index: Item index accessor.

    Returns:
        Page of matching items and the next cursor.
    """
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    ordered = sorted(items, key=index)
    if cursor is not None:
        last_index = int(cursor.id)
        ordered = [item for item in ordered if index(item) > last_index]

    page = ordered[: list_filter.size + 1]
    next_cursor = None
    if len(page) > list_filter.size:
        page = page[: list_filter.size]
        next_cursor = encode_cursor(list_filter.sort, str(index(page[-1])), filter_hash)
    return page, next_cursor


class FakeSessionNodeRepository:
    """In-memory session node repository."""

    def __init__(
        self,
        sessions: "FakeSessionRepository | None" = None,
        cohort_versions: "FakeCohortVersionRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            sessions: Fake session repository, consulted by the agent-scope
                history search.
            cohort_versions: Fake cohort version repository, consulted by
                the cohort-version-scope history search.
        """
        self._nodes: dict[uuid.UUID, SessionNode] = {}
        self._sessions = sessions
        self._cohort_versions = cohort_versions

    async def get_by_indexes(
        self, session_id: uuid.UUID, indexes: Sequence[int]
    ) -> dict[int, SessionNode]:
        """Bulk-load the stored nodes of a session at the given indexes.

        Args:
            session_id: Id of the owning session.
            indexes: Indexes to load.

        Returns:
            Stored nodes keyed by index, missing indexes omitted.
        """
        wanted = set(indexes)
        return {
            node.index: node.model_copy()
            for node in self._nodes.values()
            if node.session_id == session_id and node.index in wanted
        }

    async def upsert_batch(
        self, session_id: uuid.UUID, nodes: list[SessionNode]
    ) -> list[SessionNode]:
        """Insert or replace nodes upserted on (session, index).

        Args:
            session_id: Id of the owning session.
            nodes: Fully resolved nodes to store, in batch order.

        Returns:
            Stored nodes in batch order.
        """
        _ = session_id
        stored: list[SessionNode] = []
        for node in nodes:
            existing = self._nodes.get(node.id)
            now = datetime.now(UTC)
            created = existing.created if existing is not None else now
            updated = (
                _renewed_timestamp(existing.updated) if existing is not None else now
            )
            row = node.model_copy(update={"created": created, "updated": updated})
            self._nodes[node.id] = row
            stored.append(row.model_copy())
        return stored

    async def query(
        self, session_node_filter: SessionNodeFilter
    ) -> tuple[list[SessionNode], str | None]:
        """Query the nodes of a session, ordered by index ascending.

        Args:
            session_node_filter: Filter and pagination parameters.

        Returns:
            Page of matching nodes and the next cursor.
        """
        nodes = [
            node
            for node in self._nodes.values()
            if node.session_id == session_node_filter.session_id
        ]
        page, next_cursor = _paginate_fake_by_index(
            nodes, session_node_filter, lambda node: node.index
        )
        result = []
        for node in page:
            if session_node_filter.include_payloads:
                result.append(node.model_copy())
            else:
                result.append(
                    node.model_copy(
                        update={"inputs": None, "outputs": None, "attributes": None}
                    )
                )
        return result, next_cursor

    async def get_index_by_id(self, session_id: uuid.UUID) -> dict[uuid.UUID, int]:
        """Bulk-load the index of every node in a session, keyed by node id.

        Args:
            session_id: Id of the owning session.

        Returns:
            Every node id in the session mapped to its index.
        """
        return {
            node.id: node.index
            for node in self._nodes.values()
            if node.session_id == session_id
        }

    def _newest_match(self, candidates: list[SessionNode]) -> SessionNode | None:
        """Pick the highest-id node from a candidate list.

        Args:
            candidates: Matching nodes.

        Returns:
            Highest-id node, or ``None`` when the list is empty.
        """
        if not candidates:
            return None
        return max(candidates, key=lambda node: node.id).model_copy()

    async def find_latest_by_cache_key_in_session(
        self, session_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest node with a cache key within one session.

        Args:
            session_id: Id of the session to search.
            cache_key: Tool call cache key to match.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        return self._newest_match(
            [
                node
                for node in self._nodes.values()
                if node.session_id == session_id and node.cache_key == cache_key
            ]
        )

    async def find_latest_by_cache_key_in_agent(
        self, agent_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest node with a cache key across an agent's recorded history.

        Args:
            agent_id: Id of the agent to search.
            cache_key: Tool call cache key to match.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        assert self._sessions is not None
        session_ids = {
            session.id
            for session in self._sessions._sessions.values()
            if session.agent_id == agent_id
            and session.origin in (SessionOrigin.RECORDED, SessionOrigin.IMPORTED)
        }
        return self._newest_match(
            [
                node
                for node in self._nodes.values()
                if node.session_id in session_ids and node.cache_key == cache_key
            ]
        )

    async def find_latest_by_cache_key_in_cohort_version(
        self, cohort_version_id: uuid.UUID, cache_key: str
    ) -> SessionNode | None:
        """Find the newest node with a cache key across a cohort version's sessions.

        Args:
            cohort_version_id: Id of the cohort version to search.
            cache_key: Tool call cache key to match.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        assert self._cohort_versions is not None
        session_ids = set(self._cohort_versions._members.get(cohort_version_id, []))
        return self._newest_match(
            [
                node
                for node in self._nodes.values()
                if node.session_id in session_ids and node.cache_key == cache_key
            ]
        )


class FakeCohortRepository:
    """In-memory cohort repository."""

    def __init__(self, tags: FakeTagRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            tags: Fake tag repository, consulted by the ``tag`` filter.
        """
        self._cohorts: dict[uuid.UUID, Cohort] = {}
        self._tags = tags

    def _check_duplicate_name(self, cohort: Cohort) -> None:
        for other in self._cohorts.values():
            if other.id != cohort.id and other.name == cohort.name:
                raise DuplicateCohortName(cohort.name)

    def increment_latest_version(self, cohort_id: uuid.UUID) -> int:
        """Bump and return the cohort's version counter.

        Mirrors the SQL repository's ``UPDATE ... RETURNING`` bump.

        Args:
            cohort_id: Id of the cohort to bump.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            New version number.
        """
        cohort = self._cohorts.get(cohort_id)
        if cohort is None:
            raise CohortNotFound(cohort_id)
        cohort.latest_version += 1
        return cohort.latest_version

    async def create(self, cohort: Cohort) -> Cohort:
        """Persist a new cohort.

        Args:
            cohort: Cohort to store.

        Raises:
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with timestamps set.
        """
        self._check_duplicate_name(cohort)
        now = datetime.now(UTC)
        stored = cohort.model_copy(update={"created": now, "updated": now})
        self._cohorts[stored.id] = stored
        return stored.model_copy()

    async def get(self, cohort_id: uuid.UUID, exclusive: bool = False) -> Cohort:
        """Load a cohort by id.

        Args:
            cohort_id: Id of the cohort.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        cohort = self._cohorts.get(cohort_id)
        if cohort is None:
            raise CohortNotFound(cohort_id)
        return cohort.model_copy()

    def _cohort_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of cohorts linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of cohorts linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id and link.resource_type == TagResourceType.COHORT
        }

    async def query(
        self, cohort_filter: CohortFilter
    ) -> tuple[list[Cohort], str | None]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the next cursor.
        """
        cohorts = list(self._cohorts.values())
        if cohort_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            cohorts = [
                c
                for c in cohorts
                if _evaluate_filter_expression(c, cohort_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(cohorts, cohort_filter)
        return [c.model_copy() for c in page], next_cursor

    def _evaluate_tag_condition(
        self, cohort: Cohort, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against a cohort.

        Args:
            cohort: Cohort to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the cohort has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(cohort.id in self._cohort_ids_tagged(name) for name in names)

    async def update(self, cohort: Cohort) -> Cohort:
        """Persist changes to an existing cohort.

        Args:
            cohort: Cohort with modified fields.

        Raises:
            CohortNotFound: No cohort has this id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with the updated timestamp renewed.
        """
        stored = self._cohorts.get(cohort.id)
        if stored is None:
            raise CohortNotFound(cohort.id)
        self._check_duplicate_name(cohort)
        now = _renewed_timestamp(stored.updated)
        updated = cohort.model_copy(update={"created": stored.created, "updated": now})
        self._cohorts[cohort.id] = updated
        return updated.model_copy()

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort by id.

        Deleting a cohort cascades its versions.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        if cohort_id not in self._cohorts:
            raise CohortNotFound(cohort_id)
        del self._cohorts[cohort_id]


async def create_cohort(
    repository: FakeCohortRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    name: str = "cohort",
    description: str | None = None,
) -> Cohort:
    """Store a cohort in the fake repository.

    Args:
        repository: Fake cohort repository.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the cohort's sessions belong to.
        name: Cohort name.
        description: Cohort description.

    Returns:
        Stored cohort.
    """
    return await repository.create(
        Cohort(
            owner_id=owner_id,
            name=name,
            description=description,
            agent_id=agent_id,
        )
    )


class FakeCohortVersionRepository:
    """In-memory cohort version repository."""

    def __init__(
        self,
        cohorts: FakeCohortRepository,
        sessions: FakeSessionRepository,
        experiment_runs: "FakeExperimentRunRepository | None" = None,
        tags: FakeTagRepository | None = None,
    ) -> None:
        """Initialize the repository.

        Args:
            cohorts: Fake cohort repository sharing the version counter.
            sessions: Fake session repository, to mark member sessions
                in-cohort-version. Also wired back onto the session
                repository so its query can resolve the
                ``cohort_version_id`` filter.
            experiment_runs: Fake experiment run repository, consulted by
                delete to check for an in-use version.
            tags: Fake tag repository, consulted by the ``tag`` filter.
        """
        self._cohorts = cohorts
        self._sessions = sessions
        self._sessions._cohort_versions = self
        self._experiment_runs = experiment_runs
        self._tags = tags
        self._versions: dict[uuid.UUID, CohortVersion] = {}
        self._members: dict[uuid.UUID, list[uuid.UUID]] = {}

    async def create(
        self, version: CohortVersion, session_ids: Sequence[uuid.UUID]
    ) -> CohortVersion:
        """Persist a new cohort version with a server-assigned version number.

        Args:
            version: Cohort version to store.
            session_ids: Ordered member session ids to link.

        Raises:
            CohortNotFound: No cohort has the version's cohort id.

        Returns:
            Stored cohort version with its assigned version number and
            timestamps set.
        """
        version_number = self._cohorts.increment_latest_version(version.cohort_id)
        now = datetime.now(UTC)
        stored = version.model_copy(
            update={"version": version_number, "created": now, "updated": now}
        )
        self._versions[stored.id] = stored
        self._members[stored.id] = list(session_ids)
        for session_id in session_ids:
            self._sessions._mark_cohort_member(session_id)
        return stored.model_copy()

    async def get(
        self, cohort_version_id: uuid.UUID, exclusive: bool = False
    ) -> CohortVersion:
        """Load a cohort version by id.

        Args:
            cohort_version_id: Id of the cohort version.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version.
        """
        version = self._versions.get(cohort_version_id)
        if version is None:
            raise CohortVersionIdNotFound(cohort_version_id)
        return version.model_copy()

    async def get_by_number(self, cohort_id: uuid.UUID, version: int) -> CohortVersion:
        """Load a cohort version by cohort id and version number.

        Args:
            cohort_id: Id of the cohort.
            version: Version number.

        Raises:
            CohortVersionNotFound: No version with this number exists for
                this cohort.

        Returns:
            Stored cohort version.
        """
        for stored in self._versions.values():
            if stored.cohort_id == cohort_id and stored.version == version:
                return stored.model_copy()
        raise CohortVersionNotFound(cohort_id, version)

    def _cohort_version_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of cohort versions linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of cohort versions linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id
            and link.resource_type == TagResourceType.COHORT_VERSION
        }

    async def query(
        self, version_filter: CohortVersionFilter
    ) -> tuple[list[CohortVersion], str | None]:
        """Query cohort versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohort versions and the next cursor.
        """
        versions = [
            version
            for version in self._versions.values()
            if version.cohort_id == version_filter.cohort_id
        ]
        if version_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            versions = [
                v
                for v in versions
                if _evaluate_filter_expression(v, version_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(versions, version_filter)
        return [version.model_copy() for version in page], next_cursor

    def _evaluate_tag_condition(
        self, version: CohortVersion, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against a cohort version.

        Args:
            version: Cohort version to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the cohort version has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            version.id in self._cohort_version_ids_tagged(name) for name in names
        )

    async def list_session_ids(self, cohort_version_id: uuid.UUID) -> list[uuid.UUID]:
        """List a version's member session ids, in order.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Ordered member session ids.
        """
        if cohort_version_id not in self._versions:
            raise CohortVersionIdNotFound(cohort_version_id)
        return list(self._members.get(cohort_version_id, []))

    async def update(self, version: CohortVersion) -> CohortVersion:
        """Persist changes to an existing cohort version.

        Args:
            version: Cohort version with modified fields.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version with the updated timestamp renewed.
        """
        stored = self._versions.get(version.id)
        if stored is None:
            raise CohortVersionIdNotFound(version.id)
        now = _renewed_timestamp(stored.updated)
        updated = version.model_copy(update={"created": stored.created, "updated": now})
        self._versions[version.id] = updated
        return updated.model_copy()

    async def delete(self, cohort_version_id: uuid.UUID) -> None:
        """Delete a cohort version by id, cascading its member links.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.
            CohortVersionInUse: An experiment run references this version.
        """
        if cohort_version_id not in self._versions:
            raise CohortVersionIdNotFound(cohort_version_id)
        if self._experiment_runs is not None and any(
            run.cohort_version_id == cohort_version_id
            for run in self._experiment_runs._runs.values()
        ):
            raise CohortVersionInUse(cohort_version_id)
        del self._versions[cohort_version_id]
        for session_id in self._members.pop(cohort_version_id, []):
            self._sessions._unmark_cohort_member(session_id)


async def create_cohort_version(
    repository: FakeCohortVersionRepository,
    owner_id: uuid.UUID,
    cohort_id: uuid.UUID,
    session_ids: Sequence[uuid.UUID] = (),
    display_version: str | None = None,
) -> CohortVersion:
    """Store a cohort version in the fake repository.

    Args:
        repository: Fake cohort version repository.
        owner_id: Id of the owning account.
        cohort_id: Id of the cohort this version belongs to.
        session_ids: Ordered member session ids.
        display_version: Human-readable designator.

    Returns:
        Stored cohort version.
    """
    return await repository.create(
        CohortVersion(
            owner_id=owner_id,
            cohort_id=cohort_id,
            display_version=display_version,
            session_count=len(session_ids),
        ),
        session_ids,
    )


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

    async def update_last_seen_at(self, worker_id: uuid.UUID, now: datetime) -> None:
        """Stamp the time the worker was last seen.

        Args:
            worker_id: Id of the worker.
            now: Current time.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            raise WorkerNotFound(worker_id)
        self._workers[worker_id] = worker.model_copy(
            update={
                "last_seen_at": now,
                "updated": _renewed_timestamp(worker.updated),
            }
        )

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
        if worker_filter.seen_after is not None:
            workers = [
                worker
                for worker in workers
                if worker.last_seen_at >= worker_filter.seen_after
            ]
        if worker_filter.expression is not None:
            workers = [
                worker
                for worker in workers
                if _evaluate_filter_expression(worker, worker_filter.expression)
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

    async def get_metadata(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob's metadata by id, leaving its content unloaded.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Blob with an empty content placeholder.
        """
        blob = self._blobs.get(blob_id)
        if blob is None:
            raise BlobNotFound(blob_id)
        return blob.model_copy(update={"data": b""})

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
        self._referenced_version_ids: set[uuid.UUID] = set()

    def mark_version_referenced(self, version_id: uuid.UUID) -> None:
        """Mark a plugin version as referenced by a stored evaluation.

        Mirrors the FK restrict, called by
        ``FakeEvaluationRepository.merge_session_evaluations``.

        Args:
            version_id: Id of the referenced plugin version.
        """
        self._referenced_version_ids.add(version_id)

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

    async def get_by_name(self, kind: PluginKind, name: str) -> Plugin:
        """Load a plugin by kind and name.

        Args:
            kind: Plugin kind.
            name: Plugin name.

        Raises:
            PluginNotFound: No plugin has this kind and name.

        Returns:
            Stored plugin.
        """
        for plugin in self._plugins.values():
            if plugin.kind == kind and plugin.name == name:
                return plugin.model_copy()
        raise PluginNotFound(name)

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
        if plugin_filter.expression is not None:
            plugins = [
                plugin
                for plugin in plugins
                if _evaluate_filter_expression(plugin, plugin_filter.expression)
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
            PluginInUse: A version is referenced by a stored evaluation.
        """
        if plugin_id not in self._plugins:
            raise PluginNotFound(plugin_id)
        stale_ids = [
            version_id
            for version_id, version in self._versions.items()
            if version.plugin_id == plugin_id
        ]
        for version_id in stale_ids:
            if version_id in self._referenced_version_ids:
                raise PluginInUse(plugin_id)
        del self._plugins[plugin_id]
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

    async def get_version_by_id(self, plugin_version_id: uuid.UUID) -> PluginVersion:
        """Load a plugin version by id.

        Args:
            plugin_version_id: Id of the plugin version.

        Raises:
            PluginVersionIdNotFound: No plugin version has this id.

        Returns:
            Stored plugin version.
        """
        stored = self._versions.get(plugin_version_id)
        if stored is None:
            raise PluginVersionIdNotFound(plugin_version_id)
        return stored.model_copy()

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


class FakeExperimentRepository:
    """In-memory experiment and replay config repository."""

    def __init__(
        self,
        tag_repository: FakeTagRepository | None = None,
        experiment_run_repository: "FakeExperimentRunRepository | None" = None,
        replay_repository: "FakeReplayRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            tag_repository: Tag repository, queried for the tag filter,
                mirroring the tag EXISTS join.
            experiment_run_repository: Experiment run repository, restricting
                the delete when the experiment has runs.
            replay_repository: Replay repository, restricting the replay
                config delete when a replay still references it.
        """
        self._experiments: dict[uuid.UUID, Experiment] = {}
        self._configs: dict[uuid.UUID, ReplayConfig] = {}
        self._tag_repository = tag_repository
        self._experiment_run_repository = experiment_run_repository
        self._replay_repository = replay_repository

    def _check_duplicate_name(self, experiment: Experiment) -> None:
        for other in self._experiments.values():
            if other.id != experiment.id and other.name == experiment.name:
                raise DuplicateExperimentName(experiment.name)

    async def create(self, experiment: Experiment) -> Experiment:
        """Persist a new experiment.

        Args:
            experiment: Experiment to store.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with timestamps set.
        """
        self._check_duplicate_name(experiment)
        now = datetime.now(UTC)
        stored = experiment.model_copy(update={"created": now, "updated": now})
        self._experiments[stored.id] = stored
        return stored.model_copy()

    async def get(
        self, experiment_id: uuid.UUID, exclusive: bool = False
    ) -> Experiment:
        """Load an experiment by id.

        Args:
            experiment_id: Id of the experiment.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment.
        """
        _ = exclusive
        experiment = self._experiments.get(experiment_id)
        if experiment is None:
            raise ExperimentNotFound(experiment_id)
        return experiment.model_copy()

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], str | None]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the next cursor.
        """
        experiments = list(self._experiments.values())
        if experiment_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            experiments = [
                experiment
                for experiment in experiments
                if _evaluate_filter_expression(
                    experiment, experiment_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(experiments, experiment_filter)
        return [experiment.model_copy() for experiment in page], next_cursor

    def _evaluate_tag_condition(
        self, experiment: Experiment, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against an experiment.

        Args:
            experiment: Experiment to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the experiment has a matching tag.
        """
        assert self._tag_repository is not None
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            self._tag_repository.has_link(
                TagResourceType.EXPERIMENT, experiment.id, name
            )
            for name in names
        )

    async def update(self, experiment: Experiment) -> Experiment:
        """Persist changes to an existing experiment.

        Args:
            experiment: Experiment with modified fields.

        Raises:
            ExperimentNotFound: No experiment has this id.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        stored = self._experiments.get(experiment.id)
        if stored is None:
            raise ExperimentNotFound(experiment.id)
        self._check_duplicate_name(experiment)
        now = _renewed_timestamp(stored.updated)
        updated = experiment.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._experiments[experiment.id] = updated
        return updated.model_copy()

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentInUse: The experiment has runs.
        """
        if experiment_id not in self._experiments:
            raise ExperimentNotFound(experiment_id)
        if self._experiment_run_repository is not None and any(
            run.experiment_id == experiment_id
            for run in self._experiment_run_repository._runs.values()
        ):
            raise ExperimentInUse(experiment_id)
        del self._experiments[experiment_id]

    async def create_replay_config(self, config: ReplayConfig) -> ReplayConfig:
        """Persist a new replay config.

        Args:
            config: Replay config to store.

        Returns:
            Stored replay config with timestamps set.
        """
        now = datetime.now(UTC)
        stored = config.model_copy(update={"created": now, "updated": now})
        self._configs[stored.id] = stored
        return stored.model_copy()

    async def get_replay_config(self, config_id: uuid.UUID) -> ReplayConfig:
        """Load a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored replay config.
        """
        config = self._configs.get(config_id)
        if config is None:
            raise ReplayConfigNotFound(config_id)
        return config.model_copy()

    async def get_many_replay_configs(
        self, config_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id, in one bulk fetch.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Replay configs keyed by id, missing ids omitted.
        """
        return {
            config_id: self._configs[config_id].model_copy()
            for config_id in config_ids
            if config_id in self._configs
        }

    async def delete_replay_config(self, config_id: uuid.UUID) -> None:
        """Delete a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.
            ReplayConfigInUse: A replay references the replay config.
        """
        if config_id not in self._configs:
            raise ReplayConfigNotFound(config_id)
        if self._replay_repository is not None and any(
            replay.replay_config_id == config_id
            for replay in self._replay_repository._replays.values()
        ):
            raise ReplayConfigInUse(config_id)
        del self._configs[config_id]


async def create_experiment(
    repository: FakeExperimentRepository,
    owner_id: uuid.UUID,
    replay_config_id: uuid.UUID,
    name: str = "smoke-test",
    description: str | None = None,
) -> Experiment:
    """Store an experiment in the fake repository.

    Args:
        repository: Fake experiment repository.
        owner_id: Id of the owning account.
        replay_config_id: Id of the experiment's replay config.
        name: Experiment name.
        description: Experiment description.

    Returns:
        Stored experiment.
    """
    return await repository.create(
        Experiment(
            owner_id=owner_id,
            name=name,
            description=description,
            replay_config_id=replay_config_id,
        )
    )


class FakeReplayRepository:
    """In-memory replay repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._replays: dict[uuid.UUID, Replay] = {}

    def _check_duplicate_baseline(self, replay: Replay) -> None:
        for other in self._replays.values():
            if (
                other.id != replay.id
                and other.experiment_run_id is not None
                and other.experiment_run_id == replay.experiment_run_id
                and other.baseline_session_id == replay.baseline_session_id
            ):
                raise DuplicateReplayForBaseline(
                    replay.experiment_run_id, replay.baseline_session_id
                )

    def _check_unique_job(self, replay: Replay) -> None:
        for other in self._replays.values():
            if other.id != replay.id and other.job_id == replay.job_id:
                raise ReplayAlreadyExistsForJob(replay.job_id)

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            DuplicateReplayForBaseline: The run already holds a replay for
                this baseline session.
            ReplayAlreadyExistsForJob: The job already has a replay.

        Returns:
            Stored replay with timestamps set.
        """
        self._check_duplicate_baseline(replay)
        self._check_unique_job(replay)
        now = datetime.now(UTC)
        stored = replay.model_copy(update={"created": now, "updated": now})
        self._replays[stored.id] = stored
        return stored.model_copy()

    async def create_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist many new replays in one round trip, skipping constraint translation.

        Args:
            replays: Replays to store.

        Returns:
            Stored replays with timestamps set, in the same order.
        """
        return [await self.create(replay) for replay in replays]

    async def get(self, replay_id: uuid.UUID) -> Replay:
        """Load a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay.
        """
        replay = self._replays.get(replay_id)
        if replay is None:
            raise ReplayNotFound(replay_id)
        return replay.model_copy()

    async def get_by_job_id(self, job_id: uuid.UUID) -> Replay | None:
        """Load the replay owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored replay, or ``None`` when the job holds no replay.
        """
        for replay in self._replays.values():
            if replay.job_id == job_id:
                return replay.model_copy()
        return None

    async def get_many_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Replay]:
        """Bulk-load the replay of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Replays keyed by job id, jobs without a replay omitted.
        """
        job_id_set = set(job_ids)
        return {
            replay.job_id: replay.model_copy()
            for replay in self._replays.values()
            if replay.job_id in job_id_set
        }

    async def query(
        self, replay_filter: ReplayFilter
    ) -> tuple[list[Replay], str | None]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the next cursor.
        """
        replays = list(self._replays.values())
        if replay_filter.expression is not None:
            replays = [
                r
                for r in replays
                if _evaluate_filter_expression(r, replay_filter.expression)
            ]
        page, next_cursor = _paginate_fake(replays, replay_filter)
        return [r.model_copy() for r in page], next_cursor

    async def list_by_experiment_run(
        self, experiment_run_id: uuid.UUID
    ) -> list[Replay]:
        """Load every replay of an experiment run.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replays of the run, in creation order.
        """
        matches = [
            r
            for r in self._replays.values()
            if r.experiment_run_id == experiment_run_id
        ]
        return [r.model_copy() for r in sorted(matches, key=lambda r: r.id)]

    async def update(self, replay: Replay) -> Replay:
        """Persist changes to an existing replay.

        Args:
            replay: Replay with modified fields.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay with the updated timestamp renewed.
        """
        stored = self._replays.get(replay.id)
        if stored is None:
            raise ReplayNotFound(replay.id)
        now = _renewed_timestamp(stored.updated)
        updated = replay.model_copy(update={"created": stored.created, "updated": now})
        self._replays[replay.id] = updated
        return updated.model_copy()

    async def update_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist changes to many existing replays in one round trip.

        Args:
            replays: Replays with modified fields.

        Raises:
            ReplayNotFound: A replay id matches no replay.

        Returns:
            Stored replays with the updated timestamp renewed, in id order.
        """
        return [
            await self.update(replay)
            for replay in sorted(replays, key=lambda replay: replay.id)
        ]

    async def count_by_status(self, experiment_run_id: uuid.UUID) -> ReplayStatusCounts:
        """Count an experiment run's replays by status.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replay counts by status.
        """
        counts = await self.count_by_status_many([experiment_run_id])
        return counts.get(experiment_run_id, ReplayStatusCounts())

    async def count_by_status_many(
        self, experiment_run_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayStatusCounts]:
        """Bulk-count replays by status for many experiment runs.

        Args:
            experiment_run_ids: Ids of the runs.

        Returns:
            Replay status counts keyed by run id, missing ids holding zero
            counts.
        """
        run_ids = set(experiment_run_ids)
        tallies: dict[uuid.UUID, dict[ReplayStatus, int]] = {}
        for replay in self._replays.values():
            if replay.experiment_run_id not in run_ids:
                continue
            tally = tallies.setdefault(replay.experiment_run_id, {})
            tally[replay.status] = tally.get(replay.status, 0) + 1
        return {
            run_id: ReplayStatusCounts(
                pending=tally.get(ReplayStatus.PENDING, 0),
                evaluating=tally.get(ReplayStatus.EVALUATING, 0),
                completed=tally.get(ReplayStatus.COMPLETED, 0),
                failed=tally.get(ReplayStatus.FAILED, 0),
                canceled=tally.get(ReplayStatus.CANCELED, 0),
            )
            for run_id, tally in tallies.items()
        }

    async def exists_for_replay_config(self, replay_config_id: uuid.UUID) -> bool:
        """Report whether any replay references a replay config.

        Args:
            replay_config_id: Id of the replay config.

        Returns:
            Whether a replay references the replay config.
        """
        return any(
            replay.replay_config_id == replay_config_id
            for replay in self._replays.values()
        )


async def create_replay(
    repository: FakeReplayRepository,
    owner_id: uuid.UUID,
    job_id: uuid.UUID,
    replay_config_id: uuid.UUID,
    baseline_session_id: uuid.UUID,
    experiment_run_id: uuid.UUID | None = None,
    evaluate_baselines: bool = False,
    status: ReplayStatus = ReplayStatus.PENDING,
) -> Replay:
    """Store a replay in the fake repository.

    Args:
        repository: Fake replay repository.
        owner_id: Id of the owning account.
        job_id: Id of the job running the replay.
        replay_config_id: Id of the replay's config.
        baseline_session_id: Id of the session replayed.
        experiment_run_id: Run this replay belongs to, ``None`` for a
            standalone replay.
        evaluate_baselines: Whether the baseline session is also scored.
        status: Replay status.

    Returns:
        Stored replay.
    """
    return await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=job_id,
            experiment_run_id=experiment_run_id,
            replay_config_id=replay_config_id,
            baseline_session_id=baseline_session_id,
            evaluate_baselines=evaluate_baselines,
            status=status,
        )
    )


class FakeExperimentRunRepository:
    """In-memory experiment run repository."""

    def __init__(self, tag_repository: FakeTagRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            tag_repository: Tag repository, queried for the tag filter,
                mirroring the tag EXISTS join.
        """
        self._runs: dict[uuid.UUID, ExperimentRun] = {}
        self._tag_repository = tag_repository

    async def create(self, run: ExperimentRun) -> ExperimentRun:
        """Persist a new experiment run.

        Args:
            run: Experiment run to store.

        Raises:
            DuplicateExperimentRunNumber: The experiment already has a run
                with this number.

        Returns:
            Stored experiment run with timestamps set.
        """
        for other in self._runs.values():
            if (
                other.id != run.id
                and other.experiment_id == run.experiment_id
                and other.number == run.number
            ):
                raise DuplicateExperimentRunNumber(run.experiment_id, run.number)
        now = datetime.now(UTC)
        stored = run.model_copy(update={"created": now, "updated": now})
        self._runs[stored.id] = stored
        return stored.model_copy()

    async def get(
        self, experiment_run_id: uuid.UUID, exclusive: bool = False
    ) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            experiment_run_id: Id of the run.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run.
        """
        _ = exclusive
        run = self._runs.get(experiment_run_id)
        if run is None:
            raise ExperimentRunNotFound(experiment_run_id)
        return run.model_copy()

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], str | None]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching runs and the next cursor.
        """
        runs = list(self._runs.values())
        if run_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            runs = [
                r
                for r in runs
                if _evaluate_filter_expression(r, run_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(runs, run_filter)
        return [r.model_copy() for r in page], next_cursor

    def _evaluate_tag_condition(
        self, run: ExperimentRun, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against an experiment run.

        Args:
            run: Experiment run to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the run has a matching tag.
        """
        assert self._tag_repository is not None
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            self._tag_repository.has_link(TagResourceType.EXPERIMENT_RUN, run.id, name)
            for name in names
        )

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run with the updated timestamp renewed.
        """
        stored = self._runs.get(run.id)
        if stored is None:
            raise ExperimentRunNotFound(run.id)
        now = _renewed_timestamp(stored.updated)
        updated = run.model_copy(update={"created": stored.created, "updated": now})
        self._runs[run.id] = updated
        return updated.model_copy()

    async def delete(self, experiment_run_id: uuid.UUID) -> None:
        """Delete an experiment run by id.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            ExperimentRunNotFound: No run has this id.
        """
        if experiment_run_id not in self._runs:
            raise ExperimentRunNotFound(experiment_run_id)
        del self._runs[experiment_run_id]

    async def get_max_number(self, experiment_id: uuid.UUID) -> int:
        """Read the highest run number an experiment has assigned.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Highest assigned run number, or 0 when the experiment has no runs.
        """
        numbers = [
            run.number
            for run in self._runs.values()
            if run.experiment_id == experiment_id
        ]
        return max(numbers, default=0)

    async def exists_for_experiment(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has any run.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Whether the experiment has any run.
        """
        return any(run.experiment_id == experiment_id for run in self._runs.values())


async def create_experiment_run(
    repository: FakeExperimentRunRepository,
    owner_id: uuid.UUID,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    number: int = 1,
    evaluate_baselines: bool = False,
    status: ExperimentRunStatus = ExperimentRunStatus.RUNNING,
) -> ExperimentRun:
    """Store an experiment run in the fake repository.

    Args:
        repository: Fake experiment run repository.
        owner_id: Id of the owning account.
        experiment_id: Id of the experiment this run belongs to.
        cohort_version_id: Id of the cohort version whose sessions are
            replayed.
        agent_version_id: Id of the agent version to replay with.
        number: Run number within the experiment.
        evaluate_baselines: Whether baseline sessions are also scored.
        status: Run status.

    Returns:
        Stored experiment run.
    """
    return await repository.create(
        ExperimentRun(
            owner_id=owner_id,
            experiment_id=experiment_id,
            number=number,
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            evaluate_baselines=evaluate_baselines,
            status=status,
        )
    )


class FakeEvaluationRepository:
    """In-memory evaluation repository."""

    def __init__(self, plugin_repository: FakePluginRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            plugin_repository: Fake plugin repository, consulted to
                denormalize the evaluator name and version.
        """
        self._evaluations: dict[uuid.UUID, Evaluation] = {}
        self._plugin_repository = plugin_repository

    def _evaluator_info(
        self, evaluator_version_id: uuid.UUID | None
    ) -> tuple[str | None, int | None]:
        """Resolve the evaluator name and version for a plugin version id.

        Args:
            evaluator_version_id: Id of the referenced plugin version.

        Returns:
            Evaluator name and version, both ``None`` when unresolved.
        """
        if evaluator_version_id is None or self._plugin_repository is None:
            return None, None
        version = self._plugin_repository._versions.get(evaluator_version_id)
        if version is None:
            return None, None
        plugin = self._plugin_repository._plugins.get(version.plugin_id)
        if plugin is None:
            return None, None
        return plugin.name, version.version

    async def get(self, evaluation_id: uuid.UUID) -> EvaluationWithEvaluator:
        """Load an evaluation by id, joined with its evaluator name and version.

        Args:
            evaluation_id: Id of the evaluation.

        Raises:
            EvaluationNotFound: No evaluation has this id.

        Returns:
            Stored evaluation paired with its evaluator name and version.
        """
        evaluation = self._evaluations.get(evaluation_id)
        if evaluation is None:
            raise EvaluationNotFound(evaluation_id)
        name, version = self._evaluator_info(evaluation.evaluator_version_id)
        return EvaluationWithEvaluator(evaluation.model_copy(), name, version)

    async def query(
        self, evaluation_filter: EvaluationFilter
    ) -> tuple[list[EvaluationWithEvaluator], str | None]:
        """Query evaluations matching a filter.

        Args:
            evaluation_filter: Filter and pagination parameters.

        Returns:
            Page of matching evaluations and the next cursor.
        """
        evaluations = list(self._evaluations.values())
        if evaluation_filter.expression is not None:
            evaluations = [
                e
                for e in evaluations
                if _evaluate_filter_expression(e, evaluation_filter.expression)
            ]
        page, next_cursor = _paginate_fake(evaluations, evaluation_filter)
        items = [
            EvaluationWithEvaluator(
                evaluation.model_copy(),
                *self._evaluator_info(evaluation.evaluator_version_id),
            )
            for evaluation in page
        ]
        return items, next_cursor

    async def merge_session_evaluations(
        self, session_id: uuid.UUID, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert or replace manual evaluations upserted on (session, name).

        Args:
            session_id: Id of the session the evaluations belong to.
            evaluations: Fully resolved evaluations to store, in request
                order.

        Returns:
            Stored evaluations in request order.
        """
        _ = session_id
        stored: list[Evaluation] = []
        for evaluation in evaluations:
            existing = next(
                (
                    e
                    for e in self._evaluations.values()
                    if e.session_id == evaluation.session_id
                    and e.task_id is None
                    and e.name == evaluation.name
                ),
                None,
            )
            now = datetime.now(UTC)
            if existing is None:
                row = evaluation.model_copy(update={"created": now, "updated": now})
            else:
                row = evaluation.model_copy(
                    update={
                        "id": existing.id,
                        "owner_id": existing.owner_id,
                        "created": existing.created,
                        "updated": _renewed_timestamp(existing.updated),
                    }
                )
            self._evaluations[row.id] = row
            stored.append(row.model_copy())
            if (
                row.evaluator_version_id is not None
                and self._plugin_repository is not None
            ):
                self._plugin_repository.mark_version_referenced(
                    row.evaluator_version_id
                )
        return stored

    def has_evaluation(self, session_id: uuid.UUID) -> bool:
        """Report whether a session has at least one stored evaluation.

        Mirrors the EXISTS probe the SQL repository runs against the
        evaluation table.

        Args:
            session_id: Id of the session.

        Returns:
            Whether the session has at least one stored evaluation.
        """
        return any(
            evaluation.session_id == session_id
            for evaluation in self._evaluations.values()
        )

    async def create_task_evaluations(
        self, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert evaluation rows produced by a completed evaluator task.

        Args:
            evaluations: Fully resolved evaluations to store, in result order.

        Returns:
            Stored evaluations in result order.
        """
        stored: list[Evaluation] = []
        for evaluation in evaluations:
            now = datetime.now(UTC)
            row = evaluation.model_copy(update={"created": now, "updated": now})
            self._evaluations[row.id] = row
            stored.append(row.model_copy())
            if (
                row.evaluator_version_id is not None
                and self._plugin_repository is not None
            ):
                self._plugin_repository.mark_version_referenced(
                    row.evaluator_version_id
                )
        return stored


async def create_evaluation(
    repository: FakeEvaluationRepository,
    owner_id: uuid.UUID,
    session_id: uuid.UUID,
    name: str = "accuracy",
    data_type: EvaluationDataType = EvaluationDataType.FLOAT,
    score: float | bool | None = 0.9,
    value: str | None = None,
    explanation: str | None = None,
    evaluator_version_id: uuid.UUID | None = None,
) -> Evaluation:
    """Store an evaluation in the fake repository through its merge upsert.

    Args:
        repository: Fake evaluation repository.
        owner_id: Id of the owning account.
        session_id: Id of the session being scored.
        name: Evaluation name.
        data_type: Data type of the result.
        score: Numeric or boolean score.
        value: Label or string value.
        explanation: Free-form explanation.
        evaluator_version_id: Evaluator version that produced the result.

    Returns:
        Stored evaluation.
    """
    evaluation = Evaluation(
        owner_id=owner_id,
        evaluator_version_id=evaluator_version_id,
        session_id=session_id,
        name=name,
        data_type=data_type,
        score=score,
        value=value,
        explanation=explanation,
    )
    stored = await repository.merge_session_evaluations(session_id, [evaluation])
    return stored[0]


class FakeJobRepository:
    """In-memory job repository."""

    def __init__(self, tasks: "FakeTaskRepository | None" = None) -> None:
        """Initialize the repository.

        Args:
            tasks: Fake task repository, cascaded on delete.
        """
        self._jobs: dict[uuid.UUID, Job] = {}
        self._tasks = tasks

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Returns:
            Stored job with timestamps set.
        """
        now = datetime.now(UTC)
        stored = job.model_copy(update={"created": now, "updated": now})
        self._jobs[stored.id] = stored
        return stored.model_copy()

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist many new jobs in one round trip.

        Args:
            jobs: Jobs to store.

        Returns:
            Stored jobs with timestamps set, in the same order.
        """
        return [await self.create(job) for job in jobs]

    async def get(self, job_id: uuid.UUID, exclusive: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        _ = exclusive
        job = self._jobs.get(job_id)
        if job is None:
            raise JobNotFound(job_id)
        return job.model_copy()

    async def get_many(self, job_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Job]:
        """Bulk-load jobs by id, keyed by id, missing ids omitted.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Stored jobs keyed by id.
        """
        return {
            job_id: self._jobs[job_id].model_copy()
            for job_id in job_ids
            if job_id in self._jobs
        }

    async def get_many_locked(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Job]:
        """Bulk-lock and load jobs by id, keyed by id, missing ids omitted.

        Row locking has no in-memory counterpart, a single process never
        contends with itself.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Locked jobs keyed by id.
        """
        return await self.get_many(job_ids)

    async def list_unpropagated_cancel_ids(self, limit: int) -> list[uuid.UUID]:
        """Read the ids of canceling jobs whose live tasks still owe the stamp.

        Args:
            limit: Maximum number of ids to read.

        Returns:
            Ids of the canceling jobs in ascending order.
        """
        if self._tasks is None:
            return []
        owing: list[uuid.UUID] = []
        for job_id in sorted(self._jobs):
            job = self._jobs[job_id]
            if job.cancel_requested_at is None or job.settled:
                continue
            tasks = await self._tasks.list_by_job(job_id)
            if any(
                not task.terminal
                and (
                    task.cancel_requested_at is None
                    or task.status is TaskStatus.PENDING
                )
                for task in tasks
            ):
                owing.append(job_id)
        return owing[:limit]

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], str | None]:
        """Query jobs matching a filter.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the next cursor.
        """
        jobs = list(self._jobs.values())
        if job_filter.expression is not None:
            jobs = [
                job
                for job in jobs
                if _evaluate_filter_expression(job, job_filter.expression)
            ]
        page, next_cursor = _paginate_fake(jobs, job_filter)
        return [job.model_copy() for job in page], next_cursor

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        stored = self._jobs.get(job.id)
        if stored is None:
            raise JobNotFound(job.id)
        renewed = job.model_copy(
            update={
                "created": stored.created,
                "updated": _renewed_timestamp(stored.updated),
            }
        )
        self._jobs[job.id] = renewed
        return renewed.model_copy()

    async def update_many(self, jobs: list[Job]) -> list[Job]:
        """Persist changes to many existing jobs in one round trip.

        Args:
            jobs: Jobs with modified fields.

        Raises:
            JobNotFound: A job id matches no job.

        Returns:
            Stored jobs with the updated timestamp renewed, in the same order.
        """
        return [await self.update(job) for job in jobs]

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id, cascading its tasks.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        if job_id not in self._jobs:
            raise JobNotFound(job_id)
        del self._jobs[job_id]
        if self._tasks is not None:
            self._tasks.cascade_job_delete(job_id)

    async def delete_many(self, job_ids: Sequence[uuid.UUID]) -> None:
        """Delete many jobs by id, cascading their tasks.

        Args:
            job_ids: Ids of the jobs.

        Raises:
            JobNotFound: A job id matches no job.
        """
        for job_id in sorted(job_ids):
            await self.delete(job_id)


class FakeTaskRepository:
    """In-memory task repository."""

    def __init__(self, sessions: FakeSessionRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            sessions: Fake session repository, marked so a linked session
                cannot be deleted, mirroring the restricting foreign keys.
        """
        self._tasks: dict[uuid.UUID, Task] = {}
        self._sessions = sessions
        # Assigned after construction, since the fake job repository takes
        # this repository in its own constructor.
        self.jobs: FakeJobRepository | None = None

    def _check_evaluator_pair(self, task: Task) -> None:
        """Mirror the unique (job_id, input_session_id, plugin_version_id) key.

        Args:
            task: Task about to be stored.

        Raises:
            DuplicateEvaluationTask: The job already scores this pair.
        """
        if not isinstance(task, EvaluationTask):
            return
        for other in self._tasks.values():
            if not isinstance(other, EvaluationTask) or other.id == task.id:
                continue
            if (
                other.job_id == task.job_id
                and other.input_session_id == task.input_session_id
                and other.plugin_version_id == task.plugin_version_id
            ):
                raise DuplicateEvaluationTask(
                    task.job_id, task.input_session_id, task.plugin_version_id
                )

    async def create(self, task: Task) -> Task:
        """Persist a new task.

        Args:
            task: Task to store.

        Raises:
            DuplicateEvaluationTask: The job already holds an evaluator task
                for this input session and plugin version.

        Returns:
            Stored task with timestamps set.
        """
        self._check_evaluator_pair(task)
        now = datetime.now(UTC)
        stored = task.model_copy(update={"created": now, "updated": now})
        self._tasks[stored.id] = stored
        return stored.model_copy()

    async def create_many(self, tasks: list[Task]) -> list[Task]:
        """Persist many new tasks in one round trip.

        Args:
            tasks: Tasks to store.

        Returns:
            Stored tasks with timestamps set, in the same order.
        """
        return [await self.create(task) for task in tasks]

    async def get(self, task_id: uuid.UUID, exclusive: bool = False) -> Task:
        """Load a task by id.

        Args:
            task_id: Id of the task.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task.
        """
        _ = exclusive
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFound(task_id)
        return task.model_copy()

    async def get_many(self, task_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Task]:
        """Bulk-load tasks by id, keyed by id, missing ids omitted.

        Args:
            task_ids: Ids of the tasks to load.

        Returns:
            Stored tasks keyed by id.
        """
        return {
            task_id: self._tasks[task_id].model_copy()
            for task_id in task_ids
            if task_id in self._tasks
        }

    def _matches_scope(self, task: Task, scope: WorkerScope) -> bool:
        """Report whether a task matches a claim scope.

        Args:
            task: Candidate task.
            scope: Claim scope narrowing the queue.

        Returns:
            Whether every scope term matches.
        """
        if scope.kinds and task.kind not in scope.kinds:
            return False
        if scope.job_id is not None and task.job_id != scope.job_id:
            return False
        for selector in scope.selectors or []:
            if selector.key not in task.labels:
                if selector.required:
                    return False
                continue
            if task.labels[selector.key] not in selector.values:
                return False
        return True

    async def query(self, task_filter: TaskFilter) -> tuple[list[Task], str | None]:
        """Query tasks matching a filter.

        Args:
            task_filter: Filter and pagination parameters.

        Returns:
            Page of matching tasks and the next cursor.
        """
        tasks = list(self._tasks.values())
        if task_filter.job_id is not None:
            tasks = [task for task in tasks if task.job_id == task_filter.job_id]
        if task_filter.stale_before is not None:
            bound = task_filter.stale_before
            tasks = [task for task in tasks if _is_stale_before(task, bound)]
        if task_filter.expression is not None:
            tasks = [
                task
                for task in tasks
                if _evaluate_filter_expression(task, task_filter.expression)
            ]
        page, next_cursor = _paginate_fake(tasks, task_filter)
        return [task.model_copy() for task in page], next_cursor

    async def list_by_job(self, job_id: uuid.UUID) -> list[Task]:
        """Load every task of a job, ordered by id.

        Args:
            job_id: Id the tasks belong to.

        Returns:
            Tasks of the job in creation order.
        """
        tasks = [task for task in self._tasks.values() if task.job_id == job_id]
        return [task.model_copy() for task in sorted(tasks, key=lambda task: task.id)]

    async def list_by_jobs(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, list[Task]]:
        """Bulk-load every task of many jobs, grouped by job id in creation order.

        Args:
            job_ids: Ids the tasks belong to.

        Returns:
            Tasks keyed by job id in creation order, jobs without tasks
            omitted.
        """
        job_id_set = set(job_ids)
        tasks_by_job: dict[uuid.UUID, list[Task]] = {}
        for task in sorted(self._tasks.values(), key=lambda task: task.id):
            if task.job_id in job_id_set:
                tasks_by_job.setdefault(task.job_id, []).append(task.model_copy())
        return tasks_by_job

    async def update(self, task: Task) -> Task:
        """Persist changes to an existing task.

        Args:
            task: Task with modified fields.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task with the updated timestamp renewed.
        """
        stored = self._tasks.get(task.id)
        if stored is None:
            raise TaskNotFound(task.id)
        renewed = task.model_copy(
            update={
                "created": stored.created,
                "updated": _renewed_timestamp(stored.updated),
            }
        )
        self._tasks[task.id] = renewed
        return renewed.model_copy()

    async def claim_pending(
        self, scope: WorkerScope, worker_id: uuid.UUID, limit: int, now: datetime
    ) -> list[Task]:
        """Hand pending tasks matching a scope to a worker, oldest first.

        Row locking has no in-memory counterpart, a single process never
        contends with itself.

        Args:
            scope: Claim scope narrowing the queue.
            worker_id: Worker claiming the tasks.
            limit: Maximum number of tasks to claim.
            now: Current time.

        Returns:
            Claimed tasks carrying their incremented attempt.
        """
        candidates = sorted(
            (
                task
                for task in self._tasks.values()
                if task.status is TaskStatus.PENDING
                and self._matches_scope(task, scope)
            ),
            key=lambda task: task.id,
        )
        claimed: list[Task] = []
        for task in candidates[:limit]:
            claimed_task = task.model_copy()
            claimed_task.claim(worker_id, now)
            claimed.append(await self.update(claimed_task))
        return claimed

    async def claim_stale(self, task_id: uuid.UUID, cutoff: datetime) -> Task | None:
        """Lock one task by id if it is still in flight and older than a cutoff.

        Args:
            task_id: Id of the candidate task.
            cutoff: Bound the last heartbeat must be older than.

        Returns:
            Locked stale task, or ``None`` when it is no longer stale.
        """
        task = self._tasks.get(task_id)
        if task is None or task.status not in (TaskStatus.CLAIMED, TaskStatus.RUNNING):
            return None
        if not _is_stale_before(task, cutoff):
            return None
        return task.model_copy()

    async def list_stale_ids(self, cutoff: datetime, limit: int) -> list[uuid.UUID]:
        """Read the ids of in-flight tasks whose last heartbeat is older than a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of ids to read.

        Returns:
            Ids of the stale tasks in ascending order.
        """
        stale = sorted(
            (
                task.id
                for task in self._tasks.values()
                if task.status in (TaskStatus.CLAIMED, TaskStatus.RUNNING)
                and _is_stale_before(task, cutoff)
            ),
        )
        return stale[:limit]

    async def stamp_heartbeats(
        self, task_ids: Sequence[uuid.UUID], worker_id: uuid.UUID, now: datetime
    ) -> dict[uuid.UUID, datetime | None]:
        """Stamp heartbeat_at on the worker's in-flight tasks among the ids.

        Args:
            task_ids: Candidate task ids.
            worker_id: Worker that must still hold the tasks.
            now: Current time.

        Returns:
            Cancel request time of the task, falling back to its job's, by id
            for every stamped task.
        """
        stamped: dict[uuid.UUID, datetime | None] = {}
        for task_id in task_ids:
            task = self._tasks.get(task_id)
            if (
                task is None
                or task.worker_id != worker_id
                or task.status not in (TaskStatus.CLAIMED, TaskStatus.RUNNING)
            ):
                continue
            self._tasks[task_id] = task.model_copy(
                update={
                    "heartbeat_at": now,
                    "updated": _renewed_timestamp(task.updated),
                }
            )
            stamped[task_id] = task.cancel_requested_at
            if stamped[task_id] is None and self.jobs is not None:
                owner = (await self.jobs.get_many([task.job_id])).get(task.job_id)
                if owner is not None:
                    stamped[task_id] = owner.cancel_requested_at
        return stamped

    async def lock_by_jobs(
        self, job_ids: Sequence[uuid.UUID], nowait: bool = False
    ) -> None:
        """Lock the jobs' non-terminal task rows in id order.

        Row locking has no in-memory counterpart, a single process never
        contends with itself.

        Args:
            job_ids: Ids the tasks belong to.
            nowait: Whether to fail instead of waiting when another
                transaction holds one of the rows.
        """
        _ = job_ids, nowait

    async def stamp_cancel_requested(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> None:
        """Stamp cancel_requested_at on the jobs' non-terminal tasks lacking it.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.
        """
        for task_id, task in list(self._tasks.items()):
            if task.job_id not in job_ids or task.terminal:
                continue
            if task.cancel_requested_at is not None:
                continue
            self._tasks[task_id] = task.model_copy(
                update={
                    "cancel_requested_at": now,
                    "updated": _renewed_timestamp(task.updated),
                }
            )

    async def cancel_pending(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> list[Task]:
        """Move each still-pending task of the jobs straight to canceled.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.

        Returns:
            Canceled tasks.
        """
        canceled: list[Task] = []
        for task in sorted(self._tasks.values(), key=lambda task: task.id):
            if task.job_id not in job_ids or task.status is not TaskStatus.PENDING:
                continue
            canceled_task = task.model_copy()
            canceled_task.request_cancel(now)
            canceled.append(await self.update(canceled_task))
        return canceled

    def cascade_job_delete(self, job_id: uuid.UUID) -> None:
        """Drop the tasks of a deleted job, mirroring the cascading key.

        Args:
            job_id: Id of the deleted job.
        """
        for task_id, task in list(self._tasks.items()):
            if task.job_id == job_id:
                del self._tasks[task_id]

    async def get_scored_evaluator_version_ids(
        self, input_session_id: uuid.UUID
    ) -> set[uuid.UUID]:
        """Read the evaluator versions that already completed against a session.

        Args:
            input_session_id: Id of the scored session.

        Returns:
            Plugin version ids of every completed evaluator task scoring the
            session.
        """
        return {
            task.plugin_version_id
            for task in self._tasks.values()
            if isinstance(task, EvaluationTask)
            and task.input_session_id == input_session_id
            and task.status is TaskStatus.COMPLETED
        }

    async def get_scored_evaluator_version_ids_many(
        self, input_session_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, set[uuid.UUID]]:
        """Read the evaluator versions that already completed against each session.

        Args:
            input_session_ids: Ids of the scored sessions.

        Returns:
            Plugin version ids of every completed evaluator task scoring the
            session, keyed by session id, sessions without one omitted.
        """
        session_id_set = set(input_session_ids)
        scored: dict[uuid.UUID, set[uuid.UUID]] = {}
        for task in self._tasks.values():
            if (
                isinstance(task, EvaluationTask)
                and task.input_session_id in session_id_set
                and task.status is TaskStatus.COMPLETED
            ):
                scored.setdefault(task.input_session_id, set()).add(
                    task.plugin_version_id
                )
        return scored

    async def get_agent_tasks_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Task]:
        """Bulk-load the agent task of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Agent tasks keyed by job id, jobs without an agent task omitted.
        """
        job_id_set = set(job_ids)
        return {
            task.job_id: task.model_copy()
            for task in self._tasks.values()
            if isinstance(task, AgentTask) and task.job_id in job_id_set
        }


def _is_stale_before(task: Task, bound: datetime) -> bool:
    """Report whether a task last showed a sign of life before a bound.

    Args:
        task: Task to read.
        bound: Time the last sign of life must precede.

    Returns:
        Whether the task heartbeated, or was claimed, before the bound.
    """
    last_seen = task.heartbeat_at if task.heartbeat_at is not None else task.claimed_at
    return last_seen is not None and last_seen < bound


async def create_job(
    repository: FakeJobRepository,
    owner_id: uuid.UUID,
    kind: JobKind = JobKind.SESSION_RUN,
    status: JobStatus = JobStatus.PENDING,
) -> Job:
    """Store a job in the fake repository.

    Args:
        repository: Fake job repository.
        owner_id: Id of the owning account.
        kind: Job kind.
        status: Job status.

    Returns:
        Stored job.
    """
    return await repository.create(Job(owner_id=owner_id, kind=kind, status=status))


async def create_agent_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    agent_version_id: uuid.UUID | None = None,
    inputs: Any = None,
    labels: dict[str, str] | None = None,
    env: dict[str, str] | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.ABORT,
) -> AgentTask:
    """Store an agent task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        agent_version_id: Agent version the task runs.
        inputs: Inputs passed to the agent's command.
        labels: Labels matched by worker scope selectors.
        env: Creator-set process environment extras.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored agent task.
    """
    task = AgentTask(
        job_id=job_id,
        agent_version_id=(
            agent_version_id if agent_version_id is not None else uuid.uuid4()
        ),
        inputs=inputs,
        labels=labels if labels is not None else {},
        env=env if env is not None else {},
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, AgentTask)
    return stored


async def create_evaluation_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    plugin_version_id: uuid.UUID | None = None,
    input_session_id: uuid.UUID | None = None,
    params: dict[str, Any] | None = None,
    labels: dict[str, str] | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.CONTINUE,
) -> EvaluationTask:
    """Store an evaluator task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        plugin_version_id: Evaluator version the task runs.
        input_session_id: Session being scored.
        params: Parameters passed to the evaluator.
        labels: Labels matched by worker scope selectors.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored evaluator task.
    """
    task = EvaluationTask(
        job_id=job_id,
        plugin_version_id=(
            plugin_version_id if plugin_version_id is not None else uuid.uuid4()
        ),
        input_session_id=(
            input_session_id if input_session_id is not None else uuid.uuid4()
        ),
        params=params if params is not None else {},
        labels=labels if labels is not None else {},
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, EvaluationTask)
    return stored


async def create_import_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    plugin_version_id: uuid.UUID | None = None,
    payload_blob_id: uuid.UUID | None = None,
    agent_id: uuid.UUID | None = None,
    agent_version_id: uuid.UUID | None = None,
    params: dict[str, Any] | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.ABORT,
) -> ImportTask:
    """Store an importer task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        plugin_version_id: Importer version the task runs.
        payload_blob_id: Blob holding the payload.
        agent_id: Agent imported sessions are created under.
        agent_version_id: Agent version recorded on the imported sessions.
        params: Parameters passed to the importer.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored importer task.
    """
    task = ImportTask(
        job_id=job_id,
        plugin_version_id=(
            plugin_version_id if plugin_version_id is not None else uuid.uuid4()
        ),
        payload_blob_id=(
            payload_blob_id if payload_blob_id is not None else uuid.uuid4()
        ),
        agent_id=agent_id if agent_id is not None else uuid.uuid4(),
        agent_version_id=agent_version_id,
        params=params if params is not None else {},
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, ImportTask)
    return stored


class TaskSubstrate(NamedTuple):
    """Fake repositories shared by every job- and task-driving service builder."""

    sessions: FakeSessionRepository
    agents: FakeAgentRepository
    agent_versions: FakeAgentVersionRepository
    blobs: FakeBlobRepository
    plugins: FakePluginRepository
    secrets: FakeSecretRepository
    workers: FakeWorkerRepository
    tasks: FakeTaskRepository
    jobs: FakeJobRepository


def _build_task_substrate() -> TaskSubstrate:
    """Wire the fake repositories every job- and task-driving service needs.

    Returns:
        Fake repositories shared by job, task, and replay service builders.
    """
    sessions = FakeSessionRepository()
    agents = FakeAgentRepository()
    agent_versions = FakeAgentVersionRepository(agents)
    blobs = FakeBlobRepository()
    plugins = FakePluginRepository(blob_repository=blobs)
    secrets = FakeSecretRepository()
    workers = FakeWorkerRepository()
    tasks = FakeTaskRepository(sessions=sessions)
    jobs = FakeJobRepository(tasks=tasks)
    tasks.jobs = jobs
    return TaskSubstrate(
        sessions=sessions,
        agents=agents,
        agent_versions=agent_versions,
        blobs=blobs,
        plugins=plugins,
        secrets=secrets,
        workers=workers,
        tasks=tasks,
        jobs=jobs,
    )


class JobAndTaskServices(NamedTuple):
    """Job and task services sharing one set of fake repositories."""

    job_service: JobService
    task_service: TaskService
    jobs: FakeJobRepository
    tasks: FakeTaskRepository
    sessions: FakeSessionRepository
    agents: FakeAgentRepository
    agent_versions: FakeAgentVersionRepository
    plugins: FakePluginRepository
    blobs: FakeBlobRepository
    secrets: FakeSecretRepository
    workers: FakeWorkerRepository


def build_job_and_task_services(
    policy: TaskPolicy | None = None,
) -> JobAndTaskServices:
    """Wire fake-backed job and task services sharing one event dispatcher.

    Mirrors the production wiring in ``dependencies.py``: both services share
    one ``TaskTransitions`` dispatch, so a transition applied through one
    service is visible to the other's repositories.

    Args:
        policy: Task execution policy, defaults applied when omitted.

    Returns:
        Job and task services plus their backing fake repositories.
    """
    substrate = _build_task_substrate()
    transitions = TaskTransitions(
        task_repository=substrate.tasks,
        job_repository=substrate.jobs,
        dispatcher=EventDispatcher(),
    )
    task_policy = policy if policy is not None else TaskPolicy()
    spec_builder = TaskSpecBuilder(
        agent_version_repository=substrate.agent_versions,
        plugin_repository=substrate.plugins,
        blob_repository=substrate.blobs,
        secret_repository=substrate.secrets,
        replay_repository=FakeReplayRepository(),
        policy=task_policy,
    )
    task_service = TaskService(
        repository=substrate.tasks,
        worker_repository=substrate.workers,
        session_repository=substrate.sessions,
        job_repository=substrate.jobs,
        spec_builder=spec_builder,
        transitions=transitions,
        policy=task_policy,
    )
    job_service = JobService(
        repository=substrate.jobs,
        task_repository=substrate.tasks,
        session_repository=substrate.sessions,
        agent_repository=substrate.agents,
        agent_version_repository=substrate.agent_versions,
        plugin_repository=substrate.plugins,
        blob_repository=substrate.blobs,
        transitions=transitions,
        policy=task_policy,
    )
    return JobAndTaskServices(
        job_service=job_service,
        task_service=task_service,
        jobs=substrate.jobs,
        tasks=substrate.tasks,
        sessions=substrate.sessions,
        agents=substrate.agents,
        agent_versions=substrate.agent_versions,
        plugins=substrate.plugins,
        blobs=substrate.blobs,
        secrets=substrate.secrets,
        workers=substrate.workers,
    )


class ReplayServices(NamedTuple):
    """Replay, experiment, and experiment run services sharing one set of fakes."""

    experiment_service: ExperimentService
    experiment_run_service: ExperimentRunService
    replay_service: ReplayService
    job_service: JobService
    task_service: TaskService
    experiments: FakeExperimentRepository
    experiment_runs: FakeExperimentRunRepository
    replays: FakeReplayRepository
    jobs: FakeJobRepository
    tasks: FakeTaskRepository
    sessions: FakeSessionRepository
    session_nodes: FakeSessionNodeRepository
    agents: FakeAgentRepository
    agent_versions: FakeAgentVersionRepository
    cohorts: FakeCohortRepository
    cohort_versions: FakeCohortVersionRepository
    plugins: FakePluginRepository
    blobs: FakeBlobRepository
    secrets: FakeSecretRepository
    workers: FakeWorkerRepository
    evaluations: FakeEvaluationRepository
    tags: FakeTagRepository


def build_replay_services(policy: TaskPolicy | None = None) -> ReplayServices:
    """Wire fake-backed replay, experiment, and run services sharing one dispatcher.

    Mirrors the production wiring in ``composition.py`` and
    ``dependencies.py``: the event dispatcher carries the same subscribers
    registered on the request-scoped dispatcher, so a task transition applied
    through one service drives the replay pipeline and run finalization the
    same way it does in production.

    Args:
        policy: Task execution policy, defaults applied when omitted.

    Returns:
        Replay, experiment, and run services plus their backing fake
        repositories.
    """
    substrate = _build_task_substrate()
    sessions = substrate.sessions
    agents = substrate.agents
    agent_versions = substrate.agent_versions
    blobs = substrate.blobs
    plugins = substrate.plugins
    secrets = substrate.secrets
    workers = substrate.workers
    tasks = substrate.tasks
    jobs = substrate.jobs
    tags = FakeTagRepository()
    cohorts = FakeCohortRepository(tags=tags)
    replays = FakeReplayRepository()
    experiment_runs = FakeExperimentRunRepository(tag_repository=tags)
    cohort_versions = FakeCohortVersionRepository(
        cohorts=cohorts, sessions=sessions, experiment_runs=experiment_runs, tags=tags
    )
    session_nodes = FakeSessionNodeRepository(
        sessions=sessions, cohort_versions=cohort_versions
    )
    experiments = FakeExperimentRepository(
        tag_repository=tags,
        experiment_run_repository=experiment_runs,
        replay_repository=replays,
    )
    evaluations = FakeEvaluationRepository(plugin_repository=plugins)

    dispatcher = EventDispatcher()
    register_subscribers(
        dispatcher,
        job_repository=jobs,
        task_repository=tasks,
        replay_repository=replays,
        experiment_repository=experiments,
        experiment_run_repository=experiment_runs,
        evaluation_repository=evaluations,
    )
    transitions = TaskTransitions(
        task_repository=tasks, job_repository=jobs, dispatcher=dispatcher
    )
    task_policy = policy if policy is not None else TaskPolicy()
    spec_builder = TaskSpecBuilder(
        agent_version_repository=agent_versions,
        plugin_repository=plugins,
        blob_repository=blobs,
        secret_repository=secrets,
        replay_repository=replays,
        policy=task_policy,
    )
    task_service = TaskService(
        repository=tasks,
        worker_repository=workers,
        session_repository=sessions,
        job_repository=jobs,
        spec_builder=spec_builder,
        transitions=transitions,
        policy=task_policy,
    )
    job_service = JobService(
        repository=jobs,
        task_repository=tasks,
        session_repository=sessions,
        agent_repository=agents,
        agent_version_repository=agent_versions,
        plugin_repository=plugins,
        blob_repository=blobs,
        transitions=transitions,
        policy=task_policy,
    )
    experiment_service = ExperimentService(
        repository=experiments,
        plugin_repository=plugins,
        experiment_run_repository=experiment_runs,
        cohort_version_repository=cohort_versions,
        session_repository=sessions,
        agent_version_repository=agent_versions,
        replay_repository=replays,
        job_repository=jobs,
        task_repository=tasks,
    )
    replay_service = ReplayService(
        repository=replays,
        experiment_repository=experiments,
        experiment_run_repository=experiment_runs,
        job_repository=jobs,
        task_repository=tasks,
        session_repository=sessions,
        session_node_repository=session_nodes,
        agent_version_repository=agent_versions,
        plugin_repository=plugins,
    )
    experiment_run_service = ExperimentRunService(
        repository=experiment_runs,
        replay_repository=replays,
        job_repository=jobs,
        transitions=transitions,
    )
    return ReplayServices(
        experiment_service=experiment_service,
        experiment_run_service=experiment_run_service,
        replay_service=replay_service,
        job_service=job_service,
        task_service=task_service,
        experiments=experiments,
        experiment_runs=experiment_runs,
        replays=replays,
        jobs=jobs,
        tasks=tasks,
        sessions=sessions,
        session_nodes=session_nodes,
        agents=agents,
        agent_versions=agent_versions,
        cohorts=cohorts,
        cohort_versions=cohort_versions,
        plugins=plugins,
        blobs=blobs,
        secrets=secrets,
        workers=workers,
        evaluations=evaluations,
        tags=tags,
    )


def build_worker_actor(account: Account, worker_id: uuid.UUID) -> WorkerAuthContext:
    """Build a worker-principal context for a worker owned by the account.

    Args:
        account: Account the context acts as.
        worker_id: Id of the worker the principal claims to be.

    Returns:
        Context carrying a worker principal.
    """
    return WorkerAuthContext(
        account=account, principal=WorkerPrincipal(worker_id=worker_id)
    )


def build_task_actor(
    account: Account,
    task_id: uuid.UUID,
    attempt: int,
    worker_id: uuid.UUID,
    job_id: uuid.UUID | None = None,
    granted_session_ids: Sequence[uuid.UUID] = (),
) -> TaskAuthContext:
    """Build a task-principal context fenced by attempt.

    Args:
        account: Account the context acts as.
        task_id: Id of the task the principal claims to run.
        attempt: Attempt the principal is fenced by.
        worker_id: Id of the worker holding the attempt.
        job_id: Id of the job the task belongs to, defaulting to an unrelated
            job so job-scoped checks fail closed.
        granted_session_ids: Ids of the sessions the task reads without owning.

    Returns:
        Context carrying a task principal.
    """
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_session_ids:
        grants[GrantKind.SESSION] = frozenset(granted_session_ids)
    return TaskAuthContext(
        account=account,
        principal=TaskPrincipal(
            task_id=task_id,
            attempt=attempt,
            worker_id=worker_id,
            job_id=job_id if job_id is not None else uuid.uuid4(),
            grants=grants,
        ),
    )


class FakeInvestigationRepository:
    """In-memory investigation repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._investigations: dict[uuid.UUID, Investigation] = {}
        self._sessions: dict[uuid.UUID, InvestigationSession] = {}
        self._annotations: FakeAnnotationRepository | None = None

    def _counts(self, investigation_id: uuid.UUID) -> tuple[int, int]:
        """Count an investigation's linked and completed or skipped sessions.

        Args:
            investigation_id: Id of the investigation.

        Returns:
            Total and completed or skipped linked session counts.
        """
        links = [
            session
            for session in self._sessions.values()
            if session.investigation_id == investigation_id
        ]
        completed = sum(
            1
            for session in links
            if session.status is not InvestigationSessionStatus.PENDING
        )
        return len(links), completed

    def _with_counts(self, investigation: Investigation) -> Investigation:
        """Attach freshly computed progress counts to an investigation.

        Args:
            investigation: Investigation to annotate.

        Returns:
            Investigation with total_sessions and completed_sessions set.
        """
        total, completed = self._counts(investigation.id)
        return investigation.model_copy(
            update={"total_sessions": total, "completed_sessions": completed}
        )

    async def create(
        self, investigation: Investigation, sessions: Sequence[InvestigationSession]
    ) -> Investigation:
        """Persist a new investigation with its linked sessions.

        Args:
            investigation: Investigation to store.
            sessions: Ordered investigation_session rows to link.

        Returns:
            Stored investigation with timestamps set.
        """
        now = datetime.now(UTC)
        stored = investigation.model_copy(
            update={
                "created": now,
                "updated": now,
                "total_sessions": 0,
                "completed_sessions": 0,
            }
        )
        self._investigations[stored.id] = stored
        for session in sessions:
            self._sessions[session.id] = session.model_copy(
                update={"created": now, "updated": now}
            )
        return self._with_counts(stored)

    async def get(
        self, investigation_id: uuid.UUID, exclusive: bool = False
    ) -> Investigation:
        """Load an investigation by id.

        Args:
            investigation_id: Id of the investigation.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation.
        """
        _ = exclusive
        stored = self._investigations.get(investigation_id)
        if stored is None:
            raise InvestigationNotFound(investigation_id)
        return self._with_counts(stored)

    async def query(
        self, investigation_filter: InvestigationFilter
    ) -> tuple[list[Investigation], str | None]:
        """Query investigations matching a filter.

        Args:
            investigation_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigations and the next cursor.
        """
        investigations = list(self._investigations.values())
        if investigation_filter.expression is not None:
            investigations = [
                investigation
                for investigation in investigations
                if _evaluate_filter_expression(
                    investigation, investigation_filter.expression
                )
            ]
        page, next_cursor = _paginate_fake(investigations, investigation_filter)
        return [self._with_counts(investigation) for investigation in page], next_cursor

    async def update(self, investigation: Investigation) -> Investigation:
        """Persist changes to an existing investigation.

        Args:
            investigation: Investigation with modified fields.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation with the updated timestamp renewed.
        """
        stored = self._investigations.get(investigation.id)
        if stored is None:
            raise InvestigationNotFound(investigation.id)
        now = _renewed_timestamp(stored.updated)
        updated = investigation.model_copy(
            update={
                "created": stored.created,
                "updated": now,
                "total_sessions": 0,
                "completed_sessions": 0,
            }
        )
        self._investigations[investigation.id] = updated
        return self._with_counts(updated)

    async def delete(self, investigation_id: uuid.UUID) -> None:
        """Delete an investigation by id, cascading its links and answers.

        Args:
            investigation_id: Id of the investigation.

        Raises:
            InvestigationNotFound: No investigation has this id.
        """
        if investigation_id not in self._investigations:
            raise InvestigationNotFound(investigation_id)
        del self._investigations[investigation_id]
        cascaded_session_ids = {
            session.id
            for session in self._sessions.values()
            if session.investigation_id == investigation_id
        }
        for session_id in cascaded_session_ids:
            del self._sessions[session_id]
        if self._annotations is not None:
            self._annotations.cascade_investigation_session_delete(cascaded_session_ids)

    async def get_session(
        self, investigation_session_id: uuid.UUID, exclusive: bool = False
    ) -> InvestigationSession:
        """Load an investigation session by id.

        Args:
            investigation_session_id: Id of the investigation session.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session.
        """
        _ = exclusive
        stored = self._sessions.get(investigation_session_id)
        if stored is None:
            raise InvestigationSessionNotFound(investigation_session_id)
        return stored.model_copy()

    async def get_session_by_session_id(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        exclusive: bool = False,
    ) -> InvestigationSession:
        """Load an investigation session by investigation id and session id.

        Args:
            investigation_id: Id of the investigation.
            session_id: Id of the linked session.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            InvestigationSessionNotFound: No investigation session links this
                investigation and session.

        Returns:
            Stored investigation session.
        """
        _ = exclusive
        for stored in self._sessions.values():
            if (
                stored.investigation_id == investigation_id
                and stored.session_id == session_id
            ):
                return stored.model_copy()
        raise InvestigationSessionNotFound(session_id)

    async def query_sessions(
        self, session_filter: InvestigationSessionFilter
    ) -> tuple[list[InvestigationSession], str | None]:
        """Query an investigation's sessions, ordered by position ascending.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigation sessions and the next cursor.
        """
        sessions = [
            session
            for session in self._sessions.values()
            if session.investigation_id == session_filter.investigation_id
        ]
        if session_filter.expression is not None:
            sessions = [
                session
                for session in sessions
                if _evaluate_filter_expression(session, session_filter.expression)
            ]
        page, next_cursor = _paginate_fake_by_index(
            sessions, session_filter, lambda session: session.position
        )
        return [session.model_copy() for session in page], next_cursor

    async def update_session(
        self, session: InvestigationSession
    ) -> InvestigationSession:
        """Persist changes to an existing investigation session.

        Args:
            session: Investigation session with modified fields.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session with the updated timestamp renewed.
        """
        stored = self._sessions.get(session.id)
        if stored is None:
            raise InvestigationSessionNotFound(session.id)
        now = _renewed_timestamp(stored.updated)
        updated = session.model_copy(update={"created": stored.created, "updated": now})
        self._sessions[session.id] = updated
        return updated.model_copy()


class FakeAnnotationRepository:
    """In-memory annotation repository."""

    def __init__(
        self, investigations: FakeInvestigationRepository | None = None
    ) -> None:
        """Initialize the repository.

        Args:
            investigations: Fake investigation repository sharing session
                links, consulted by the investigation_id filter and wired
                back onto the investigation repository to cascade its
                deletes.
        """
        self._annotations: dict[uuid.UUID, Annotation] = {}
        self._investigations = investigations
        if investigations is not None:
            investigations._annotations = self

    def _find_upsert_target(self, annotation: Annotation) -> uuid.UUID | None:
        """Resolve the id of the row an insert would upsert onto.

        Manual annotations, and any insert missing either half of the
        (investigation_session_id, question_key) pair, never conflict,
        mirroring Postgres treating null as distinct within a unique index.

        Args:
            annotation: Annotation about to be inserted.

        Returns:
            Id of the conflicting row, or None when the insert is a plain
            create.
        """
        if (
            annotation.investigation_session_id is None
            or annotation.question_key is None
        ):
            return None
        for stored in self._annotations.values():
            if (
                stored.investigation_session_id == annotation.investigation_session_id
                and stored.question_key == annotation.question_key
            ):
                return stored.id
        return None

    async def create(self, annotation: Annotation) -> Annotation:
        """Persist a new annotation, upserting an investigation answer.

        An investigation answer upserts on (investigation_session_id,
        question_key), renewing the selector, value, and updated timestamp
        of the existing row. A manual annotation always inserts, since
        Postgres treats its null investigation_session_id as distinct from
        every other row.

        Args:
            annotation: Annotation to store.

        Returns:
            Stored annotation with timestamps set.
        """
        target_id = self._find_upsert_target(annotation)
        if target_id is not None:
            stored = self._annotations[target_id]
            updated = stored.model_copy(
                update={
                    "selector": annotation.selector,
                    "value": annotation.value,
                    "updated": _renewed_timestamp(stored.updated),
                }
            )
            self._annotations[target_id] = updated
            return updated.model_copy()
        now = datetime.now(UTC)
        stored = annotation.model_copy(update={"created": now, "updated": now})
        self._annotations[stored.id] = stored
        return stored.model_copy()

    async def get(self, annotation_id: uuid.UUID) -> Annotation:
        """Load an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation.
        """
        stored = self._annotations.get(annotation_id)
        if stored is None:
            raise AnnotationNotFound(annotation_id)
        return stored.model_copy()

    def _evaluate_investigation_id_condition(
        self, annotation: Annotation, condition: FilterCondition
    ) -> bool:
        """Evaluate an investigation id condition against an annotation.

        Annotation rows only carry an investigation_session_id, so the
        investigation id is resolved through the investigation repository's
        session links.

        Args:
            annotation: Annotation to evaluate.
            condition: Validated investigation id condition.

        Returns:
            Whether the annotation's linked investigation matches.
        """
        matched = False
        if (
            self._investigations is not None
            and annotation.investigation_session_id is not None
        ):
            session = self._investigations._sessions.get(
                annotation.investigation_session_id
            )
            if session is not None:
                ids = (
                    condition.value
                    if condition.op is FilterOp.IN
                    else (condition.value,)
                )
                matched = session.investigation_id in ids
        return not matched if condition.op is FilterOp.NE else matched

    async def query(
        self, annotation_filter: AnnotationFilter
    ) -> tuple[list[Annotation], str | None]:
        """Query annotations matching a filter.

        Args:
            annotation_filter: Filter and pagination parameters.

        Returns:
            Page of matching annotations and the next cursor.
        """
        annotations = list(self._annotations.values())
        if annotation_filter.expression is not None:
            resolvers = {"investigation_id": self._evaluate_investigation_id_condition}
            annotations = [
                annotation
                for annotation in annotations
                if _evaluate_filter_expression(
                    annotation, annotation_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(annotations, annotation_filter)
        return [annotation.model_copy() for annotation in page], next_cursor

    async def update(self, annotation: Annotation) -> Annotation:
        """Persist changes to an existing annotation.

        Args:
            annotation: Annotation with modified fields.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation with the updated timestamp renewed.
        """
        stored = self._annotations.get(annotation.id)
        if stored is None:
            raise AnnotationNotFound(annotation.id)
        now = _renewed_timestamp(stored.updated)
        updated = stored.model_copy(update={"value": annotation.value, "updated": now})
        self._annotations[annotation.id] = updated
        return updated.model_copy()

    async def delete(self, annotation_id: uuid.UUID) -> None:
        """Delete an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.
        """
        if annotation_id not in self._annotations:
            raise AnnotationNotFound(annotation_id)
        del self._annotations[annotation_id]

    def cascade_investigation_session_delete(
        self, investigation_session_ids: Iterable[uuid.UUID]
    ) -> None:
        """Drop the answers of deleted investigation sessions.

        Manual annotations, whose investigation_session_id is null, are
        unaffected.

        Args:
            investigation_session_ids: Ids of the deleted investigation
                sessions.
        """
        deleted = set(investigation_session_ids)
        for annotation_id, annotation in list(self._annotations.items()):
            if annotation.investigation_session_id in deleted:
                del self._annotations[annotation_id]
