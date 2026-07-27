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

import os
import uuid
from collections.abc import AsyncGenerator, Sequence
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
from sqlmodel import SQLModel

from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.adapters.db.blob_storage import DatabaseBlobStorage
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
    get_blob_service,
    get_cohort_service,
    get_experiment_run_service,
    get_experiment_service,
    get_importer_service,
    get_job_service,
    get_scorer_service,
    get_session_node_service,
    get_session_service,
    get_tag_service,
    get_worker_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.application.models.accounts import AccountFilter
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.application.models.api_keys import ApiKeyFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import (
    CohortFilter,
    CohortSessionsFilter,
)
from kitaru.server.application.models.experiment_runs import ExperimentRunFilter
from kitaru.server.application.models.experiments import ExperimentFilter
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.application.models.secrets import SecretFilter
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.application.models.tags import TagFilter
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import (
    ExperimentService,
)
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.application.services.worker_service import WorkerService
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
    AgentVersion,
    AgentVersionInUse,
    AgentVersionNotFound,
    DuplicateAgentVersion,
)
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
    encode_api_key,
    generate_secret,
    hash_secret,
)
from kitaru.server.domain.blob import (
    Blob,
    BlobNotFound,
    DuplicateBlobContent,
)
from kitaru.server.domain.cohort import (
    Cohort,
    CohortInUse,
    CohortNotFound,
    DuplicateCohortName,
)
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentInUse,
    ExperimentNotFound,
)
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunNotFound,
)
from kitaru.server.domain.job import (
    DuplicateReplaySession,
    DuplicateScoreJob,
    Import,
    Job,
    JobNotFound,
    JobStatus,
    Replay,
    Score,
)
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginVersion,
    PluginVersionIdNotFound,
    PluginVersionNotFound,
)
from kitaru.server.domain.replay_config import (
    ReplayConfig,
    ReplayConfigNotFound,
)
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretInUse,
    SecretNotFound,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionInUse,
    SessionNotFound,
)
from kitaru.server.domain.session_node import (
    DuplicateNodeExternalId,
    DuplicateNodeKey,
    DuplicateNodeSequence,
    DuplicateSessionNodeId,
    NodeStatus,
    NodeType,
    SessionNode,
)
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
    TagResourceType,
)
from kitaru.server.domain.worker import (
    DuplicateWorkerName,
    Worker,
    WorkerNotFound,
)


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
        DB_NAME=os.environ.get("KITARU_TEST_DB_NAME", "kitaru_test"),
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
    await DatabaseService.create_db(settings, force_drop=True)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client


@asynccontextmanager
async def pg_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a session on a fresh test database with all tables created.

    Yields:
        Session bound to the test database engine.
    """
    settings = db_settings()
    await DatabaseService.create_db(settings, force_drop=True)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(SQLModel.metadata.create_all)
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as session:
            yield session
    finally:
        await engine.dispose()


def asgi_api_client(app: FastAPI) -> KitaruAPIClient:
    """Build an SDK client routed to the app instead of the network.

    Args:
        app: Application to route requests to.

    Returns:
        Client wired to an ASGI transport.
    """
    client = KitaruAPIClient(base_url="http://test")
    client._http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
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
        self.version_repository: FakeAgentVersionRepository | None = None

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

    async def get_many(self, secret_ids: list[uuid.UUID]) -> dict[uuid.UUID, Secret]:
        """Load secrets by id.

        Args:
            secret_ids: Ids of the secrets.

        Returns:
            Stored secrets keyed by id, missing ids omitted.
        """
        return {
            secret_id: self._secrets[secret_id].model_copy()
            for secret_id in secret_ids
            if secret_id in self._secrets
        }

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
            SecretInUse: The secret is referenced by an agent version.
        """
        if secret_id not in self._secrets:
            raise SecretNotFound(secret_id)
        if (
            self.version_repository is not None
            and self.version_repository.references_secret(secret_id)
        ):
            raise SecretInUse(secret_id)
        del self._secrets[secret_id]


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

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], int]:
        """Query tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.

        Returns:
            Page of matching tags and the total match count.
        """
        tags = sorted(self._tags.values(), key=lambda tag: tag.id.int)
        if tag_filter.name is not None:
            tags = [tag for tag in tags if tag.name == tag_filter.name]
        if tag_filter.owner_id is not None:
            tags = [tag for tag in tags if tag.owner_id == tag_filter.owner_id]
        total = len(tags)
        start = (tag_filter.page - 1) * tag_filter.page_size
        page = tags[start : start + tag_filter.page_size]
        return [tag.model_copy() for tag in page], total

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag by id, including its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.
        """
        if tag_id not in self._tags:
            raise TagNotFound(tag_id)
        del self._tags[tag_id]
        self._links = {
            link_id: link
            for link_id, link in self._links.items()
            if link.tag_id != tag_id
        }

    async def create_link(self, link: TagLink) -> TagLink:
        """Persist a new tag link.

        Args:
            link: Tag link to store.

        Raises:
            DuplicateTagLink: The tag link is already registered.

        Returns:
            Stored tag link with timestamps set.
        """
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
        """Delete a tag link.

        Args:
            tag_id: Id of the tag.
            resource_type: Type of the linked resource.
            resource_id: Id of the linked resource.

        Raises:
            TagLinkNotFound: No tag link matches.
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

    def linked_resource_ids(
        self, tag_name: str, resource_type: TagResourceType
    ) -> set[uuid.UUID]:
        """Return the resource ids a tag name is attached to.

        Args:
            tag_name: Name of the tag.
            resource_type: Type of the linked resources.

        Returns:
            Ids of the linked resources.
        """
        tag_ids = {tag.id for tag in self._tags.values() if tag.name == tag_name}
        return {
            link.resource_id
            for link in self._links.values()
            if link.tag_id in tag_ids and link.resource_type == resource_type
        }

    def remove_links_for_resource(
        self, resource_type: TagResourceType, resource_id: uuid.UUID
    ) -> None:
        """Remove every tag link of a resource.

        Args:
            resource_type: Type of the resource.
            resource_id: Id of the resource.
        """
        self._links = {
            link_id: link
            for link_id, link in self._links.items()
            if link.resource_type != resource_type or link.resource_id != resource_id
        }


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


class FakeWorkerRepository:
    """In-memory worker repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._workers: dict[uuid.UUID, Worker] = {}

    def _check_duplicate_name(self, worker: Worker) -> None:
        for other in self._workers.values():
            if other.id != worker.id and other.name == worker.name:
                raise DuplicateWorkerName(worker.name)

    async def create(self, worker: Worker) -> Worker:
        """Persist a new worker.

        Args:
            worker: Worker to store.

        Raises:
            DuplicateWorkerName: The worker name is already registered.

        Returns:
            Stored worker with timestamps set.
        """
        self._check_duplicate_name(worker)
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

    async def get_by_name(self, name: str) -> Worker:
        """Load a worker by name.

        Args:
            name: Name of the worker.

        Raises:
            WorkerNotFound: No worker has this name.

        Returns:
            Stored worker.
        """
        for worker in self._workers.values():
            if worker.name == name:
                return worker.model_copy()
        raise WorkerNotFound(name)

    async def query(self, worker_filter: WorkerFilter) -> tuple[list[Worker], int]:
        """Query workers matching a filter.

        The agent id filter matches workers serving the agent, including
        workers serving all agents.

        Args:
            worker_filter: Filter and pagination parameters.

        Returns:
            Page of matching workers and the total match count.
        """
        workers = sorted(self._workers.values(), key=lambda worker: worker.id.int)
        if worker_filter.name is not None:
            workers = [
                worker for worker in workers if worker.name == worker_filter.name
            ]
        if worker_filter.agent_id is not None:
            workers = [
                worker
                for worker in workers
                if not worker.agent_ids or worker_filter.agent_id in worker.agent_ids
            ]
        if worker_filter.seen_after is not None:
            workers = [
                worker
                for worker in workers
                if worker.last_seen_at >= worker_filter.seen_after
            ]
        total = len(workers)
        start = (worker_filter.page - 1) * worker_filter.page_size
        page = workers[start : start + worker_filter.page_size]
        return [worker.model_copy() for worker in page], total

    async def update(self, worker: Worker) -> Worker:
        """Persist changes to an existing worker.

        Args:
            worker: Worker with modified fields.

        Raises:
            WorkerNotFound: No worker has this id.
            DuplicateWorkerName: The worker name is already registered.

        Returns:
            Stored worker with the updated timestamp renewed.
        """
        stored = self._workers.get(worker.id)
        if stored is None:
            raise WorkerNotFound(worker.id)
        self._check_duplicate_name(worker)
        now = _renewed_timestamp(stored.updated)
        updated = worker.model_copy(update={"created": stored.created, "updated": now})
        self._workers[worker.id] = updated
        return updated.model_copy()

    async def touch(self, worker_id: uuid.UUID, last_seen_at: datetime) -> None:
        """Record a worker sighting, bumping only the last seen time.

        Args:
            worker_id: Id of the worker.
            last_seen_at: Time of the sighting.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        stored = self._workers.get(worker_id)
        if stored is None:
            raise WorkerNotFound(worker_id)
        self._workers[worker_id] = stored.model_copy(
            update={
                "last_seen_at": last_seen_at,
                "updated": _renewed_timestamp(stored.updated),
            }
        )

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
    name: str = "runner",
    agent_ids: list[uuid.UUID] | None = None,
    last_seen_at: datetime | None = None,
) -> Worker:
    """Store a worker in the fake repository.

    Args:
        repository: Fake worker repository.
        owner_id: Id of the owning account.
        name: Worker name.
        agent_ids: Ids of the served agents.
        last_seen_at: Time of the last sighting.

    Returns:
        Stored worker.
    """
    return await repository.create(
        Worker(
            owner_id=owner_id,
            name=name,
            agent_ids=agent_ids or [],
            last_seen_at=last_seen_at or datetime.now(UTC),
        )
    )


class FakeAgentRepository:
    """In-memory agent repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._agents: dict[uuid.UUID, Agent] = {}
        self.version_repository: FakeAgentVersionRepository | None = None
        self.session_repository: FakeSessionRepository | None = None
        self.cohort_repository: FakeCohortRepository | None = None

    def _check_duplicate_name(self, agent: Agent) -> None:
        for other in self._agents.values():
            if other.id != agent.id and other.name == agent.name:
                raise DuplicateAgentName(agent.name)

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

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], int]:
        """Query agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.

        Returns:
            Page of matching agents and the total match count.
        """
        agents = sorted(self._agents.values(), key=lambda agent: agent.id.int)
        if agent_filter.name is not None:
            agents = [agent for agent in agents if agent.name == agent_filter.name]
        if agent_filter.owner_id is not None:
            agents = [
                agent for agent in agents if agent.owner_id == agent_filter.owner_id
            ]
        total = len(agents)
        start = (agent_filter.page - 1) * agent_filter.page_size
        page = agents[start : start + agent_filter.page_size]
        return [agent.model_copy() for agent in page], total

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
            AgentInUse: The agent is referenced by an agent version, a
                session, or a cohort.
        """
        if agent_id not in self._agents:
            raise AgentNotFound(agent_id)
        if self.version_repository is not None and self.version_repository.has_versions(
            agent_id
        ):
            raise AgentInUse(agent_id, "agent versions")
        if (
            self.session_repository is not None
            and self.session_repository.session_ids_for_agent(agent_id)
        ):
            raise AgentInUse(agent_id, "sessions")
        if (
            self.cohort_repository is not None
            and self.cohort_repository.references_agent(agent_id)
        ):
            raise AgentInUse(agent_id, "cohorts")
        del self._agents[agent_id]


class FakeAgentVersionRepository:
    """In-memory agent version repository."""

    def __init__(
        self,
        agent_repository: FakeAgentRepository,
        secret_repository: FakeSecretRepository | None = None,
    ) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            agent_repository: Fake agent repository backing the agent ids.
            secret_repository: Fake secret repository backing the secret ids.
        """
        self._versions: dict[uuid.UUID, AgentVersion] = {}
        self._agent_repository = agent_repository
        self.session_repository: FakeSessionRepository | None = None
        self.run_repository: FakeExperimentRunRepository | None = None
        self.job_repository: FakeJobRepository | None = None
        agent_repository.version_repository = self
        if secret_repository is not None:
            secret_repository.version_repository = self

    def has_versions(self, agent_id: uuid.UUID) -> bool:
        """Report whether an agent has stored versions.

        Args:
            agent_id: Id of the agent.

        Returns:
            ``True`` when a stored version belongs to the agent.
        """
        return any(version.agent_id == agent_id for version in self._versions.values())

    def agent_id_of(self, version_id: uuid.UUID) -> uuid.UUID | None:
        """Resolve the agent id of a stored version.

        Args:
            version_id: Id of the agent version.

        Returns:
            Agent id, ``None`` when no version has this id.
        """
        version = self._versions.get(version_id)
        if version is None:
            return None
        return version.agent_id

    def references_secret(self, secret_id: uuid.UUID) -> bool:
        """Report whether a stored version references a secret.

        Args:
            secret_id: Id of the secret.

        Returns:
            ``True`` when a stored run spec references the secret.
        """
        return any(
            version.run_spec is not None and secret_id in version.run_spec.secret_ids
            for version in self._versions.values()
        )

    def _check_duplicate_version(self, version: AgentVersion) -> None:
        for other in self._versions.values():
            if (
                other.id != version.id
                and other.agent_id == version.agent_id
                and other.version == version.version
            ):
                raise DuplicateAgentVersion(version.version)

    async def create(self, version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        Args:
            version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the version's agent id.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Stored agent version with timestamps set.
        """
        await self._agent_repository.get(version.agent_id)
        self._check_duplicate_version(version)
        now = datetime.now(UTC)
        stored = version.model_copy(update={"created": now, "updated": now})
        self._versions[stored.id] = stored
        return stored.model_copy()

    async def get(self, version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        version = self._versions.get(version_id)
        if version is None:
            raise AgentVersionNotFound(version_id)
        return version.model_copy()

    async def get_many(
        self, version_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, AgentVersion]:
        """Load agent versions by id.

        Args:
            version_ids: Ids of the agent versions.

        Returns:
            Stored agent versions keyed by id, missing ids omitted.
        """
        return {
            version_id: self._versions[version_id].model_copy()
            for version_id in version_ids
            if version_id in self._versions
        }

    async def query(
        self, version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], int]:
        """Query agent versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the total match count.
        """
        versions = sorted(self._versions.values(), key=lambda version: version.id.int)
        if version_filter.agent_id is not None:
            versions = [
                version
                for version in versions
                if version.agent_id == version_filter.agent_id
            ]
        total = len(versions)
        start = (version_filter.page - 1) * version_filter.page_size
        page = versions[start : start + version_filter.page_size]
        return [version.model_copy() for version in page], total

    async def get_latest_runnable(self, agent_id: uuid.UUID) -> AgentVersion | None:
        """Load the most recently created runnable version of an agent.

        Args:
            agent_id: Id of the agent.

        Returns:
            Latest version with a run spec, ``None`` when none exists.
        """
        runnable = [
            version
            for version in self._versions.values()
            if version.agent_id == agent_id and version.run_spec is not None
        ]
        if not runnable:
            return None
        return max(runnable, key=lambda version: version.id.int).model_copy()

    async def update(self, version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Args:
            version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        stored = self._versions.get(version.id)
        if stored is None:
            raise AgentVersionNotFound(version.id)
        self._check_duplicate_version(version)
        now = _renewed_timestamp(stored.updated)
        updated = version.model_copy(update={"created": stored.created, "updated": now})
        self._versions[version.id] = updated
        return updated.model_copy()

    async def delete(self, version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            AgentVersionInUse: The version is referenced by a session, an
                experiment run, or a job.
        """
        if version_id not in self._versions:
            raise AgentVersionNotFound(version_id)
        if (
            self.session_repository is not None
            and self.session_repository.references_version(version_id)
        ):
            raise AgentVersionInUse(version_id, "sessions")
        if self.run_repository is not None and self.run_repository.references_version(
            version_id
        ):
            raise AgentVersionInUse(version_id, "experiment runs")
        if (
            self.job_repository is not None
            and await self.job_repository.references_agent_version(version_id)
        ):
            raise AgentVersionInUse(version_id, "jobs")
        del self._versions[version_id]


class FakeSessionRepository:
    """In-memory session repository."""

    def __init__(
        self,
        agent_repository: FakeAgentRepository,
        agent_version_repository: FakeAgentVersionRepository | None = None,
        tag_repository: FakeTagRepository | None = None,
    ) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            agent_repository: Fake agent repository backing the agent ids.
            agent_version_repository: Fake agent version repository backing
                the agent version ids.
            tag_repository: Fake tag repository backing the tag filter and
                link cleanup.
        """
        self._sessions: dict[uuid.UUID, Session] = {}
        self._agent_repository = agent_repository
        self._agent_version_repository = agent_version_repository
        self._tag_repository = tag_repository
        self.node_repository: FakeSessionNodeRepository | None = None
        self.cohort_repository: FakeCohortRepository | None = None
        self.job_repository: FakeJobRepository | None = None
        agent_repository.session_repository = self
        if agent_version_repository is not None:
            agent_version_repository.session_repository = self

    def references_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored session references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored session references the version.
        """
        return any(
            session.agent_version_id == version_id
            for session in self._sessions.values()
        )

    def session_ids_for_agent(self, agent_id: uuid.UUID) -> list[uuid.UUID]:
        """Collect the ids of an agent's stored sessions.

        Args:
            agent_id: Id of the agent.

        Returns:
            Ids of the agent's sessions.
        """
        return [
            session.id
            for session in self._sessions.values()
            if session.agent_id == agent_id
        ]

    def _check_duplicate_external_id(self, session: Session) -> None:
        if session.provider is None or session.external_id is None:
            return
        for other in self._sessions.values():
            if (
                other.id != session.id
                and other.provider == session.provider
                and other.external_id == session.external_id
            ):
                raise DuplicateSessionExternalId(session.provider, session.external_id)

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            AgentNotFound: No agent has the session's agent id.
            AgentVersionNotFound: No agent version has the session's agent
                version id.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        await self._agent_repository.get(session.agent_id)
        if (
            session.agent_version_id is not None
            and self._agent_version_repository is not None
        ):
            await self._agent_version_repository.get(session.agent_version_id)
        self._check_duplicate_external_id(session)
        now = datetime.now(UTC)
        stored = session.model_copy(update={"created": now, "updated": now})
        self._sessions[stored.id] = stored
        return stored.model_copy()

    async def get(self, session_id: uuid.UUID) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        session = self._sessions.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return session.model_copy()

    async def get_many(self, session_ids: list[uuid.UUID]) -> dict[uuid.UUID, Session]:
        """Load sessions by id.

        Args:
            session_ids: Ids of the sessions.

        Returns:
            Stored sessions keyed by id, missing ids omitted.
        """
        return {
            session_id: self._sessions[session_id].model_copy()
            for session_id in session_ids
            if session_id in self._sessions
        }

    def _matches(self, session: Session, session_filter: SessionFilter) -> bool:
        if (
            session_filter.agent_id is not None
            and session.agent_id != session_filter.agent_id
        ):
            return False
        if (
            session_filter.agent_version_id is not None
            and session.agent_version_id != session_filter.agent_version_id
        ):
            return False
        if (
            session_filter.origin is not None
            and session.origin != session_filter.origin
        ):
            return False
        if (
            session_filter.status is not None
            and session.status != session_filter.status
        ):
            return False
        if (
            session_filter.provider is not None
            and session.provider != session_filter.provider
        ):
            return False
        if (
            session_filter.external_id is not None
            and session.external_id != session_filter.external_id
        ):
            return False
        if session_filter.name is not None and session.name != session_filter.name:
            return False
        if session_filter.tag is not None:
            tagged_ids: set[uuid.UUID] = set()
            if self._tag_repository is not None:
                tagged_ids = self._tag_repository.linked_resource_ids(
                    session_filter.tag, TagResourceType.SESSION
                )
            if session.id not in tagged_ids:
                return False
        if session_filter.started_after is not None and (
            session.started_at is None
            or session.started_at < session_filter.started_after
        ):
            return False
        if session_filter.started_before is not None and (
            session.started_at is None
            or session.started_at > session_filter.started_before
        ):
            return False
        if session_filter.ended_after is not None and (
            session.ended_at is None or session.ended_at < session_filter.ended_after
        ):
            return False
        if session_filter.ended_before is not None and (
            session.ended_at is None or session.ended_at > session_filter.ended_before
        ):
            return False
        if session_filter.has_score is not None and (
            bool(session.scores) != session_filter.has_score
        ):
            return False
        if session_filter.min_cost is not None and (
            session.cost is None or session.cost < session_filter.min_cost
        ):
            return False
        if session_filter.max_cost is not None and (
            session.cost is None or session.cost > session_filter.max_cost
        ):
            return False
        tokens = session.tokens
        total_tokens = 0
        if tokens is not None:
            total_tokens = (
                (tokens.input_tokens or 0)
                + (tokens.output_tokens or 0)
                + (tokens.cached_input_tokens or 0)
                + (tokens.reasoning_tokens or 0)
            )
        if (
            session_filter.min_total_tokens is not None
            and total_tokens < session_filter.min_total_tokens
        ):
            return False
        return (
            session_filter.max_total_tokens is None
            or total_tokens <= session_filter.max_total_tokens
        )

    async def query(self, session_filter: SessionFilter) -> tuple[list[Session], int]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching sessions and the total match count.
        """
        sessions = sorted(self._sessions.values(), key=lambda session: session.id.int)
        sessions = [
            session for session in sessions if self._matches(session, session_filter)
        ]
        total = len(sessions)
        start = (session_filter.page - 1) * session_filter.page_size
        page = sessions[start : start + session_filter.page_size]
        return [session.model_copy() for session in page], total

    async def update(self, session: Session) -> Session:
        """Persist changes to an existing session.

        Args:
            session: Session with modified fields.

        Raises:
            SessionNotFound: No session has this id.
            DuplicateSessionExternalId: The provider and external id pair is
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
        """Delete a session by id, including its nodes and tag links.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session is a member of a cohort or referenced
                by a job.
        """
        if session_id not in self._sessions:
            raise SessionNotFound(session_id)
        if (
            self.cohort_repository is not None
            and self.cohort_repository.references_session(session_id)
        ):
            raise SessionInUse(session_id, "cohorts")
        if self.job_repository is not None and self.job_repository.references_session(
            session_id
        ):
            raise SessionInUse(session_id, "jobs")
        del self._sessions[session_id]
        if self.node_repository is not None:
            self.node_repository.remove_for_session(session_id)
        if self._tag_repository is not None:
            self._tag_repository.remove_links_for_resource(
                TagResourceType.SESSION, session_id
            )


class FakeSessionNodeRepository:
    """In-memory session node repository."""

    def __init__(self, session_repository: FakeSessionRepository) -> None:
        """Initialize the repository and wire the cascade.

        Args:
            session_repository: Fake session repository backing the session
                ids.
        """
        self._nodes: dict[uuid.UUID, SessionNode] = {}
        self._session_repository = session_repository
        session_repository.node_repository = self

    def remove_for_session(self, session_id: uuid.UUID) -> None:
        """Remove every node of a session.

        Args:
            session_id: Id of the session.
        """
        self._nodes = {
            node_id: node
            for node_id, node in self._nodes.items()
            if node.session_id != session_id
        }

    def _check_conflicts(
        self, node: SessionNode, staged: dict[uuid.UUID, SessionNode]
    ) -> None:
        stored = staged.get(node.id)
        if stored is not None and stored.session_id != node.session_id:
            raise DuplicateSessionNodeId(node.id)
        for other in staged.values():
            if other.id == node.id or other.session_id != node.session_id:
                continue
            if other.sequence == node.sequence:
                raise DuplicateNodeSequence(node.session_id)
            if node.external_id is not None and other.external_id == node.external_id:
                raise DuplicateNodeExternalId(node.session_id)
            if other.key == node.key:
                raise DuplicateNodeKey(node.session_id)

    async def upsert(self, nodes: list[SessionNode]) -> list[SessionNode]:
        """Insert or update nodes by id as one atomic batch.

        Args:
            nodes: Nodes to store, all belonging to one session.

        Raises:
            SessionNotFound: No session has the nodes' session id.
            DuplicateSessionNodeId: A node id is already registered in
                another session.
            DuplicateNodeSequence: A node sequence is already registered in
                the session.
            DuplicateNodeExternalId: A node external id is already
                registered in the session.
            DuplicateNodeKey: A node key is already registered in the
                session.

        Returns:
            Stored nodes in batch order with timestamps set.
        """
        staged = dict(self._nodes)
        results: list[uuid.UUID] = []
        for node in nodes:
            await self._session_repository.get(node.session_id)
            self._check_conflicts(node, staged)
            stored = staged.get(node.id)
            now = datetime.now(UTC)
            if stored is None:
                staged[node.id] = node.model_copy(
                    update={"created": now, "updated": now}
                )
            else:
                staged[node.id] = node.model_copy(
                    update={
                        "created": stored.created,
                        "updated": _renewed_timestamp(stored.updated),
                    }
                )
            results.append(node.id)
        self._nodes = staged
        return [self._nodes[node_id].model_copy() for node_id in results]

    async def list_for_session(
        self, session_id: uuid.UUID, include_payloads: bool
    ) -> list[SessionNode]:
        """Load the nodes of a session ordered by sequence.

        Args:
            session_id: Id of the session.
            include_payloads: Whether to load inputs, outputs, and
                attributes.

        Returns:
            Nodes ordered by sequence.
        """
        nodes = sorted(
            (node for node in self._nodes.values() if node.session_id == session_id),
            key=lambda node: node.sequence,
        )
        if include_payloads:
            return [node.model_copy() for node in nodes]
        return [
            node.model_copy(update={"inputs": None, "outputs": None, "attributes": {}})
            for node in nodes
        ]

    async def find_tool_result(
        self,
        cache_key: str,
        session_ids: list[uuid.UUID] | None,
        agent_id: uuid.UUID | None,
    ) -> SessionNode | None:
        """Find the most recent completed tool call with a cache key.

        Nodes whose attributes mark them mocked are excluded. Exactly one
        of the scope arguments is set.

        Args:
            cache_key: Cache key to match.
            session_ids: Sessions to search within.
            agent_id: Agent whose sessions to search within.

        Returns:
            Most recent matching node with payloads, ``None`` on a miss.
        """
        if agent_id is not None:
            session_ids = self._session_repository.session_ids_for_agent(agent_id)
        assert session_ids is not None
        scope = set(session_ids)
        candidates = [
            node
            for node in self._nodes.values()
            if node.cache_key == cache_key
            and node.node_type is NodeType.TOOL_CALL
            and node.status is NodeStatus.COMPLETED
            and node.attributes.get("mocked") not in (True, "true")
            and node.session_id in scope
        ]
        with_time = sorted(
            (node for node in candidates if node.started_at is not None),
            key=lambda node: (node.started_at, node.id.int),
            reverse=True,
        )
        without_time = sorted(
            (node for node in candidates if node.started_at is None),
            key=lambda node: node.id.int,
            reverse=True,
        )
        ordered = with_time + without_time
        if not ordered:
            return None
        return ordered[0].model_copy()


class FakeCohortRepository:
    """In-memory cohort repository."""

    def __init__(
        self,
        session_repository: FakeSessionRepository,
        agent_repository: FakeAgentRepository | None = None,
        tag_repository: FakeTagRepository | None = None,
    ) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            session_repository: Fake session repository backing the session
                ids.
            agent_repository: Fake agent repository backing the agent ids.
            tag_repository: Fake tag repository backing the tag filter and
                link cleanup.
        """
        self._cohorts: dict[uuid.UUID, Cohort] = {}
        self._members: dict[uuid.UUID, list[uuid.UUID]] = {}
        self._session_repository = session_repository
        self._agent_repository = agent_repository
        self._tag_repository = tag_repository
        self.experiment_repository: FakeExperimentRepository | None = None
        session_repository.cohort_repository = self
        if agent_repository is not None:
            agent_repository.cohort_repository = self

    def references_agent(self, agent_id: uuid.UUID) -> bool:
        """Report whether a stored cohort references an agent.

        Args:
            agent_id: Id of the agent.

        Returns:
            ``True`` when a stored cohort references the agent.
        """
        return any(cohort.agent_id == agent_id for cohort in self._cohorts.values())

    def references_session(self, session_id: uuid.UUID) -> bool:
        """Report whether a stored cohort contains a session.

        Args:
            session_id: Id of the session.

        Returns:
            ``True`` when a stored membership references the session.
        """
        return any(session_id in members for members in self._members.values())

    def _check_duplicate_name(self, cohort: Cohort) -> None:
        for other in self._cohorts.values():
            if other.id != cohort.id and other.name == cohort.name:
                raise DuplicateCohortName(cohort.name)

    async def create(self, cohort: Cohort, session_ids: list[uuid.UUID]) -> Cohort:
        """Persist a new cohort with its ordered membership.

        Args:
            cohort: Cohort to store.
            session_ids: Ids of the member sessions, in position order.

        Raises:
            DuplicateCohortName: The cohort name is already registered.
            AgentNotFound: No agent has the cohort's agent id.

        Returns:
            Stored cohort with timestamps set.
        """
        if self._agent_repository is not None:
            await self._agent_repository.get(cohort.agent_id)
        self._check_duplicate_name(cohort)
        now = datetime.now(UTC)
        stored = cohort.model_copy(update={"created": now, "updated": now})
        self._cohorts[stored.id] = stored
        self._members[stored.id] = list(session_ids)
        return stored.model_copy()

    async def get(self, cohort_id: uuid.UUID) -> Cohort:
        """Load a cohort by id.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        cohort = self._cohorts.get(cohort_id)
        if cohort is None:
            raise CohortNotFound(cohort_id)
        return cohort.model_copy()

    async def query(self, cohort_filter: CohortFilter) -> tuple[list[Cohort], int]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the total match count.
        """
        cohorts = sorted(self._cohorts.values(), key=lambda cohort: cohort.id.int)
        if cohort_filter.name is not None:
            cohorts = [
                cohort for cohort in cohorts if cohort.name == cohort_filter.name
            ]
        if cohort_filter.tag is not None:
            tagged_ids: set[uuid.UUID] = set()
            if self._tag_repository is not None:
                tagged_ids = self._tag_repository.linked_resource_ids(
                    cohort_filter.tag, TagResourceType.COHORT
                )
            cohorts = [cohort for cohort in cohorts if cohort.id in tagged_ids]
        total = len(cohorts)
        start = (cohort_filter.page - 1) * cohort_filter.page_size
        page = cohorts[start : start + cohort_filter.page_size]
        return [cohort.model_copy() for cohort in page], total

    async def query_sessions(
        self, cohort_id: uuid.UUID, sessions_filter: CohortSessionsFilter
    ) -> tuple[list[Session], int]:
        """Query the member sessions of a cohort ordered by position.

        Args:
            cohort_id: Id of the cohort.
            sessions_filter: Pagination parameters.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Page of member sessions and the total member count.
        """
        if cohort_id not in self._cohorts:
            raise CohortNotFound(cohort_id)
        member_ids = self._members[cohort_id]
        total = len(member_ids)
        start = (sessions_filter.page - 1) * sessions_filter.page_size
        page_ids = member_ids[start : start + sessions_filter.page_size]
        return [
            await self._session_repository.get(session_id) for session_id in page_ids
        ], total

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
        """Delete a cohort by id, including its membership and tag links.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
            CohortInUse: The cohort is referenced by an experiment.
        """
        if cohort_id not in self._cohorts:
            raise CohortNotFound(cohort_id)
        if (
            self.experiment_repository is not None
            and self.experiment_repository.references_cohort(cohort_id)
        ):
            raise CohortInUse(cohort_id)
        del self._cohorts[cohort_id]
        del self._members[cohort_id]
        if self._tag_repository is not None:
            self._tag_repository.remove_links_for_resource(
                TagResourceType.COHORT, cohort_id
            )


class FakeBlobRepository:
    """In-memory blob repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._blobs: dict[uuid.UUID, Blob] = {}

    def _find_by_sha256(self, sha256: str) -> Blob | None:
        for blob in self._blobs.values():
            if blob.sha256 == sha256:
                return blob
        return None

    async def create(self, blob: Blob) -> Blob:
        """Persist a new blob.

        Args:
            blob: Blob to store.

        Raises:
            DuplicateBlobContent: The content hash is already stored.

        Returns:
            Stored blob with timestamps set.
        """
        if self._find_by_sha256(blob.sha256) is not None:
            raise DuplicateBlobContent(blob.sha256)
        stored = blob.model_copy(update={"created": datetime.now(UTC)})
        self._blobs[stored.id] = stored
        return stored.model_copy()

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id.

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

    async def get_hashes(self, blob_ids: list[uuid.UUID]) -> dict[uuid.UUID, str]:
        """Load blob content hashes by id, without reading the content.

        Args:
            blob_ids: Ids of the blobs.

        Returns:
            Content hashes keyed by blob id, missing ids omitted.
        """
        return {
            blob_id: self._blobs[blob_id].sha256
            for blob_id in blob_ids
            if blob_id in self._blobs
        }

    async def get_by_sha256(self, sha256: str) -> Blob | None:
        """Load a blob by content hash.

        Args:
            sha256: Hash of the content.

        Returns:
            Stored blob, ``None`` when the content is not stored.
        """
        blob = self._find_by_sha256(sha256)
        if blob is None:
            return None
        return blob.model_copy()


class FakePluginRepository:
    """In-memory plugin repository."""

    def __init__(self, blob_repository: FakeBlobRepository | None = None) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            blob_repository: Fake blob repository backing the blob ids.
        """
        self._plugins: dict[uuid.UUID, Plugin] = {}
        self._versions: dict[uuid.UUID, PluginVersion] = {}
        self._blob_repository = blob_repository

    def _check_duplicate_name(self, plugin: Plugin) -> None:
        for other in self._plugins.values():
            if (
                other.id != plugin.id
                and other.kind is plugin.kind
                and other.name == plugin.name
            ):
                raise DuplicatePluginName(plugin.name)

    async def create(self, plugin: Plugin) -> Plugin:
        """Persist a new plugin.

        Args:
            plugin: Plugin to store.

        Raises:
            DuplicatePluginName: The plugin name is already registered for
                the kind.

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

    async def get_many(self, plugin_ids: list[uuid.UUID]) -> dict[uuid.UUID, Plugin]:
        """Load plugins by id.

        Args:
            plugin_ids: Ids of the plugins.

        Returns:
            Stored plugins keyed by id, missing ids omitted.
        """
        return {
            plugin_id: self._plugins[plugin_id].model_copy()
            for plugin_id in plugin_ids
            if plugin_id in self._plugins
        }

    async def query(self, plugin_filter: PluginFilter) -> tuple[list[Plugin], int]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the total match count.
        """
        plugins = sorted(self._plugins.values(), key=lambda plugin: plugin.id.int)
        plugins = [plugin for plugin in plugins if plugin.kind is plugin_filter.kind]
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
        if plugin_filter.owner_id is not None:
            plugins = [
                plugin
                for plugin in plugins
                if plugin.owner_id == plugin_filter.owner_id
            ]
        total = len(plugins)
        start = (plugin_filter.page - 1) * plugin_filter.page_size
        page = plugins[start : start + plugin_filter.page_size]
        return [plugin.model_copy() for plugin in page], total

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, including its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
        """
        if plugin_id not in self._plugins:
            raise PluginNotFound(plugin_id)
        del self._plugins[plugin_id]
        for version_id in [
            version.id
            for version in self._versions.values()
            if version.plugin_id == plugin_id
        ]:
            del self._versions[version_id]

    async def create_version(self, version: PluginVersion) -> PluginVersion:
        """Persist a new plugin version under the next version number.

        Args:
            version: Plugin version to store.

        Raises:
            PluginNotFound: No plugin has the version's plugin id.
            BlobNotFound: No blob has the version's blob id.

        Returns:
            Stored plugin version with the version number and timestamp
            set.
        """
        plugin = self._plugins.get(version.plugin_id)
        if plugin is None:
            raise PluginNotFound(version.plugin_id)
        if self._blob_repository is not None:
            await self._blob_repository.get(version.blob_id)
        allocated = plugin.latest_version + 1
        self._plugins[plugin.id] = plugin.model_copy(
            update={
                "latest_version": allocated,
                "updated": _renewed_timestamp(plugin.updated),
            }
        )
        stored = version.model_copy(
            update={"version": allocated, "created": datetime.now(UTC)}
        )
        self._versions[stored.id] = stored
        return stored.model_copy()

    async def get_version(self, plugin_id: uuid.UUID, version: int) -> PluginVersion:
        """Load a plugin version by version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.

        Raises:
            PluginVersionNotFound: The plugin has no such version.

        Returns:
            Stored plugin version.
        """
        for stored in self._versions.values():
            if stored.plugin_id == plugin_id and stored.version == version:
                return stored.model_copy()
        raise PluginVersionNotFound(plugin_id, version)

    async def get_version_by_id(self, version_id: uuid.UUID) -> PluginVersion:
        """Load a plugin version by id.

        Args:
            version_id: Id of the plugin version.

        Raises:
            PluginVersionIdNotFound: No plugin version has this id.

        Returns:
            Stored plugin version.
        """
        stored = self._versions.get(version_id)
        if stored is None:
            raise PluginVersionIdNotFound(version_id)
        return stored.model_copy()

    async def get_versions_by_ids(
        self, version_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, PluginVersion]:
        """Load plugin versions by id.

        Args:
            version_ids: Ids of the plugin versions.

        Returns:
            Stored plugin versions keyed by id, missing ids omitted.
        """
        return {
            version_id: self._versions[version_id].model_copy()
            for version_id in version_ids
            if version_id in self._versions
        }

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], int]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the total match count.
        """
        versions = sorted(
            (
                version
                for version in self._versions.values()
                if version.plugin_id == version_filter.plugin_id
            ),
            key=lambda version: version.version,
        )
        total = len(versions)
        start = (version_filter.page - 1) * version_filter.page_size
        page = versions[start : start + version_filter.page_size]
        return [version.model_copy() for version in page], total


class FakeReplayConfigRepository:
    """In-memory replay config repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._configs: dict[uuid.UUID, ReplayConfig] = {}
        self.experiment_repository: FakeExperimentRepository | None = None
        self.job_repository: FakeJobRepository | None = None

    async def create(self, config: ReplayConfig) -> ReplayConfig:
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

    async def get(self, config_id: uuid.UUID) -> ReplayConfig:
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

    async def get_many(
        self, config_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Stored replay configs keyed by id, missing ids omitted.
        """
        return {
            config_id: self._configs[config_id].model_copy()
            for config_id in config_ids
            if config_id in self._configs
        }

    async def delete_if_unreferenced(self, config_id: uuid.UUID) -> bool:
        """Delete a replay config unless something still references it.

        Args:
            config_id: Id of the replay config.

        Returns:
            ``True`` when the config was deleted.
        """
        if config_id not in self._configs:
            return False
        if (
            self.experiment_repository is not None
            and self.experiment_repository.references_config(config_id)
        ):
            return False
        if self.job_repository is not None and self.job_repository.references_config(
            config_id
        ):
            return False
        del self._configs[config_id]
        return True


class FakeExperimentRepository:
    """In-memory experiment repository."""

    def __init__(
        self,
        cohort_repository: FakeCohortRepository,
        replay_config_repository: FakeReplayConfigRepository,
        tag_repository: FakeTagRepository | None = None,
    ) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            cohort_repository: Fake cohort repository backing the cohort
                ids.
            replay_config_repository: Fake replay config repository backing
                the config ids.
            tag_repository: Fake tag repository backing the tag filter and
                link cleanup.
        """
        self._experiments: dict[uuid.UUID, Experiment] = {}
        self._cohort_repository = cohort_repository
        self._replay_config_repository = replay_config_repository
        self._tag_repository = tag_repository
        self.run_repository: FakeExperimentRunRepository | None = None
        cohort_repository.experiment_repository = self
        replay_config_repository.experiment_repository = self

    def references_cohort(self, cohort_id: uuid.UUID) -> bool:
        """Report whether a stored experiment references a cohort.

        Args:
            cohort_id: Id of the cohort.

        Returns:
            ``True`` when a stored experiment references the cohort.
        """
        return any(
            experiment.cohort_id == cohort_id
            for experiment in self._experiments.values()
        )

    def references_config(self, config_id: uuid.UUID) -> bool:
        """Report whether a stored experiment references a replay config.

        Args:
            config_id: Id of the replay config.

        Returns:
            ``True`` when a stored experiment references the config.
        """
        return any(
            experiment.replay_config_id == config_id
            for experiment in self._experiments.values()
        )

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
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.

        Returns:
            Stored experiment with timestamps set.
        """
        await self._cohort_repository.get(experiment.cohort_id)
        await self._replay_config_repository.get(experiment.replay_config_id)
        self._check_duplicate_name(experiment)
        now = datetime.now(UTC)
        stored = experiment.model_copy(update={"created": now, "updated": now})
        self._experiments[stored.id] = stored
        return stored.model_copy()

    async def get(self, experiment_id: uuid.UUID) -> Experiment:
        """Load an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment.
        """
        experiment = self._experiments.get(experiment_id)
        if experiment is None:
            raise ExperimentNotFound(experiment_id)
        return experiment.model_copy()

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], int]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the total match count.
        """
        experiments = sorted(
            self._experiments.values(), key=lambda experiment: experiment.id.int
        )
        if experiment_filter.name is not None:
            experiments = [
                experiment
                for experiment in experiments
                if experiment.name == experiment_filter.name
            ]
        if experiment_filter.tag is not None:
            tagged_ids: set[uuid.UUID] = set()
            if self._tag_repository is not None:
                tagged_ids = self._tag_repository.linked_resource_ids(
                    experiment_filter.tag, TagResourceType.EXPERIMENT
                )
            experiments = [
                experiment for experiment in experiments if experiment.id in tagged_ids
            ]
        total = len(experiments)
        start = (experiment_filter.page - 1) * experiment_filter.page_size
        page = experiments[start : start + experiment_filter.page_size]
        return [experiment.model_copy() for experiment in page], total

    async def update(self, experiment: Experiment) -> Experiment:
        """Persist changes to an existing experiment.

        Args:
            experiment: Experiment with modified fields.

        Raises:
            ExperimentNotFound: No experiment has this id.
            DuplicateExperimentName: The experiment name is already
                registered.
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        stored = self._experiments.get(experiment.id)
        if stored is None:
            raise ExperimentNotFound(experiment.id)
        await self._cohort_repository.get(experiment.cohort_id)
        await self._replay_config_repository.get(experiment.replay_config_id)
        self._check_duplicate_name(experiment)
        now = _renewed_timestamp(stored.updated)
        updated = experiment.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._experiments[experiment.id] = updated
        return updated.model_copy()

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id, including its tag links.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentInUse: The experiment has runs.
        """
        if experiment_id not in self._experiments:
            raise ExperimentNotFound(experiment_id)
        if self.run_repository is not None and await self.run_repository.has_runs(
            experiment_id
        ):
            raise ExperimentInUse(experiment_id)
        del self._experiments[experiment_id]
        if self._tag_repository is not None:
            self._tag_repository.remove_links_for_resource(
                TagResourceType.EXPERIMENT, experiment_id
            )


class FakeJobRepository:
    """In-memory job repository."""

    def __init__(
        self,
        session_repository: FakeSessionRepository,
        agent_version_repository: FakeAgentVersionRepository,
        replay_config_repository: FakeReplayConfigRepository,
        plugin_repository: "FakePluginRepository | None" = None,
        blob_repository: FakeBlobRepository | None = None,
    ) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            session_repository: Fake session repository backing the session
                ids.
            agent_version_repository: Fake agent version repository backing
                the agent version ids.
            replay_config_repository: Fake replay config repository backing
                the config ids.
            plugin_repository: Fake plugin repository backing the plugin
                version ids.
            blob_repository: Fake blob repository backing the payload blob
                ids.
        """
        self._jobs: dict[uuid.UUID, Job] = {}
        self._session_repository = session_repository
        self.agent_version_repository = agent_version_repository
        self._replay_config_repository = replay_config_repository
        self._plugin_repository = plugin_repository
        self._blob_repository = blob_repository
        self.run_repository: FakeExperimentRunRepository | None = None
        session_repository.job_repository = self
        agent_version_repository.job_repository = self
        replay_config_repository.job_repository = self

    def references_session(self, session_id: uuid.UUID) -> bool:
        """Report whether a stored job references a session.

        Args:
            session_id: Id of the session.

        Returns:
            ``True`` when a stored job references the session.
        """
        return any(
            job.result_session_id == session_id
            or (isinstance(job, Replay | Score) and job.input_session_id == session_id)
            for job in self._jobs.values()
        )

    def references_config(self, config_id: uuid.UUID) -> bool:
        """Report whether a stored job references a replay config.

        Args:
            config_id: Id of the replay config.

        Returns:
            ``True`` when a stored job references the config.
        """
        return any(
            isinstance(job, Replay) and job.replay_config_id == config_id
            for job in self._jobs.values()
        )

    def remove_for_run(self, run_id: uuid.UUID) -> None:
        """Remove every job of an experiment run.

        Args:
            run_id: Id of the experiment run.
        """
        self._jobs = {
            job_id: job
            for job_id, job in self._jobs.items()
            if not (isinstance(job, Replay) and job.experiment_run_id == run_id)
        }

    def _check_duplicate_session(self, job: Job) -> None:
        if not isinstance(job, Replay) or job.experiment_run_id is None:
            return
        for other in self._jobs.values():
            if (
                other.id != job.id
                and isinstance(other, Replay)
                and other.experiment_run_id == job.experiment_run_id
                and other.input_session_id == job.input_session_id
            ):
                raise DuplicateReplaySession(
                    job.experiment_run_id, job.input_session_id
                )

    def _check_duplicate_scorer(self, job: Job) -> None:
        if not isinstance(job, Score) or job.parent_job_id is None:
            return
        for other in self._jobs.values():
            if (
                other.id != job.id
                and isinstance(other, Score)
                and other.parent_job_id == job.parent_job_id
                and other.input_session_id == job.input_session_id
                and other.scorer_config.name == job.scorer_config.name
            ):
                raise DuplicateScoreJob(
                    job.parent_job_id,
                    job.input_session_id,
                    job.scorer_config.name,
                )

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Raises:
            ExperimentRunNotFound: No experiment run has the job's
                experiment run id.
            ReplayConfigNotFound: No replay config has the job's job
                config id.
            AgentVersionNotFound: No agent version has the job's agent
                version id.
            SessionNotFound: No session has the job's original session
                id.
            DuplicateReplaySession: The run already replays the original
                session.

        Returns:
            Stored job with timestamps set.
        """
        await self._check_references(job)
        now = datetime.now(UTC)
        stored = job.model_copy(update={"created": now, "updated": now})
        self._jobs[stored.id] = stored
        return stored.model_copy()

    async def _check_references(self, job: Job) -> None:
        """Check the referenced rows and uniqueness of a job.

        Args:
            job: Job to store.

        Raises:
            ExperimentRunNotFound: No experiment run has the job's
                experiment run id.
            ReplayConfigNotFound: No replay config has the job's job
                config id.
            AgentVersionNotFound: No agent version has the job's agent
                version id.
            PluginVersionIdNotFound: No plugin version has the job's
                plugin version id.
            BlobNotFound: No blob has the job's payload blob id.
            SessionNotFound: No session has the job's input session id.
        """
        if isinstance(job, Import):
            if self._plugin_repository is not None:
                await self._plugin_repository.get_version_by_id(job.plugin_version_id)
            if self._blob_repository is not None:
                await self._blob_repository.get(job.payload_blob_id)
        if isinstance(job, Replay):
            if job.experiment_run_id is not None and self.run_repository is not None:
                await self.run_repository.get(job.experiment_run_id)
            await self._replay_config_repository.get(job.replay_config_id)
            await self._session_repository.get(job.input_session_id)
        if isinstance(job, Score):
            await self._session_repository.get(job.input_session_id)
            if (
                job.plugin_version_id is not None
                and self._plugin_repository is not None
            ):
                await self._plugin_repository.get_version_by_id(job.plugin_version_id)
        if job.agent_version_id is not None:
            await self.agent_version_repository.get(job.agent_version_id)
        self._check_duplicate_session(job)
        self._check_duplicate_scorer(job)

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist new jobs as one batch.

        Args:
            jobs: Jobs to store.

        Raises:
            DuplicateScoreJob: A parent job already scores an input
                session with a scorer.
            AgentVersionNotFound: No agent version has a job's agent
                version id.
            PluginVersionIdNotFound: No plugin version has a job's plugin
                version id.
            SessionNotFound: No session has a job's input session id.

        Returns:
            Stored jobs with timestamps set.
        """
        stored: list[Job] = []
        snapshot = dict(self._jobs)
        try:
            for job in jobs:
                await self._check_references(job)
                now = datetime.now(UTC)
                row = job.model_copy(update={"created": now, "updated": now})
                self._jobs[row.id] = row
                stored.append(row.model_copy())
        except Exception:
            self._jobs = snapshot
            raise
        return stored

    async def get(self, job_id: uuid.UUID, for_update: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            for_update: Lock the row for the transaction.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        _ = for_update
        job = self._jobs.get(job_id)
        if job is None:
            raise JobNotFound(job_id)
        return job.model_copy()

    def _effective_status(self, job: Job, job_filter: JobFilter) -> JobStatus:
        """Compute a job's status with the filter's staleness rule applied.

        Args:
            job: Stored job.
            job_filter: Filter carrying the staleness context.

        Returns:
            Job status.
        """
        if job_filter.stale_before is None or job_filter.max_attempts is None:
            return job.status
        return job.with_staleness(
            job_filter.stale_before, job_filter.max_attempts
        ).status

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], int]:
        """Query jobs matching a filter.

        With the staleness context set, the status filter matches claimed
        or running jobs with lost heartbeats as pending, or as timed
        out once the attempt count reached the maximum.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the total match count.
        """
        jobs = sorted(self._jobs.values(), key=lambda job: job.id.int)
        if job_filter.experiment_run_id is not None:
            jobs = [
                job
                for job in jobs
                if isinstance(job, Replay)
                and job.experiment_run_id == job_filter.experiment_run_id
            ]
        if job_filter.input_session_id is not None:
            jobs = [
                job
                for job in jobs
                if isinstance(job, Replay | Score)
                and job.input_session_id == job_filter.input_session_id
            ]
        if job_filter.kind is not None:
            jobs = [job for job in jobs if job.kind == job_filter.kind]
        if job_filter.execution_target is not None:
            jobs = [
                job
                for job in jobs
                if job.execution_target == job_filter.execution_target
            ]
        if job_filter.status is not None:
            jobs = [
                job
                for job in jobs
                if self._effective_status(job, job_filter) == job_filter.status
            ]
        if job_filter.worker_id is not None:
            jobs = [job for job in jobs if job.worker_id == job_filter.worker_id]
        if job_filter.standalone is not None:
            jobs = [job for job in jobs if job.standalone == job_filter.standalone]
        total = len(jobs)
        start = (job_filter.page - 1) * job_filter.page_size
        page = jobs[start : start + job_filter.page_size]
        return [job.model_copy() for job in page], total

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.
            SessionNotFound: No session has the job's result session id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        stored = self._jobs.get(job.id)
        if stored is None:
            raise JobNotFound(job.id)
        if job.result_session_id is not None:
            await self._session_repository.get(job.result_session_id)
        self._check_duplicate_session(job)
        self._check_duplicate_scorer(job)
        now = _renewed_timestamp(stored.updated)
        updated = job.model_copy(update={"created": stored.created, "updated": now})
        self._jobs[job.id] = updated
        return updated.model_copy()

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        if job_id not in self._jobs:
            raise JobNotFound(job_id)
        del self._jobs[job_id]
        self._delete_children(job_id)

    def _delete_children(self, parent_job_id: uuid.UUID) -> None:
        """Remove every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.
        """
        for job_id in [
            job.id
            for job in self._jobs.values()
            if isinstance(job, Score) and job.parent_job_id == parent_job_id
        ]:
            del self._jobs[job_id]

    async def list_children(self, parent_job_id: uuid.UUID) -> list[Job]:
        """Load every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.

        Returns:
            Child jobs in id order.
        """
        children = [
            job
            for job in self._jobs.values()
            if isinstance(job, Score) and job.parent_job_id == parent_job_id
        ]
        children.sort(key=lambda job: job.id.int)
        return [job.model_copy() for job in children]

    async def delete_children(self, parent_job_id: uuid.UUID) -> None:
        """Delete every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.
        """
        self._delete_children(parent_job_id)

    def _in_scope(
        self,
        job: Job,
        agent_ids: Sequence[uuid.UUID] | None,
        experiment_run_id: uuid.UUID | None,
        parent_job_id: uuid.UUID | None,
    ) -> bool:
        """Report whether a job is within the claim scope.

        Args:
            job: Stored job.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.

        Returns:
            ``True`` when the job is in scope.
        """
        if parent_job_id is not None:
            if not (isinstance(job, Score) and job.parent_job_id == parent_job_id):
                return False
        elif experiment_run_id is not None:
            if not self._belongs_to_run(job, experiment_run_id):
                return False
        elif job.execution_target is not ExecutionTarget.POOL:
            return False
        if agent_ids is not None and job.agent_version_id is not None:
            agent_id = self.agent_version_repository.agent_id_of(job.agent_version_id)
            if agent_id not in agent_ids:
                return False
        return True

    def _belongs_to_run(self, job: Job, experiment_run_id: uuid.UUID) -> bool:
        """Report whether a job or its parent belongs to an experiment run.

        Args:
            job: Stored job.
            experiment_run_id: Id of the experiment run.

        Returns:
            ``True`` when the job is part of the run.
        """
        if isinstance(job, Replay):
            return job.experiment_run_id == experiment_run_id
        if isinstance(job, Score) and job.parent_job_id is not None:
            parent = self._jobs.get(job.parent_job_id)
            return (
                isinstance(parent, Replay)
                and parent.experiment_run_id == experiment_run_id
            )
        return False

    async def requeue_stale(
        self,
        stale_before: datetime,
        max_attempts: int,
        agent_ids: Sequence[uuid.UUID] | None = None,
        experiment_run_id: uuid.UUID | None = None,
        parent_job_id: uuid.UUID | None = None,
    ) -> list[Job]:
        """Requeue or time out jobs with lost heartbeats within a scope.

        Args:
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.

        Returns:
            Jobs the staleness rule moved.
        """
        resolved: list[Job] = []
        for job_id, job in list(self._jobs.items()):
            if not self._in_scope(job, agent_ids, experiment_run_id, parent_job_id):
                continue
            changed = job.with_staleness(stale_before, max_attempts)
            if changed is job:
                continue
            stored = changed.model_copy(
                update={"updated": _renewed_timestamp(job.updated)}
            )
            self._jobs[job_id] = stored
            resolved.append(stored.model_copy())
        return resolved

    async def claim_pending(
        self,
        worker_id: uuid.UUID,
        limit: int,
        agent_ids: Sequence[uuid.UUID] | None = None,
        experiment_run_id: uuid.UUID | None = None,
        parent_job_id: uuid.UUID | None = None,
    ) -> list[Job]:
        """Atomically claim pending jobs within a scope for a worker.

        Args:
            worker_id: Id of the claiming worker.
            limit: Maximum number of jobs to claim.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.

        Returns:
            Claimed jobs.
        """
        pending = sorted(
            (
                job
                for job in self._jobs.values()
                if job.status is JobStatus.PENDING
                and self._in_scope(job, agent_ids, experiment_run_id, parent_job_id)
            ),
            key=lambda job: job.id.int,
        )
        claimed: list[Job] = []
        for job in pending[:limit]:
            changed = job.model_copy()
            changed.claim(worker_id)
            stored = changed.model_copy(
                update={"updated": _renewed_timestamp(job.updated)}
            )
            self._jobs[job.id] = stored
            claimed.append(stored.model_copy())
        return claimed

    async def heartbeat_many(
        self,
        worker_id: uuid.UUID,
        job_ids: Sequence[uuid.UUID],
        heartbeat_at: datetime,
    ) -> list[Job]:
        """Record one worker heartbeat on every claimed or running job it owns.

        Args:
            worker_id: Id of the heartbeating worker.
            job_ids: Ids of the jobs the worker reports.
            heartbeat_at: Time of the heartbeat.

        Returns:
            Jobs the heartbeat reached.
        """
        reached: list[Job] = []
        for job_id in job_ids:
            job = self._jobs.get(job_id)
            if job is None or job.worker_id != worker_id:
                continue
            if job.status not in (JobStatus.CLAIMED, JobStatus.RUNNING):
                continue
            stored = job.model_copy(
                update={
                    "heartbeat_at": heartbeat_at,
                    "updated": _renewed_timestamp(job.updated),
                }
            )
            self._jobs[job_id] = stored
            reached.append(stored.model_copy())
        return reached

    async def count_by_status(
        self, run_ids: list[uuid.UUID], stale_before: datetime, max_attempts: int
    ) -> dict[uuid.UUID, dict[JobStatus, int]]:
        """Count jobs by status for a set of experiment runs.

        Claimed or running jobs with lost heartbeats count as pending,
        or as timed out once the attempt count reached the maximum, without
        writing.

        Args:
            run_ids: Ids of the experiment runs.
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.

        Returns:
            Job counts by status, keyed by experiment run id.
        """
        counts: dict[uuid.UUID, dict[JobStatus, int]] = {}
        for job in self._jobs.values():
            if not isinstance(job, Replay) or job.experiment_run_id not in run_ids:
                continue
            status = job.with_staleness(stale_before, max_attempts).status
            run_counts = counts.setdefault(job.experiment_run_id, {})
            run_counts[status] = run_counts.get(status, 0) + 1
        return counts

    async def references_agent_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored job references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored job references the version.
        """
        return any(job.agent_version_id == version_id for job in self._jobs.values())


class FakeExperimentRunRepository:
    """In-memory experiment run repository."""

    def __init__(
        self,
        experiment_repository: FakeExperimentRepository,
        job_repository: FakeJobRepository,
        tag_repository: FakeTagRepository | None = None,
    ) -> None:
        """Initialize the repository and wire the reference checks.

        Args:
            experiment_repository: Fake experiment repository backing the
                experiment ids.
            job_repository: Fake job repository storing the run's
                jobs.
            tag_repository: Fake tag repository backing the tag filter.
        """
        self._runs: dict[uuid.UUID, ExperimentRun] = {}
        self._experiment_repository = experiment_repository
        self._job_repository = job_repository
        self._tag_repository = tag_repository
        experiment_repository.run_repository = self
        job_repository.run_repository = self
        job_repository.agent_version_repository.run_repository = self

    def references_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored run references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored run references the version.
        """
        return any(run.agent_version_id == version_id for run in self._runs.values())

    async def create(self, run: ExperimentRun, jobs: list[Replay]) -> ExperimentRun:
        """Persist a new experiment run with its jobs as one batch.

        Args:
            run: Experiment run to store.
            jobs: Jobs to store with the run.

        Raises:
            ExperimentNotFound: No experiment has the run's experiment id.

        Returns:
            Stored experiment run with the number and timestamps set.
        """
        await self._experiment_repository.get(run.experiment_id)
        number = (
            max(
                (
                    other.number
                    for other in self._runs.values()
                    if other.experiment_id == run.experiment_id
                ),
                default=0,
            )
            + 1
        )
        now = datetime.now(UTC)
        stored = run.model_copy(
            update={"number": number, "created": now, "updated": now}
        )
        self._runs[stored.id] = stored
        for job in jobs:
            await self._job_repository.create(job)
        return stored.model_copy()

    async def get(self, run_id: uuid.UUID) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            run_id: Id of the experiment run.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Stored experiment run.
        """
        run = self._runs.get(run_id)
        if run is None:
            raise ExperimentRunNotFound(run_id)
        return run.model_copy()

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], int]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiment runs and the total match count.
        """
        runs = sorted(self._runs.values(), key=lambda run: run.id.int)
        if run_filter.experiment_id is not None:
            runs = [
                run for run in runs if run.experiment_id == run_filter.experiment_id
            ]
        if run_filter.status is not None:
            runs = [run for run in runs if run.status == run_filter.status]
        if run_filter.tag is not None:
            tagged_ids: set[uuid.UUID] = set()
            if self._tag_repository is not None:
                tagged_ids = self._tag_repository.linked_resource_ids(
                    run_filter.tag, TagResourceType.EXPERIMENT_RUN
                )
            runs = [run for run in runs if run.id in tagged_ids]
        total = len(runs)
        start = (run_filter.page - 1) * run_filter.page_size
        page = runs[start : start + run_filter.page_size]
        return [run.model_copy() for run in page], total

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

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

    async def delete(self, run_id: uuid.UUID) -> None:
        """Delete an experiment run by id, including its jobs and tag links.

        Args:
            run_id: Id of the experiment run.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.
        """
        if run_id not in self._runs:
            raise ExperimentRunNotFound(run_id)
        del self._runs[run_id]
        self._job_repository.remove_for_run(run_id)
        if self._tag_repository is not None:
            self._tag_repository.remove_links_for_resource(
                TagResourceType.EXPERIMENT_RUN, run_id
            )

    async def has_runs(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has stored runs.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            ``True`` when a stored run belongs to the experiment.
        """
        return any(run.experiment_id == experiment_id for run in self._runs.values())


EXPERIMENT_APP_ACCOUNT_ID = uuid.uuid4()


def experiment_app(
    heartbeat_timeout_seconds: int = 60, max_attempts: int = 3
) -> FastAPI:
    """Build the app with every service bound to one set of shared fakes.

    Args:
        heartbeat_timeout_seconds: Seconds after which a heartbeat counts
            as lost, a negative value marks every claim stale immediately.
        max_attempts: Attempt count at which a stale job times out.

    Returns:
        Application for experiment, run, and job tests.
    """
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    secret_repository = FakeSecretRepository()
    tag_repository = FakeTagRepository()
    version_repository = FakeAgentVersionRepository(agent_repository, secret_repository)
    session_repository = FakeSessionRepository(
        agent_repository, version_repository, tag_repository
    )
    node_repository = FakeSessionNodeRepository(session_repository)
    cohort_repository = FakeCohortRepository(
        session_repository, agent_repository, tag_repository
    )
    config_repository = FakeReplayConfigRepository()
    experiment_repository = FakeExperimentRepository(
        cohort_repository, config_repository, tag_repository
    )
    blob_repository = FakeBlobRepository()
    plugin_repository = FakePluginRepository(blob_repository)
    job_repository = FakeJobRepository(
        session_repository,
        version_repository,
        config_repository,
        plugin_repository,
        blob_repository,
    )
    run_repository = FakeExperimentRunRepository(
        experiment_repository, job_repository, tag_repository
    )
    worker_repository = FakeWorkerRepository()
    agent_service = AgentService(repository=agent_repository)
    version_service = AgentVersionService(
        repository=version_repository,
        agent_repository=agent_repository,
        secret_repository=secret_repository,
        job_repository=job_repository,
    )
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        job_repository=job_repository,
    )
    cohort_service = CohortService(
        repository=cohort_repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
    )
    experiment_service = ExperimentService(
        repository=experiment_repository,
        run_repository=run_repository,
        cohort_repository=cohort_repository,
        agent_version_repository=version_repository,
        replay_config_repository=config_repository,
        worker_repository=worker_repository,
        plugin_repository=plugin_repository,
        worker_liveness_timeout_seconds=60,
    )
    run_service = ExperimentRunService(
        repository=run_repository,
        job_repository=job_repository,
        replay_config_repository=config_repository,
        experiment_repository=experiment_repository,
        session_repository=session_repository,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        max_attempts=max_attempts,
    )
    job_service = JobService(
        repository=job_repository,
        replay_config_repository=config_repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        session_node_repository=node_repository,
        experiment_run_repository=run_repository,
        experiment_repository=experiment_repository,
        cohort_repository=cohort_repository,
        secret_repository=secret_repository,
        worker_repository=worker_repository,
        plugin_repository=plugin_repository,
        blob_repository=blob_repository,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        max_attempts=max_attempts,
        worker_liveness_timeout_seconds=60,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
    )
    tag_service = TagService(repository=tag_repository)
    worker_service = WorkerService(
        repository=worker_repository, liveness_timeout_seconds=60
    )
    scorer_service = PluginService(
        repository=plugin_repository,
        blob_repository=blob_repository,
        kind=PluginKind.SCORER,
    )
    importer_service = PluginService(
        repository=plugin_repository,
        blob_repository=blob_repository,
        kind=PluginKind.IMPORTER,
    )
    blob_service = BlobService(
        repository=blob_repository,
        storage=DatabaseBlobStorage(),
        max_size_bytes=APISettings.model_fields["MAX_BLOB_SIZE_BYTES"].default,
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_agent_version_service] = lambda: version_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[get_cohort_service] = lambda: cohort_service
    app.dependency_overrides[get_experiment_service] = lambda: experiment_service
    app.dependency_overrides[get_experiment_run_service] = lambda: run_service
    app.dependency_overrides[get_job_service] = lambda: job_service
    app.dependency_overrides[get_tag_service] = lambda: tag_service
    app.dependency_overrides[get_worker_service] = lambda: worker_service
    app.dependency_overrides[get_scorer_service] = lambda: scorer_service
    app.dependency_overrides[get_importer_service] = lambda: importer_service
    app.dependency_overrides[get_blob_service] = lambda: blob_service
    app.dependency_overrides[authorize] = lambda: AuthContext(
        account=Account(id=EXPERIMENT_APP_ACCOUNT_ID, name="ann")
    )
    return app
