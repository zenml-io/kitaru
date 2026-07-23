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
from sqlmodel import SQLModel

from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings, AuthScheme
from kitaru.server.application.models.accounts import AccountFilter
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.application.models.api_keys import ApiKeyFilter
from kitaru.server.application.models.secrets import SecretFilter
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.application.models.tags import TagFilter
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
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretInUse,
    SecretNotFound,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionNotFound,
)
from kitaru.server.domain.session_node import (
    DuplicateNodeExternalId,
    DuplicateNodeKey,
    DuplicateNodeSequence,
    DuplicateSessionNodeId,
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
        DB_NAME="kitaru_test",
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


class FakeAgentRepository:
    """In-memory agent repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._agents: dict[uuid.UUID, Agent] = {}
        self.version_repository: FakeAgentVersionRepository | None = None

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
            AgentInUse: The agent still has versions.
        """
        if agent_id not in self._agents:
            raise AgentNotFound(agent_id)
        if self.version_repository is not None and self.version_repository.has_versions(
            agent_id
        ):
            raise AgentInUse(agent_id)
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
        """
        if version_id not in self._versions:
            raise AgentVersionNotFound(version_id)
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
        """
        if session_id not in self._sessions:
            raise SessionNotFound(session_id)
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
