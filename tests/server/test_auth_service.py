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
"""Tests for the authentication service."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakePasswordHasher,
    control_plane_settings,
    create_api_key,
    local_settings,
)
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.auth_service import (
    LAST_USED_UPDATE_INTERVAL_SECONDS,
    AuthenticationError,
    AuthService,
)
from kitaru.server.adapters.auth.jwt import JWTToken, TaskSubject
from kitaru.server.application.models.auth import (
    GrantKind,
    TaskPrincipal,
    WorkerPrincipal,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.api_key import encode_api_key
from kitaru.server.domain.keys import generate_secret


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
def api_key_repository() -> FakeApiKeyRepository:
    """Provide a fake API key repository."""
    return FakeApiKeyRepository()


@pytest.fixture
def service(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> AuthService:
    """Provide an authentication service backed by the fake repositories."""
    return AuthService(
        settings=local_settings(),
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )


async def create_account(
    repository: FakeAccountRepository,
    name: str = "alice",
    password: str | None = "secret",
    active: bool = True,
) -> Account:
    """Store an account in the fake repository.

    Args:
        repository: Fake account repository.
        name: Account name.
        password: Login password, stored unhashed via the fake hasher.
        active: Active state.

    Returns:
        Stored account.
    """
    password_hash = None
    if password is not None:
        password_hash = FakePasswordHasher().hash(password)
    return await repository.create(
        Account(name=name, password_hash=password_hash, active=active)
    )


async def test_api_key_auth(
    service: AuthService,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Authenticate a raw API key and record its use."""
    account = await create_account(account_repository)
    api_key, key = await create_api_key(api_key_repository, account.id)

    context = await service.resolve(key)
    assert context.account.id == account.id
    assert context.account.name == account.name
    stored = await api_key_repository.get(api_key.id)
    assert stored.last_used is not None


async def test_api_key_auth_throttles_last_used(
    service: AuthService,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Skip the last_used write while the stored value is fresh."""
    account = await create_account(account_repository)
    api_key, key = await create_api_key(api_key_repository, account.id)

    await service.resolve(key)
    first = (await api_key_repository.get(api_key.id)).last_used
    await service.resolve(key)
    assert (await api_key_repository.get(api_key.id)).last_used == first

    stale = datetime.now(UTC) - timedelta(seconds=LAST_USED_UPDATE_INTERVAL_SECONDS + 1)
    stored = await api_key_repository.get(api_key.id)
    stored.mark_used(stale)
    await api_key_repository.update(stored)
    await service.resolve(key)
    refreshed = (await api_key_repository.get(api_key.id)).last_used
    assert refreshed is not None
    assert refreshed > stale


async def test_api_key_auth_wrong_secret(
    service: AuthService,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Reject an API key with a wrong secret."""
    account = await create_account(account_repository)
    api_key, _ = await create_api_key(api_key_repository, account.id)
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await service.resolve(encode_api_key(api_key.id, generate_secret()))


async def test_api_key_auth_unknown_id(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Reject an API key with an unknown id."""
    await create_account(account_repository)
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await service.resolve(encode_api_key(uuid.uuid4(), generate_secret()))


async def test_api_key_auth_inactive_key(
    service: AuthService,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Reject an inactive API key."""
    account = await create_account(account_repository)
    _, key = await create_api_key(api_key_repository, account.id, active=False)
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await service.resolve(key)


async def test_api_key_auth_inactive_owner(
    service: AuthService,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Reject an API key whose owner account is inactive."""
    account = await create_account(account_repository, active=False)
    _, key = await create_api_key(api_key_repository, account.id)
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await service.resolve(key)


@pytest.mark.parametrize(
    "credential",
    ["KITKEY_", "KITKEY_garbage", "KITKEY_bm90IGpzb24="],
)
async def test_api_key_auth_malformed(service: AuthService, credential: str) -> None:
    """Reject malformed API key strings."""
    with pytest.raises(AuthenticationError, match="Invalid API key"):
        await service.resolve(credential)


async def test_login_with_password(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Issue a session token for valid password credentials."""
    account = await create_account(account_repository)

    issued = await service.login_with_password("alice", "secret")
    assert issued.csrf_token is None
    decoded = JWTToken.decode(issued.token, local_settings())
    assert decoded.subject.account_id == account.id
    assert decoded.expires_at == issued.expires_at.replace(microsecond=0)

    context = await service.resolve(issued.token)
    assert context.account.id == account.id


async def test_login_with_password_failures(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Reject every invalid password login with one message."""
    await create_account(account_repository)
    await create_account(account_repository, name="bob", password=None)
    await create_account(account_repository, name="carol", active=False)

    for username, password in [
        ("alice", "wrong"),
        ("unknown", "secret"),
        ("carol", "secret"),
        ("bob", "secret"),
    ]:
        with pytest.raises(AuthenticationError, match="Invalid username or password"):
            await service.login_with_password(username, password)


async def test_resolved_token_with_deactivated_account(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Reject a session token whose account was deactivated after issuance."""
    account = await create_account(account_repository)
    token = (await service.login_with_password("alice", "secret")).token

    stored = await account_repository.get(account.id)
    stored.update_active(False)
    await account_repository.update(stored)

    with pytest.raises(AuthenticationError, match="Invalid session token"):
        await service.resolve(token)


async def test_csrf_token(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Enforce the CSRF token on cookie credentials only."""
    settings = local_settings(AUTH_COOKIE_NAME="kitaru_session")
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    await create_account(account_repository)

    issued = await service.login_with_password("alice", "secret")
    assert issued.csrf_token is not None

    await service.resolve(issued.token, csrf_token=issued.csrf_token, from_cookie=True)

    with pytest.raises(AuthenticationError, match="Missing or invalid CSRF token"):
        await service.resolve(issued.token, from_cookie=True)
    with pytest.raises(AuthenticationError, match="Missing or invalid CSRF token"):
        await service.resolve(issued.token, csrf_token="wrong", from_cookie=True)


async def test_csrf_token_not_required_for_header_credentials(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Accept a session token from the authorization header without a CSRF token."""
    settings = local_settings(AUTH_COOKIE_NAME="kitaru_session")
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    await create_account(account_repository)

    token = (await service.login_with_password("alice", "secret")).token

    context = await service.resolve(token)
    assert context.account.name == "alice"


async def test_worker_token_resolves_to_a_worker_principal(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Resolve a worker token into a worker principal scoped to its account."""
    account = await create_account(account_repository)
    worker_id = uuid.uuid4()
    token = service.issue_worker_token(worker_id=worker_id, account_id=account.id).token

    context = await service.resolve(token)

    assert context.account.id == account.id
    assert isinstance(context.principal, WorkerPrincipal)
    assert context.principal.worker_id == worker_id


async def test_task_token_resolves_to_a_task_principal(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Resolve a task token into a task principal scoped to its account."""
    account = await create_account(account_repository)
    task_id = uuid.uuid4()
    worker_id = uuid.uuid4()
    job_id = uuid.uuid4()
    token = service.issue_task_token(
        TaskSubject(
            task_id=task_id,
            attempt=2,
            worker_id=worker_id,
            account_id=account.id,
            job_id=job_id,
        ),
        timeout_seconds=3600,
    ).token

    context = await service.resolve(token)

    assert context.account.id == account.id
    assert isinstance(context.principal, TaskPrincipal)
    assert context.principal.task_id == task_id
    assert context.principal.attempt == 2
    assert context.principal.worker_id == worker_id
    assert context.principal.job_id == job_id


async def test_task_token_carries_grants(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Resolve a task token's grants claim onto its task principal."""
    account = await create_account(account_repository)
    session_id = uuid.uuid4()
    token = service.issue_task_token(
        TaskSubject(
            task_id=uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=account.id,
            job_id=uuid.uuid4(),
            grants={GrantKind.SESSION: frozenset({session_id})},
        ),
        timeout_seconds=3600,
    ).token

    context = await service.resolve(token)

    assert isinstance(context.principal, TaskPrincipal)
    assert context.principal.has_grant(GrantKind.SESSION, session_id)


async def test_task_token_grants_default_to_empty(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Leave the grants empty on a task token that names none."""
    account = await create_account(account_repository)
    token = service.issue_task_token(
        TaskSubject(
            task_id=uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=account.id,
            job_id=uuid.uuid4(),
        ),
        timeout_seconds=3600,
    ).token

    context = await service.resolve(token)

    assert isinstance(context.principal, TaskPrincipal)
    assert context.principal.grants == {}


async def test_worker_token_rejects_a_deactivated_account(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Reject a worker token whose account was deactivated after issuance."""
    account = await create_account(account_repository, active=False)
    token = service.issue_worker_token(
        worker_id=uuid.uuid4(), account_id=account.id
    ).token

    with pytest.raises(AuthenticationError, match="Invalid worker token"):
        await service.resolve(token)


async def test_task_token_rejects_a_deactivated_account(
    service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Reject a task token whose account was deactivated after issuance."""
    account = await create_account(account_repository, active=False)
    token = service.issue_task_token(
        TaskSubject(
            task_id=uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=account.id,
            job_id=uuid.uuid4(),
        ),
        timeout_seconds=3600,
    ).token

    with pytest.raises(AuthenticationError, match="Invalid task token"):
        await service.resolve(token)


async def test_try_resolve_worker_or_task_resolves_a_worker_token(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Resolve a worker token under the none auth scheme."""
    settings = local_settings(AUTH_SCHEME=AuthScheme.NONE)
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    account = await create_account(account_repository)
    worker_id = uuid.uuid4()
    token = service.issue_worker_token(worker_id=worker_id, account_id=account.id).token

    context = await service.try_resolve_worker_or_task(token)

    assert context is not None
    assert isinstance(context.principal, WorkerPrincipal)
    assert context.principal.worker_id == worker_id


async def test_try_resolve_worker_or_task_resolves_a_task_token(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Resolve a task token under the none auth scheme."""
    settings = local_settings(AUTH_SCHEME=AuthScheme.NONE)
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    account = await create_account(account_repository)
    task_id = uuid.uuid4()
    token = service.issue_task_token(
        TaskSubject(
            task_id=task_id,
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=account.id,
            job_id=uuid.uuid4(),
        ),
        timeout_seconds=3600,
    ).token

    context = await service.try_resolve_worker_or_task(token)

    assert context is not None
    assert isinstance(context.principal, TaskPrincipal)
    assert context.principal.task_id == task_id


async def test_try_resolve_worker_or_task_returns_none_for_an_account_token(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Return None for an account session token, deferring to the default account."""
    settings = local_settings(AUTH_SCHEME=AuthScheme.NONE)
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    await create_account(account_repository)
    token = (await service.login_with_password("alice", "secret")).token

    assert await service.try_resolve_worker_or_task(token) is None


async def test_try_resolve_worker_or_task_returns_none_for_garbage(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Return None for a credential that does not decode as a token at all."""
    settings = local_settings(AUTH_SCHEME=AuthScheme.NONE)
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    assert await service.try_resolve_worker_or_task("not-a-token") is None


async def test_control_plane_scheme_resolves_a_worker_token_without_an_external_account(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """A worker token resolves under the control plane scheme without an external id."""
    settings = control_plane_settings()
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    account = await create_account(account_repository)
    assert account.external_id is None
    worker_id = uuid.uuid4()
    token = service.issue_worker_token(worker_id=worker_id, account_id=account.id).token

    context = await service.resolve(token)

    assert isinstance(context.principal, WorkerPrincipal)
    assert context.account.id == account.id


async def test_control_plane_scheme_resolves_a_task_token_without_an_external_account(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """A task token resolves under the control plane scheme without an external id."""
    settings = control_plane_settings()
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    account = await create_account(account_repository)
    assert account.external_id is None
    task_id = uuid.uuid4()
    token = service.issue_task_token(
        TaskSubject(
            task_id=task_id,
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=account.id,
            job_id=uuid.uuid4(),
        ),
        timeout_seconds=3600,
    ).token

    context = await service.resolve(token)

    assert isinstance(context.principal, TaskPrincipal)
    assert context.account.id == account.id
