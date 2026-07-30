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
    FakeControlPlaneClient,
    FakePasswordHasher,
    control_plane_settings,
    create_api_key,
    local_settings,
)
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.auth_service import (
    LAST_USED_UPDATE_INTERVAL_SECONDS,
    AuthenticationError,
    AuthenticationServiceUnavailableError,
    AuthService,
)
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneUnavailableError,
    ControlPlaneUser,
)
from kitaru.server.adapters.auth.jwt import JWTToken
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


def create_cloud_service(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    client: FakeControlPlaneClient,
) -> AuthService:
    """Build a Cloud authentication service backed by fakes."""
    settings = control_plane_settings().model_copy(
        update={"AUTH_SCHEME": AuthScheme.CLOUD}
    )
    return AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
        control_plane=ControlPlaneAuthenticator(
            client=client,
            account_repository=account_repository,
            server_id=settings.SERVER_ID,
        ),
    )


async def test_cloud_authenticates_raw_bearer_credential(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Authorize every Cloud bearer credential against the control plane."""
    user = ControlPlaneUser(id=uuid.uuid4(), username="alice")
    client = FakeControlPlaneClient(user=user)
    service = create_cloud_service(account_repository, api_key_repository, client)

    context = await service.resolve("raw-cloud-token")

    assert client.received_credentials == ["raw-cloud-token"]
    assert context.account.external_id == user.id


async def test_cloud_reports_control_plane_outage(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Keep an authorization outage distinct from an invalid credential."""
    client = FakeControlPlaneClient(error=ControlPlaneUnavailableError("timeout"))
    service = create_cloud_service(account_repository, api_key_repository, client)

    with pytest.raises(
        AuthenticationServiceUnavailableError,
        match="temporarily unavailable",
    ):
        await service.resolve("raw-cloud-token")


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

    token, expires_at, csrf_token = await service.login_with_password("alice", "secret")
    assert csrf_token is None
    decoded = JWTToken.decode(token, local_settings())
    assert decoded.account_id == account.id
    assert decoded.expires_at == expires_at.replace(microsecond=0)

    context = await service.resolve(token)
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
    token, _, _ = await service.login_with_password("alice", "secret")

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

    token, _, csrf_token = await service.login_with_password("alice", "secret")
    assert csrf_token is not None

    await service.resolve(token, csrf_token=csrf_token, from_cookie=True)

    with pytest.raises(AuthenticationError, match="Missing or invalid CSRF token"):
        await service.resolve(token, from_cookie=True)
    with pytest.raises(AuthenticationError, match="Missing or invalid CSRF token"):
        await service.resolve(token, csrf_token="wrong", from_cookie=True)


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

    token, _, _ = await service.login_with_password("alice", "secret")

    context = await service.resolve(token)
    assert context.account.name == "alice"
