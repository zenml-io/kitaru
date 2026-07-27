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
"""Tests for the control plane authenticator."""

import uuid

import pytest

from conftest import FakeAccountRepository, FakeControlPlaneClient
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneError,
    ControlPlaneUser,
    ServerAuthorization,
)
from kitaru.server.domain.account import Account

SERVER_ID = uuid.uuid4()


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


def control_plane_user(
    username: str | None = "alice",
    email: str | None = "alice@example.com",
    is_service_account: bool = False,
    user_id: uuid.UUID | None = None,
) -> ControlPlaneUser:
    """Build a control plane user.

    Args:
        username: Control plane username.
        email: Control plane email.
        is_service_account: Whether the user is a service account.
        user_id: Control plane user id.

    Returns:
        Control plane user.
    """
    return ControlPlaneUser(
        id=user_id or uuid.uuid4(),
        username=username,
        email=email,
        is_service_account=is_service_account,
    )


def build_authenticator(
    account_repository: FakeAccountRepository,
    user: ControlPlaneUser | None,
) -> tuple[ControlPlaneAuthenticator, FakeControlPlaneClient]:
    """Build an authenticator wired to a fake control plane client.

    Args:
        account_repository: Fake account repository.
        user: Control plane user returned by the fake client.

    Returns:
        Authenticator under test and the fake client it calls.
    """
    client = FakeControlPlaneClient(
        authorization=ServerAuthorization(user=user, server_id=SERVER_ID)
    )
    authenticator = ControlPlaneAuthenticator(
        client=client, account_repository=account_repository, server_id=SERVER_ID
    )
    return authenticator, client


async def test_authenticate_creates_new_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Create a mirrored account when no account matches the control plane user."""
    user = control_plane_user()
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    account = context.account
    assert account.external_id == user.id
    assert account.name == user.username
    assert account.email == user.email
    assert account.is_service_account == user.is_service_account
    assert account.active is True


async def test_authenticate_refreshes_existing_mirrored_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Refresh the name and email on an account already mirroring this user."""
    external_id = uuid.uuid4()
    stored = await account_repository.create(
        Account(external_id=external_id, name="alice", email="old@example.com")
    )
    user = control_plane_user(
        username="alice2", email="new@example.com", user_id=external_id
    )
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.id == stored.id
    assert context.account.name == "alice2"
    assert context.account.email == "new@example.com"


async def test_authenticate_reactivates_deactivated_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Reactivate a mirrored account that was deactivated locally."""
    external_id = uuid.uuid4()
    stored = await account_repository.create(
        Account(external_id=external_id, name="alice", active=False)
    )
    user = control_plane_user(username="alice", user_id=external_id)
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.id == stored.id
    assert context.account.active is True


async def test_authenticate_never_claims_a_local_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise instead of turning a same-named local account into an external one."""
    local_account = await account_repository.create(Account(name="alice"))
    user = control_plane_user(username="alice")
    authenticator, _ = build_authenticator(account_repository, user)

    with pytest.raises(ControlPlaneError):
        await authenticator.authenticate("credential")

    stored = await account_repository.get(local_account.id)
    assert stored.external_id is None


async def test_authenticate_keeps_user_and_service_account_namespaces_separate(
    account_repository: FakeAccountRepository,
) -> None:
    """Mirror a service account alongside a same-named local user account."""
    user_account = await account_repository.create(
        Account(name="alice", is_service_account=False)
    )
    user = control_plane_user(username="alice", is_service_account=True)
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.id != user_account.id
    assert context.account.is_service_account is True


async def test_authenticate_raises_when_no_user(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise when the control plane authorization identifies no user."""
    client = FakeControlPlaneClient(
        authorization=ServerAuthorization(user=None, server_id=SERVER_ID)
    )
    authenticator = ControlPlaneAuthenticator(
        client=client, account_repository=account_repository, server_id=SERVER_ID
    )

    with pytest.raises(ControlPlaneError, match="identifies no user"):
        await authenticator.authenticate("credential")


async def test_authenticate_raises_when_username_missing(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise when the control plane user has no username to mirror."""
    user = control_plane_user(username=None)
    authenticator, _ = build_authenticator(account_repository, user)

    with pytest.raises(ControlPlaneError, match="no username to mirror"):
        await authenticator.authenticate("credential")


async def test_authenticate_raises_when_username_invalid(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise when the control plane username is not a valid account name."""
    user = control_plane_user(username="alice@example.com")
    authenticator, _ = build_authenticator(account_repository, user)

    with pytest.raises(ControlPlaneError, match="not a valid account name"):
        await authenticator.authenticate("credential")


async def test_authenticate_passes_server_id_to_client(
    account_repository: FakeAccountRepository,
) -> None:
    """Pass the configured server id through to the control plane client."""
    user = control_plane_user()
    authenticator, client = build_authenticator(account_repository, user)

    await authenticator.authenticate("credential")

    assert client.received_server_id == SERVER_ID
    assert client.received_credential == "credential"
