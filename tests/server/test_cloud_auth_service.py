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
"""Tests for Cloud-backed authentication."""

import uuid

import pytest

from conftest import FakeAccountRepository
from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthenticationServiceUnavailableError,
)
from kitaru.server.adapters.auth.cloud_auth_service import CloudAuthService
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthorizationError,
    ControlPlaneUnavailableError,
    ControlPlaneUser,
)
from kitaru.server.domain.account import Account


class FakeControlPlaneClient:
    """Control-plane client returning a configured result."""

    def __init__(
        self,
        user: ControlPlaneUser | None = None,
        error: Exception | None = None,
    ) -> None:
        """Initialize the fake client."""
        self.user = user
        self.error = error
        self.calls: list[tuple[str, uuid.UUID]] = []

    async def authorize_user(
        self, credential: str, server_id: uuid.UUID
    ) -> ControlPlaneUser:
        """Return the configured authorization result."""
        self.calls.append((credential, server_id))
        if self.error is not None:
            raise self.error
        assert self.user is not None
        return self.user


async def test_cloud_auth_creates_workspace_account() -> None:
    """Create one shared data account for the Cloud workspace."""
    server_id = uuid.uuid4()
    user = ControlPlaneUser(
        id=uuid.uuid4(),
        username="alice@example.com",
        email="alice@example.com",
    )
    client = FakeControlPlaneClient(user=user)
    repository = FakeAccountRepository()
    service = CloudAuthService(
        server_id=server_id,
        control_plane=client,
        account_repository=repository,
    )

    context = await service.resolve("cloud-token")

    assert client.calls == [("cloud-token", server_id)]
    assert context.account.id == server_id
    assert context.account.name == f"cloud-workspace-{server_id.hex}"
    assert context.principal_id == user.id


async def test_cloud_auth_reuses_workspace_account() -> None:
    """All authorized principals share the workspace data account."""
    server_id = uuid.uuid4()
    repository = FakeAccountRepository()
    await repository.create(
        Account(
            id=server_id,
            name=f"cloud-workspace-{server_id.hex}",
        )
    )
    user = ControlPlaneUser(
        id=uuid.uuid4(),
        username="automation",
        email="automation@example.com",
        is_service_account=True,
    )
    service = CloudAuthService(
        server_id=server_id,
        control_plane=FakeControlPlaneClient(user=user),
        account_repository=repository,
    )

    context = await service.resolve("service-key")

    assert context.account.id == server_id
    assert context.principal_id == user.id


@pytest.mark.parametrize(
    ("user", "error"),
    [
        (None, ControlPlaneAuthorizationError("denied")),
        (ControlPlaneUser(id=uuid.uuid4(), is_active=False), None),
    ],
)
async def test_cloud_auth_rejects_invalid_identity(
    user: ControlPlaneUser | None, error: Exception | None
) -> None:
    """Reject control-plane failures and inactive identities."""
    service = CloudAuthService(
        server_id=uuid.uuid4(),
        control_plane=FakeControlPlaneClient(user=user, error=error),
        account_repository=FakeAccountRepository(),
    )

    with pytest.raises(AuthenticationError, match="Invalid Cloud credential"):
        await service.resolve("invalid")


async def test_cloud_auth_reports_control_plane_outage() -> None:
    """Keep dependency failures distinct from invalid credentials."""
    service = CloudAuthService(
        server_id=uuid.uuid4(),
        control_plane=FakeControlPlaneClient(
            error=ControlPlaneUnavailableError("timeout")
        ),
        account_repository=FakeAccountRepository(),
    )

    with pytest.raises(
        AuthenticationServiceUnavailableError,
        match="temporarily unavailable",
    ):
        await service.resolve("valid")
