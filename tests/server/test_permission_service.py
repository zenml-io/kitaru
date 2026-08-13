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
"""Tests for permission providers and the permission service."""

import uuid

import pytest

from kitaru.server.adapters.permissions.admin_flag import (
    ADMIN_ONLY_PERMISSIONS,
    AdminFlagPermissionProvider,
)
from kitaru.server.adapters.permissions.allow_all import AllowAllPermissionProvider
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskPrincipal,
    WorkerPrincipal,
)
from kitaru.server.application.models.permissions import (
    ALL_IDS,
    Action,
    AllowedIds,
    ResourceType,
)
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ForbiddenError

ADMIN_ACCOUNT = Account(id=uuid.uuid4(), name="admin", is_admin=True)
NON_ADMIN_ACCOUNT = Account(id=uuid.uuid4(), name="alice", is_admin=False)


class DenyAllPermissionProvider:
    """Permission provider that denies everything."""

    async def has_permission(
        self,
        account: Account,
        resource_type: ResourceType,
        action: Action,
        resource_id: uuid.UUID | None = None,
    ) -> bool:
        """Check whether an account may perform an action.

        Args:
            account: Account to check.
            resource_type: Resource type the action applies to.
            action: Action to check.
            resource_id: Id of the specific resource, checked broadly when
                ``None``.

        Returns:
            Whether the account may perform the action.
        """
        _ = account, resource_type, action, resource_id
        return False

    async def get_allowed_ids(
        self, account: Account, resource_type: ResourceType, action: Action
    ) -> AllowedIds:
        """List the resource ids an account may perform an action on.

        Args:
            account: Account to check.
            resource_type: Resource type the action applies to.
            action: Action to check.

        Returns:
            Allowed resource ids, or every id when unrestricted.
        """
        _ = account, resource_type, action
        return frozenset()


def task_principal() -> TaskPrincipal:
    """Build a task principal for tests.

    Returns:
        Task principal with fresh ids.
    """
    return TaskPrincipal(
        task_id=uuid.uuid4(), attempt=1, worker_id=uuid.uuid4(), job_id=uuid.uuid4()
    )


@pytest.mark.parametrize("resource_type,action", sorted(ADMIN_ONLY_PERMISSIONS))
async def test_admin_flag_provider_allows_admin(
    resource_type: ResourceType, action: Action
) -> None:
    """Allow an admin account on every admin-only pair."""
    provider = AdminFlagPermissionProvider()
    assert await provider.has_permission(ADMIN_ACCOUNT, resource_type, action) is True


@pytest.mark.parametrize("resource_type,action", sorted(ADMIN_ONLY_PERMISSIONS))
async def test_admin_flag_provider_denies_non_admin(
    resource_type: ResourceType, action: Action
) -> None:
    """Deny a non-admin account on every admin-only pair."""
    provider = AdminFlagPermissionProvider()
    assert (
        await provider.has_permission(NON_ADMIN_ACCOUNT, resource_type, action) is False
    )


async def test_admin_flag_provider_get_allowed_ids_returns_all_ids() -> None:
    """Return every id for both admin and non-admin accounts."""
    provider = AdminFlagPermissionProvider()
    for account in (ADMIN_ACCOUNT, NON_ADMIN_ACCOUNT):
        assert (
            await provider.get_allowed_ids(account, ResourceType.ACCOUNT, Action.CREATE)
            is ALL_IDS
        )


async def test_allow_all_provider_allows_admin_only_pair_for_non_admin() -> None:
    """Allow a non-admin account on an admin-only pair."""
    provider = AllowAllPermissionProvider()
    assert (
        await provider.has_permission(
            NON_ADMIN_ACCOUNT, ResourceType.ACCOUNT, Action.SET_ADMIN
        )
        is True
    )


async def test_check_denial_raises_forbidden_error() -> None:
    """Raise ForbiddenError when the provider denies the account."""
    service = PermissionService(DenyAllPermissionProvider())
    actor = AuthContext(account=NON_ADMIN_ACCOUNT)
    with pytest.raises(ForbiddenError):
        await service.check(actor, ResourceType.ACCOUNT, Action.CREATE)


async def test_check_worker_principal_bypasses_provider() -> None:
    """Pass a worker principal through without consulting the provider."""
    service = PermissionService(DenyAllPermissionProvider())
    actor = AuthContext(
        account=NON_ADMIN_ACCOUNT, principal=WorkerPrincipal(worker_id=uuid.uuid4())
    )
    await service.check(actor, ResourceType.ACCOUNT, Action.CREATE)


async def test_check_task_principal_bypasses_provider() -> None:
    """Pass a task principal through without consulting the provider."""
    service = PermissionService(DenyAllPermissionProvider())
    actor = AuthContext(account=NON_ADMIN_ACCOUNT, principal=task_principal())
    await service.check(actor, ResourceType.ACCOUNT, Action.CREATE)


async def test_get_allowed_ids_task_principal_returns_all_ids() -> None:
    """Return every id for a task principal even with a restrictive provider."""
    service = PermissionService(DenyAllPermissionProvider())
    actor = AuthContext(account=NON_ADMIN_ACCOUNT, principal=task_principal())
    result = await service.get_allowed_ids(actor, ResourceType.ACCOUNT, Action.CREATE)
    assert result is ALL_IDS
