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
"""Permission checks."""

import uuid

from kitaru.server.application.interfaces.permissions import PermissionProvider
from kitaru.server.application.models.auth import AccountPrincipal, AuthContext
from kitaru.server.application.models.permissions import (
    ALL_IDS,
    Action,
    AllowedIds,
    ResourceType,
)
from kitaru.server.domain.base import ForbiddenError


class PermissionService:
    """Permission checks."""

    def __init__(self, provider: PermissionProvider) -> None:
        """Initialize the service.

        Args:
            provider: Permission provider.
        """
        self._provider = provider

    async def check(
        self,
        actor: AuthContext,
        resource_type: ResourceType,
        action: Action,
        resource_id: uuid.UUID | None = None,
    ) -> None:
        """Require an account principal to be permitted an action.

        Worker and task principals pass through, since they are covered by
        the grant checks in ``resource_access.py``.

        Args:
            actor: Caller context.
            resource_type: Resource type the action applies to.
            action: Action to check.
            resource_id: Id of the specific resource, checked broadly when
                ``None``.

        Raises:
            ForbiddenError: The account is not permitted the action.
        """
        if not isinstance(actor.principal, AccountPrincipal):
            return
        allowed = await self._provider.has_permission(
            actor.account, resource_type, action, resource_id
        )
        if not allowed:
            raise ForbiddenError(
                f"Insufficient permissions for {resource_type.value}:{action.value}"
            )

    async def get_allowed_ids(
        self, actor: AuthContext, resource_type: ResourceType, action: Action
    ) -> AllowedIds:
        """List the resource ids an actor may perform an action on.

        Args:
            actor: Caller context.
            resource_type: Resource type the action applies to.
            action: Action to check.

        Returns:
            Allowed resource ids, or every id when unrestricted.
        """
        if not isinstance(actor.principal, AccountPrincipal):
            return ALL_IDS
        return await self._provider.get_allowed_ids(
            actor.account, resource_type, action
        )
