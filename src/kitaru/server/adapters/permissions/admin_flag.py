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
"""Admin flag permission provider."""

import uuid

from kitaru.server.application.models.permissions import (
    ALL_IDS,
    Action,
    AllowedIds,
    ResourceType,
)
from kitaru.server.domain.account import Account

ADMIN_ONLY_PERMISSIONS: frozenset[tuple[ResourceType, Action]] = frozenset(
    {
        (ResourceType.ACCOUNT, Action.CREATE),
        (ResourceType.ACCOUNT, Action.DEACTIVATE),
        (ResourceType.ACCOUNT, Action.SET_ADMIN),
    }
)


class AdminFlagPermissionProvider:
    """Admin flag permission provider."""

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
        _ = resource_id
        if account.is_admin:
            return True
        return (resource_type, action) not in ADMIN_ONLY_PERMISSIONS

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
        return ALL_IDS
