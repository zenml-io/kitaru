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
"""Authentication and caller context for use cases."""

import uuid
from enum import StrEnum

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.account import Account


class GrantKind(StrEnum):
    """Kind of resource a grant names."""

    SESSION = "session"
    BLOB = "blob"


class AccountPrincipal(FrozenModel):
    """Account principal."""


class WorkerPrincipal(FrozenModel):
    """Worker principal."""

    worker_id: uuid.UUID


class TaskPrincipal(FrozenModel):
    """Task principal."""

    task_id: uuid.UUID
    attempt: int
    worker_id: uuid.UUID
    job_id: uuid.UUID
    grants: dict[GrantKind, frozenset[uuid.UUID]] = Field(default_factory=dict)

    def has_grant(self, kind: GrantKind, resource_id: uuid.UUID) -> bool:
        """Whether the principal is granted the named resource.

        Args:
            kind: Kind of resource.
            resource_id: Id of the resource.

        Returns:
            Whether the grant is held.
        """
        return resource_id in self.grants.get(kind, frozenset())


Principal = AccountPrincipal | WorkerPrincipal | TaskPrincipal


class AuthContext(FrozenModel):
    """Resolved caller for application use cases."""

    # Depending on the principal, the account will be:
    # - The account that registered the worker
    # - The account that owns the task's job
    # - The caller itself
    account: Account
    principal: Principal = Field(default_factory=AccountPrincipal)
    csrf_token: str | None = None


class WorkerAuthContext(AuthContext):
    """Resolved worker caller for application use cases."""

    principal: WorkerPrincipal


class TaskAuthContext(AuthContext):
    """Resolved task caller for application use cases."""

    principal: TaskPrincipal
