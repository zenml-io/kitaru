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

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.account import Account


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
    input_session_id: uuid.UUID | None = None


Principal = AccountPrincipal | WorkerPrincipal | TaskPrincipal


class AuthContext(FrozenModel):
    """Resolved caller for application use cases."""

    # The registering account for a worker principal, the owner of the
    # task's job for a task principal, and the caller itself otherwise.
    account: Account
    principal: Principal = Field(default_factory=AccountPrincipal)
    csrf_token: str | None = None


class WorkerAuthContext(AuthContext):
    """Resolved worker caller for application use cases."""

    principal: WorkerPrincipal


class TaskAuthContext(AuthContext):
    """Resolved task caller for application use cases."""

    principal: TaskPrincipal
