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
"""Cloud-backed authentication for managed Kitaru workspaces."""

import uuid
from typing import Protocol

from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthenticationServiceUnavailableError,
)
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthorizationError,
    ControlPlaneUnavailableError,
    ControlPlaneUser,
)
from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account


class ControlPlaneAuthorizer(Protocol):
    """Control-plane operation required by Cloud authentication."""

    async def authorize_server(
        self, credential: str, server_id: uuid.UUID, action: str
    ) -> ControlPlaneUser:
        """Authorize a credential for one workspace."""
        ...


class CloudAuthService:
    """Resolve Cloud credentials and mirror their principals locally."""

    def __init__(
        self,
        server_id: uuid.UUID,
        control_plane: ControlPlaneAuthorizer,
        account_repository: AccountRepository,
    ) -> None:
        """Initialize the service.

        Args:
            server_id: Cloud workspace ID represented by this server.
            control_plane: Client used to validate Cloud credentials.
            account_repository: Local account persistence.
        """
        self._server_id = server_id
        self._control_plane = control_plane
        self._account_repository = account_repository

    async def resolve(self, credential: str, action: str) -> AuthContext:
        """Resolve a Cloud bearer credential.

        Args:
            credential: Cloud bearer token, API key, or service-account key.
            action: CRUD action requested by the caller.

        Raises:
            AuthenticationError: The credential is rejected by Cloud.

        Returns:
            Request context backed by the mirrored Cloud account.
        """
        try:
            user = await self._control_plane.authorize_server(
                credential, self._server_id, action
            )
        except ControlPlaneAuthorizationError as exc:
            raise AuthenticationError("Invalid Cloud credential.") from exc
        except ControlPlaneUnavailableError as exc:
            raise AuthenticationServiceUnavailableError(
                "Cloud authorization is temporarily unavailable."
            ) from exc
        if not user.is_active:
            raise AuthenticationError("Invalid Cloud credential.")
        account = await self._get_workspace_account()
        return AuthContext(account=account, principal_id=user.id)

    async def _get_workspace_account(self) -> Account:
        name = f"cloud-workspace-{self._server_id.hex}"
        return await self._account_repository.ensure(
            Account(
                id=self._server_id,
                name=name,
            )
        )
