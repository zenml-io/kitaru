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
"""Secret use cases."""

import uuid

from pydantic import SecretStr

from kitaru.server.application.interfaces.secret_repository import (
    SecretRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.secrets import SecretFilter, SecretUpdate
from kitaru.server.domain.secret import InvalidSecret, Secret, SecretNotFound


class SecretService:
    """Secret use cases."""

    def __init__(self, repository: SecretRepository) -> None:
        """Initialize the service.

        Args:
            repository: Secret repository.
        """
        self._repository = repository

    async def create_secret(
        self,
        name: str,
        type: str | None,
        values: dict[str, SecretStr],
        actor: AuthContext,
    ) -> Secret:
        """Create a secret owned by the caller.

        Args:
            name: Secret name.
            type: Secret type.
            values: Secret values.
            actor: Caller context.

        Raises:
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Created secret.
        """
        owner_id = actor.account.id
        secret = Secret(owner_id=owner_id, name=name, type=type, values=values)
        return await self._repository.create(secret)

    async def get_secret(self, secret_id: uuid.UUID, actor: AuthContext) -> Secret:
        """Get a secret by id.

        Args:
            secret_id: Id of the secret.
            actor: Caller context.

        Raises:
            SecretNotFound: No secret has this id, or the secret is
                internal.

        Returns:
            Stored secret.
        """
        _ = actor
        secret = await self._repository.get(secret_id)
        if secret.internal:
            raise SecretNotFound(secret_id)
        return secret

    async def list_secrets(
        self, secret_filter: SecretFilter, actor: AuthContext
    ) -> tuple[list[Secret], int]:
        """List secrets matching a filter.

        Args:
            secret_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching secrets and the total match count.
        """
        _ = actor
        scoped_filter = secret_filter.model_copy(update={"internal": False})
        return await self._repository.query(scoped_filter)

    async def update_secret(
        self,
        secret_id: uuid.UUID,
        command: SecretUpdate,
        actor: AuthContext,
    ) -> Secret:
        """Partially update a secret.

        Fields absent from the command stay unchanged. An explicit null
        clears the type and is rejected for the values.

        Args:
            secret_id: Id of the secret.
            command: Secret update command.
            actor: Caller context.

        Raises:
            SecretNotFound: No secret has this id, or the secret is
                internal.
            InvalidSecret: The values are null.

        Returns:
            Updated secret.
        """
        secret = await self.get_secret(secret_id, actor=actor)
        if "type" in command.model_fields_set:
            secret.update_type(command.type)
        if "values" in command.model_fields_set:
            if command.values is None:
                raise InvalidSecret("Secret values cannot be null")
            secret.update_values(command.values)
        return await self._repository.update(secret)

    async def delete_secret(self, secret_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a secret.

        Args:
            secret_id: Id of the secret.
            actor: Caller context.

        Raises:
            SecretNotFound: No secret has this id, or the secret is
                internal.
            SecretInUse: The secret is referenced by an agent version.
        """
        await self.get_secret(secret_id, actor=actor)
        await self._repository.delete(secret_id)
