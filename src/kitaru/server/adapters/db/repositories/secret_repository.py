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
"""SQL secret repository."""

import json
import uuid
from collections.abc import Mapping

from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.secret import (
    SECRET_NAME_UNIQUE_CONSTRAINT,
    SecretORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretNotFound,
)

SECRET_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "name": SecretORM.name,
}


class SQLSecretRepository(BaseSQLRepository[SecretORM]):
    """Secret repository backed by the application database."""

    orm_class = SecretORM

    def __init__(self, session: AsyncSession, cipher: AesGcmCipher) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
            cipher: Cipher for secret values at rest.
        """
        super().__init__(session)
        self._cipher = cipher

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return SecretNotFound(entity_id)

    def _encrypt_values(self, values: dict[str, SecretStr]) -> str:
        """Serialize and encrypt secret values for storage.

        Args:
            values: Plaintext values.

        Returns:
            Encrypted values.
        """
        plaintext = {key: entry.get_secret_value() for key, entry in values.items()}
        return self._cipher.encrypt(json.dumps(plaintext))

    def _decrypt_values(self, values_encrypted: str) -> dict[str, SecretStr]:
        """Decrypt and deserialize stored secret values.

        Args:
            values_encrypted: Encrypted values.

        Returns:
            Plaintext values.
        """
        plaintext = json.loads(self._cipher.decrypt(values_encrypted))
        return {key: SecretStr(entry) for key, entry in plaintext.items()}

    async def create(self, secret: Secret) -> Secret:
        """Persist a new secret.

        Args:
            secret: Secret to store.

        Raises:
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Stored secret with timestamps set.
        """
        row = SecretORM.from_domain(secret, self._encrypt_values(secret.values))
        await self._add(
            row,
            {SECRET_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateSecretName(secret.name)},
        )
        return row.to_domain(secret.values)

    async def get(self, secret_id: uuid.UUID) -> Secret:
        """Load a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.

        Returns:
            Stored secret.
        """
        row = await self._get_row(secret_id)
        return row.to_domain(self._decrypt_values(row.values_encrypted))

    async def query(
        self, secret_filter: SecretFilter
    ) -> tuple[list[Secret], str | None]:
        """Query secrets matching a filter.

        Args:
            secret_filter: Filter and pagination parameters.

        Returns:
            Page of matching secrets and the next cursor.
        """
        statement = select(SecretORM)
        if secret_filter.owner_id is not None:
            statement = statement.where(SecretORM.owner_id == secret_filter.owner_id)
        if secret_filter.internal is not None:
            statement = statement.where(SecretORM.internal == secret_filter.internal)
        if secret_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    secret_filter.expression, SECRET_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session,
            statement,
            secret_filter,
            id_column=SecretORM.id,
        )
        return [
            row.to_domain(self._decrypt_values(row.values_encrypted)) for row in rows
        ], next_cursor

    async def update(self, secret: Secret) -> Secret:
        """Persist changes to an existing secret.

        Args:
            secret: Secret with modified fields.

        Raises:
            SecretNotFound: No secret has this id.
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Stored secret with the updated timestamp renewed.
        """
        row = await self._get_row(secret.id)
        row.owner_id = secret.owner_id
        row.name = secret.name
        row.internal = secret.internal
        row.type = secret.type
        row.values_encrypted = self._encrypt_values(secret.values)
        await self._flush(
            {SECRET_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateSecretName(secret.name)}
        )
        return row.to_domain(secret.values)

    async def delete(self, secret_id: uuid.UUID) -> None:
        """Delete a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.
        """
        await self._delete_row(secret_id)
