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

from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.agent_version import (
    AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.schemas.secret import (
    SECRET_NAME_UNIQUE_CONSTRAINT,
    SecretSchema,
)
from kitaru.server.application.models.secrets import SecretFilter
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretInUse,
    SecretNotFound,
)


class SQLSecretRepository:
    """Secret repository backed by the application database."""

    def __init__(self, session: AsyncSession, cipher: AesGcmCipher) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
            cipher: Cipher for secret values at rest.
        """
        self._session = session
        self._cipher = cipher

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
        row = SecretSchema.from_domain(secret, self._encrypt_values(secret.values))
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == SECRET_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateSecretName(secret.name) from exc
            raise
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
        row = await self._session.get(SecretSchema, secret_id)
        if row is None:
            raise SecretNotFound(secret_id)
        return row.to_domain(self._decrypt_values(row.values_encrypted))

    async def query(self, secret_filter: SecretFilter) -> tuple[list[Secret], int]:
        """Query secrets matching a filter.

        Args:
            secret_filter: Filter and pagination parameters.

        Returns:
            Page of matching secrets and the total match count.
        """
        statement = select(SecretSchema)
        if secret_filter.name is not None:
            statement = statement.where(col(SecretSchema.name) == secret_filter.name)
        if secret_filter.owner_id is not None:
            statement = statement.where(
                col(SecretSchema.owner_id) == secret_filter.owner_id
            )
        if secret_filter.internal is not None:
            statement = statement.where(
                col(SecretSchema.internal) == secret_filter.internal
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(SecretSchema.id),
            page=secret_filter.page,
            page_size=secret_filter.page_size,
        )
        return [
            row.to_domain(self._decrypt_values(row.values_encrypted)) for row in rows
        ], total

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
        row = await self._session.get(SecretSchema, secret.id)
        if row is None:
            raise SecretNotFound(secret.id)
        row.owner_id = secret.owner_id
        row.name = secret.name
        row.internal = secret.internal
        row.type = secret.type
        row.values_encrypted = self._encrypt_values(secret.values)
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == SECRET_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateSecretName(secret.name) from exc
            raise
        return row.to_domain(secret.values)

    async def delete(self, secret_id: uuid.UUID) -> None:
        """Delete a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.
            SecretInUse: The secret is referenced by an agent version.
        """
        row = await self._session.get(SecretSchema, secret_id)
        if row is None:
            raise SecretNotFound(secret_id)
        try:
            async with self._session.begin_nested():
                await self._session.delete(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY:
                raise SecretInUse(secret_id) from exc
            raise
