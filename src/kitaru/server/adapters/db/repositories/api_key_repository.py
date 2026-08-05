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
"""SQL API key repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import select

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.api_key import (
    API_KEY_NAME_UNIQUE_CONSTRAINT,
    ApiKeyORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
)
from kitaru.server.domain.base import NotFoundError

API_KEY_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "name": ApiKeyORM.name,
}


class SQLApiKeyRepository(BaseSQLRepository[ApiKeyORM]):
    """API key repository backed by the application database."""

    orm_class = ApiKeyORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return ApiKeyNotFound(entity_id)

    async def create(self, api_key: ApiKey) -> ApiKey:
        """Persist a new API key.

        Args:
            api_key: API key to store.

        Raises:
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with timestamps set.
        """
        row = ApiKeyORM.from_domain(api_key)
        await self._add(
            row,
            {API_KEY_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateApiKeyName(api_key.name)},
        )
        return row.to_domain()

    async def get(self, api_key_id: uuid.UUID, exclusive: bool = False) -> ApiKey:
        """Load an API key by id.

        Args:
            api_key_id: Id of the API key.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            ApiKeyNotFound: No API key has this id.

        Returns:
            Stored API key.
        """
        row = await self._get_row(api_key_id, exclusive=exclusive)
        return row.to_domain()

    async def query(
        self, api_key_filter: ApiKeyFilter
    ) -> tuple[list[ApiKey], str | None]:
        """Query API keys matching a filter.

        Args:
            api_key_filter: Filter and pagination parameters.

        Returns:
            Page of matching API keys and the next cursor.
        """
        statement = select(ApiKeyORM)
        if api_key_filter.owner_id is not None:
            statement = statement.where(ApiKeyORM.owner_id == api_key_filter.owner_id)
        if api_key_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    api_key_filter.expression, API_KEY_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session,
            statement,
            api_key_filter,
            id_column=ApiKeyORM.id,
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, api_key: ApiKey) -> ApiKey:
        """Persist changes to an existing API key.

        Args:
            api_key: API key with modified fields.

        Raises:
            ApiKeyNotFound: No API key has this id.
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with the updated timestamp renewed.
        """
        row = await self._get_row(api_key.id)
        row.owner_id = api_key.owner_id
        row.name = api_key.name
        row.key_hash = api_key.key_hash
        row.previous_key_hash = api_key.previous_key_hash
        row.retain_period_minutes = api_key.retain_period_minutes
        row.active = api_key.active
        row.last_used = api_key.last_used
        row.last_rotated = api_key.last_rotated
        await self._flush(
            {API_KEY_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateApiKeyName(api_key.name)}
        )
        return row.to_domain()

    async def delete(self, api_key_id: uuid.UUID) -> None:
        """Delete an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            ApiKeyNotFound: No API key has this id.
        """
        await self._delete_row(api_key_id)
