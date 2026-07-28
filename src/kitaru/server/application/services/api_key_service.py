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
"""API key use cases."""

import uuid

from kitaru.server.application.interfaces.api_key_repository import (
    ApiKeyRepository,
)
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    encode_api_key,
)
from kitaru.server.domain.keys import generate_secret, hash_secret


class ApiKeyService:
    """API key use cases."""

    def __init__(self, repository: ApiKeyRepository) -> None:
        """Initialize the service.

        Args:
            repository: API key repository.
        """
        self._repository = repository

    async def create_api_key(self, name: str, actor: AuthContext) -> tuple[ApiKey, str]:
        """Create an API key owned by the caller.

        Args:
            name: API key name.
            actor: Caller context.

        Raises:
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Created API key and the encoded plaintext key.
        """
        owner_id = actor.account.id
        secret = generate_secret()
        api_key = ApiKey(owner_id=owner_id, name=name, key_hash=hash_secret(secret))
        stored = await self._repository.create(api_key)
        return stored, encode_api_key(stored.id, secret)

    async def get_api_key(self, api_key_id: uuid.UUID, actor: AuthContext) -> ApiKey:
        """Get an API key owned by the caller by id.

        Args:
            api_key_id: Id of the API key.
            actor: Caller context.

        Raises:
            ApiKeyNotFound: No API key of the caller has this id.

        Returns:
            Stored API key.
        """
        owner_id = actor.account.id
        api_key = await self._repository.get(api_key_id)
        if api_key.owner_id != owner_id:
            raise ApiKeyNotFound(api_key_id)
        return api_key

    async def list_api_keys(
        self, api_key_filter: ApiKeyFilter, actor: AuthContext
    ) -> tuple[list[ApiKey], str | None]:
        """List API keys of the caller matching a filter.

        Args:
            api_key_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching API keys and the next cursor.
        """
        owner_id = actor.account.id
        scoped_filter = api_key_filter.model_copy(update={"owner_id": owner_id})
        return await self._repository.query(scoped_filter)

    async def update_api_key(
        self, api_key_id: uuid.UUID, active: bool, actor: AuthContext
    ) -> ApiKey:
        """Update an API key owned by the caller.

        Args:
            api_key_id: Id of the API key.
            active: New active state.
            actor: Caller context.

        Raises:
            ApiKeyNotFound: No API key of the caller has this id.

        Returns:
            Updated API key.
        """
        api_key = await self.get_api_key(api_key_id, actor=actor)
        api_key.update_active(active)
        return await self._repository.update(api_key)

    async def delete_api_key(self, api_key_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an API key owned by the caller.

        Args:
            api_key_id: Id of the API key.
            actor: Caller context.

        Raises:
            ApiKeyNotFound: No API key of the caller has this id.
        """
        await self.get_api_key(api_key_id, actor=actor)
        await self._repository.delete(api_key_id)
