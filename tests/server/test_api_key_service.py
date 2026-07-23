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
"""Tests for API key use cases."""

import uuid

import pytest

from conftest import FakeApiKeyRepository
from kitaru.server.application.models.api_keys import ApiKeyFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.domain.account import Account
from kitaru.server.domain.api_key import (
    API_KEY_PREFIX,
    ApiKeyNotFound,
    DuplicateApiKeyName,
    decode_api_key,
    hash_secret,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))


@pytest.fixture
def service() -> ApiKeyService:
    """Provide an API key service backed by the fake repository."""
    return ApiKeyService(repository=FakeApiKeyRepository())


async def test_create_api_key(service: ApiKeyService) -> None:
    """Create an API key owned by the caller."""
    api_key, key = await service.create_api_key(name="ci", actor=ACTOR)
    assert api_key.name == "ci"
    assert api_key.owner_id == ACTOR.account.id
    assert api_key.active is True
    assert api_key.last_used is None
    assert api_key.created is not None
    assert api_key.updated is not None
    assert key.startswith(API_KEY_PREFIX)


async def test_create_api_key_plaintext_matches_hash(service: ApiKeyService) -> None:
    """Return a decodable plaintext key whose hash matches the stored hash."""
    api_key, key = await service.create_api_key(name="ci", actor=ACTOR)
    key_id, secret = decode_api_key(key)
    assert key_id == api_key.id
    assert hash_secret(secret) == api_key.key_hash


async def test_create_api_key_duplicate_name(service: ApiKeyService) -> None:
    """Reject a second API key with the same name."""
    await service.create_api_key(name="ci", actor=ACTOR)
    with pytest.raises(
        DuplicateApiKeyName, match="API key name 'ci' is already registered"
    ):
        await service.create_api_key(name="ci", actor=ACTOR)


async def test_get_api_key(service: ApiKeyService) -> None:
    """Load a stored API key by id."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    loaded = await service.get_api_key(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_api_key_not_found(service: ApiKeyService) -> None:
    """Raise for an unknown API key id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ApiKeyNotFound, match=f"API key {missing_id} was not found"):
        await service.get_api_key(missing_id, actor=ACTOR)


async def test_get_api_key_foreign_owner(service: ApiKeyService) -> None:
    """Raise not found for a key owned by another account."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    with pytest.raises(ApiKeyNotFound, match=f"API key {created.id} was not found"):
        await service.get_api_key(created.id, actor=FOREIGN_ACTOR)


async def test_list_api_keys(service: ApiKeyService) -> None:
    """List API keys with filters and pagination."""
    for name in ["ci", "deploy", "local"]:
        await service.create_api_key(name=name, actor=ACTOR)

    api_keys, total = await service.list_api_keys(ApiKeyFilter(), actor=ACTOR)
    assert total == 3
    assert [api_key.name for api_key in api_keys] == ["ci", "deploy", "local"]

    api_keys, total = await service.list_api_keys(
        ApiKeyFilter(name="deploy"), actor=ACTOR
    )
    assert total == 1
    assert api_keys[0].name == "deploy"

    api_keys, total = await service.list_api_keys(
        ApiKeyFilter(page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [api_key.name for api_key in api_keys] == ["local"]


async def test_list_api_keys_scoped_to_caller(service: ApiKeyService) -> None:
    """Force the caller's owner id into the filter."""
    await service.create_api_key(name="mine", actor=ACTOR)
    await service.create_api_key(name="theirs", actor=FOREIGN_ACTOR)

    api_keys, total = await service.list_api_keys(ApiKeyFilter(), actor=ACTOR)
    assert total == 1
    assert api_keys[0].name == "mine"

    api_keys, total = await service.list_api_keys(
        ApiKeyFilter(owner_id=FOREIGN_ACTOR.account.id), actor=ACTOR
    )
    assert total == 1
    assert api_keys[0].name == "mine"


async def test_update_api_key(service: ApiKeyService) -> None:
    """Deactivate and reactivate an API key."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    updated = await service.update_api_key(created.id, active=False, actor=ACTOR)
    assert updated.active is False
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    updated = await service.update_api_key(created.id, active=True, actor=ACTOR)
    assert updated.active is True


async def test_update_api_key_not_found(service: ApiKeyService) -> None:
    """Raise for an unknown API key id."""
    with pytest.raises(ApiKeyNotFound):
        await service.update_api_key(uuid.uuid4(), active=False, actor=ACTOR)


async def test_update_api_key_foreign_owner(service: ApiKeyService) -> None:
    """Raise not found for a key owned by another account."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    with pytest.raises(ApiKeyNotFound):
        await service.update_api_key(created.id, active=False, actor=FOREIGN_ACTOR)
    loaded = await service.get_api_key(created.id, actor=ACTOR)
    assert loaded.active is True


async def test_delete_api_key(service: ApiKeyService) -> None:
    """Delete a stored API key."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    await service.delete_api_key(created.id, actor=ACTOR)
    with pytest.raises(ApiKeyNotFound):
        await service.get_api_key(created.id, actor=ACTOR)


async def test_delete_api_key_not_found(service: ApiKeyService) -> None:
    """Raise for an unknown API key id."""
    with pytest.raises(ApiKeyNotFound):
        await service.delete_api_key(uuid.uuid4(), actor=ACTOR)


async def test_delete_api_key_foreign_owner(service: ApiKeyService) -> None:
    """Raise not found for a key owned by another account."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    with pytest.raises(ApiKeyNotFound):
        await service.delete_api_key(created.id, actor=FOREIGN_ACTOR)
    loaded = await service.get_api_key(created.id, actor=ACTOR)
    assert loaded == created
