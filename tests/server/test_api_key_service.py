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
from datetime import UTC, datetime, timedelta

import pytest

from conftest import FakeApiKeyRepository
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.domain.account import Account
from kitaru.server.domain.api_key import (
    API_KEY_PREFIX,
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
    decode_api_key,
)
from kitaru.server.domain.keys import hash_secret, verify_secret
from kitaru.server.filtering import FilterCondition

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
    """List API keys newest-first with filters."""
    for name in ["ci", "deploy", "local"]:
        await service.create_api_key(name=name, actor=ACTOR)

    api_keys, next_cursor = await service.list_api_keys(ApiKeyFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [api_key.name for api_key in api_keys] == ["local", "deploy", "ci"]

    api_keys, next_cursor = await service.list_api_keys(
        ApiKeyFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="deploy")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert api_keys[0].name == "deploy"


async def test_list_api_keys_scoped_to_caller(service: ApiKeyService) -> None:
    """Force the caller's owner id into the filter."""
    await service.create_api_key(name="mine", actor=ACTOR)
    await service.create_api_key(name="theirs", actor=FOREIGN_ACTOR)

    api_keys, next_cursor = await service.list_api_keys(ApiKeyFilter(), actor=ACTOR)
    assert next_cursor is None
    assert api_keys[0].name == "mine"

    api_keys, next_cursor = await service.list_api_keys(
        ApiKeyFilter(owner_id=FOREIGN_ACTOR.account.id), actor=ACTOR
    )
    assert next_cursor is None
    assert api_keys[0].name == "mine"


async def test_list_api_keys_scoped_across_cursor(service: ApiKeyService) -> None:
    """Keep the caller's owner id forced across every page of a cursor walk."""
    for name in ["ci", "deploy", "local"]:
        await service.create_api_key(name=name, actor=ACTOR)
    await service.create_api_key(name="theirs", actor=FOREIGN_ACTOR)

    collected: list[str] = []
    cursor = None
    while True:
        api_keys, next_cursor = await service.list_api_keys(
            ApiKeyFilter(cursor=cursor, size=1), actor=ACTOR
        )
        collected.extend(api_key.name for api_key in api_keys)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == ["local", "deploy", "ci"]


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


async def test_rotate_api_key(service: ApiKeyService) -> None:
    """Rotate an API key, retaining the previous hash."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    rotated, key = await service.rotate_api_key(
        created.id, retain_period_minutes=5, actor=ACTOR
    )
    assert key.startswith(API_KEY_PREFIX)
    key_id, secret = decode_api_key(key)
    assert key_id == rotated.id
    assert verify_secret(secret, rotated.key_hash)
    assert rotated.previous_key_hash == created.key_hash
    assert rotated.retain_period_minutes == 5
    assert rotated.last_rotated is not None


async def test_rotate_api_key_not_found(service: ApiKeyService) -> None:
    """Raise for an unknown API key id."""
    with pytest.raises(ApiKeyNotFound):
        await service.rotate_api_key(uuid.uuid4(), retain_period_minutes=0, actor=ACTOR)


async def test_rotate_api_key_foreign_owner(service: ApiKeyService) -> None:
    """Raise not found for a key owned by another account."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    with pytest.raises(ApiKeyNotFound):
        await service.rotate_api_key(
            created.id, retain_period_minutes=0, actor=FOREIGN_ACTOR
        )
    loaded = await service.get_api_key(created.id, actor=ACTOR)
    assert loaded == created


async def test_rotate_api_key_twice_overwrites_previous_hash(
    service: ApiKeyService,
) -> None:
    """Overwrite the previous hash with the most recently rotated hash."""
    created, _ = await service.create_api_key(name="ci", actor=ACTOR)
    first_rotation, _ = await service.rotate_api_key(
        created.id, retain_period_minutes=5, actor=ACTOR
    )
    second_rotation, _ = await service.rotate_api_key(
        created.id, retain_period_minutes=5, actor=ACTOR
    )
    assert second_rotation.previous_key_hash == first_rotation.key_hash
    assert second_rotation.previous_key_hash != created.key_hash


async def test_is_previous_key_valid_no_previous_hash() -> None:
    """Reject a check when no previous hash has ever been stored."""
    api_key = ApiKey(owner_id=ACTOR.account.id, name="ci", key_hash="hash")
    assert api_key.is_previous_key_valid(datetime.now(UTC)) is False


async def test_is_previous_key_valid_zero_retain_period() -> None:
    """Reject a check when the retain period is zero."""
    api_key = ApiKey(owner_id=ACTOR.account.id, name="ci", key_hash="hash")
    now = datetime.now(UTC)
    api_key.rotate("new-hash", retain_period_minutes=0, when=now)
    assert api_key.is_previous_key_valid(now) is False


async def test_is_previous_key_valid_inside_window() -> None:
    """Accept a check inside the retain window."""
    api_key = ApiKey(owner_id=ACTOR.account.id, name="ci", key_hash="hash")
    now = datetime.now(UTC)
    api_key.rotate("new-hash", retain_period_minutes=5, when=now)
    assert api_key.is_previous_key_valid(now + timedelta(minutes=4)) is True


async def test_is_previous_key_valid_after_window() -> None:
    """Reject a check once the retain window has passed."""
    api_key = ApiKey(owner_id=ACTOR.account.id, name="ci", key_hash="hash")
    now = datetime.now(UTC)
    api_key.rotate("new-hash", retain_period_minutes=5, when=now)
    assert api_key.is_previous_key_valid(now + timedelta(minutes=5)) is False
