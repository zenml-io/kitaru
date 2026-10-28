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
"""In-memory fakes for accounts, API keys, devices, and idempotency keys."""

import uuid
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.routing import iter_route_contexts

from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.db.encryption import AesGcmCipher, DecryptionError
from kitaru.server.adapters.rest.dependencies import (
    _resolve_auth_context,
    get_idempotency_key_repository,
)
from kitaru.server.adapters.rest.route import is_idempotent
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.domain.account import Account, AccountNotFound, DuplicateAccountName
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
    encode_api_key,
)
from kitaru.server.domain.device import Device, DeviceNotFound, DeviceStatus
from kitaru.server.domain.idempotency_key import (
    IdempotencyKey,
    IdempotencyKeyAlreadyExists,
    IdempotencyKeyResponseUndecryptable,
)
from kitaru.server.domain.keys import generate_secret, hash_secret
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)


class FakeAccountRepository:
    """In-memory account repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._accounts: dict[uuid.UUID, Account] = {}

    def _check_duplicate_name(self, account: Account) -> None:
        for other in self._accounts.values():
            if (
                other.id != account.id
                and other.name == account.name
                and other.is_service_account == account.is_service_account
            ):
                raise DuplicateAccountName(account.name)

    async def create(self, account: Account) -> Account:
        """Persist a new account.

        Args:
            account: Account to store.

        Raises:
            DuplicateAccountName: The account name is already registered.

        Returns:
            Stored account with timestamps set.
        """
        self._check_duplicate_name(account)
        now = datetime.now(UTC)
        stored = account.model_copy(update={"created": now, "updated": now})
        self._accounts[stored.id] = stored
        return stored.model_copy()

    async def get(
        self, account_id: uuid.UUID, is_service_account: bool | None = None
    ) -> Account:
        """Load an account by id.

        Args:
            account_id: Id of the account.
            is_service_account: Whether to look up a service account, ``None``
                allows both kinds.

        Raises:
            AccountNotFound: No account has this id.

        Returns:
            Stored account.
        """
        account = self._accounts.get(account_id)
        if account is None:
            raise AccountNotFound(account_id)
        if (
            is_service_account is not None
            and account.is_service_account != is_service_account
        ):
            raise AccountNotFound(account_id)
        return account.model_copy()

    async def get_by_name(self, name: str, is_service_account: bool = False) -> Account:
        """Load an account by name.

        Args:
            name: Name of the account.
            is_service_account: Whether to look up a service account.

        Raises:
            AccountNotFound: No account has this name.

        Returns:
            Stored account.
        """
        for account in self._accounts.values():
            if (
                account.name == name
                and account.is_service_account == is_service_account
            ):
                return account.model_copy()
        raise AccountNotFound(name)

    async def get_by_external_id(
        self, external_id: uuid.UUID, is_service_account: bool = False
    ) -> Account:
        """Load an account by external id.

        Args:
            external_id: External id of the account.
            is_service_account: Whether to look up a service account.

        Raises:
            AccountNotFound: No account has this external id.

        Returns:
            Stored account.
        """
        for account in self._accounts.values():
            if (
                account.external_id == external_id
                and account.is_service_account == is_service_account
            ):
                return account.model_copy()
        raise AccountNotFound(external_id)

    async def query(
        self, account_filter: AccountFilter
    ) -> tuple[list[Account], str | None]:
        """Query accounts matching a filter.

        Args:
            account_filter: Filter and pagination parameters.

        Returns:
            Page of matching accounts and the next cursor.
        """
        accounts = list(self._accounts.values())
        if account_filter.expression is not None:
            accounts = [
                account
                for account in accounts
                if _evaluate_filter_expression(account, account_filter.expression)
            ]
        page, next_cursor = _paginate_fake(accounts, account_filter)
        return [account.model_copy() for account in page], next_cursor

    async def update(self, account: Account) -> Account:
        """Persist changes to an existing account.

        Args:
            account: Account with modified fields.

        Raises:
            AccountNotFound: No account has this id.
            DuplicateAccountName: The account name is already registered.

        Returns:
            Stored account with the updated timestamp renewed.
        """
        stored = self._accounts.get(account.id)
        if stored is None:
            raise AccountNotFound(account.id)
        self._check_duplicate_name(account)
        now = _renewed_timestamp(stored.updated)
        updated = account.model_copy(update={"created": stored.created, "updated": now})
        self._accounts[account.id] = updated
        return updated.model_copy()


class FakeApiKeyRepository:
    """In-memory API key repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._api_keys: dict[uuid.UUID, ApiKey] = {}

    def _check_duplicate_name(self, api_key: ApiKey) -> None:
        for other in self._api_keys.values():
            if other.id != api_key.id and other.name == api_key.name:
                raise DuplicateApiKeyName(api_key.name)

    async def create(self, api_key: ApiKey) -> ApiKey:
        """Persist a new API key.

        Args:
            api_key: API key to store.

        Raises:
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with timestamps set.
        """
        self._check_duplicate_name(api_key)
        now = datetime.now(UTC)
        stored = api_key.model_copy(update={"created": now, "updated": now})
        self._api_keys[stored.id] = stored
        return stored.model_copy()

    async def get(self, api_key_id: uuid.UUID, exclusive: bool = False) -> ApiKey:
        """Load an API key by id.

        Args:
            api_key_id: Id of the API key.
            exclusive: Ignored, the fake has no concurrent callers to lock
                against.

        Raises:
            ApiKeyNotFound: No API key has this id.

        Returns:
            Stored API key.
        """
        _ = exclusive
        api_key = self._api_keys.get(api_key_id)
        if api_key is None:
            raise ApiKeyNotFound(api_key_id)
        return api_key.model_copy()

    async def query(
        self, api_key_filter: ApiKeyFilter
    ) -> tuple[list[ApiKey], str | None]:
        """Query API keys matching a filter.

        Args:
            api_key_filter: Filter and pagination parameters.

        Returns:
            Page of matching API keys and the next cursor.
        """
        api_keys = list(self._api_keys.values())
        if api_key_filter.owner_id is not None:
            api_keys = [
                api_key
                for api_key in api_keys
                if api_key.owner_id == api_key_filter.owner_id
            ]
        if api_key_filter.expression is not None:
            api_keys = [
                api_key
                for api_key in api_keys
                if _evaluate_filter_expression(api_key, api_key_filter.expression)
            ]
        page, next_cursor = _paginate_fake(api_keys, api_key_filter)
        return [api_key.model_copy() for api_key in page], next_cursor

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
        stored = self._api_keys.get(api_key.id)
        if stored is None:
            raise ApiKeyNotFound(api_key.id)
        self._check_duplicate_name(api_key)
        now = _renewed_timestamp(stored.updated)
        updated = api_key.model_copy(update={"created": stored.created, "updated": now})
        self._api_keys[api_key.id] = updated
        return updated.model_copy()

    async def delete(self, api_key_id: uuid.UUID) -> None:
        """Delete an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            ApiKeyNotFound: No API key has this id.
        """
        if api_key_id not in self._api_keys:
            raise ApiKeyNotFound(api_key_id)
        del self._api_keys[api_key_id]


async def create_api_key(
    repository: FakeApiKeyRepository,
    owner_id: uuid.UUID,
    name: str = "ci",
    active: bool = True,
) -> tuple[ApiKey, str]:
    """Store an API key in the fake repository.

    Args:
        repository: Fake API key repository.
        owner_id: Id of the owning account.
        name: API key name.
        active: Active state.

    Returns:
        Stored API key and the encoded plaintext key.
    """
    secret = generate_secret()
    api_key = await repository.create(
        ApiKey(
            owner_id=owner_id, name=name, key_hash=hash_secret(secret), active=active
        )
    )
    return api_key, encode_api_key(api_key.id, secret)


def mint_worker_token(
    auth_service: AuthService, worker_id: uuid.UUID, account: Account
) -> str:
    """Mint a worker token for a worker registered under the given account.

    Args:
        auth_service: Authentication service backing the app.
        worker_id: Id of the worker the token is scoped to.
        account: Account the worker registered under.

    Returns:
        Encoded worker bearer token.
    """
    return auth_service.issue_worker_token(
        worker_id=worker_id, account_id=account.id
    ).token


class FakeDeviceRepository:
    """In-memory device repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._devices: dict[uuid.UUID, Device] = {}

    async def create(self, device: Device) -> Device:
        """Persist a new device.

        Args:
            device: Device to store.

        Returns:
            Stored device with timestamps set.
        """
        now = datetime.now(UTC)
        stored = device.model_copy(update={"created": now, "updated": now})
        self._devices[stored.id] = stored
        return stored.model_copy()

    async def get(self, device_id: uuid.UUID) -> Device:
        """Load a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device.
        """
        device = self._devices.get(device_id)
        if device is None:
            raise DeviceNotFound(device_id)
        return device.model_copy()

    async def query(
        self, device_filter: DeviceFilter
    ) -> tuple[list[Device], str | None]:
        """Query devices matching a filter.

        Args:
            device_filter: Filter and pagination parameters.

        Returns:
            Page of matching devices and the next cursor.
        """
        devices = list(self._devices.values())
        if device_filter.account_id is not None:
            devices = [
                device
                for device in devices
                if device.account_id == device_filter.account_id
            ]
        if device_filter.expression is not None:
            devices = [
                device
                for device in devices
                if _evaluate_filter_expression(device, device_filter.expression)
            ]
        page, next_cursor = _paginate_fake(devices, device_filter)
        return [device.model_copy() for device in page], next_cursor

    async def update(self, device: Device) -> Device:
        """Persist changes to an existing device.

        Args:
            device: Device with modified fields.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device with the updated timestamp renewed.
        """
        stored = self._devices.get(device.id)
        if stored is None:
            raise DeviceNotFound(device.id)
        now = _renewed_timestamp(stored.updated)
        updated = device.model_copy(update={"created": stored.created, "updated": now})
        self._devices[device.id] = updated
        return updated.model_copy()

    async def delete(self, device_id: uuid.UUID) -> None:
        """Delete a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.
        """
        if device_id not in self._devices:
            raise DeviceNotFound(device_id)
        del self._devices[device_id]

    async def record_failed_attempt(self, device: Device) -> None:
        """Persist a failed code check in its own transaction.

        Args:
            device: Device whose attempt counter and locked state changed.
        """
        stored = self._devices.get(device.id)
        if stored is None:
            raise DeviceNotFound(device.id)
        now = _renewed_timestamp(stored.updated)
        updated = stored.model_copy(
            update={
                "failed_auth_attempts": device.failed_auth_attempts,
                "locked": device.locked,
                "updated": now,
            }
        )
        self._devices[device.id] = updated

    async def delete_expired(self, now: datetime) -> int:
        """Delete every device past its expiry.

        Args:
            now: Current time.

        Returns:
            Number of deleted devices.
        """
        expired = [
            device_id
            for device_id, device in self._devices.items()
            if device.is_expired(now)
        ]
        for device_id in expired:
            del self._devices[device_id]
        return len(expired)


async def create_device(
    repository: FakeDeviceRepository,
    account_id: uuid.UUID | None = None,
    status: DeviceStatus = DeviceStatus.PENDING,
    trusted: bool = False,
    locked: bool = False,
    expires: datetime | None = None,
    user_code: str = "user-code",
    device_code: str = "device-code",
) -> tuple[Device, str, str]:
    """Store a device in the fake repository.

    Args:
        repository: Fake device repository.
        account_id: Id of the approving account, unset for a pending device.
        status: Device status.
        trusted: Trusted state.
        locked: Locked state.
        expires: Expiry time.
        user_code: Plaintext user code to hash and store.
        device_code: Plaintext device code to hash and store.

    Returns:
        Stored device and the plaintext user code and device code.
    """
    device = await repository.create(
        Device(
            account_id=account_id,
            user_code_hash=hash_secret(user_code),
            device_code_hash=hash_secret(device_code),
            status=status,
            trusted=trusted,
            locked=locked,
            expires=expires,
        )
    )
    return device, user_code, device_code


class FakeIdempotencyKeyRepository:
    """In-memory idempotency key repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._idempotency_keys: dict[uuid.UUID, IdempotencyKey] = {}
        self._cipher = AesGcmCipher("test-key")
        self.encrypted_ids: set[uuid.UUID] = set()

    async def create(self, idempotency_key: IdempotencyKey) -> IdempotencyKey:
        """Persist a new idempotency key.

        Args:
            idempotency_key: Idempotency key to store.

        Raises:
            IdempotencyKeyAlreadyExists: The account already has this key.

        Returns:
            Stored idempotency key with the created timestamp set.
        """
        for other in self._idempotency_keys.values():
            if (
                other.account_id == idempotency_key.account_id
                and other.key == idempotency_key.key
            ):
                raise IdempotencyKeyAlreadyExists(
                    idempotency_key.account_id, idempotency_key.key
                )
        stored = idempotency_key.model_copy(update={"created": datetime.now(UTC)})
        self._idempotency_keys[stored.id] = stored
        return stored.model_copy()

    async def get(self, account_id: uuid.UUID, key: str) -> IdempotencyKey | None:
        """Load an idempotency key by account and key.

        Args:
            account_id: Id of the account the key is scoped to.
            key: Idempotency key.

        Returns:
            Stored idempotency key, or ``None`` when no row matches.
        """
        for stored in self._idempotency_keys.values():
            if stored.account_id == account_id and stored.key == key:
                return stored.model_copy()
        return None

    def decrypt_response_body(self, response_body: bytes) -> bytes:
        """Decrypt a response body stored encrypted at rest.

        Args:
            response_body: Stored response body.

        Raises:
            IdempotencyKeyResponseUndecryptable: The body cannot be decrypted.

        Returns:
            Decrypted response body.
        """
        try:
            return self._cipher.decrypt_bytes(response_body)
        except DecryptionError:
            raise IdempotencyKeyResponseUndecryptable() from None

    async def store_response(
        self,
        idempotency_key_id: uuid.UUID,
        response_status: int,
        response_body: bytes,
        response_content_type: str | None,
        encrypt: bool = False,
    ) -> None:
        """Record the response a request committed under this key.

        Args:
            idempotency_key_id: Id of the idempotency key.
            response_status: HTTP status code of the committed response.
            response_body: Raw response body.
            response_content_type: Content type of the response, when set.
            encrypt: Whether to store the body encrypted at rest.
        """
        stored = self._idempotency_keys.get(idempotency_key_id)
        if stored is None:
            return
        if encrypt:
            response_body = self._cipher.encrypt_bytes(response_body)
            self.encrypted_ids.add(idempotency_key_id)
        self._idempotency_keys[idempotency_key_id] = stored.model_copy(
            update={
                "response_status": response_status,
                "response_body": response_body,
                "response_content_type": response_content_type,
            }
        )

    async def delete_expired(self, cutoff: datetime, limit: int) -> int:
        """Delete idempotency keys created before a cutoff, up to a limit.

        Args:
            cutoff: Rows created before this time are eligible for deletion.
            limit: Maximum number of rows to delete in this batch.

        Returns:
            Number of deleted rows.
        """
        if limit <= 0:
            return 0
        expired = [
            idempotency_key_id
            for idempotency_key_id, stored in self._idempotency_keys.items()
            if stored.created is not None and stored.created < cutoff
        ][:limit]
        for idempotency_key_id in expired:
            del self._idempotency_keys[idempotency_key_id]
        return len(expired)


def override_idempotency(
    app: FastAPI, account: Account
) -> FakeIdempotencyKeyRepository:
    """Route idempotency enforcement through a fake repository for ``account``.

    Args:
        app: App to override dependencies on.
        account: Account the resolved auth context authenticates as.

    Returns:
        Fake repository backing the override.
    """
    repository = FakeIdempotencyKeyRepository()
    app.dependency_overrides[_resolve_auth_context] = lambda: AuthContext(
        account=account
    )
    app.dependency_overrides[get_idempotency_key_repository] = lambda: repository
    return repository


def marked_idempotent_routes(app: FastAPI) -> set[tuple[str, str]]:
    """Return the method and path pairs an app marks idempotent.

    Args:
        app: App whose routes are walked.

    Returns:
        Marked routes.
    """
    return {
        (method, context.path)
        for context in iter_route_contexts(app.routes)
        if context.path is not None
        and context.endpoint is not None
        and is_idempotent(context.endpoint)
        for method in context.methods or ()
    }
