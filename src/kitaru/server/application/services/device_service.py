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
"""Device authorization use cases."""

import uuid
from datetime import UTC, datetime, timedelta

from kitaru.server.application.interfaces.device_repository import (
    DeviceRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import (
    DeviceFilter,
    DeviceFingerprint,
    DevicePolicy,
)
from kitaru.server.domain.device import (
    Device,
    DeviceAuthorizationPending,
    DeviceNotFound,
    DeviceNotVerified,
    DeviceStatus,
    InvalidDeviceCode,
)
from kitaru.server.domain.keys import (
    generate_secret,
    generate_user_code,
    hash_secret,
)
from kitaru.server.utils import is_stale

# Skip the last_login write while the stored value is this fresh, so requests
# sharing a device do not serialize on its row lock.
LAST_LOGIN_UPDATE_INTERVAL_SECONDS = 60


class DeviceService:
    """Device authorization use cases."""

    def __init__(self, repository: DeviceRepository, policy: DevicePolicy) -> None:
        """Initialize the service.

        Args:
            repository: Device repository.
            policy: Device authorization policy.
        """
        self._repository = repository
        self._policy = policy

    @property
    def policy(self) -> DevicePolicy:
        """Return the device authorization policy.

        Returns:
            Policy governing code lifetime, polling, and lockout.
        """
        return self._policy

    async def request_authorization(
        self, fingerprint: DeviceFingerprint
    ) -> tuple[Device, str, str]:
        """Start a device authorization and issue its codes.

        Args:
            fingerprint: Description of the device requesting authorization.

        Returns:
            Created device and the plaintext user code and device code.
        """
        now = datetime.now(UTC)
        await self._repository.delete_expired(now)
        user_code = generate_user_code()
        device_code = generate_secret()
        device = Device(
            user_code_hash=hash_secret(user_code),
            device_code_hash=hash_secret(device_code),
            expires=now + timedelta(seconds=self._policy.auth_timeout_seconds),
            **fingerprint.model_dump(),
        )
        stored = await self._repository.create(device)
        return stored, user_code, device_code

    async def verify_device(
        self,
        device_id: uuid.UUID,
        user_code: str,
        trusted: bool,
        actor: AuthContext,
    ) -> Device:
        """Approve a pending device authorization on behalf of the caller.

        Args:
            device_id: Id of the device.
            user_code: Plaintext user code shown on the device.
            trusted: Whether the device should get the trusted lifetime.
            actor: Caller context.

        Raises:
            DeviceNotFound: No device has this id, or another account already
                approved it.
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
            InvalidDeviceCode: The device is no longer pending or the code
                does not match.

        Returns:
            Verified device.
        """
        device = await self._get_owned_device(
            device_id, actor.account.id, allow_unclaimed=True
        )
        try:
            device.verify_user_code(user_code, datetime.now(UTC))
        except InvalidDeviceCode:
            await self._record_failed_attempt(device)
            raise
        device.mark_verified(actor.account.id, trusted=trusted)
        return await self._repository.update(device)

    async def authenticate_device(
        self, device_id: uuid.UUID, device_code: str
    ) -> Device:
        """Exchange a device code for an authenticated device.

        Args:
            device_id: Id of the device.
            device_code: Plaintext device code held by the polling client.

        Raises:
            DeviceNotFound: No device has this id.
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
            InvalidDeviceCode: The code does not match.
            DeviceAuthorizationPending: No account has approved the device yet.

        Returns:
            Active device bound to the account that approved it.
        """
        now = datetime.now(UTC)
        device = await self._repository.get(device_id)
        try:
            device.verify_device_code(device_code, now)
        except InvalidDeviceCode:
            await self._record_failed_attempt(device)
            raise
        if device.status is DeviceStatus.PENDING or device.account_id is None:
            raise DeviceAuthorizationPending
        if device.status is DeviceStatus.VERIFIED:
            device.mark_active(self._expiry(device.trusted, now), now)
            return await self._repository.update(device)
        device.mark_used(now)
        return await self._repository.update(device)

    async def authorize_session(
        self, device_id: uuid.UUID, account_id: uuid.UUID
    ) -> Device:
        """Re-check the device a session token was issued for.

        Args:
            device_id: Id of the device named by the token.
            account_id: Id of the account named by the token.

        Raises:
            DeviceNotFound: No device has this id, or it belongs to another
                account.
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
            DeviceNotVerified: The device is not active.

        Returns:
            Device backing the session.
        """
        now = datetime.now(UTC)
        device = await self._get_owned_device(device_id, account_id)
        device.check_usable(now)
        if device.status is not DeviceStatus.ACTIVE:
            raise DeviceNotVerified
        await self._touch(device, now)
        return device

    async def get_device(self, device_id: uuid.UUID, actor: AuthContext) -> Device:
        """Get a device of the caller by id.

        Args:
            device_id: Id of the device.
            actor: Caller context.

        Raises:
            DeviceNotFound: No device of the caller has this id.

        Returns:
            Stored device.
        """
        return await self._get_owned_device(device_id, actor.account.id)

    async def list_devices(
        self, device_filter: DeviceFilter, actor: AuthContext
    ) -> tuple[list[Device], str | None]:
        """List devices of the caller matching a filter.

        Args:
            device_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching devices and the next cursor.
        """
        scoped_filter = device_filter.model_copy(
            update={"account_id": actor.account.id}
        )
        return await self._repository.query(scoped_filter)

    async def update_device(
        self,
        device_id: uuid.UUID,
        actor: AuthContext,
        locked: bool | None = None,
        trusted: bool | None = None,
    ) -> Device:
        """Update a device of the caller.

        Args:
            device_id: Id of the device.
            actor: Caller context.
            locked: New locked state, left unchanged when None.
            trusted: New trusted state, left unchanged when None.

        Raises:
            DeviceNotFound: No device of the caller has this id.

        Returns:
            Updated device.
        """
        device = await self.get_device(device_id, actor=actor)
        if locked is not None:
            device.update_locked(locked)
        if trusted is not None:
            device.update_trusted(trusted)
        return await self._repository.update(device)

    async def delete_device(self, device_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a device of the caller.

        Args:
            device_id: Id of the device.
            actor: Caller context.

        Raises:
            DeviceNotFound: No device of the caller has this id.
        """
        await self.get_device(device_id, actor=actor)
        await self._repository.delete(device_id)

    async def _get_owned_device(
        self,
        device_id: uuid.UUID,
        account_id: uuid.UUID,
        allow_unclaimed: bool = False,
    ) -> Device:
        """Get a device by id, requiring it to belong to the account.

        Args:
            device_id: Id of the device.
            account_id: Id of the owning account.
            allow_unclaimed: Whether a device no account approved yet passes.

        Raises:
            DeviceNotFound: No device has this id, or it belongs to another
                account.

        Returns:
            Stored device.
        """
        device = await self._repository.get(device_id)
        if device.account_id == account_id:
            return device
        if allow_unclaimed and device.account_id is None:
            return device
        raise DeviceNotFound(device_id)

    def _expiry(self, trusted: bool, now: datetime) -> datetime | None:
        """Return the expiry a device gets once it goes active.

        Args:
            trusted: Whether the approving account trusted the device.
            now: Current time.

        Returns:
            Expiry time, or None for a device that never expires.
        """
        minutes = (
            self._policy.trusted_expiration_minutes
            if trusted
            else self._policy.expiration_minutes
        )
        if minutes is None:
            return None
        return now + timedelta(minutes=minutes)

    async def _record_failed_attempt(self, device: Device) -> None:
        """Count a failed code check against the device and persist it.

        Args:
            device: Device whose code did not match.
        """
        device.register_failed_attempt(self._policy.max_failed_attempts)
        await self._repository.record_failed_attempt(device)

    async def _touch(self, device: Device, now: datetime) -> None:
        """Record the device login unless the stored time is still fresh.

        Args:
            device: Device that just authenticated.
            now: Current time.
        """
        if not is_stale(device.last_login, LAST_LOGIN_UPDATE_INTERVAL_SECONDS, now):
            return
        device.mark_used(now)
        await self._repository.update(device)
