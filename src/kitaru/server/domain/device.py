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
"""Authorized device entity and errors."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.domain.base import DomainModel, NotFoundError, ValidationError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.keys import verify_secret
from kitaru.server.utils import to_tz_aware

MAX_DEVICE_FIELD_LENGTH = 255


class DeviceStatus(StrEnum):
    """Authorized device status."""

    PENDING = "pending"
    VERIFIED = "verified"
    ACTIVE = "active"


class DeviceNotFound(NotFoundError):
    """Raised when a device lookup does not resolve."""

    def __init__(self, device_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            device_id: Id of the missing device.
        """
        super().__init__(f"Device {device_id} was not found")


class DeviceError(ValidationError):
    """Base class for device authorization failures."""


class DeviceExpired(DeviceError):
    """Raised when a device authorization is past its expiry."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Device authorization has expired")


class DeviceLocked(DeviceError):
    """Raised when a device is locked."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Device is locked")


class DeviceNotVerified(DeviceError):
    """Raised when a device has not been verified yet."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Device has not been verified")


class DeviceAuthorizationPending(DeviceError):
    """Raised while a device authorization is waiting to be approved."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Device authorization is pending")


class InvalidDeviceCode(DeviceError):
    """Raised when a device code or user code does not match."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Invalid device code")


class Device(DomainModel):
    """Authorized device."""

    id: uuid.UUID = Field(default_factory=uuid7)
    account_id: uuid.UUID | None = None
    user_code_hash: str
    device_code_hash: str
    status: DeviceStatus = DeviceStatus.PENDING
    locked: bool = False
    trusted: bool = False
    failed_auth_attempts: int = 0
    expires: datetime | None = None
    last_login: datetime | None = None
    hostname: str | None = None
    os: str | None = None
    ip_address: str | None = None
    python_version: str | None = None
    client_version: str | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def is_expired(self, now: datetime) -> bool:
        """Report whether the device authorization is past its expiry.

        Args:
            now: Current time.

        Returns:
            Whether the device has expired. A device without an expiry never
            expires.
        """
        if self.expires is None:
            return False
        return to_tz_aware(self.expires) <= now

    def verify_user_code(self, user_code: str, now: datetime) -> None:
        """Check the user code presented by the browser half of the flow.

        Args:
            user_code: Plaintext user code.
            now: Current time.

        Raises:
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
            InvalidDeviceCode: The device is no longer pending or the code
                does not match.
        """
        self.check_usable(now)
        if self.status is not DeviceStatus.PENDING:
            raise InvalidDeviceCode
        if not verify_secret(user_code, self.user_code_hash):
            raise InvalidDeviceCode

    def verify_device_code(self, device_code: str, now: datetime) -> None:
        """Check the device code presented by the polling half of the flow.

        Args:
            device_code: Plaintext device code.
            now: Current time.

        Raises:
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
            InvalidDeviceCode: The code does not match.
        """
        self.check_usable(now)
        if not verify_secret(device_code, self.device_code_hash):
            raise InvalidDeviceCode

    def register_failed_attempt(self, max_attempts: int) -> None:
        """Count a failed code check and lock the device once the limit is hit.

        Args:
            max_attempts: Failed attempts tolerated before the device locks.
        """
        self.failed_auth_attempts += 1
        if self.failed_auth_attempts >= max_attempts:
            self.locked = True

    def mark_verified(self, account_id: uuid.UUID, trusted: bool) -> None:
        """Record that an account approved this device.

        Args:
            account_id: Id of the approving account.
            trusted: Whether the account marked the device as trusted.
        """
        self.account_id = account_id
        self.trusted = trusted
        self.failed_auth_attempts = 0
        self.status = DeviceStatus.VERIFIED

    def mark_active(self, expires: datetime | None, now: datetime) -> None:
        """Record that the device picked up its first token.

        Args:
            expires: New expiry time, or None for a device that never expires.
            now: Current time.
        """
        self.status = DeviceStatus.ACTIVE
        self.failed_auth_attempts = 0
        self.expires = expires
        self.last_login = now

    def mark_used(self, when: datetime) -> None:
        """Record the time of the last authentication with this device.

        Args:
            when: Time of use.
        """
        self.last_login = when

    def update_locked(self, locked: bool) -> None:
        """Set whether the device may authenticate.

        Args:
            locked: New locked state.
        """
        self.locked = locked
        if not locked:
            self.failed_auth_attempts = 0

    def update_trusted(self, trusted: bool) -> None:
        """Set whether the device is trusted.

        Args:
            trusted: New trusted state.
        """
        self.trusted = trusted

    def check_usable(self, now: datetime) -> None:
        """Reject a device that cannot take part in the flow.

        Args:
            now: Current time.

        Raises:
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
        """
        if self.locked:
            raise DeviceLocked
        if self.is_expired(now):
            raise DeviceExpired
