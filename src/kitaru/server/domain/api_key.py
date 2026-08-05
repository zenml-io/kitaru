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
"""API key entity, key material helpers, and errors."""

import base64
import json
import uuid
from datetime import datetime, timedelta

from pydantic import Field

from kitaru.api_models.v1.auth import API_KEY_PREFIX
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class ApiKeyNotFound(NotFoundError):
    """Raised when an API key lookup does not resolve."""

    def __init__(self, api_key_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            api_key_id: Id of the missing API key.
        """
        super().__init__(f"API key {api_key_id} was not found")


class DuplicateApiKeyName(ConflictError):
    """Raised when an API key name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"API key name '{name}' is already registered")


class InvalidApiKey(ValidationError):
    """Raised when an encoded API key cannot be decoded."""


class ApiKey(DomainModel):
    """API key."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    key_hash: str
    previous_key_hash: str | None = None
    retain_period_minutes: int = 0
    active: bool = True
    last_used: datetime | None = None
    last_rotated: datetime | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def update_active(self, active: bool) -> None:
        """Set whether the key may authenticate.

        Args:
            active: New active state.
        """
        self.active = active

    def mark_used(self, when: datetime) -> None:
        """Record the time of the last authentication with this key.

        Args:
            when: Time of use.
        """
        self.last_used = when

    def rotate(
        self, new_key_hash: str, retain_period_minutes: int, when: datetime
    ) -> None:
        """Replace the key hash, retaining the previous one for the retain period.

        Args:
            new_key_hash: Hash of the newly generated secret.
            retain_period_minutes: Minutes the previous key remains valid.
            when: Time of rotation.
        """
        self.previous_key_hash = self.key_hash
        self.key_hash = new_key_hash
        self.retain_period_minutes = retain_period_minutes
        self.last_rotated = when

    def is_previous_key_valid(self, now: datetime) -> bool:
        """Check whether the previous key is still within its retain period.

        Args:
            now: Current time.

        Returns:
            Whether the previous key hash still authenticates.
        """
        if self.previous_key_hash is None or self.last_rotated is None:
            return False
        if self.retain_period_minutes <= 0:
            return False
        return now - self.last_rotated < timedelta(minutes=self.retain_period_minutes)


def encode_api_key(key_id: uuid.UUID, secret: str) -> str:
    """Encode an API key id and secret into the client-facing key string.

    Args:
        key_id: Id of the API key.
        secret: Plaintext secret.

    Returns:
        Key string of the form ``KITKEY_<base64 payload>``.
    """
    payload = json.dumps({"id": str(key_id), "key": secret})
    return API_KEY_PREFIX + base64.b64encode(payload.encode("utf-8")).decode("utf-8")


def decode_api_key(value: str) -> tuple[uuid.UUID, str]:
    """Decode a client-facing key string into its id and secret.

    Args:
        value: Key string of the form ``KITKEY_<base64 payload>``.

    Raises:
        InvalidApiKey: ``value`` is not a well-formed API key.

    Returns:
        Id of the API key and the plaintext secret.
    """
    if not value.startswith(API_KEY_PREFIX):
        raise InvalidApiKey("API key is malformed")
    encoded = value[len(API_KEY_PREFIX) :]
    try:
        payload = json.loads(base64.b64decode(encoded, validate=True))
    except ValueError as exc:
        raise InvalidApiKey("API key is malformed") from exc
    if not isinstance(payload, dict):
        raise InvalidApiKey("API key is malformed")
    key_id = payload.get("id")
    secret = payload.get("key")
    if not isinstance(key_id, str) or not isinstance(secret, str):
        raise InvalidApiKey("API key is malformed")
    try:
        return uuid.UUID(key_id), secret
    except ValueError as exc:
        raise InvalidApiKey("API key is malformed") from exc
