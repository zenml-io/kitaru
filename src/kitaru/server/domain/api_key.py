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
import hashlib
import json
import secrets
import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name

API_KEY_PREFIX = "KITKEY_"


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
    active: bool = True
    last_used: datetime | None = None
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


def generate_secret() -> str:
    """Generate a random API key secret.

    Returns:
        Hex-encoded 256-bit secret.
    """
    return secrets.token_hex(32)


def hash_secret(secret: str) -> str:
    """Hash an API key secret for storage.

    Args:
        secret: Plaintext secret.

    Returns:
        SHA-256 hex digest.
    """
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


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
