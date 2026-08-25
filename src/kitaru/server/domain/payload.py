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
"""Payload value type."""

import uuid
from enum import StrEnum
from typing import Any

from pydantic import PrivateAttr

from kitaru.server.domain.base import DomainModel


class PayloadMediaType(StrEnum):
    """Payload media type."""

    JSON = "application/json"
    TEXT = "text/plain"


class Payload(DomainModel):
    """Session or node payload, inline, offloaded, or both in memory."""

    blob_id: uuid.UUID | None = None
    media_type: PayloadMediaType | None = None

    _value: Any = PrivateAttr(default=None)

    @classmethod
    def from_json(cls, value: Any) -> "Payload":
        """Build an inline payload serialized as JSON.

        Raises:
            ValueError: The value is None.

        Returns:
            Payload holding the value, with no blob ref yet.
        """
        if value is None:
            raise ValueError("A JSON payload value cannot be None")
        payload = cls(media_type=PayloadMediaType.JSON)
        payload._value = value
        return payload

    @classmethod
    def from_text(cls, value: str) -> "Payload":
        """Build an inline payload serialized as plain text.

        Raises:
            ValueError: The value is None.

        Returns:
            Payload holding the value, with no blob ref yet.
        """
        if value is None:
            raise ValueError("A text payload value cannot be None")
        payload = cls(media_type=PayloadMediaType.TEXT)
        payload._value = value
        return payload

    @classmethod
    def from_ref(cls, blob_id: uuid.UUID) -> "Payload":
        """Build a payload referencing blob content, unresolved.

        Returns:
            Payload holding the ref, with no value until resolved.
        """
        return cls(blob_id=blob_id)

    @property
    def resolved(self) -> bool:
        """Whether the value is set.

        Returns:
            Whether the value is set.
        """
        return self._value is not None

    @property
    def value(self) -> Any:
        """Payload value.

        Raises:
            RuntimeError: The payload is an unresolved ref.

        Returns:
            The payload value.
        """
        if self._value is None:
            raise RuntimeError(f"Payload for blob {self.blob_id} was not resolved")
        return self._value

    @value.setter
    def value(self, value: Any) -> None:
        self._value = value

    def __eq__(self, other: object) -> bool:
        """Compare by value-or-absence and blob id."""
        if not isinstance(other, Payload):
            return NotImplemented
        return (self._value, self.blob_id) == (other._value, other.blob_id)

    def __repr__(self) -> str:
        """Represent the payload for debugging."""
        value = self._value if self._value is not None else "<unresolved>"
        return (
            f"Payload(value={value!r}, blob_id={self.blob_id!r}, "
            f"media_type={self.media_type!r})"
        )
