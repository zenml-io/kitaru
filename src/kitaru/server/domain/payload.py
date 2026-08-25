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
from typing import Any

JSON_MEDIA_TYPE = "application/json"
TEXT_MEDIA_TYPE = "text/plain"

_UNSET = object()


class Payload:
    """Session or node payload, inline, offloaded, or both in memory."""

    __slots__ = ("_value", "blob_id", "media_type")

    def __init__(
        self, value: Any, blob_id: uuid.UUID | None, media_type: str | None
    ) -> None:
        """Initialize the payload state directly, prefer json, text, or ref.

        Raises:
            ValueError: Neither a value nor a blob ref is given.
        """
        if value is _UNSET and blob_id is None:
            raise ValueError("A payload requires a value, a blob ref, or both")
        self._value = value
        self.blob_id = blob_id
        self.media_type = media_type

    @classmethod
    def json(cls, value: Any) -> "Payload":
        """Build an inline payload serialized as JSON.

        Raises:
            ValueError: The value is None.

        Returns:
            Payload holding the value, with no blob ref yet.
        """
        if value is None:
            raise ValueError("A JSON payload value cannot be None")
        return cls(value, blob_id=None, media_type=JSON_MEDIA_TYPE)

    @classmethod
    def text(cls, value: str) -> "Payload":
        """Build an inline payload serialized as plain text.

        Raises:
            ValueError: The value is None.

        Returns:
            Payload holding the value, with no blob ref yet.
        """
        if value is None:
            raise ValueError("A text payload value cannot be None")
        return cls(value, blob_id=None, media_type=TEXT_MEDIA_TYPE)

    @classmethod
    def ref(cls, blob_id: uuid.UUID) -> "Payload":
        """Build a payload referencing blob content, unresolved.

        Returns:
            Payload holding the ref, with no value until resolved.
        """
        return cls(_UNSET, blob_id=blob_id, media_type=None)

    @property
    def resolved(self) -> bool:
        """Whether the value is set.

        Returns:
            Whether the value is set.
        """
        return self._value is not _UNSET

    @property
    def value(self) -> Any:
        """Payload value.

        Raises:
            RuntimeError: The payload is an unresolved ref.

        Returns:
            The payload value.
        """
        if self._value is _UNSET:
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
        value = self._value if self._value is not _UNSET else "<unresolved>"
        return (
            f"Payload(value={value!r}, blob_id={self.blob_id!r}, "
            f"media_type={self.media_type!r})"
        )
