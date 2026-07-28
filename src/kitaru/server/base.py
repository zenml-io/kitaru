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
"""Shared server model primitives."""

import hashlib
import json
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from kitaru.server.domain.base import ValidationError


class FrozenModel(BaseModel):
    """Base type for immutable value objects."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ListFilter(FrozenModel):
    """Base type for list filter models using cursor pagination."""

    sortable_fields: ClassVar[frozenset[str]] = frozenset({"created"})

    cursor: str | None = None
    size: int = Field(default=20, ge=1, le=1000)
    sort: str = "created:desc"

    @field_validator("sort")
    @classmethod
    def _validate_sort(cls, value: str) -> str:
        """Validate the sort field and direction.

        Args:
            value: Sort string.

        Raises:
            ValidationError: The sort string is malformed or names a field
                outside the sortable allowlist.

        Returns:
            Validated sort string.
        """
        field, _, direction = value.partition(":")
        if direction not in ("asc", "desc") or field not in cls.sortable_fields:
            raise ValidationError(f"Invalid sort '{value}'")
        return value

    def compute_filter_hash(self) -> str:
        """Hash the filter's non-pagination fields.

        Returns:
            First 16 hex characters of the SHA-256 digest.
        """
        payload = self.model_dump(mode="json")
        for key in ("cursor", "size", "sort"):
            payload.pop(key, None)
        canonical = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
