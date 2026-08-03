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
from collections.abc import Mapping
from typing import ClassVar

from pydantic import Field, field_validator

from kitaru.base import FrozenModel
from kitaru.server.domain.base import ValidationError
from kitaru.server.filtering import (
    FilterExpression,
    FilterField,
    validate_filter_expression,
)

__all__ = ["FrozenModel", "ListFilter"]


class ListFilter(FrozenModel):
    """Base type for list filter models using cursor pagination."""

    sortable_fields: ClassVar[frozenset[str]] = frozenset({"created"})
    filterable_fields: ClassVar[Mapping[str, FilterField]] = {}

    cursor: str | None = None
    size: int = Field(default=20, ge=1, le=1000)
    sort: str = "created:desc"
    expression: FilterExpression | None = None

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

    @field_validator("expression")
    @classmethod
    def _validate_expression(
        cls, value: FilterExpression | None
    ) -> FilterExpression | None:
        """Validate the filter expression against the filterable fields.

        Args:
            value: Filter expression.

        Raises:
            ValidationError: The expression exceeds the size caps or uses a
                field, operator, or value outside the filterable allowlist.

        Returns:
            Validated expression with condition values coerced.
        """
        if value is None:
            return None
        return validate_filter_expression(value, cls.filterable_fields)

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
