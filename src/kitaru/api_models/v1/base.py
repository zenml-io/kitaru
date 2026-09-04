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
"""Shared DTO bases, pagination envelope, and error body."""

import math
import re
import uuid
from datetime import datetime
from typing import Annotated, Any, Generic, TypeVar

from pydantic import (
    AfterValidator,
    AllowInfNan,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    SecretStr,
    WithJsonSchema,
)

PlainSerializedSecretStr = Annotated[
    SecretStr,
    PlainSerializer(lambda value: value.get_secret_value(), when_used="json"),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]

FiniteFloat = Annotated[float, AllowInfNan(False)]


def _check_finite(value: Any) -> Any:
    """Reject non-finite floats anywhere in a nested value.

    Args:
        value: Value to check.

    Raises:
        ValueError: A float in the value is inf or nan.

    Returns:
        The value unchanged.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Value must be finite")
    if isinstance(value, dict):
        for item in value.values():
            _check_finite(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_finite(item)
    return value


JsonValue = Annotated[Any, BeforeValidator(_check_finite)]

_CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _validate_no_control_chars(value: str) -> str:
    """Reject NUL and other C0 control characters except tab, newline, and CR.

    Args:
        value: String to validate.

    Raises:
        ValueError: The string contains a rejected control character.

    Returns:
        Validated string.
    """
    if _CONTROL_CHAR_PATTERN.search(value):
        raise ValueError("String must not contain control characters")
    return value


PlainStr = Annotated[str, AfterValidator(_validate_no_control_chars)]


class RequestModel(BaseModel):
    """Request model."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class CursorParams(RequestModel):
    """Cursor params."""

    cursor: str | None = Field(
        default=None, description="Cursor from the previous page."
    )
    size: int = Field(default=20, ge=1, le=1000, description="Items per page.")


class ListParams(CursorParams):
    """List params."""

    sort: str = Field(
        default="created:desc",
        description="Sort field and direction, as field:asc or field:desc.",
        pattern=r"^[a-z][a-z0-9_]*:(asc|desc)$",
    )


class DiscriminatedRequestModel(RequestModel):
    """Discriminated request model."""

    def model_post_init(self, context: Any) -> None:
        """Mark the type discriminator as set so exclude_unset dumps keep it.

        Args:
            context: Pydantic context.
        """
        _ = context
        self.model_fields_set.add("type")


class ResponseModel(BaseModel):
    """Response model."""

    model_config = ConfigDict(protected_namespaces=())


class TimestampedResponseModel(ResponseModel):
    """Timestamped response model."""

    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")


class OwnedResponseModel(TimestampedResponseModel):
    """Owned response model."""

    owner_id: uuid.UUID = Field(description="Id of the owning account.")


ItemT = TypeVar("ItemT", bound=ResponseModel)


class Page(ResponseModel, Generic[ItemT]):
    """Pagination envelope."""

    items: list[ItemT] = Field(description="Items on this page.")
    next_cursor: str | None = Field(
        description="Cursor for the next page, null on the last page."
    )


class ErrorBody(ResponseModel):
    """Error body."""

    detail: str = Field(description="Error message.")


class ValidationErrorItem(ResponseModel):
    """Validation error item."""

    loc: list[str | int] = Field(description="Path to the invalid input.")
    msg: str = Field(description="Error message.")
    type: str = Field(description="Error type identifier.")
    input: Any = Field(default=None, description="Invalid input value.")
    ctx: dict[str, Any] = Field(default_factory=dict, description="Error context.")


class ValidationErrorBody(ResponseModel):
    """Validation error body."""

    detail: str | list[ValidationErrorItem] = Field(description="Error detail.")
