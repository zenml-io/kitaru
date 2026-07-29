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

import uuid
from datetime import datetime
from math import isfinite
from typing import Annotated, Any, Generic, TypeVar

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    SecretStr,
)

PlainSerializedSecretStr = Annotated[
    SecretStr,
    PlainSerializer(lambda value: value.get_secret_value(), when_used="json"),
]


class RequestModel(BaseModel):
    """Request model."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class ListParams(RequestModel):
    """List params."""

    cursor: str | None = Field(
        default=None, description="Cursor from the previous page."
    )
    size: int = Field(default=20, ge=1, le=1000, description="Items per page.")
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

    detail: str = Field(description="Error detail.")


def _validate_finite_json(value: Any) -> Any:
    """Reject non-finite floats anywhere in a JSON-compatible value."""
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("JSON numbers must be finite")
    if isinstance(value, dict):
        for item in value.values():
            _validate_finite_json(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _validate_finite_json(item)
    return value


FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
JsonValue = Annotated[Any, AfterValidator(_validate_finite_json)]
