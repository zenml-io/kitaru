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
from typing import Annotated, Any, Generic, TypeVar

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


def _check_finite(value: Any) -> Any:
    """Reject non-finite floats nested in a JSON value.

    Args:
        value: JSON value to check.

    Raises:
        ValueError: The value contains a non-finite number.

    Returns:
        Unchanged value.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Value contains a non-finite number")
    if isinstance(value, dict):
        for item in value.values():
            _check_finite(item)
    elif isinstance(value, list):
        for item in value:
            _check_finite(item)
    return value


FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
JsonValue = Annotated[Any, AfterValidator(_check_finite)]


class RequestModel(BaseModel):
    """Request model."""

    model_config = ConfigDict(extra="forbid")


class ResponseModel(BaseModel):
    """Response model."""


ItemT = TypeVar("ItemT", bound=ResponseModel)


class Page(ResponseModel, Generic[ItemT]):
    """Pagination envelope."""

    items: list[ItemT]
    total: int
    page: int
    page_size: int


class ErrorBody(ResponseModel):
    """Error body."""

    detail: str
