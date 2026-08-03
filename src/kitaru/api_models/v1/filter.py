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
"""Filter API models."""

import json
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BeforeValidator, Field, PlainSerializer

from kitaru.api_models.v1.base import JsonValue, ListParams, RequestModel


class FilterOp(StrEnum):
    """Filter condition operator."""

    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    IN = "in"
    IS_NULL = "is_null"
    STARTSWITH = "startswith"
    ENDSWITH = "endswith"
    CONTAINS = "contains"


class FilterCondition(RequestModel):
    """Filter condition."""

    field: str = Field(pattern=r"^[a-z][a-z0-9_]*$", description="Field to filter on.")
    op: FilterOp = Field(description="Comparison operator.")
    value: JsonValue = Field(default=None, description="Comparison value.")


class AndFilter(RequestModel):
    """And filter."""

    and_: list["Filter"] = Field(alias="and", min_length=1, description="Operands.")


class OrFilter(RequestModel):
    """Or filter."""

    or_: list["Filter"] = Field(alias="or", min_length=1, description="Operands.")


class NotFilter(RequestModel):
    """Not filter."""

    not_: "Filter" = Field(alias="not", description="Operand.")


Filter = FilterCondition | AndFilter | OrFilter | NotFilter

AndFilter.model_rebuild()
OrFilter.model_rebuild()
NotFilter.model_rebuild()


def _parse_filter_json(value: Any) -> Any:
    """Parse a JSON-encoded filter, passing parsed input through.

    Args:
        value: JSON string or already parsed filter.

    Returns:
        Parsed filter input.
    """
    if isinstance(value, (str, bytes)):
        return json.loads(value)
    return value


def _serialize_filter_json(value: "Filter") -> str:
    """Serialize a filter to its JSON encoding.

    Args:
        value: Filter to serialize.

    Returns:
        JSON string.
    """
    return json.dumps(value.model_dump(mode="json", by_alias=True))


FilterParam = Annotated[
    Filter,
    BeforeValidator(_parse_filter_json),
    PlainSerializer(_serialize_filter_json, when_used="json"),
]


class FilterableListParams(ListParams):
    """List params with a filter expression."""

    filter: FilterParam | None = Field(
        default=None,
        description="Filter expression, JSON-encoded in the query string.",
    )
