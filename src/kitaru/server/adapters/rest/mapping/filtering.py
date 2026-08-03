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
"""Filter DTO conversions."""

from kitaru.api_models.v1.filter import AndFilter, Filter, NotFilter, OrFilter
from kitaru.api_models.v1.filter import FilterCondition as FilterConditionRequest
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    FilterExpression,
    NotExpression,
    OrExpression,
)


def filter_to_expression(filter_: Filter) -> FilterExpression:
    """Convert a filter DTO to its application filter expression.

    Args:
        filter_: Filter DTO.

    Returns:
        Filter expression.
    """
    if isinstance(filter_, AndFilter):
        return AndExpression(
            operands=tuple(filter_to_expression(operand) for operand in filter_.and_)
        )
    if isinstance(filter_, OrFilter):
        return OrExpression(
            operands=tuple(filter_to_expression(operand) for operand in filter_.or_)
        )
    if isinstance(filter_, NotFilter):
        return NotExpression(operand=filter_to_expression(filter_.not_))
    assert isinstance(filter_, FilterConditionRequest)
    return FilterCondition(field=filter_.field, op=filter_.op, value=filter_.value)
