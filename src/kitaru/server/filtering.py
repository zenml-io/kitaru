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
"""Filter expression primitives."""

from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from pydantic import TypeAdapter

from kitaru.api_models.v1.filter import FilterOp
from kitaru.base import FrozenModel
from kitaru.server.domain.base import ValidationError

MAX_FILTER_DEPTH = 5
MAX_FILTER_CONDITIONS = 30
MAX_FILTER_IN_VALUES = 100

EQUALITY_OPS = frozenset({FilterOp.EQ, FilterOp.NE, FilterOp.IN})
ORDERED_OPS = EQUALITY_OPS | {FilterOp.LT, FilterOp.LE, FilterOp.GT, FilterOp.GE}
STRING_OPS = EQUALITY_OPS | {
    FilterOp.STARTSWITH,
    FilterOp.ENDSWITH,
    FilterOp.CONTAINS,
}
BOOLEAN_OPS = frozenset({FilterOp.EQ, FilterOp.NE})
NULLABLE_OPS = frozenset({FilterOp.IS_NULL})
# Ops for a field resolved through other rows rather than held by the filtered
# table. NE is left out because it reads as the complement of EQ but compiles
# to "points at a related row whose value is not X", which is a different
# question wherever a row reaches several related rows, or none. Negation is
# spelled `not`, which the bindings compile to NOT EXISTS.
SCOPE_OPS = frozenset({FilterOp.EQ, FilterOp.IN})


class FilterField(FrozenModel):
    """Filterable field declaration."""

    value_type: Any
    ops: frozenset[FilterOp]


class FilterCondition(FrozenModel):
    """Filter condition."""

    field: str
    op: FilterOp
    value: Any = None


class AndExpression(FrozenModel):
    """And expression."""

    operands: tuple["FilterExpression", ...]


class OrExpression(FrozenModel):
    """Or expression."""

    operands: tuple["FilterExpression", ...]


class NotExpression(FrozenModel):
    """Not expression."""

    operand: "FilterExpression"


FilterExpression = FilterCondition | AndExpression | OrExpression | NotExpression

AndExpression.model_rebuild()
OrExpression.model_rebuild()
NotExpression.model_rebuild()


@lru_cache
def _get_type_adapter(value_type: Any) -> TypeAdapter[Any]:
    """Build the type adapter for a filterable field's value type.

    Args:
        value_type: Value type to adapt.

    Returns:
        Type adapter.
    """
    return TypeAdapter(value_type)


def _coerce_value(field: str, value: Any, value_type: Any) -> Any:
    """Coerce a condition value to a field's value type.

    Args:
        field: Field the value filters on.
        value: Raw condition value.
        value_type: Value type of the field.

    Raises:
        ValidationError: The value does not fit the field's value type.

    Returns:
        Coerced value.
    """
    try:
        return _get_type_adapter(value_type).validate_python(value)
    except Exception as error:
        raise ValidationError(f"Invalid filter value for field '{field}'") from error


def _validate_condition(
    condition: FilterCondition, fields: Mapping[str, FilterField]
) -> FilterCondition:
    """Validate a filter condition against the filterable fields.

    Args:
        condition: Filter condition.
        fields: Filterable fields keyed by name.

    Raises:
        ValidationError: The condition uses a field, operator, or value
            outside the filterable allowlist.

    Returns:
        Condition with the value coerced to the field's value type.
    """
    field_spec = fields.get(condition.field)
    if field_spec is None:
        raise ValidationError(f"Field '{condition.field}' is not filterable")
    if condition.op not in field_spec.ops:
        raise ValidationError(
            f"Operator '{condition.op.value}' is not supported for field "
            f"'{condition.field}'"
        )
    if condition.op is FilterOp.IS_NULL:
        if condition.value is not None:
            raise ValidationError(
                f"Filter 'is_null' on field '{condition.field}' takes no value"
            )
        value: Any = None
    elif condition.value is None:
        raise ValidationError(f"Invalid filter value for field '{condition.field}'")
    elif condition.op is FilterOp.IN:
        if not isinstance(condition.value, (list, tuple)):
            raise ValidationError(
                f"Filter value for 'in' on field '{condition.field}' must be a list"
            )
        if not 1 <= len(condition.value) <= MAX_FILTER_IN_VALUES:
            raise ValidationError(
                f"Filter value for 'in' on field '{condition.field}' must have "
                f"between 1 and {MAX_FILTER_IN_VALUES} items"
            )
        value = _coerce_value(
            condition.field, condition.value, tuple[field_spec.value_type, ...]
        )
    else:
        value = _coerce_value(condition.field, condition.value, field_spec.value_type)
    return FilterCondition(field=condition.field, op=condition.op, value=value)


def _validate_node(
    expression: FilterExpression, fields: Mapping[str, FilterField], depth: int
) -> tuple[FilterExpression, int]:
    """Validate one expression node and its children.

    Args:
        expression: Expression node.
        fields: Filterable fields keyed by name.
        depth: Nesting depth of the node.

    Raises:
        ValidationError: The node exceeds the depth cap or fails condition
            validation.

    Returns:
        Validated node and the number of conditions it contains.
    """
    if depth > MAX_FILTER_DEPTH:
        raise ValidationError(f"Filter is nested deeper than {MAX_FILTER_DEPTH} levels")
    if isinstance(expression, FilterCondition):
        return _validate_condition(expression, fields), 1
    if isinstance(expression, NotExpression):
        operand, count = _validate_node(expression.operand, fields, depth + 1)
        return NotExpression(operand=operand), count
    if not expression.operands:
        raise ValidationError("Filter 'and' and 'or' need at least one operand")
    operands = []
    count = 0
    for operand in expression.operands:
        validated, operand_count = _validate_node(operand, fields, depth + 1)
        operands.append(validated)
        count += operand_count
    if isinstance(expression, AndExpression):
        return AndExpression(operands=tuple(operands)), count
    return OrExpression(operands=tuple(operands)), count


def validate_filter_expression(
    expression: FilterExpression, fields: Mapping[str, FilterField]
) -> FilterExpression:
    """Validate a filter expression against the filterable fields.

    Args:
        expression: Filter expression.
        fields: Filterable fields keyed by name.

    Raises:
        ValidationError: The expression exceeds the size caps or uses a
            field, operator, or value outside the filterable allowlist.

    Returns:
        Validated expression with condition values coerced.
    """
    validated, count = _validate_node(expression, fields, depth=1)
    if count > MAX_FILTER_CONDITIONS:
        raise ValidationError(
            f"Filter has more than {MAX_FILTER_CONDITIONS} conditions"
        )
    return validated
