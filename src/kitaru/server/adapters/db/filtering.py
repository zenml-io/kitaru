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
"""Filter expression SQL compilation."""

import uuid
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import ColumnElement, and_, not_, or_, select
from sqlalchemy.orm import InstrumentedAttribute

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    FilterExpression,
    NotExpression,
    OrExpression,
)

FilterBinding = (
    InstrumentedAttribute[Any] | Callable[[FilterCondition], ColumnElement[bool]]
)


def build_tag_condition_binding(
    resource_type: TagResourceType,
    id_column: InstrumentedAttribute[uuid.UUID],
) -> Callable[[FilterCondition], ColumnElement[bool]]:
    """Build the tag filter binding for a tagged resource.

    Args:
        resource_type: Tag link resource type of the resource.
        id_column: Primary key column of the resource.

    Returns:
        Binding compiling a tag condition into an EXISTS predicate.
    """

    def compile_tag_condition(condition: FilterCondition) -> ColumnElement[bool]:
        """Compile a tag filter condition into an EXISTS predicate.

        Args:
            condition: Validated tag condition.

        Returns:
            SQL predicate.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        tag_exists = (
            select(TagLinkORM.id)
            .join(TagORM, TagORM.id == TagLinkORM.tag_id)
            .where(
                TagLinkORM.resource_type == resource_type.value,
                TagLinkORM.resource_id == id_column,
                TagORM.name.in_(names),
            )
            .correlate(id_column.class_)
        )
        return tag_exists.exists()

    return compile_tag_condition


def build_scope_condition_binding(
    local_column: InstrumentedAttribute[Any],
    related_key: InstrumentedAttribute[Any],
    scope_column: InstrumentedAttribute[Any],
) -> Callable[[FilterCondition], ColumnElement[bool]]:
    """Build a filter binding for a field held by a related row.

    Args:
        local_column: Column on the filtered table referencing the related row.
        related_key: Column on the related table that ``local_column`` points at.
        scope_column: Column on the related table the condition applies to.

    Returns:
        Binding compiling a scope condition into a membership predicate.
    """

    def compile_scope_condition(condition: FilterCondition) -> ColumnElement[bool]:
        """Compile a scope condition into a membership predicate.

        Args:
            condition: Validated scope condition.

        Returns:
            SQL predicate.
        """
        related = select(related_key).where(
            compile_column_condition(scope_column, condition)
        )
        return local_column.in_(related)

    return compile_scope_condition


def compile_column_condition(
    column: InstrumentedAttribute[Any], condition: FilterCondition
) -> ColumnElement[bool]:
    """Compile a filter condition against a column.

    Args:
        column: Column the condition applies to.
        condition: Validated filter condition.

    Returns:
        SQL predicate.
    """
    value = condition.value
    match condition.op:
        case FilterOp.EQ:
            return column == value
        case FilterOp.NE:
            return column != value
        case FilterOp.LT:
            return column < value
        case FilterOp.LE:
            return column <= value
        case FilterOp.GT:
            return column > value
        case FilterOp.GE:
            return column >= value
        case FilterOp.IN:
            return column.in_(value)
        case FilterOp.IS_NULL:
            return column.is_(None)
        case FilterOp.STARTSWITH:
            return column.startswith(value, autoescape=True)
        case FilterOp.ENDSWITH:
            return column.endswith(value, autoescape=True)
        case FilterOp.CONTAINS:
            return column.contains(value, autoescape=True)


def _compile_condition(
    condition: FilterCondition,
    bindings: Mapping[str, FilterBinding],
) -> ColumnElement[bool]:
    """Compile a filter condition into a SQL predicate.

    Args:
        condition: Validated filter condition.
        bindings: Filterable columns or predicate factories keyed by field
            name.

    Returns:
        SQL predicate.
    """
    binding = bindings[condition.field]
    if not isinstance(binding, InstrumentedAttribute):
        return binding(condition)
    return compile_column_condition(binding, condition)


def compile_filter_expression(
    expression: FilterExpression,
    bindings: Mapping[str, FilterBinding],
) -> ColumnElement[bool]:
    """Compile a filter expression into a SQL predicate.

    Args:
        expression: Validated filter expression.
        bindings: Filterable columns or predicate factories keyed by field
            name.

    Returns:
        SQL predicate.
    """
    if isinstance(expression, AndExpression):
        return and_(
            *(compile_filter_expression(node, bindings) for node in expression.operands)
        )
    if isinstance(expression, OrExpression):
        return or_(
            *(compile_filter_expression(node, bindings) for node in expression.operands)
        )
    if isinstance(expression, NotExpression):
        return not_(compile_filter_expression(expression.operand, bindings))
    return _compile_condition(expression, bindings)
