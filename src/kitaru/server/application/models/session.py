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
"""Session filter and command models."""

import uuid
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from pydantic import AwareDatetime, Field, field_validator

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import (
    BOOLEAN_OPS,
    EQUALITY_OPS,
    NULLABLE_OPS,
    ORDERED_OPS,
    SCOPE_OPS,
    STRING_OPS,
    AndExpression,
    FilterCondition,
    FilterExpression,
    FilterField,
    NotExpression,
    OrExpression,
    validate_filter_expression,
)


class SessionFilter(ListFilter):
    """Session list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "agent_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "agent_version_id": FilterField(
            value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS
        ),
        "task_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
        "import_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
        "origin": FilterField(value_type=SessionOrigin, ops=EQUALITY_OPS),
        "status": FilterField(value_type=SessionStatus, ops=EQUALITY_OPS),
        "imported_from": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "framework": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "external_id": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "name": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
        "tag": FilterField(value_type=str, ops=frozenset({FilterOp.EQ, FilterOp.IN})),
        "cohort_version_id": FilterField(value_type=uuid.UUID, ops=SCOPE_OPS),
        "session_group_id": FilterField(value_type=uuid.UUID, ops=SCOPE_OPS),
        "experiment_run_id": FilterField(value_type=uuid.UUID, ops=SCOPE_OPS),
        "has_evaluation": FilterField(value_type=bool, ops=BOOLEAN_OPS),
        "started_at": FilterField(
            value_type=AwareDatetime, ops=ORDERED_OPS | NULLABLE_OPS
        ),
        "ended_at": FilterField(
            value_type=AwareDatetime, ops=ORDERED_OPS | NULLABLE_OPS
        ),
        "cost": FilterField(value_type=Decimal, ops=ORDERED_OPS | NULLABLE_OPS),
        "llm_call_count": FilterField(value_type=int, ops=ORDERED_OPS),
        "tool_call_count": FilterField(value_type=int, ops=ORDERED_OPS),
        "created": FilterField(value_type=AwareDatetime, ops=ORDERED_OPS),
    }

    @field_validator("expression")
    @classmethod
    def _validate_expression(
        cls, value: FilterExpression | None
    ) -> FilterExpression | None:
        """Validate session conditions and canonicalize the tags alias.

        Args:
            value: Session filter expression.

        Returns:
            Validated expression using canonical field names.
        """
        if value is None:
            return None
        validated = validate_filter_expression(
            value, {**cls.filterable_fields, "tags": cls.filterable_fields["tag"]}
        )
        return cls._normalize_tag_alias(validated)

    @classmethod
    def _normalize_tag_alias(cls, expression: FilterExpression) -> FilterExpression:
        """Replace tags with tag in a bounded, validated expression.

        Args:
            expression: Validated session filter expression.

        Returns:
            Expression using canonical field names.
        """
        if isinstance(expression, FilterCondition):
            return (
                expression.model_copy(update={"field": "tag"})
                if expression.field == "tags"
                else expression
            )
        if isinstance(expression, NotExpression):
            return NotExpression(operand=cls._normalize_tag_alias(expression.operand))
        operands = tuple(
            cls._normalize_tag_alias(operand) for operand in expression.operands
        )
        if isinstance(expression, AndExpression):
            return AndExpression(operands=operands)
        return OrExpression(operands=operands)


class SessionCreate(FrozenModel):
    """Session create command."""

    agent_id: uuid.UUID | None = None
    agent_version_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus | None = None
    name: str | None = None
    input_text_selector: str | None = None
    output_text_selector: str | None = None
    inputs: Any = None
    outputs: Any = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    imported_from: str | None = None
    framework: str | None = None
    adapter_version: str | None = None


class SessionUpdate(FrozenModel):
    """Session update command."""

    status: SessionStatus | None = None
    outputs: Any = None
    inputs: Any = None
    output_text_selector: str | None = None
    error: str | None = None
    ended_at: datetime | None = None
    name: str | None = None
    metadata: dict[str, Any] | None = None
