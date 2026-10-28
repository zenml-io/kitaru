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
"""Pagination, filtering, and auth helpers shared by the in-memory fakes."""

import uuid
from collections.abc import Callable, Collection, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneClient,
    ControlPlaneError,
    ControlPlaneUser,
)
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.base import ListFilter
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    FilterExpression,
    NotExpression,
    OrExpression,
)

# Scope claiming every kind with no agent pin, the former empty-scope default.
UNSCOPED_WORKER_SCOPE = WorkerScope(
    claims=[WorkerClaim(kind=kind) for kind in TaskKind]
)


def _renewed_timestamp(previous: datetime | None) -> datetime:
    """Return an update time strictly after the stored updated timestamp.

    Args:
        previous: Stored updated timestamp.

    Returns:
        Update time.
    """
    now = datetime.now(UTC)
    if previous is not None and now <= previous:
        now = previous + timedelta(microseconds=1)
    return now


class FakePasswordHasher:
    """Reversible in-memory password hasher."""

    @property
    def dummy_hash(self) -> str:
        """Fake-scheme hash matching no password.

        Returns:
            Hash for timing-uniform verification when no stored hash exists.
        """
        return "hashed:cf3a4a1bc2f04ee5b2e2f36b93d1c05f"

    def hash(self, password: str) -> str:
        """Hash a password.

        Args:
            password: Plaintext password.

        Returns:
            Marked plaintext standing in for a hash.
        """
        return f"hashed:{password}"

    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against a fake hash.

        Args:
            password: Plaintext password.
            password_hash: Stored fake hash.

        Returns:
            ``True`` when the password matches the hash.
        """
        return password_hash == f"hashed:{password}"


class FakeControlPlaneClient(ControlPlaneClient):
    """Fake control plane API client returning a scripted user."""

    def __init__(
        self,
        user: ControlPlaneUser | None = None,
        error: ControlPlaneError | None = None,
    ) -> None:
        """Create a fake control plane API client.

        Args:
            user: User returned by authorize_user.
            error: Error raised by authorize_user instead of returning.
        """
        self.user = user
        self.error = error
        self.received_credentials: list[str] = []
        self.received_server_id: uuid.UUID | None = None

    async def authorize_user(
        self, credential: str, server_id: uuid.UUID
    ) -> ControlPlaneUser:
        """Record the call and return the scripted user.

        Args:
            credential: Bearer token supplied by the caller.
            server_id: Server instance this API represents.

        Raises:
            ControlPlaneError: The fake was configured to raise.

        Returns:
            Scripted user.
        """
        self.received_credentials.append(credential)
        self.received_server_id = server_id
        if self.error is not None:
            raise self.error
        assert self.user is not None
        return self.user

    async def close(self) -> None:
        """Close the fake client, which holds no connections."""


class _HasId(Protocol):
    """Protocol for domain objects with a stable id."""

    id: uuid.UUID


ListItemT = TypeVar("ListItemT", bound=_HasId)


def _paginate_fake(
    items: list[ListItemT], list_filter: ListFilter
) -> tuple[list[ListItemT], str | None]:
    """Apply cursor pagination to an in-memory list of domain objects.

    Args:
        items: Candidate items already filtered by non-pagination fields.
        list_filter: List filter carrying the cursor, size, and sort.

    Returns:
        Page of matching items and the next cursor.
    """
    _, _, direction = list_filter.sort.partition(":")
    descending = direction == "desc"
    filter_hash = list_filter.compute_filter_hash()
    cursor = None
    if list_filter.cursor is not None:
        cursor = decode_cursor(list_filter.cursor, list_filter.sort, filter_hash)

    ordered = sorted(items, key=lambda item: item.id, reverse=descending)

    if cursor is not None:
        last_id = uuid.UUID(cursor.id)
        ordered = [
            item
            for item in ordered
            if (item.id < last_id if descending else item.id > last_id)
        ]

    page = ordered[: list_filter.size + 1]
    next_cursor = None
    if len(page) > list_filter.size:
        page = page[: list_filter.size]
        last_item = page[-1]
        next_cursor = encode_cursor(list_filter.sort, str(last_item.id), filter_hash)
    return page, next_cursor


def _evaluate_filter_expression(
    item: Any,
    expression: FilterExpression,
    resolvers: Mapping[str, Callable[[Any, FilterCondition], bool]] | None = None,
) -> bool:
    """Evaluate a filter expression against a domain object.

    Args:
        item: Domain object to evaluate the expression against.
        expression: Filter expression.
        resolvers: Condition evaluators keyed by field name, for fields that
            are not attributes of the object.

    Returns:
        Whether the object matches the expression.
    """
    if isinstance(expression, AndExpression):
        return all(
            _evaluate_filter_expression(item, operand, resolvers)
            for operand in expression.operands
        )
    if isinstance(expression, OrExpression):
        return any(
            _evaluate_filter_expression(item, operand, resolvers)
            for operand in expression.operands
        )
    if isinstance(expression, NotExpression):
        return not _evaluate_filter_expression(item, expression.operand, resolvers)
    assert isinstance(expression, FilterCondition)
    if resolvers is not None and expression.field in resolvers:
        return resolvers[expression.field](item, expression)
    return _matches_condition(getattr(item, expression.field), expression)


def _matches_condition(value: Any, condition: FilterCondition) -> bool:
    """Evaluate a filter condition against a resolved value.

    Args:
        value: Value the condition applies to.
        condition: Validated filter condition.

    Returns:
        Whether the value matches the condition.
    """
    if condition.op is FilterOp.IS_NULL:
        return value is None
    if value is None:
        # Comparisons with a null column value never match, mirroring SQL.
        return False
    match condition.op:
        case FilterOp.EQ:
            return value == condition.value
        case FilterOp.NE:
            return value != condition.value
        case FilterOp.LT:
            return value < condition.value
        case FilterOp.LE:
            return value <= condition.value
        case FilterOp.GT:
            return value > condition.value
        case FilterOp.GE:
            return value >= condition.value
        case FilterOp.IN:
            return value in condition.value
        case FilterOp.STARTSWITH:
            return value.startswith(condition.value)
        case FilterOp.ENDSWITH:
            return value.endswith(condition.value)
        case FilterOp.CONTAINS:
            return condition.value in value


def _refuse_unresolvable_fields(
    expression: FilterExpression | None, fields: Collection[str]
) -> None:
    """Refuse an expression naming a field the fake cannot resolve.

    Walks the whole expression before any item is evaluated. Refusing per item
    would stay silent on an empty store, and on an `and` whose earlier operand
    already answered false, so a test could pass without the filter ever
    running.

    Args:
        expression: Filter expression, or ``None`` when the query is unfiltered.
        fields: Field names that resolve through rows the fake has no handle on.

    Raises:
        NotImplementedError: The expression names one of those fields.
    """
    if expression is None:
        return
    if isinstance(expression, (AndExpression, OrExpression)):
        for operand in expression.operands:
            _refuse_unresolvable_fields(operand, fields)
        return
    if isinstance(expression, NotExpression):
        _refuse_unresolvable_fields(expression.operand, fields)
        return
    if expression.field in fields:
        raise NotImplementedError(
            f"The fake cannot resolve the {expression.field} filter"
        )
