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
"""Generic helpers shared across server layers."""

from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, tzinfo
from typing import TypeVar

ItemT = TypeVar("ItemT")


async def paginate_all(
    query: Callable[[str | None], Awaitable[tuple[Sequence[ItemT], str | None]]],
) -> list[ItemT]:
    """Collect all items of a cursor-paginated query.

    Args:
        query: Runs one page of the query by cursor, returning the page items
            and the next cursor.

    Returns:
        All matching items.
    """
    items: list[ItemT] = []
    cursor: str | None = None
    while True:
        batch, cursor = await query(cursor)
        items.extend(batch)
        if not batch or cursor is None:
            return items


def is_stale(value: datetime | None, max_age_seconds: float, now: datetime) -> bool:
    """Report whether a timestamp is missing or older than a maximum age.

    Args:
        value: Timestamp to check, treated as UTC when it carries no timezone.
        max_age_seconds: Age at which the timestamp counts as stale.
        now: Current time.

    Returns:
        Whether the timestamp needs renewing.
    """
    if value is None:
        return True
    return (now - to_tz_aware(value)).total_seconds() >= max_age_seconds


def to_tz_aware(value: datetime, tz: tzinfo = UTC) -> datetime:
    """Normalize a datetime to the given timezone, treating naive values as being in it.

    Args:
        value: Datetime to normalize.
        tz: Target timezone.

    Returns:
        Aware datetime in the target timezone.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)
