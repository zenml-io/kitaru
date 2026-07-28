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
    query: Callable[[int], Awaitable[tuple[Sequence[ItemT], int]]],
) -> list[ItemT]:
    """Collect all items of a paginated query.

    Args:
        query: Runs one page of the query by one-based page number, returning
            the page items and the total match count.

    Returns:
        All matching items.
    """
    items: list[ItemT] = []
    page = 1
    while True:
        batch, total = await query(page)
        items.extend(batch)
        if not batch or len(items) >= total:
            return items
        page += 1


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
