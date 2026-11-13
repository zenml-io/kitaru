#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded reads over the SDK's paginated iterators."""

from collections.abc import AsyncIterator
from typing import TypeVar

ItemT = TypeVar("ItemT")


async def read_up_to(
    iterator: AsyncIterator[ItemT], limit: int
) -> tuple[list[ItemT], bool]:
    """Collect at most `limit` items and report whether more were available."""
    items: list[ItemT] = []
    async for item in iterator:
        if len(items) == limit:
            return items, True
        items.append(item)
    return items, False
