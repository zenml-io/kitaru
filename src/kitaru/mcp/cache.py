#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Small in-process cache with expiry and a size bound."""

import time
from collections import OrderedDict
from typing import Generic, TypeVar

ValueT = TypeVar("ValueT")


class TTLCache(Generic[ValueT]):
    """Keep the most recently stored values for a limited time."""

    def __init__(self, max_entries: int, ttl_seconds: float) -> None:
        """Create an empty cache.

        Args:
            max_entries: Entries kept before the oldest is evicted.
            ttl_seconds: Seconds a stored value stays readable.
        """
        self._max_entries = max_entries
        self._ttl = ttl_seconds
        self._entries: OrderedDict[str, tuple[float, ValueT]] = OrderedDict()

    def get(self, key: str) -> ValueT | None:
        """Return the stored value, or `None` when missing or expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        stored_at, value = entry
        if time.monotonic() - stored_at > self._ttl:
            del self._entries[key]
            return None
        return value

    def put(self, key: str, value: ValueT) -> None:
        """Store a value, evicting the oldest entry when full."""
        self._entries[key] = (time.monotonic(), value)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
