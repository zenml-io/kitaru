"""Shared helpers for temporarily binding the active ZenML stack."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

STACK_BINDING_LOCK = threading.RLock()


@contextmanager
def temporary_active_stack(
    stack_name_or_id: str | None,
    *,
    client_factory: Callable[[], Any],
) -> Iterator[Any | None]:
    """Temporarily activate a stack while holding the process stack lock."""
    with STACK_BINDING_LOCK:
        if not stack_name_or_id:
            yield None
            return

        client = client_factory()
        old_stack_id = client.active_stack_model.id
        client.activate_stack(stack_name_or_id)
        try:
            yield client
        finally:
            client.activate_stack(old_stack_id)


__all__ = ["STACK_BINDING_LOCK", "temporary_active_stack"]
