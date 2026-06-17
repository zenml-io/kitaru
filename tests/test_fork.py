"""Tests for the top-level kitaru.fork dispatcher."""

from typing import Any, cast

import pytest

import kitaru
from kitaru.errors import KitaruUsageError


class FakeForkTarget:
    def __init__(self) -> None:
        self.seen_kwargs: dict[str, Any] | None = None

    def fork(self, **kwargs: Any) -> dict[str, Any]:
        self.seen_kwargs = dict(kwargs)
        return {"ok": True, "kwargs": kwargs}


def test_top_level_fork_delegates_to_target_fork() -> None:
    target = FakeForkTarget()

    result = kitaru.fork(target, thread_id="thread-1", update_values={"x": 1})

    assert result == {
        "ok": True,
        "kwargs": {"thread_id": "thread-1", "update_values": {"x": 1}},
    }
    assert target.seen_kwargs == {"thread_id": "thread-1", "update_values": {"x": 1}}


def test_top_level_fork_rejects_unsupported_target() -> None:
    with pytest.raises(KitaruUsageError, match="KitaruGraphRunner"):
        kitaru.fork(cast(Any, object()), thread_id="thread-1")
