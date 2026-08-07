"""Capture-policy contracts for the LangGraph adapter."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import overload

import pytest

from adapters.langgraph.capture import (
    CapturePolicy,
    capture_execution_view,
    capture_value,
)


@pytest.mark.parametrize(
    ("field", "factory"),
    [
        ("max_child_nodes", lambda: CapturePolicy(max_child_nodes=0)),
        ("max_field_bytes", lambda: CapturePolicy(max_field_bytes=0)),
        ("max_buffer_bytes", lambda: CapturePolicy(max_buffer_bytes=0)),
        ("max_depth", lambda: CapturePolicy(max_depth=0)),
        ("max_collection_items", lambda: CapturePolicy(max_collection_items=0)),
    ],
)
def test_capture_policy_rejects_non_positive_limits(
    field: str, factory: Callable[[], CapturePolicy]
) -> None:
    with pytest.raises(ValueError, match=field):
        factory()


def test_capture_redacts_sensitive_keys_without_mutating_input() -> None:
    value = {
        "Authorization": "Bearer sentinel",
        "nested": {"api_key": "sentinel", "safe": "kept"},
    }

    captured = capture_value(value, CapturePolicy())

    assert captured.value == {
        "Authorization": "[REDACTED]",
        "nested": {"api_key": "[REDACTED]", "safe": "kept"},
    }
    assert value["Authorization"] == "Bearer sentinel"
    assert value["nested"]["api_key"] == "sentinel"
    assert not captured.replayable
    assert captured.reasons == ("sensitive_key_redacted",)


def test_capture_custom_redactor_marks_result_as_lossy() -> None:
    captured = capture_value(
        {"private": "sentinel"},
        CapturePolicy(redactor=lambda value: {"private": "removed"}),
    )

    assert captured.value == {"private": "removed"}
    assert not captured.replayable
    assert captured.reasons == ("custom_redactor",)


@dataclass
class _Payload:
    value: str


def test_capture_marks_depth_collection_and_field_limits_as_lossy() -> None:
    policy = CapturePolicy(
        max_depth=1,
        max_collection_items=1,
        max_field_bytes=80,
    )

    captured = capture_value({"items": [_Payload("first"), _Payload("second")]}, policy)

    assert captured.lossy
    assert captured.truncated
    assert not captured.replayable
    assert captured.reasons


def test_capture_reads_only_one_item_past_collection_limit() -> None:
    class TrackingSequence(Sequence[int]):
        def __init__(self) -> None:
            self.accessed: list[int] = []

        def __len__(self) -> int:
            return 1_000_000

        @overload
        def __getitem__(self, index: int) -> int: ...

        @overload
        def __getitem__(self, index: slice) -> Sequence[int]: ...

        def __getitem__(self, index: int | slice) -> int | Sequence[int]:
            if isinstance(index, slice):
                return []
            self.accessed.append(index)
            return index

    value = TrackingSequence()

    captured = capture_value(value, CapturePolicy(max_collection_items=2))

    assert captured.value == [0, 1]
    assert value.accessed == [0, 1, 2]
    assert captured.reasons == ("max_collection_items",)


def test_capture_stops_measuring_oversized_scalar_at_field_limit() -> None:
    class TrackingString(str):
        iterated: int

        def __new__(cls, value: str) -> "TrackingString":
            instance = super().__new__(cls, value)
            instance.iterated = 0
            return instance

        def __iter__(self):  # type: ignore[no-untyped-def]
            for character in super().__iter__():
                self.iterated += 1
                yield character

    value = TrackingString("x" * 1_000_000)

    captured = capture_value(value, CapturePolicy(max_field_bytes=64))

    assert captured.value == {"__kitaru_capture__": "max_field_bytes"}
    assert captured.reasons == ("max_field_bytes",)
    assert value.iterated < 100


def test_field_limit_bounds_nested_collection_conversion() -> None:
    class TrackingSequence(Sequence[int]):
        def __init__(self) -> None:
            self.accessed: list[int] = []

        def __len__(self) -> int:
            return 1_000_000

        @overload
        def __getitem__(self, index: int) -> int: ...

        @overload
        def __getitem__(self, index: slice) -> Sequence[int]: ...

        def __getitem__(self, index: int | slice) -> int | Sequence[int]:
            if isinstance(index, slice):
                return []
            self.accessed.append(index)
            return index

    value = TrackingSequence()

    captured = capture_value(
        [value],
        CapturePolicy(max_field_bytes=8, max_collection_items=1_000),
    )

    assert captured.reasons == ("max_field_bytes",)
    assert len(value.accessed) <= 8


def test_capture_drops_unknown_objects_without_stringifying_them() -> None:
    class _SecretObject:
        def __str__(self) -> str:
            return "sentinel-secret"

    captured = capture_value(_SecretObject(), CapturePolicy())

    assert captured.value == {
        "__kitaru_capture__": "serialization_failed",
        "type": "_SecretObject",
    }
    assert "sentinel-secret" not in repr(captured.value)
    assert not captured.replayable


def test_execution_view_omits_callbacks_store_and_checkpointer() -> None:
    callback = object()
    captured = capture_execution_view(
        {
            "callbacks": [callback],
            "store": object(),
            "checkpointer": object(),
            "tags": ["tag"],
            "metadata": {"password": "sentinel", "safe": True},
            "configurable": {"thread_id": "thread-1", "secret": "sentinel"},
        },
        CapturePolicy(),
    )

    assert captured.value == {
        "metadata": {"password": "[REDACTED]", "safe": True},
        "tags": ["tag"],
        "thread_id": "thread-1",
    }
    assert "callbacks" not in captured.value
