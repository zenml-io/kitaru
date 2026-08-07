"""Typed tool-outcome codec contracts."""

from collections.abc import Sequence
from typing import overload

import pytest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from adapters.langgraph import ToolPolicyError
from adapters.langgraph.capture import CapturePolicy
from adapters.langgraph.codec import (
    coerce_static_tool_result,
    decode_tool_outcome,
    encode_tool_outcome,
)


def test_tool_message_round_trip_remaps_current_identity() -> None:
    original = ToolMessage(
        content="sunny",
        artifact={"temperature": 21},
        status="success",
        name="old-name",
        tool_call_id="old-call",
    )

    decoded = decode_tool_outcome(
        encode_tool_outcome(original),
        tool_call_id="current-call",
        tool_name="weather",
    )

    assert isinstance(decoded, ToolMessage)
    assert decoded.content == "sunny"
    assert decoded.artifact == {"temperature": 21}
    assert decoded.name == "weather"
    assert decoded.tool_call_id == "current-call"


def test_command_round_trip_preserves_control_fields_and_nested_message() -> None:
    original = Command(
        graph="parent",
        update={
            "messages": [ToolMessage(content="done", tool_call_id="old", name="old")],
            "approved": True,
        },
        resume={"answer": "yes"},
        goto="review",
    )

    decoded = decode_tool_outcome(
        encode_tool_outcome(original),
        tool_call_id="current",
        tool_name="approval",
    )

    assert isinstance(decoded, Command)
    assert decoded.graph == "parent"
    assert decoded.resume == {"answer": "yes"}
    assert decoded.goto == "review"
    assert isinstance(decoded.update, dict)
    assert decoded.update["approved"] is True
    message = decoded.update["messages"][0]
    assert isinstance(message, ToolMessage)
    assert message.tool_call_id == "current"
    assert message.name == "approval"


def test_command_round_trip_preserves_mapping_with_reserved_type_key() -> None:
    original = Command(
        update={
            "state": {
                "__kitaru_langgraph_type__": "tool_message",
                "content": "ordinary application state",
            }
        }
    )

    decoded = decode_tool_outcome(
        encode_tool_outcome(original),
        tool_call_id="current",
        tool_name="tool",
    )

    assert isinstance(decoded, Command)
    assert decoded.update == original.update


def test_cyclic_command_state_is_lossy_instead_of_recursing() -> None:
    state: dict[str, object] = {}
    state["self"] = state

    envelope = encode_tool_outcome(Command(update=state))

    assert envelope["replayable"] is False
    assert "cycle" in envelope["loss_reasons"]
    with pytest.raises(ToolPolicyError, match="not replayable"):
        decode_tool_outcome(envelope, tool_call_id="current", tool_name="tool")


def test_pre_encoding_reads_only_one_item_past_nested_collection_limit() -> None:
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

    values = TrackingSequence()

    envelope = encode_tool_outcome(
        Command(update={"values": values}),
        policy=CapturePolicy(max_collection_items=2),
    )

    assert values.accessed == [0, 1, 2]
    assert envelope["replayable"] is False
    assert "max_collection_items" in envelope["loss_reasons"]


def test_pre_encoding_field_limit_bounds_nested_collection_work() -> None:
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

    values = TrackingSequence()

    envelope = encode_tool_outcome(
        Command(update={"values": values}),
        policy=CapturePolicy(max_field_bytes=8, max_collection_items=1_000),
    )

    assert envelope["replayable"] is False
    assert "max_field_bytes" in envelope["loss_reasons"]
    assert len(values.accessed) <= 8


def test_redacted_tool_outcome_is_not_replayable() -> None:
    envelope = encode_tool_outcome(
        ToolMessage(
            content="done",
            artifact={"api_key": "sentinel"},
            tool_call_id="old",
        )
    )

    assert envelope["replayable"] is False
    assert "sensitive_key_redacted" in envelope["loss_reasons"]
    with pytest.raises(ToolPolicyError, match="not replayable"):
        decode_tool_outcome(envelope, tool_call_id="current", tool_name="tool")


def test_lossy_envelope_is_not_replayable() -> None:
    envelope = encode_tool_outcome(
        ToolMessage(content="x" * 100, tool_call_id="old"),
        policy=CapturePolicy(max_field_bytes=60),
    )

    assert envelope["replayable"] is False
    with pytest.raises(ToolPolicyError, match="not replayable"):
        decode_tool_outcome(
            envelope,
            tool_call_id="current",
            tool_name="tool",
        )


@pytest.mark.parametrize(
    "value",
    [None, {}, {"schema": "unknown"}, {"schema": "kitaru.langgraph.tool_result.v1"}],
)
def test_malformed_or_unknown_envelopes_are_rejected(value: object) -> None:
    with pytest.raises(ToolPolicyError):
        decode_tool_outcome(value, tool_call_id="call", tool_name="tool")


def test_plain_static_json_becomes_tool_message() -> None:
    result = coerce_static_tool_result(
        {"weather": "sunny"}, tool_call_id="call", tool_name="weather"
    )

    assert isinstance(result, ToolMessage)
    assert result.tool_call_id == "call"
    assert result.name == "weather"
    assert result.artifact == {"weather": "sunny"}
