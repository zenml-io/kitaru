"""Typed tool-outcome codec contracts."""

import asyncio
import json
import traceback
from collections.abc import Sequence
from typing import Any, Literal, NoReturn, overload

import pytest
from hypothesis import given
from hypothesis import strategies as st
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from kitaru_langgraph import ToolPolicyError
from kitaru_langgraph.capture import CapturePolicy
from kitaru_langgraph.codec import (
    TOOL_OUTCOME_SCHEMA,
    coerce_static_tool_result,
    decode_tool_outcome,
    encode_tool_outcome,
)


def _assert_same_nested_types(actual: Any, expected: Any) -> None:
    assert type(actual) is type(expected)
    assert actual == expected
    if isinstance(expected, dict):
        for key, item in expected.items():
            _assert_same_nested_types(actual[key], item)
    elif isinstance(expected, (list, tuple)):
        for decoded_item, item in zip(actual, expected, strict=True):
            _assert_same_nested_types(decoded_item, item)


_native_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=12),
    lambda children: (
        st.lists(children, max_size=3)
        | st.lists(children, max_size=3).map(tuple)
        | st.dictionaries(
            st.sampled_from(["state", "value", "__kitaru_langgraph_type__"]),
            children,
            max_size=3,
        )
    ),
    max_leaves=10,
)


@given(value=_native_values, command=st.booleans())
def test_supported_outcomes_round_trip_through_json(value: Any, command: bool) -> None:
    original = (
        Command(update=value, resume=value)
        if command
        else ToolMessage(content="done", artifact=value, tool_call_id="old")
    )
    envelope = encode_tool_outcome(original)
    serialized = json.loads(json.dumps(envelope))
    if not envelope["replayable"]:
        assert envelope["loss_reasons"]
        with pytest.raises(ToolPolicyError, match="not replayable"):
            decode_tool_outcome(serialized, tool_call_id="new", tool_name="tool")
        return

    result = decode_tool_outcome(serialized, tool_call_id="new", tool_name="tool")

    if command:
        assert isinstance(result, Command)
        _assert_same_nested_types(result.update, value)
        _assert_same_nested_types(result.resume, value)
        _assert_same_nested_types(result.goto, ())
    else:
        assert isinstance(result, ToolMessage)
        _assert_same_nested_types(result.artifact, value)
        assert result.tool_call_id == "new"
        assert result.name == "tool"


def test_command_tuple_pairs_and_default_goto_round_trip() -> None:
    original = Command(update=[("count", 1), ("nested", ((), [1, (2,)]))])

    envelope = encode_tool_outcome(original)
    assert envelope["replayable"] is True
    decoded = decode_tool_outcome(
        json.loads(json.dumps(envelope)), tool_call_id="new", tool_name="tool"
    )

    assert isinstance(decoded, Command)
    _assert_same_nested_types(decoded.goto, ())
    _assert_same_nested_types(decoded.update, original.update)


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("status", ["success", "error"])
def test_explicit_status_round_trips(
    nested: bool, status: Literal["success", "error"]
) -> None:
    message = ToolMessage(content="done", tool_call_id="old", status=status)
    original = Command(update={"message": message}) if nested else message
    decoded = decode_tool_outcome(
        json.loads(json.dumps(encode_tool_outcome(original))),
        tool_call_id="new",
        tool_name="tool",
    )
    if isinstance(decoded, Command):
        assert isinstance(decoded.update, dict)
        result = decoded.update["message"]
    else:
        result = decoded
    assert result.status == status
    assert result.tool_call_id == "new"
    assert result.name == "tool"


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("status", ["missing", None, "invalid"])
def test_missing_or_invalid_status_is_rejected(nested: bool, status: Any) -> None:
    message = ToolMessage(content="done", tool_call_id="old")
    envelope = encode_tool_outcome(
        Command(update={"message": message}) if nested else message
    )
    payload = envelope["payload"]
    if nested:
        payload = payload["update"]["message"]
    if status == "missing":
        payload.pop("status")
    else:
        payload["status"] = status

    with pytest.raises(ToolPolicyError):
        decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")


@pytest.mark.parametrize("operation", ["get", "items"])
def test_mapping_failures_do_not_leak_payloads(operation: str) -> None:
    class BrokenMapping(dict[str, Any]):
        def get(self, key: object, default: Any = None) -> Any:
            if operation == "get":
                raise RuntimeError("private-payload-sentinel")
            return super().get(key, default)

        def items(self) -> NoReturn:
            raise RuntimeError("private-payload-sentinel")

    envelope = encode_tool_outcome(Command())
    if operation == "get":
        value = BrokenMapping(envelope)
    else:
        envelope["payload"]["update"] = BrokenMapping()
        value = envelope

    with pytest.raises(ToolPolicyError) as caught:
        decode_tool_outcome(value, tool_call_id="new", tool_name="tool")
    assert "private-payload-sentinel" not in "".join(
        traceback.format_exception(caught.value)
    )


def test_validation_exception_chain_does_not_leak_payloads() -> None:
    envelope = encode_tool_outcome(ToolMessage(content="done", tool_call_id="old"))
    envelope["payload"]["id"] = {"private-payload-sentinel": 1}

    with pytest.raises(ToolPolicyError) as caught:
        decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")
    assert "private-payload-sentinel" not in "".join(
        traceback.format_exception(caught.value)
    )


@pytest.mark.parametrize("cycle", [False, True])
def test_deep_or_cyclic_stored_payload_is_rejected(cycle: bool) -> None:
    value: list[Any] = []
    if cycle:
        value.append(value)
    else:
        for _ in range(3000):
            value = [value]
    envelope = encode_tool_outcome(Command())
    envelope["payload"]["update"] = value

    with pytest.raises(ToolPolicyError):
        decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")


@pytest.mark.parametrize("exception", [SystemExit(), asyncio.CancelledError()])
def test_process_exit_and_cancellation_are_not_swallowed(
    exception: BaseException,
) -> None:
    class InterruptedMapping(dict[str, Any]):
        def get(self, key: object, default: Any = None) -> NoReturn:
            raise exception

    with pytest.raises(type(exception)):
        decode_tool_outcome(InterruptedMapping(), tool_call_id="new", tool_name="tool")


@pytest.mark.parametrize("tuple_value", [None, {}, "wrong", 1])
def test_malformed_tuple_tags_are_rejected(tuple_value: Any) -> None:
    envelope = encode_tool_outcome(Command())
    envelope["payload"]["update"] = {
        "__kitaru_langgraph_type__": "tuple",
        "value": tuple_value,
    }

    with pytest.raises(ToolPolicyError):
        decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")


def test_legacy_command_lists_stay_lists() -> None:
    envelope = {
        "schema": TOOL_OUTCOME_SCHEMA,
        "kind": "command",
        "replayable": True,
        "loss_reasons": [],
        "payload": {
            "graph": None,
            "update": [["count", 1]],
            "resume": None,
            "goto": [],
        },
    }

    decoded = decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")

    assert isinstance(decoded, Command)
    _assert_same_nested_types(decoded.update, [["count", 1]])
    _assert_same_nested_types(decoded.goto, [])


def test_legacy_tool_message_with_explicit_status_still_decodes() -> None:
    envelope = {
        "schema": TOOL_OUTCOME_SCHEMA,
        "kind": "tool_message",
        "replayable": True,
        "loss_reasons": [],
        "payload": {
            "__kitaru_langgraph_type__": "tool_message",
            "content": "failed",
            "artifact": [[1, 2]],
            "status": "error",
        },
    }

    decoded = decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")

    assert isinstance(decoded, ToolMessage)
    assert decoded.status == "error"
    assert decoded.tool_call_id == "new"
    _assert_same_nested_types(decoded.artifact, [[1, 2]])


def test_unknown_nested_type_fails_closed() -> None:
    envelope = encode_tool_outcome(Command())
    envelope["payload"]["update"] = {"__kitaru_langgraph_type__": "future-type"}

    with pytest.raises(ToolPolicyError, match="type is unsupported"):
        decode_tool_outcome(envelope, tool_call_id="new", tool_name="tool")


def test_existing_tool_policy_error_is_preserved() -> None:
    error = ToolPolicyError("Already classified")

    class RejectedMapping(dict[str, Any]):
        def get(self, key: object, default: Any = None) -> NoReturn:
            raise error

    with pytest.raises(ToolPolicyError) as caught:
        decode_tool_outcome(RejectedMapping(), tool_call_id="new", tool_name="tool")
    assert caught.value is error


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


@pytest.mark.parametrize("tag", ["tool_message", "tuple", "mapping"])
def test_command_round_trip_preserves_mapping_with_reserved_type_key(tag: str) -> None:
    original = Command(
        update={
            "state": {
                "__kitaru_langgraph_type__": tag,
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


@pytest.mark.parametrize("kind", ["tool_message", "command"])
@pytest.mark.parametrize("key", [1, True, (1,)])
@pytest.mark.parametrize("reverse", [False, True])
def test_key_collision_in_native_outcome_is_not_replayable(
    kind: str, key: Any, reverse: bool
) -> None:
    items = [(key, "first"), (str(key), "second")]
    if reverse:
        items.reverse()
    mapping = dict(items)
    value = {"nested": [mapping, mapping]}
    original = (
        ToolMessage(content="done", artifact=value, tool_call_id="old")
        if kind == "tool_message"
        else Command(update=value)
    )

    envelope = encode_tool_outcome(original)

    assert envelope["replayable"] is False
    assert set(envelope["loss_reasons"]) == {"non_string_key", "key_collision"}
    assert len(envelope["loss_reasons"]) == 2
    assert list(mapping.items()) == items
    with pytest.raises(ToolPolicyError, match="not replayable"):
        decode_tool_outcome(envelope, tool_call_id="current", tool_name="tool")


def test_noncolliding_key_coercion_in_outcome_is_not_replayable() -> None:
    envelope = encode_tool_outcome(Command(update={1: "kept"}))

    assert envelope["payload"]["update"] == {"1": "kept"}
    assert envelope["loss_reasons"] == ["non_string_key"]
    assert envelope["replayable"] is False


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
    [
        None,
        {},
        {"schema": "unknown"},
        {"schema": TOOL_OUTCOME_SCHEMA},
        {"schema": TOOL_OUTCOME_SCHEMA, "replayable": True, "payload": []},
        {
            "schema": TOOL_OUTCOME_SCHEMA,
            "replayable": True,
            "payload": {},
            "kind": "unknown",
        },
    ],
)
def test_malformed_or_unknown_envelopes_are_rejected(value: object) -> None:
    with pytest.raises(ToolPolicyError):
        decode_tool_outcome(value, tool_call_id="call", tool_name="tool")


@pytest.mark.parametrize("field", ["graph", "update", "resume", "goto"])
def test_command_requires_every_stored_field(field: str) -> None:
    envelope = encode_tool_outcome(Command(update={"value": 1}))
    envelope["payload"].pop(field)

    with pytest.raises(ToolPolicyError, match="Command payload"):
        decode_tool_outcome(envelope, tool_call_id="call", tool_name="tool")


def test_plain_static_json_becomes_tool_message() -> None:
    result = coerce_static_tool_result(
        {"weather": "sunny"}, tool_call_id="call", tool_name="weather"
    )

    assert isinstance(result, ToolMessage)
    assert result.tool_call_id == "call"
    assert result.name == "weather"
    assert result.artifact == {"weather": "sunny"}
