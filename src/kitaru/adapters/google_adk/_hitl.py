"""Human-action event extraction for Google ADK runner results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ._types import ADKHandoffRequest

_REQUEST_CONFIRMATION = "adk_request_confirmation"
_REQUEST_CREDENTIAL = "adk_request_credential"
_REQUEST_INPUT = "adk_request_input"
_HANDOFF_FUNCTION_NAMES = frozenset(
    {_REQUEST_CONFIRMATION, _REQUEST_CREDENTIAL, _REQUEST_INPUT}
)


def has_handoff_markers(events: list[dict[str, Any]]) -> bool:
    """Return whether serialized ADK events contain human-action markers."""
    for event in events:
        for function_call in _function_calls(event):
            if _string(_get(function_call, "name")) in _HANDOFF_FUNCTION_NAMES:
                return True

        actions = _mapping(_get(event, "actions"))
        if _mapping(
            _get(
                actions,
                "requestedToolConfirmations",
                "requested_tool_confirmations",
            )
        ):
            return True
        if _mapping(_get(actions, "requestedAuthConfigs", "requested_auth_configs")):
            return True
    return False


def extract_handoff_requests(events: list[dict[str, Any]]) -> list[ADKHandoffRequest]:
    """Return pending human-action requests from serialized ADK events.

    ADK records human-in-the-loop work in two places: synthetic function calls
    that tell the client what to answer, and ``Event.actions`` maps that keep
    the original tool call id. Kitaru serializes ADK events before this helper
    runs, so the parser intentionally works on plain dictionaries rather than
    importing ADK classes.
    """
    synthetic_by_original: dict[tuple[str, str], dict[str, Any]] = {}
    request_input_calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for event in events:
        for function_call in _function_calls(event):
            name = _string(_get(function_call, "name"))
            args = _mapping(_get(function_call, "args"))
            if name == _REQUEST_CONFIRMATION:
                original = _mapping(
                    _get(args, "originalFunctionCall", "original_function_call")
                )
                original_id = _string(_get(original, "id"))
                if original_id:
                    synthetic_by_original[("tool_confirmation", original_id)] = {
                        "event": event,
                        "function_call": function_call,
                        "original": original,
                        "details": _mapping(
                            _get(args, "toolConfirmation", "tool_confirmation")
                        ),
                    }
            elif name == _REQUEST_CREDENTIAL:
                original_id = _string(_get(args, "functionCallId", "function_call_id"))
                if original_id:
                    synthetic_by_original[("credential_request", original_id)] = {
                        "event": event,
                        "function_call": function_call,
                        "details": _mapping(_get(args, "authConfig", "auth_config")),
                    }
            elif name == _REQUEST_INPUT:
                request_input_calls.append((event, function_call))

    handoffs: list[ADKHandoffRequest] = []
    seen: set[tuple[str, str | None, str | None]] = set()

    for event in events:
        actions = _mapping(_get(event, "actions"))
        confirmations = _mapping(
            _get(
                actions,
                "requestedToolConfirmations",
                "requested_tool_confirmations",
            )
        )
        for original_id, confirmation in confirmations.items():
            call_id = str(original_id)
            synthetic = synthetic_by_original.get(("tool_confirmation", call_id), {})
            handoff = _tool_confirmation_handoff(
                event=event,
                call_id=call_id,
                synthetic=synthetic,
                confirmation=_mapping(confirmation),
            )
            _append_once(handoffs, seen, handoff)

        auth_configs = _mapping(
            _get(actions, "requestedAuthConfigs", "requested_auth_configs")
        )
        for original_id, auth_config in auth_configs.items():
            call_id = str(original_id)
            synthetic = synthetic_by_original.get(("credential_request", call_id), {})
            handoff = _credential_handoff(
                event=event,
                call_id=call_id,
                synthetic=synthetic,
                auth_config=_mapping(auth_config),
            )
            _append_once(handoffs, seen, handoff)

    # If ADK preserves the synthetic client request but a serializer drops the
    # matching actions map, still report the handoff. The synthetic function
    # call contains the client response name/id and the original call identity.
    for (kind, call_id), synthetic in synthetic_by_original.items():
        if kind == "tool_confirmation":
            handoff = _tool_confirmation_handoff(
                event=_mapping(synthetic.get("event")),
                call_id=call_id,
                synthetic=synthetic,
                confirmation={},
            )
        else:
            handoff = _credential_handoff(
                event=_mapping(synthetic.get("event")),
                call_id=call_id,
                synthetic=synthetic,
                auth_config={},
            )
        _append_once(handoffs, seen, handoff)

    for event, function_call in request_input_calls:
        handoff = _human_input_handoff(event=event, function_call=function_call)
        _append_once(handoffs, seen, handoff)

    return handoffs


def _tool_confirmation_handoff(
    *,
    event: Mapping[str, Any],
    call_id: str,
    synthetic: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> ADKHandoffRequest:
    function_call = _mapping(_get(synthetic, "function_call"))
    original = _mapping(_get(synthetic, "original"))
    details = _mapping(_get(synthetic, "details")) or dict(confirmation)
    return ADKHandoffRequest(
        kind="tool_confirmation",
        event_id=_string(_get(event, "id")),
        invocation_id=_string(_get(event, "invocationId", "invocation_id")),
        author=_string(_get(event, "author")),
        function_call_id=call_id,
        request_function_call_id=_string(_get(function_call, "id")),
        tool_name=_string(_get(original, "name")) or _function_response_name(event),
        tool_args=_dict_or_none(_get(original, "args")),
        message=_string(_get(details, "hint")),
        payload=_get(details, "payload"),
        raw=_compact_raw(event=event, details=confirmation),
    )


def _credential_handoff(
    *,
    event: Mapping[str, Any],
    call_id: str,
    synthetic: Mapping[str, Any],
    auth_config: Mapping[str, Any],
) -> ADKHandoffRequest:
    function_call = _mapping(_get(synthetic, "function_call"))
    details = _mapping(_get(synthetic, "details")) or dict(auth_config)
    return ADKHandoffRequest(
        kind="credential_request",
        event_id=_string(_get(event, "id")),
        invocation_id=_string(_get(event, "invocationId", "invocation_id")),
        author=_string(_get(event, "author")),
        function_call_id=call_id,
        request_function_call_id=_string(_get(function_call, "id")),
        tool_name=_function_response_name(event),
        auth_config=details,
        raw=_compact_raw(event=event, details=auth_config),
    )


def _human_input_handoff(
    *,
    event: Mapping[str, Any],
    function_call: Mapping[str, Any],
) -> ADKHandoffRequest:
    args = _mapping(_get(function_call, "args"))
    interrupt_id = _string(_get(args, "interruptId", "interrupt_id")) or _string(
        _get(function_call, "id")
    )
    return ADKHandoffRequest(
        kind="human_input",
        event_id=_string(_get(event, "id")),
        invocation_id=_string(_get(event, "invocationId", "invocation_id")),
        author=_string(_get(event, "author")),
        function_call_id=interrupt_id,
        request_function_call_id=_string(_get(function_call, "id")),
        message=_string(_get(args, "message")),
        payload=_get(args, "payload"),
        response_schema=_get(args, "response_schema", "responseSchema"),
        raw=_compact_raw(event=event, details=args),
    )


def _append_once(
    handoffs: list[ADKHandoffRequest],
    seen: set[tuple[str, str | None, str | None]],
    handoff: ADKHandoffRequest,
) -> None:
    key = (handoff.kind, handoff.function_call_id, handoff.request_function_call_id)
    if key in seen:
        return
    seen.add(key)
    handoffs.append(handoff)


def _function_calls(event: Mapping[str, Any]) -> list[dict[str, Any]]:
    content = _mapping(_get(event, "content"))
    parts = _get(content, "parts")
    if not isinstance(parts, Sequence) or isinstance(parts, str | bytes | bytearray):
        return []
    calls: list[dict[str, Any]] = []
    for part in parts:
        part_mapping = _mapping(part)
        function_call = _mapping(_get(part_mapping, "functionCall", "function_call"))
        if function_call:
            calls.append(dict(function_call))
    return calls


def _function_response_name(event: Mapping[str, Any]) -> str | None:
    content = _mapping(_get(event, "content"))
    parts = _get(content, "parts")
    if not isinstance(parts, Sequence) or isinstance(parts, str | bytes | bytearray):
        return None
    for part in parts:
        response = _mapping(
            _get(_mapping(part), "functionResponse", "function_response")
        )
        if name := _string(_get(response, "name")):
            return name
    return None


def _get(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _compact_raw(
    *,
    event: Mapping[str, Any],
    details: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "event_id": _get(event, "id"),
        "node_info": _get(event, "nodeInfo", "node_info"),
        "details": dict(details),
    }
