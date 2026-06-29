"""Human-action event extraction for Google ADK runner results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._types import ADKHandoffRequest, ADKRunRequest, ADKRunResult

_REQUEST_CONFIRMATION = "adk_request_confirmation"
_REQUEST_CREDENTIAL = "adk_request_credential"
_REQUEST_INPUT = "adk_request_input"
_HANDOFF_FUNCTION_NAMES = frozenset(
    {_REQUEST_CONFIRMATION, _REQUEST_CREDENTIAL, _REQUEST_INPUT}
)
_HANDOFF_KIND_TOOL_CONFIRMATION = "tool_confirmation"
_HANDOFF_KIND_CREDENTIAL_REQUEST = "credential_request"
_HANDOFF_KIND_HUMAN_INPUT = "human_input"
_NO_PAYLOAD = object()


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
                    synthetic_by_original[
                        (_HANDOFF_KIND_TOOL_CONFIRMATION, original_id)
                    ] = {
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
                    synthetic_by_original[
                        (_HANDOFF_KIND_CREDENTIAL_REQUEST, original_id)
                    ] = {
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
            synthetic = synthetic_by_original.get(
                (_HANDOFF_KIND_TOOL_CONFIRMATION, call_id), {}
            )
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
            synthetic = synthetic_by_original.get(
                (_HANDOFF_KIND_CREDENTIAL_REQUEST, call_id), {}
            )
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
        if kind == _HANDOFF_KIND_TOOL_CONFIRMATION:
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


def build_tool_confirmation_message(
    handoff: ADKHandoffRequest,
    confirmed: bool,
    *,
    payload: Any = _NO_PAYLOAD,
) -> Any:
    """Build the ADK follow-up message for a tool-confirmation handoff.

    The returned object is a ``google.genai.types.Content`` with ``role="user"``
    and one ``function_response`` named ``adk_request_confirmation``. ADK uses
    the response id to match the human answer to its pending synthetic
    confirmation request, so Kitaru refuses to build a message when that id is
    missing.
    """
    _validate_tool_confirmation_handoff(
        handoff,
        api_name="build_tool_confirmation_message",
    )
    if not isinstance(confirmed, bool):
        raise KitaruUsageError(
            "`confirmed` must be a boolean for ADK tool-confirmation resume."
        )

    response: dict[str, Any] = {"confirmed": confirmed}
    if payload is not _NO_PAYLOAD:
        response["payload"] = payload

    genai_types = _google_genai_types()
    try:
        function_response = genai_types.FunctionResponse(
            name=_REQUEST_CONFIRMATION,
            id=handoff.request_function_call_id,
            response=response,
        )
        part = genai_types.Part(function_response=function_response)
        return genai_types.Content(role="user", parts=[part])
    except Exception as exc:  # pragma: no cover - depends on installed genai shape
        raise KitaruFeatureNotAvailableError(
            "Installed `google.genai.types` does not expose the Content/Part/"
            "FunctionResponse constructor shape required for ADK "
            "tool-confirmation resume."
        ) from exc


def build_tool_confirmation_request(
    handoff: ADKHandoffRequest,
    confirmed: bool,
    *,
    user_id: str,
    session_id: str,
    payload: Any = _NO_PAYLOAD,
    run_kwargs: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> ADKRunRequest:
    """Build an ``ADKRunRequest`` that resumes a tool-confirmation handoff."""
    return ADKRunRequest(
        user_id=user_id,
        session_id=session_id,
        message=build_tool_confirmation_message(
            handoff,
            confirmed,
            payload=payload,
        ),
        run_kwargs=dict(run_kwargs or {}),
        metadata=dict(metadata or {}),
    )


def wait_for_tool_confirmation(
    result: ADKRunResult,
    *,
    user_id: str,
    session_id: str,
    handoff_index: int | None = None,
    name: str | None = None,
    question: str | None = None,
    timeout: int | None = None,
    payload: Any = _NO_PAYLOAD,
    run_kwargs: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> ADKRunRequest:
    """Wait at flow scope and return an ADK tool-confirmation resume request."""
    if not is_inside_flow():
        raise KitaruUsageError(
            "`wait_for_tool_confirmation()` must be called inside a flow."
        )
    if is_inside_checkpoint():
        raise KitaruUsageError(
            "`wait_for_tool_confirmation()` must be called from the flow body, "
            "not inside a checkpoint."
        )
    if result.status != "requires_action":
        raise KitaruUsageError(
            "`wait_for_tool_confirmation()` requires an ADK result with "
            "status='requires_action'."
        )

    handoff = _select_tool_confirmation_handoff(result, handoff_index)
    _validate_tool_confirmation_handoff(
        handoff,
        api_name="wait_for_tool_confirmation",
    )
    wait_metadata: dict[str, Any] = {
        "adapter": "google_adk",
        "source": "tool_confirmation_wait",
        "handoff_kind": handoff.kind,
        "tool_name": handoff.tool_name,
        "function_call_id": handoff.function_call_id,
        "request_function_call_id": handoff.request_function_call_id,
        "invocation_id": handoff.invocation_id,
    }
    if metadata:
        wait_metadata["user_metadata"] = dict(metadata)

    import kitaru

    confirmed = kitaru.wait(
        schema=bool,
        name=name or "Google ADK tool confirmation",
        question=question or _tool_confirmation_question(handoff),
        timeout=timeout,
        metadata=wait_metadata,
    )
    if not isinstance(confirmed, bool):
        raise KitaruUsageError(
            "`wait_for_tool_confirmation()` expected `kitaru.wait()` to return "
            "a boolean approval decision."
        )
    return build_tool_confirmation_request(
        handoff,
        confirmed,
        user_id=user_id,
        session_id=session_id,
        payload=payload,
        run_kwargs=run_kwargs,
        metadata=metadata,
    )


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
        kind=_HANDOFF_KIND_TOOL_CONFIRMATION,
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
        kind=_HANDOFF_KIND_CREDENTIAL_REQUEST,
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
        kind=_HANDOFF_KIND_HUMAN_INPUT,
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


def _validate_tool_confirmation_handoff(
    handoff: ADKHandoffRequest,
    *,
    api_name: str,
) -> None:
    if handoff.kind != _HANDOFF_KIND_TOOL_CONFIRMATION:
        raise KitaruUsageError(
            f"`{api_name}()` expects an ADK handoff with "
            f"kind='tool_confirmation', got {handoff.kind!r}."
        )
    if not handoff.request_function_call_id:
        raise KitaruUsageError(
            "ADK tool-confirmation handoff cannot be resumed safely because "
            "it is missing `request_function_call_id`. ADK requires the "
            "follow-up FunctionResponse id to match the synthetic "
            "`adk_request_confirmation` call id."
        )


def _google_genai_types() -> Any:
    try:
        from google.genai import types as genai_types
    except ImportError as exc:
        raise KitaruFeatureNotAvailableError(
            "ADK tool-confirmation resume helpers require "
            "`google.genai.types`. Install the Google ADK extra with "
            "`uv sync --extra google-adk` or `pip install 'kitaru[google-adk]'`."
        ) from exc
    return genai_types


def _select_tool_confirmation_handoff(
    result: ADKRunResult,
    handoff_index: int | None,
) -> ADKHandoffRequest:
    if handoff_index is not None:
        try:
            handoff = result.handoffs[handoff_index]
        except IndexError as exc:
            raise KitaruUsageError(
                "ADK result does not contain handoff index "
                f"{handoff_index}. Available indexes: "
                f"0..{len(result.handoffs) - 1}."
            ) from exc
        _validate_tool_confirmation_handoff(
            handoff,
            api_name="wait_for_tool_confirmation",
        )
        return handoff

    confirmations = [
        handoff
        for handoff in result.handoffs
        if handoff.kind == _HANDOFF_KIND_TOOL_CONFIRMATION
    ]
    if not confirmations:
        raise KitaruUsageError(
            "ADK result has no `tool_confirmation` handoffs to resume."
        )
    if len(confirmations) > 1:
        raise KitaruUsageError(
            "ADK result has multiple `tool_confirmation` handoffs. Pass "
            "`handoff_index` to choose one."
        )
    return confirmations[0]


def _tool_confirmation_question(handoff: ADKHandoffRequest) -> str:
    if handoff.message:
        return handoff.message
    if handoff.tool_name:
        return f"Approve ADK tool `{handoff.tool_name}`?"
    return "Approve the pending ADK tool call?"


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
