"""Defensive shape helpers and vocabulary for Gemini stream events."""

from collections.abc import Mapping, Sequence
from typing import Any

from . import _tokens
from ._serialization import to_json_safe

CALL_ID_TYPE_FRAGMENTS = _tokens.CALL_ID_TYPE_FRAGMENTS
normalize_token = _tokens.normalize_token
normalized_token_contains_any = _tokens.normalized_token_contains_any

_STREAM_RECONSTRUCTION_POLICY = "stream_accumulator_v1"

INTERACTION_EVENT_TYPES = frozenset(
    {
        "interaction.created",
        "interaction.status_update",
        "interaction.completed",
        "interaction.complete",
    }
)
INTERACTION_COMPLETED_EVENT_TYPES = frozenset(
    {"interaction.completed", "interaction.complete"}
)
STEP_START_EVENT_TYPE = "step.start"
STEP_DELTA_EVENT_TYPE = "step.delta"
STEP_STOP_EVENT_TYPE = "step.stop"
CONTENT_START_EVENT_TYPE = "content.start"
CONTENT_DELTA_EVENT_TYPE = "content.delta"
CONTENT_STOP_EVENT_TYPE = "content.stop"
START_EVENT_TYPES = frozenset({STEP_START_EVENT_TYPE, CONTENT_START_EVENT_TYPE})
DELTA_EVENT_TYPES = frozenset({STEP_DELTA_EVENT_TYPE, CONTENT_DELTA_EVENT_TYPE})
STOP_EVENT_TYPES = frozenset({STEP_STOP_EVENT_TYPE, CONTENT_STOP_EVENT_TYPE})
ERROR_EVENT_TYPE = "error"
DONE_EVENT_TYPE = "done"
DEFAULT_DELTA_TYPE = "delta"

TEXT_DELTA_TYPES = frozenset({"text", "output_text", "text_delta"})
ARGUMENT_DELTA_TYPES = frozenset({"arguments_delta", "input_json_delta"})
THOUGHT_DELTA_TYPES = frozenset({"thought_summary", "thought_signature", "thinking"})
MEDIA_DELTA_TYPES = frozenset({"image", "audio", "document", "video"})
SAFE_OUTPUT_ROLES = frozenset({"assistant", "model"})
SAFE_OUTPUT_STEP_TYPES = frozenset(
    {
        "model_output",
        "model_response",
        "assistant_message",
        "assistant_output",
        "output_text",
    }
)
UNSAFE_ROLES = frozenset({"user", "tool", "function", "system", "developer"})
UNSAFE_TYPE_FRAGMENTS = (
    "user_input",
    "input",
    "tool_result",
    "tool_call",
    "tool",
    "function_result",
    "function_call",
    "function",
    "sandbox",
    "code",
    "shell",
    "terminal",
    "web",
    "mcp",
)
USAGE_FIELD_NAMES = ("usage", "usage_metadata", "usageMetadata")


def extract(value: Any, key: str) -> Any:
    """Read ``key`` from mapping-like or attribute-like SDK objects."""
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def sequence_or_empty(value: Any) -> list[Any]:
    """Normalize optional SDK scalar/sequence fields into a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str | bytes | bytearray):
        return [value]
    if isinstance(value, Sequence):
        return list(value)
    return [value]


def string_from(value: Any, key: str) -> str | None:
    """Return a stream field only when the SDK already exposed it as a string."""
    field = extract(value, key)
    return field if isinstance(field, str) else None


def coerce_string(value: Any) -> str | None:
    """Coerce SDK metadata fields to strings while preserving ``None``."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def dict_or_none(value: Any) -> dict[str, Any] | None:
    """Convert optional SDK metadata to a JSON-safe dictionary."""
    if value is None:
        return None
    if hasattr(value, "__dict__"):
        return {
            str(key): to_json_safe(nested)
            for key, nested in vars(value).items()
            if not str(key).startswith("_")
        }
    safe = to_json_safe(value)
    return safe if isinstance(safe, dict) else {"value": safe}


def usage_from(value: Any) -> dict[str, Any] | None:
    """Return usage metadata from known Gemini SDK field aliases."""
    for field_name in USAGE_FIELD_NAMES:
        usage = dict_or_none(extract(value, field_name))
        if usage is not None:
            return usage
    return None


def event_type(event: Any) -> str:
    """Return the safe categorical type for one stream event."""
    return (
        string_from(event, "type")
        or string_from(event, "event_type")
        or type(event).__name__
    )


def interaction_from_event(event: Any) -> Any:
    """Return the nested interaction payload when a stream event carries one."""
    for key in ("interaction", "data"):
        value = extract(event, key)
        if value is not None:
            return value
    return event


def step_from_event(event: Any) -> Any:
    """Return the nested step/content payload when a stream event carries one."""
    for key in ("step", "content", "data"):
        value = extract(event, key)
        if value is not None:
            return value
    return {}


def delta_from_event(event: Any) -> Any:
    """Return the nested step-delta payload when a stream event carries one."""
    delta = extract(event, "delta")
    if delta is not None:
        return delta
    step = step_from_event(event)
    return extract(step, "delta") or step


def delta_type(delta: Any) -> str | None:
    """Return a safe categorical type for one nested delta payload."""
    return string_from(delta, "type") or string_from(delta, "delta_type")


def delta_type_value(event: Any, delta: Any) -> str:
    """Return the display/storage delta type value for one stream delta event."""
    return delta_type(delta) or string_from(event, "delta_type") or DEFAULT_DELTA_TYPE


def normalized_delta_type(event: Any, delta: Any) -> str:
    """Return the normalized delta type for branching."""
    return normalize_token(delta_type_value(event, delta)) or DEFAULT_DELTA_TYPE


def delta_text(delta: Any, *, include_arguments_delta: bool = False) -> str | None:
    """Return textual delta content for accumulator/display code.

    ``arguments_delta`` is opt-in because live-event publisher code must never
    accidentally treat tool/function argument fragments as displayable text.
    """
    keys = (
        ("text", "delta", "content", "arguments_delta")
        if include_arguments_delta
        else ("text", "delta", "content")
    )
    for key in keys:
        value = extract(delta, key)
        if isinstance(value, str):
            return value
    return None


def function_name_from_step(value: Any) -> str | None:
    """Return a Gemini function/tool name from known provider field names."""
    for field_name in ("name", "tool_name", "function_name"):
        function_name = coerce_string(extract(value, field_name))
        if function_name is not None:
            return function_name
    return None


def safe_event_identity(event: Any) -> dict[str, Any]:
    """Return safe id metadata for a stream event."""
    fields: dict[str, Any] = {}
    event_id = string_from(event, "id") or string_from(event, "event_id")
    interaction_id = string_from(event, "interaction_id")
    if event_id is not None:
        fields["event_id"] = event_id
    if interaction_id is not None:
        fields["interaction_id"] = interaction_id
    return fields


def step_index(event: Any, step: Any) -> int | None:
    """Return a safe integer step index when the SDK exposes one."""
    for value in (
        extract(event, "step_index"),
        extract(event, "index"),
        extract(step, "index"),
    ):
        index = int_or_none(value)
        if index is not None:
            return index
    return None


def int_or_none(value: Any) -> int | None:
    """Coerce a value to int when safe enough for SDK metadata fields."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def environment_id(interaction: Any) -> str | None:
    """Return the environment id from known Gemini interaction shapes."""
    for key in ("environment_id", "environment"):
        value = coerce_string(extract(interaction, key))
        if value:
            return value
    agent_config = extract(interaction, "agent_config")
    if agent_config is not None:
        value = coerce_string(extract(agent_config, "environment_id"))
        if value:
            return value
    return None


def role_from_step(value: Any) -> str | None:
    """Return normalized role/author/speaker metadata from a step-like object."""
    for key in ("role", "author", "speaker"):
        role = extract(value, key)
        if isinstance(role, Mapping):
            for nested_key in ("role", "name", "type"):
                nested_role = normalize_token(coerce_string(extract(role, nested_key)))
                if nested_role:
                    return nested_role
        normalized_role = normalize_token(coerce_string(role))
        if normalized_role:
            return normalized_role
    return None


def step_type_from_step(value: Any) -> str | None:
    """Return normalized step type metadata from a step-like object."""
    return normalize_token(coerce_string(extract(value, "type")))


def has_unsafe_role_or_type_marker(
    *,
    role: str | None,
    step_type: str | None,
) -> bool:
    """Return whether a role/type marker is unsafe for output extraction."""
    return role in UNSAFE_ROLES or (
        step_type is not None
        and any(fragment in step_type for fragment in UNSAFE_TYPE_FRAGMENTS)
    )


def is_safe_output_text_source(
    *,
    role: str | None,
    step_type: str | None,
) -> bool:
    """Return whether streamed text belongs to a clear assistant/model output."""
    safe = role in SAFE_OUTPUT_ROLES or step_type in SAFE_OUTPUT_STEP_TYPES
    return safe and not has_unsafe_role_or_type_marker(
        role=role,
        step_type=step_type,
    )


def is_safe_stream_text_delta_source(
    *,
    event_type_value: str,
    role: str | None,
    step_type: str | None,
) -> bool:
    """Return whether a text delta may be surfaced as output text."""
    if event_type_value == CONTENT_DELTA_EVENT_TYPE:
        return True
    return is_safe_output_text_source(role=role, step_type=step_type)
