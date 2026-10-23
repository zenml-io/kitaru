#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Rebuild OpenAI chat requests from recorded `llm_call` nodes.

Importers store each provider's payload as recorded, so the message shape
depends on the source rather than on the importer. Two shapes are rebuilt:

* OTel GenAI messages, whose content is a list of typed `parts` (Langfuse and
  Logfire exports of Pydantic AI, Phoenix `gen_ai.*` spans).
* OpenAI chat messages with `content` and `tool_calls` (Braintrust, MLflow,
  LangSmith OpenAI wrappers, most Kitaru JSONL files).

Anything else is skipped with a reason instead of guessed.
"""

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from kitaru.api_models.v1.session_node import NodeStatus, SessionNodeResponse

TOOL_DEFINITIONS_METADATA_KEY = "kitaru.tool_definitions"
_REQUEST_PARAMETERS_KEY = "model_request_parameters"

SkipReason = Literal[
    "failed_recorded_call",
    "missing_inputs",
    "unsupported_input_format",
    "non_text_content",
    "unrecognized_recorded_output",
    "missing_tool_schema",
    "missing_tool_call_id",
    "unsupported_tool",
    "unsupported_request_control",
]
SchemaSource = Literal["recorded", "supplied", "inferred", "none"]

_CHAT_ROLES = frozenset({"system", "developer", "user", "assistant", "tool"})
_TOOL_CHOICE_MODES = frozenset({"auto", "none", "required"})
_RESPONSE_FORMATS = frozenset({"text", "json_object", "json_schema"})
# Legacy function calling has no faithful rebuild in either request shape.
_LEGACY_CONTROLS = ("functions", "function_call")
_EMPTY_PARAMETERS: dict[str, Any] = {"type": "object", "properties": {}}


class UnsupportedShapeError(ValueError):
    """The recorded payload cannot be rebuilt faithfully."""

    def __init__(self, reason: SkipReason, detail: str) -> None:
        """Store the skip reason and a short detail."""
        super().__init__(detail)
        self.reason: SkipReason = reason


@dataclass(frozen=True)
class ToolCall:
    """A tool call with its arguments parsed from JSON when possible."""

    name: str
    arguments: Any


@dataclass(frozen=True)
class RecordedOutcome:
    """What the recorded model answered."""

    tool_calls: tuple[ToolCall, ...]
    text: str | None


@dataclass(frozen=True)
class ReplayRequest:
    """A recorded call rebuilt as an OpenAI chat request."""

    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    schema_source: SchemaSource
    source_format: str
    recorded: RecordedOutcome
    dropped_reasoning_parts: int = 0
    controls: Mapping[str, Any] = field(default_factory=dict)
    """Recorded `tool_choice`, `parallel_tool_calls` and `response_format`,
    in chat-completions form, which constrain what the model could answer.
    A Pydantic AI call that could not answer in text carries the
    `tool_choice: required` it sent."""


@dataclass(frozen=True)
class Skipped:
    """A recorded call that cannot be replayed."""

    reason: SkipReason
    detail: str


def parse_arguments(value: Any) -> Any:
    """Parse tool-call arguments recorded as a JSON string."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def build_replay_request(
    node: SessionNodeResponse,
    *,
    tool_schemas: Mapping[str, Mapping[str, Any]],
    tool_descriptions: Mapping[str, str],
    use_recorded_tools: bool,
    infer_missing_schemas: bool,
) -> ReplayRequest | Skipped:
    """Rebuild one recorded `llm_call` node as an OpenAI chat request."""
    if node.status is NodeStatus.FAILED:
        return Skipped("failed_recorded_call", node.error or "the call failed")
    try:
        messages_in, recorded_tools = _split_inputs(node.inputs)
        source_format = _detect_format(messages_in)
        if source_format == "otel_parts":
            messages, dropped = _convert_otel_messages(messages_in)
        else:
            messages, dropped = _convert_openai_messages(messages_in), 0
        recorded = parse_recorded_output(node.outputs)
        instructions = _system_instructions(node.attributes)
        # Pydantic AI can record a system prompt in the messages and separate
        # instructions in the attributes, so both are kept unless repeated.
        if instructions and not any(
            m["role"] in {"system", "developer"} and m.get("content") == instructions
            for m in messages
        ):
            messages = [{"role": "system", "content": instructions}, *messages]
    except UnsupportedShapeError as error:
        return Skipped(error.reason, str(error))
    # Traces are third-party data, so any other malformed value skips the
    # call instead of failing the whole analysis.
    except (TypeError, AttributeError, KeyError, ValueError) as error:
        return Skipped("unsupported_input_format", f"malformed payload: {error}")

    definitions: dict[str, dict[str, Any]] = {}
    schema_source: SchemaSource = "none"
    try:
        if use_recorded_tools:
            for definition in _recorded_tool_definitions(node, recorded_tools):
                definitions.setdefault(definition["function"]["name"], definition)
            if definitions:
                schema_source = "recorded"
        raw_controls = _recorded_controls(node.inputs, node.model_params)
    except UnsupportedShapeError as error:
        return Skipped(error.reason, str(error))
    used = _tool_names_used(messages, recorded)
    if used or definitions:
        # A model that may call tools must see every tool the agent offered,
        # so supplied schemas fill in whatever the trace did not record. Calls
        # with no sign of tools get none, since a multi-agent import also
        # holds routers and summarizers that never had tools.
        for name, schema in tool_schemas.items():
            if name not in definitions:
                definitions[name] = _definition(
                    name, schema, tool_descriptions.get(name)
                )
                if schema_source == "none":
                    schema_source = "supplied"
    missing = sorted(used - definitions.keys())
    if missing:
        if not infer_missing_schemas:
            return Skipped(
                "missing_tool_schema", f"no schema for: {', '.join(missing)}"
            )
        for name in missing:
            definitions[name] = _definition(
                name, _infer_schema(name, messages, recorded), None
            )
        schema_source = "inferred"
    if (
        "tool_choice" not in raw_controls
        and definitions
        and _text_output_disallowed(node)
    ):
        # Pydantic AI sends `tool_choice: required` whenever tools are offered
        # and the agent may only answer through a tool; without it, models
        # answer output-tool steps in plain text.
        raw_controls["tool_choice"] = "required"
    try:
        controls = _normalize_controls(raw_controls, definitions.keys())
    except UnsupportedShapeError as error:
        return Skipped(error.reason, str(error))
    return ReplayRequest(
        messages=messages,
        tools=list(definitions.values()),
        schema_source=schema_source,
        source_format=source_format,
        recorded=recorded,
        dropped_reasoning_parts=dropped,
        controls=controls,
    )


def _recorded_controls(inputs: Any, model_params: Any) -> dict[str, Any]:
    """Collect request controls recorded beside the messages or as parameters.

    Raises:
        UnsupportedShapeError: The call used legacy function calling.
    """
    if isinstance(inputs, str):
        try:
            inputs = json.loads(inputs)
        except json.JSONDecodeError:
            inputs = None
    scopes = [s for s in (inputs, model_params) if isinstance(s, dict)]
    for scope in scopes:
        for key in _LEGACY_CONTROLS:
            if scope.get(key) is not None:
                raise UnsupportedShapeError(
                    "unsupported_request_control", f"legacy `{key}` is not supported"
                )
    controls: dict[str, Any] = {}
    for key in ("tool_choice", "parallel_tool_calls", "response_format"):
        for scope in scopes:
            if scope.get(key) is not None:
                controls[key] = scope[key]
                break
    return controls


def _text_output_disallowed(node: SessionNodeResponse) -> bool:
    """Tell whether Pydantic AI recorded that the call could not answer in text.

    Importers keep `model_request_parameters` in different places: Logfire
    and Phoenix nest the raw span attributes under their own attribute key,
    and Langfuse keeps it in metadata under a prefixed key.
    """
    scopes: list[Any] = []
    for container in (node.attributes, node.metadata):
        if isinstance(container, dict):
            scopes.append(container)
            scopes.extend(v for v in container.values() if isinstance(v, dict))
    for scope in scopes:
        for key, value in scope.items():
            if key != _REQUEST_PARAMETERS_KEY and not key.endswith(
                f".{_REQUEST_PARAMETERS_KEY}"
            ):
                continue
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    continue
            # Pydantic AI forces a tool only when it can accept neither text
            # nor an image as the answer.
            if (
                isinstance(value, dict)
                and value.get("allow_text_output") is False
                and not value.get("allow_image_output")
            ):
                return True
    return False


def _normalize_controls(
    controls: Mapping[str, Any], offered: Iterable[str]
) -> dict[str, Any]:
    """Return the recorded controls in chat-completions form.

    A control changes which answers the model may give, so one that cannot be
    sent faithfully skips the call rather than being dropped.

    Raises:
        UnsupportedShapeError: A control has a form neither request shape
            can carry, or names a tool that is not offered.
    """
    normalized: dict[str, Any] = {}
    offered = set(offered)
    if "tool_choice" in controls:
        choice = _tool_choice(controls["tool_choice"], offered)
        if not offered and choice == "required":
            raise UnsupportedShapeError(
                "unsupported_request_control", "tool_choice requires a tool"
            )
        # Endpoints reject `tool_choice` without tools; with none offered,
        # `auto` and `none` both mean a text answer.
        if offered:
            normalized["tool_choice"] = choice
    if "parallel_tool_calls" in controls:
        value = controls["parallel_tool_calls"]
        if not isinstance(value, bool):
            raise UnsupportedShapeError(
                "unsupported_request_control", "parallel_tool_calls is not a boolean"
            )
        normalized["parallel_tool_calls"] = value
    if "response_format" in controls:
        normalized["response_format"] = _response_format(controls["response_format"])
    return normalized


def _tool_choice(value: Any, offered: set[str]) -> str | dict[str, Any]:
    """Normalize a recorded `tool_choice` to the chat-completions form.

    Raises:
        UnsupportedShapeError: The form is unknown or the named tool is not
            offered.
    """
    if isinstance(value, str) and value in _TOOL_CHOICE_MODES:
        return value
    name = None
    if isinstance(value, dict) and value.get("type") == "function":
        function = value.get("function")
        # The Responses API names the function beside `type`.
        name = function.get("name") if isinstance(function, dict) else value.get("name")
    if isinstance(name, str) and name in offered:
        return {"type": "function", "function": {"name": name}}
    raise UnsupportedShapeError(
        "unsupported_request_control",
        "tool_choice names a tool that is not offered"
        if isinstance(name, str)
        else "tool_choice has an unsupported form",
    )


def _response_format(value: Any) -> dict[str, Any]:
    """Check a recorded chat-completions `response_format`.

    Raises:
        UnsupportedShapeError: The format is not one both request shapes carry.
    """
    if isinstance(value, dict) and value.get("type") in _RESPONSE_FORMATS:
        schema = value.get("json_schema")
        if value["type"] != "json_schema" or (
            isinstance(schema, dict)
            and isinstance(schema.get("name"), str)
            and isinstance(schema.get("schema"), dict)
        ):
            return dict(value)
    raise UnsupportedShapeError(
        "unsupported_request_control", "response_format has an unsupported form"
    )


def _system_instructions(attributes: Any) -> str | None:
    """Find OTel GenAI system instructions kept with the node's attributes.

    Pydantic AI records instructions in `gen_ai.system_instructions` rather
    than in the messages, and importers keep the raw span attributes nested
    under their own key.
    """
    if not isinstance(attributes, dict):
        return None
    scopes = [attributes, *(v for v in attributes.values() if isinstance(v, dict))]
    for scope in scopes:
        value = scope.get("gen_ai.system_instructions")
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return value or None
            # Plain text that happens to parse as JSON stays plain text.
            if not isinstance(parsed, list | str):
                return value or None
            value = parsed
        if isinstance(value, str):
            return value or None
        if isinstance(value, list):
            texts = [
                str(part.get("content", ""))
                for part in value
                if isinstance(part, dict) and part.get("type") == "text"
            ]
            return "\n".join(texts) or None
    return None


def _split_inputs(inputs: Any) -> tuple[list[Any], Any]:
    """Return the message list and any tools recorded beside it."""
    if inputs is None:
        raise UnsupportedShapeError("missing_inputs", "the call has no inputs")
    if isinstance(inputs, str):
        try:
            inputs = json.loads(inputs)
        except json.JSONDecodeError as error:
            raise UnsupportedShapeError(
                "unsupported_input_format", "inputs are plain text"
            ) from error
    if isinstance(inputs, list):
        return inputs, None
    if isinstance(inputs, dict) and isinstance(inputs.get("messages"), list):
        if "system" in inputs:
            raise UnsupportedShapeError(
                "unsupported_input_format", "Anthropic messages are not supported"
            )
        return inputs["messages"], inputs.get("tools")
    raise UnsupportedShapeError(
        "unsupported_input_format", "inputs carry no message list"
    )


def _detect_format(messages: list[Any]) -> str:
    """Name the message shape, or raise when it is not recognized."""
    if not messages or not all(isinstance(message, dict) for message in messages):
        raise UnsupportedShapeError(
            "unsupported_input_format", "messages are not a list of objects"
        )
    if not all(message.get("role") in _CHAT_ROLES for message in messages):
        raise UnsupportedShapeError(
            "unsupported_input_format", "messages use unknown roles"
        )
    if any("parts" in message for message in messages):
        return "otel_parts"
    return "openai_chat"


def _convert_otel_messages(
    messages: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Convert OTel GenAI part messages and count dropped reasoning parts."""
    converted: list[dict[str, Any]] = []
    dropped = 0
    for message in messages:
        role = message["role"]
        if "parts" not in message:
            converted.extend(_convert_openai_messages([message]))
            continue
        texts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        tool_results: list[dict[str, Any]] = []
        parts = message["parts"]
        if not isinstance(parts, list):
            raise UnsupportedShapeError(
                "unsupported_input_format", "message parts are not a list"
            )
        for part in parts:
            kind = part.get("type") if isinstance(part, dict) else None
            if kind == "text":
                texts.append(str(part.get("content", "")))
            elif kind in {"thinking", "reasoning"}:
                dropped += 1
            elif kind == "tool_call":
                tool_calls.append(
                    _openai_tool_call(
                        part.get("id"), part.get("name"), part.get("arguments")
                    )
                )
            elif kind == "tool_call_response":
                result = part.get("result", part.get("response"))
                tool_results.append(
                    {
                        "role": "tool",
                        "tool_call_id": _require_id(part.get("id")),
                        "content": (
                            result if isinstance(result, str) else json.dumps(result)
                        ),
                    }
                )
            else:
                raise UnsupportedShapeError(
                    "non_text_content", f"unsupported message part: {kind}"
                )
        converted.extend(tool_results)
        if role == "assistant":
            if texts or tool_calls:
                entry: dict[str, Any] = {
                    "role": "assistant",
                    "content": "\n".join(texts) or None,
                }
                if tool_calls:
                    entry["tool_calls"] = tool_calls
                converted.append(entry)
        elif texts:
            if role == "tool":
                raise UnsupportedShapeError(
                    "missing_tool_call_id", "a tool message has no call id"
                )
            converted.append({"role": role, "content": "\n".join(texts)})
    return converted, dropped


def _convert_openai_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Copy OpenAI chat messages, keeping only fields a request accepts."""
    converted: list[dict[str, Any]] = []
    for message in messages:
        role = message["role"]
        entry: dict[str, Any] = {"role": role, "content": _text(message.get("content"))}
        if role == "assistant" and message.get("tool_calls"):
            entry["tool_calls"] = [
                _openai_tool_call(
                    call.get("id"),
                    _function(call).get("name"),
                    _function(call).get("arguments"),
                )
                for call in message["tool_calls"]
            ]
        if role == "tool":
            entry["tool_call_id"] = _require_id(message.get("tool_call_id"))
        converted.append(entry)
    return converted


def _function(call: Any) -> dict[str, Any]:
    """Return the `function` object of an OpenAI tool call, or an empty one."""
    function = call.get("function") if isinstance(call, dict) else None
    return function if isinstance(function, dict) else {}


def _text(content: Any) -> str | None:
    """Flatten message content to text, rejecting images and other media."""
    if content is None or isinstance(content, str):
        return content
    if isinstance(content, list):
        texts = []
        for part in content:
            if isinstance(part, str):
                texts.append(part)
            elif isinstance(part, dict) and part.get("type") in {"text", "input_text"}:
                texts.append(str(part.get("text", "")))
            else:
                kind = part.get("type") if isinstance(part, dict) else type(part)
                raise UnsupportedShapeError(
                    "non_text_content", f"unsupported content part: {kind}"
                )
        return "\n".join(texts)
    raise UnsupportedShapeError("non_text_content", "content is not text")


def _require_id(call_id: Any) -> str:
    """Return a tool-call id, shortening oversized ids consistently.

    Raises:
        UnsupportedShapeError: The recording has no id.
    """
    if call_id is None or call_id == "":
        raise UnsupportedShapeError(
            "missing_tool_call_id", "a tool call or result has no id"
        )
    value = str(call_id)
    return hashlib.sha256(value.encode()).hexdigest() if len(value) > 64 else value


def _openai_tool_call(call_id: Any, name: Any, arguments: Any) -> dict[str, Any]:
    """Build an OpenAI `tool_calls` entry with string arguments."""
    if not isinstance(name, str) or not name:
        raise UnsupportedShapeError(
            "unsupported_input_format", "a tool call has no name"
        )
    return {
        "id": _require_id(call_id),
        "type": "function",
        "function": {
            "name": name,
            "arguments": (
                arguments if isinstance(arguments, str) else json.dumps(arguments)
            ),
        },
    }


def parse_recorded_output(outputs: Any) -> RecordedOutcome:
    """Read the recorded answer from any supported output shape.

    A string is decoded only when it holds a recognized response envelope;
    any other string, including plain text that happens to be valid JSON
    such as `42` or a structured answer, is the answer's text.
    """
    if isinstance(outputs, str):
        try:
            decoded = json.loads(outputs)
        except json.JSONDecodeError:
            return RecordedOutcome(tool_calls=(), text=outputs)
        try:
            return _parse_envelope(decoded)
        except (UnsupportedShapeError, TypeError, AttributeError, KeyError):
            return RecordedOutcome(tool_calls=(), text=outputs)
    return _parse_envelope(outputs)


def _parse_envelope(outputs: Any) -> RecordedOutcome:
    """Read the recorded answer from a decoded response or message list.

    Raises:
        UnsupportedShapeError: The value is not a recognized answer shape.
    """
    if isinstance(outputs, dict) and isinstance(outputs.get("choices"), list):
        if not outputs["choices"]:
            raise UnsupportedShapeError(
                "unrecognized_recorded_output", "the output has no choices"
            )
        outputs = outputs["choices"][0].get("message")
    if isinstance(outputs, dict) and "parts" not in outputs and "role" in outputs:
        return _outcome_from_openai(outputs)
    if isinstance(outputs, dict) and "parts" in outputs:
        outputs = [outputs]
    if isinstance(outputs, list) and all(isinstance(item, dict) for item in outputs):
        tool_calls: list[ToolCall] = []
        texts: list[str] = []
        for message in outputs:
            if message.get("role") not in {None, "assistant"}:
                continue
            if "parts" in message:
                for part in message["parts"]:
                    if part.get("type") == "text":
                        texts.append(str(part.get("content", "")))
                    elif part.get("type") == "tool_call":
                        tool_calls.append(
                            ToolCall(
                                str(part.get("name")),
                                parse_arguments(part.get("arguments")),
                            )
                        )
            else:
                outcome = _outcome_from_openai(message)
                tool_calls.extend(outcome.tool_calls)
                if outcome.text:
                    texts.append(outcome.text)
        if tool_calls or texts:
            return RecordedOutcome(tuple(tool_calls), "\n".join(texts) or None)
    raise UnsupportedShapeError(
        "unrecognized_recorded_output", "the recorded answer could not be read"
    )


def _outcome_from_openai(message: Mapping[str, Any]) -> RecordedOutcome:
    """Read one OpenAI assistant message."""
    calls = tuple(
        ToolCall(
            str(_function(call).get("name")),
            parse_arguments(_function(call).get("arguments")),
        )
        for call in message.get("tool_calls") or ()
    )
    return RecordedOutcome(calls, _text(message.get("content")) or None)


def normalize_tool_definition(value: Any) -> dict[str, Any] | None:
    """Return a tool definition in nested OpenAI form, or None.

    A function declared with `type: function` but no parameters takes no
    arguments, so it gets an empty object schema.
    """
    if not isinstance(value, dict):
        return None
    function = value.get("function") if "function" in value else value
    if not isinstance(function, dict):
        return None
    name = function.get("name")
    parameters = function.get(
        "parameters",
        function.get("parameters_json_schema", function.get("input_schema")),
    )
    if parameters is None and value.get("type") == "function":
        parameters = _EMPTY_PARAMETERS
    if not isinstance(name, str) or not name or not isinstance(parameters, dict):
        return None
    strict = function.get("strict")
    return _definition(
        name,
        parameters,
        function.get("description"),
        strict=strict if isinstance(strict, bool) else None,
    )


def _recorded_tool_definitions(
    node: SessionNodeResponse, recorded_tools: Any
) -> list[dict[str, Any]]:
    """Collect tool definitions from the node, most specific source first.

    The first source that holds definitions sets them. Tools recorded in the
    request itself are checked too, since the request is what the model was
    offered: one missing from the chosen source is added, and its `strict`
    flag is kept.

    Raises:
        UnsupportedShapeError: An offered tool is not a function definition
            this analyzer can send, so replaying without it would narrow the
            model's choices.
    """
    sources: list[Any] = [
        node.metadata.get(TOOL_DEFINITIONS_METADATA_KEY),
        recorded_tools,
        (node.model_params or {}).get("tools"),
    ]
    definitions: dict[str, dict[str, Any]] = {}
    for source in sources:
        for definition in _read_definitions(source):
            definitions.setdefault(definition["function"]["name"], definition)
        if definitions:
            break
    for definition in _read_definitions(recorded_tools):
        function = definition["function"]
        known = definitions.setdefault(function["name"], definition)["function"]
        if "strict" in function and "strict" not in known:
            known["strict"] = function["strict"]
    return list(definitions.values())


def _read_definitions(source: Any) -> list[dict[str, Any]]:
    """Normalize every tool in one recorded list.

    Raises:
        UnsupportedShapeError: An item is not a function definition.
    """
    if not isinstance(source, Sequence) or isinstance(source, str):
        return []
    definitions = []
    for item in source:
        definition = normalize_tool_definition(item)
        if definition is None:
            kind = item.get("type") if isinstance(item, dict) else type(item).__name__
            raise UnsupportedShapeError(
                "unsupported_tool", f"an offered tool is not supported: {kind}"
            )
        definitions.append(definition)
    return definitions


def _definition(
    name: str,
    parameters: Mapping[str, Any],
    description: str | None,
    *,
    strict: bool | None = None,
) -> dict[str, Any]:
    """Build one nested OpenAI tool definition."""
    function: dict[str, Any] = {"name": name, "parameters": dict(parameters)}
    if description:
        function["description"] = description
    if strict is not None:
        function["strict"] = strict
    return {"type": "function", "function": function}


def _tool_names_used(
    messages: list[dict[str, Any]], recorded: RecordedOutcome
) -> set[str]:
    """Return the tools the history or the recorded answer called."""
    names = {call.name for call in recorded.tool_calls}
    for message in messages:
        for call in message.get("tool_calls", ()):
            names.add(call["function"]["name"])
    return names


def _infer_schema(
    name: str, messages: list[dict[str, Any]], recorded: RecordedOutcome
) -> dict[str, Any]:
    """Infer a loose object schema from the arguments a tool was called with."""
    samples = [call.arguments for call in recorded.tool_calls if call.name == name]
    for message in messages:
        for call in message.get("tool_calls", ()):
            if call["function"]["name"] == name:
                samples.append(parse_arguments(call["function"]["arguments"]))
    properties: dict[str, Any] = {}
    for sample in samples:
        if isinstance(sample, dict):
            for key, value in sample.items():
                properties.setdefault(key, {"type": _json_type(value)})
    return {"type": "object", "properties": properties}


def _json_type(value: Any) -> str:
    """Name the JSON schema type of a value."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    if value is None:
        return "null"
    return "string"
