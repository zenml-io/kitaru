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

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from kitaru.api_models.v1.session_node import NodeStatus, SessionNodeResponse

TOOL_DEFINITIONS_METADATA_KEY = "kitaru.tool_definitions"

SkipReason = Literal[
    "failed_recorded_call",
    "missing_inputs",
    "unsupported_input_format",
    "non_text_content",
    "unrecognized_recorded_output",
    "missing_tool_schema",
]
SchemaSource = Literal["recorded", "supplied", "inferred", "none"]

_CHAT_ROLES = frozenset({"system", "developer", "user", "assistant", "tool"})


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
    except UnsupportedShapeError as error:
        return Skipped(error.reason, str(error))

    definitions: dict[str, dict[str, Any]] = {}
    schema_source: SchemaSource = "none"
    if use_recorded_tools:
        for definition in _recorded_tool_definitions(node, recorded_tools):
            definitions.setdefault(definition["function"]["name"], definition)
        if definitions:
            schema_source = "recorded"
    used = _tool_names_used(messages, recorded)
    if used or definitions:
        # A model that may call tools must see every tool the agent offered,
        # so supplied schemas fill in whatever the trace did not record.
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
    return ReplayRequest(
        messages=messages,
        tools=list(definitions.values()),
        schema_source=schema_source,
        source_format=source_format,
        recorded=recorded,
        dropped_reasoning_parts=dropped,
    )


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
        for part in message["parts"]:
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
                        "tool_call_id": str(part.get("id")),
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
            entry["tool_call_id"] = str(message.get("tool_call_id"))
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


def _openai_tool_call(call_id: Any, name: Any, arguments: Any) -> dict[str, Any]:
    """Build an OpenAI `tool_calls` entry with string arguments."""
    if not isinstance(name, str) or not name:
        raise UnsupportedShapeError(
            "unsupported_input_format", "a tool call has no name"
        )
    return {
        "id": str(call_id),
        "type": "function",
        "function": {
            "name": name,
            "arguments": (
                arguments if isinstance(arguments, str) else json.dumps(arguments)
            ),
        },
    }


def parse_recorded_output(outputs: Any) -> RecordedOutcome:
    """Read the recorded answer from any supported output shape."""
    if isinstance(outputs, str):
        try:
            outputs = json.loads(outputs)
        except json.JSONDecodeError:
            return RecordedOutcome(tool_calls=(), text=outputs)
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
    """Return a tool definition in nested OpenAI form, or None."""
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
    if not isinstance(name, str) or not isinstance(parameters, dict):
        return None
    return _definition(name, parameters, function.get("description"))


def _recorded_tool_definitions(
    node: SessionNodeResponse, recorded_tools: Any
) -> list[dict[str, Any]]:
    """Collect tool definitions from the node, most specific source first."""
    sources: list[Any] = [
        node.metadata.get(TOOL_DEFINITIONS_METADATA_KEY),
        recorded_tools,
        (node.model_params or {}).get("tools"),
    ]
    definitions: list[dict[str, Any]] = []
    for source in sources:
        if isinstance(source, Sequence) and not isinstance(source, str):
            for item in source:
                definition = normalize_tool_definition(item)
                if definition is not None:
                    definitions.append(definition)
        if definitions:
            return definitions
    return []


def _definition(
    name: str, parameters: Mapping[str, Any], description: str | None
) -> dict[str, Any]:
    """Build one nested OpenAI tool definition."""
    function: dict[str, Any] = {"name": name, "parameters": dict(parameters)}
    if description:
        function["description"] = description
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
