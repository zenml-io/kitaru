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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Metadata stored on each imported node, including model-call tool definitions."""

from typing import Any

from kitaru.api_models.v1.session_node import NodeType
from kitaru_langfuse_importer.records import (
    decode_dict,
    decode_json,
    get_first,
    get_metadata_value,
)

TOOL_DEFINITIONS_METADATA_KEY = "kitaru.tool_definitions"


REQUEST_PARAMETERS_METADATA_KEY = "langfuse.model_request_parameters"


# Tool lists are already normalized under TOOL_DEFINITIONS_METADATA_KEY.
_REQUEST_PARAMETER_TOOL_LISTS = frozenset({"function_tools", "output_tools"})


_METADATA_KEYS = {
    "agent_name",
    "deployment.environment.name",
    "environment",
    "gen_ai.agent.name",
    "gen_ai.operation.name",
    "gen_ai.provider.name",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "gen_ai.system",
    "name",
    "sdk_span_type",
    "service.name",
    "service.version",
}


_RESOURCE_METADATA_KEYS = {
    "deployment.environment.name",
    "service.name",
    "service.version",
}


_TRACE_CONTEXT_FIELDS = {
    "traceEnvironment": "environment",
    "traceHtmlPath": "html_path",
    "traceRelease": "release",
    "traceTags": "tags",
    "traceVersion": "version",
}


def _source_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Return bounded metadata useful for debugging and filtering."""
    metadata = decode_dict(record.get("metadata"))
    attributes = decode_dict(metadata.get("attributes"))
    resource_attributes = decode_dict(metadata.get("resourceAttributes"))
    selected: dict[str, Any] = {}
    for source in (metadata, attributes):
        for key in _METADATA_KEYS:
            if key in source:
                selected[f"langfuse.{key}"] = source[key]
            flattened_key = f"attributes.{key}"
            if flattened_key in metadata:
                selected[f"langfuse.{key}"] = metadata[flattened_key]
    for key in _RESOURCE_METADATA_KEYS:
        if key in resource_attributes:
            selected[f"langfuse.{key}"] = resource_attributes[key]
        flattened_key = f"resourceAttributes.{key}"
        if flattened_key in metadata:
            selected[f"langfuse.{key}"] = metadata[flattened_key]
    for source_field, target_field in _TRACE_CONTEXT_FIELDS.items():
        if source_field in record:
            selected[f"langfuse.trace.{target_field}"] = record[source_field]
    for source_field, target_field in (
        ("environment", "environment"),
        ("promptName", "prompt_name"),
        ("promptVersion", "prompt_version"),
        ("version", "version"),
    ):
        if source_field in record and record[source_field] not in (None, ""):
            selected[f"langfuse.{target_field}"] = record[source_field]
    return selected


def get_node_metadata(record: dict[str, Any], node_type: NodeType) -> dict[str, Any]:
    """Return source metadata plus normalized tool definitions for model calls."""
    metadata = _source_metadata(record)
    if node_type is NodeType.LLM_CALL:
        definitions = _get_tool_definitions(record)
        if definitions:
            metadata[TOOL_DEFINITIONS_METADATA_KEY] = definitions
        if request_parameters := _get_request_parameters(record):
            metadata[REQUEST_PARAMETERS_METADATA_KEY] = request_parameters
    return metadata


def _get_request_parameters(record: dict[str, Any]) -> dict[str, Any]:
    """Return PydanticAI's `model_request_parameters` without its tool lists.

    Fields such as `allow_text_output` decide request controls the provider
    received (PydanticAI sends `tool_choice: required` when text output is
    not allowed) but never appear in the generation input.
    """
    request_parameters = decode_dict(
        get_metadata_value(record, "model_request_parameters")
    )
    return {
        key: value
        for key, value in request_parameters.items()
        if key not in _REQUEST_PARAMETER_TOOL_LISTS
    }


def _tool_definition(value: Any) -> dict[str, Any] | None:
    """Normalize one tool definition to the OpenAI chat-completions form."""
    value = decode_json(value)
    # Anthropic marks its client tools `custom` and keeps the schema under
    # `input_schema`; both forms describe a callable function.
    if not isinstance(value, dict) or value.get("type") not in (
        None,
        "function",
        "custom",
    ):
        return None
    nested = value.get("function")
    function = nested if isinstance(nested, dict) else value
    name = function.get("name")
    parameters = decode_json(
        get_first(function, "parameters", "parameters_json_schema", "input_schema")
    )
    # OpenAI lets a function that takes no arguments omit its parameters.
    if parameters is None and value.get("type") == "function":
        parameters = {"type": "object", "properties": {}}
    # Keep only definitions that carry a real schema; a bare name cannot be
    # replayed against another model.
    if not isinstance(name, str) or not name or not isinstance(parameters, dict):
        return None
    normalized: dict[str, Any] = {"name": name}
    description = function.get("description")
    if isinstance(description, str) and description:
        normalized["description"] = description
    normalized["parameters"] = parameters
    # Strict schemas constrain the arguments the model may send, so a replay
    # without the flag would pose a looser task.
    if isinstance(function.get("strict"), bool):
        normalized["strict"] = function["strict"]
    return {"type": "function", "function": normalized}


def _tool_definitions(values: Any) -> list[dict[str, Any]]:
    """Normalize a list of tool definitions, dropping entries without schemas."""
    values = decode_json(values)
    if not isinstance(values, list):
        return []
    return [
        definition
        for value in values
        if (definition := _tool_definition(value)) is not None
    ]


def _get_tool_definitions(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the tool definitions offered to the model in one generation."""
    request_input = decode_json(record.get("input"))
    if isinstance(request_input, dict):
        definitions = _tool_definitions(request_input.get("tools"))
        if definitions:
            return definitions
    request_parameters = decode_dict(
        get_metadata_value(record, "model_request_parameters")
    )
    definitions = [
        *_tool_definitions(request_parameters.get("function_tools")),
        *_tool_definitions(request_parameters.get("output_tools")),
    ]
    if definitions:
        return definitions
    return _tool_definitions(get_metadata_value(record, "gen_ai.tool.definitions"))
