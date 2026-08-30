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
"""Property tests for the MCP tool boundary."""

import asyncio
import json
import re
import typing
from functools import cache
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis_jsonschema import from_schema
from mcp.types import CallToolResult, TextContent
from mcp_fakes import NullClient, build_server_context
from pydantic import TypeAdapter, ValidationError

from kitaru.mcp.registry import TOOL_SPECS, ToolSpec, get_tool_specs
from kitaru.mcp.settings import CapabilityMode

_CODE = re.compile(r"^[a-z_]+$")
_MARKER = re.compile(r"(KITKEY_|Bearer )FUZZ[0-9a-f]{8}")

# `hypothesis_jsonschema` knows the standard formats but not Pydantic's `uuid`.
_FORMATS: dict[str, st.SearchStrategy[str]] = {"uuid": st.uuids().map(str)}


@cache
def request_adapter_for(name: str) -> TypeAdapter[Any]:
    """Return a validator for the request payload a named tool accepts."""
    spec = next(candidate for candidate in TOOL_SPECS if candidate.name == name)
    return TypeAdapter(typing.get_type_hints(spec.handler)["request"])


def call(server: Any, context: Any, name: str, request: object) -> CallToolResult:
    """Invoke one tool through the full SDK path from a sync test body."""
    return asyncio.run(server.call_tool(name, {"request": request}, context))


def assert_envelope(result: CallToolResult) -> dict[str, Any]:
    """Assert a Kitaru result envelope and return it."""
    assert result.structured_content is not None, result
    body = result.structured_content
    assert body.get("schema_version") == "1", body
    assert isinstance(body.get("ok"), bool), body
    if not body["ok"]:
        assert _CODE.match(body["error"]["code"]), body["error"]
    content = result.content[0]
    assert isinstance(content, TextContent), content
    assert json.loads(content.text) == body
    assert not _MARKER.search(content.text), "secret marker leaked"
    return body


def _bound_free_json(schema: dict[str, Any]) -> dict[str, Any]:
    # zenml-io/zenml-internal#139: `_check_finite` recurses without a depth cap;
    # keep free-form JSON shallow.
    for name, prop in schema.get("properties", {}).items():
        if prop == {} or prop.get("title") in {"Value", "Metadata", "Params"}:
            schema["properties"][name] = {
                "type": ["object", "array", "string", "number", "boolean", "null"],
                "maxProperties": 5,
                "maxItems": 5,
            }
    for definition in schema.get("$defs", {}).values():
        _bound_free_json(definition)
    return schema


def _ref_name(node: object) -> str:
    """Return the `$defs` name a node references, or an empty string."""
    if not isinstance(node, dict):
        return ""
    ref = node.get("$ref")
    return ref.removeprefix("#/$defs/") if isinstance(ref, str) else ""


def _self_referencing_defs(defs: dict[str, Any]) -> set[str]:
    """Return the `$defs` names that can reach themselves through `$ref`."""

    def direct(node: object) -> set[str]:
        if isinstance(node, dict):
            found = {_ref_name(node)} - {""}
            return found | {ref for value in node.values() for ref in direct(value)}
        if isinstance(node, list):
            return {ref for item in node for ref in direct(item)}
        return set()

    edges = {name: direct(body) for name, body in defs.items()}
    reachable = {name: set(targets) for name, targets in edges.items()}
    changed = True
    while changed:
        changed = False
        for name, targets in reachable.items():
            grown = targets | {step for hop in targets for step in edges.get(hop, ())}
            if grown != targets:
                reachable[name] = grown
                changed = True
    return {name for name, targets in reachable.items() if name in targets}


def _unroll(node: Any, defs: dict[str, Any], cyclic: set[str], depth: int) -> Any:
    """Inline self-referencing `$defs` up to a fixed nesting depth."""
    if isinstance(node, list):
        return [_unroll(item, defs, cyclic, depth) for item in node]
    if not isinstance(node, dict):
        return node
    name = _ref_name(node)
    if name in cyclic:
        assert depth > 0, f"unbounded recursion through {name}"
        return _unroll(defs[name], defs, cyclic, depth - 1)
    unrolled: dict[str, Any] = {}
    for key, value in node.items():
        if key == "anyOf" and depth <= 0:
            value = [option for option in value if _ref_name(option) not in cyclic]
            assert value, "every recursive branch pruned away"
        unrolled[key] = _unroll(value, defs, cyclic, depth)
    return unrolled


def _bound_recursion(schema: dict[str, Any], depth: int = 2) -> dict[str, Any]:
    # `hypothesis_jsonschema` cannot resolve self-referencing `$defs` (the nested
    # boolean filter tree), so expand them to a fixed depth before generating.
    defs = schema.get("$defs", {})
    cyclic = _self_referencing_defs(defs)
    if not cyclic:
        return schema
    bounded = _unroll(
        {key: value for key, value in schema.items() if key != "$defs"},
        defs,
        cyclic,
        depth,
    )
    kept = {name: body for name, body in defs.items() if name not in cyclic}
    return {**bounded, "$defs": _unroll(kept, defs, cyclic, depth)}


def _is_model_valid(name: str, request: object) -> bool:
    # A request model's JSON schema cannot express its cross-field validators, so
    # drop the payloads the model itself would reject before the SDK sees them.
    try:
        request_adapter_for(name).validate_python(request)
    except ValidationError:
        return False
    return True


@cache
def _schema_strategy(name: str) -> st.SearchStrategy[Any]:
    schema = _bound_recursion(_bound_free_json(request_adapter_for(name).json_schema()))
    return from_schema(schema, custom_formats=_FORMATS).filter(
        lambda request: _is_model_valid(name, request)
    )


@pytest.mark.parametrize("spec", TOOL_SPECS, ids=lambda s: s.name)
@given(data=st.data())
@settings(deadline=None)
def test_schema_valid_request_yields_envelope(
    spec: ToolSpec, data: st.DataObject
) -> None:
    request = data.draw(_schema_strategy(spec.name))
    server, context = build_server_context(
        NullClient(), mode=CapabilityMode.DESTRUCTIVE
    )
    assert_envelope(call(server, context, spec.name, request))


@pytest.mark.parametrize("mode", [CapabilityMode.READ_ONLY, CapabilityMode.STANDARD])
def test_gated_tools_are_unlisted_and_uncallable(mode: CapabilityMode) -> None:
    server, context = build_server_context(NullClient(), mode=mode)
    allowed = {spec.name for spec in get_tool_specs(mode)}
    listed = {tool.name for tool in asyncio.run(server.list_tools())}
    assert listed == allowed
    for spec in TOOL_SPECS:
        if spec.name in allowed:
            continue
        try:
            result = call(server, context, spec.name, {})
        except Exception:
            continue  # The SDK refuses unknown tools by raising; that is also a pass.
        assert result.is_error, spec.name
