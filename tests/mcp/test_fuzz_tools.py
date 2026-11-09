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
import uuid
from datetime import datetime
from functools import cache
from typing import Any

import pytest
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis import strategies as st
from hypothesis_jsonschema import from_schema
from mcp.server.mcpserver.exceptions import ToolError, UnexpectedToolError
from mcp.types import CallToolResult, TextContent
from mcp_fakes import EchoClient, NullClient, build_server_context
from pydantic import TypeAdapter, ValidationError

from kitaru.mcp.registry import TOOL_SPECS, ToolSpec, get_tool_specs
from kitaru.mcp.settings import CapabilityMode

_CODE = re.compile(r"^[a-z_]+$")
_MARKER = re.compile(r"(KITKEY_|Bearer )FUZZ[0-9a-f]{8}")

# `hypothesis_jsonschema` knows the standard formats but not Pydantic's `uuid`.
_FORMATS: dict[str, st.SearchStrategy[str]] = {"uuid": st.uuids().map(str)}


@cache
def server_context_for(
    mode: CapabilityMode, client: type[NullClient] = NullClient
) -> tuple[Any, Any]:
    """Return one reusable server and request context for a capability mode.

    Building a server costs about 30ms, so rebuilding it inside every generated
    example dominated the runtime of these properties. Neither fake stores
    anything and the handlers never write to the server, so one instance of
    each serves every example.
    """
    return build_server_context(client(), mode=mode)


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
        # `internal_error` is the catch-all for an unhandled exception, so treating
        # it as a normal envelope would green-light the crashes this test hunts for.
        assert body["error"]["code"] != "internal_error", body
    content = result.content[0]
    assert isinstance(content, TextContent), content
    assert json.loads(content.text) == body
    assert not _MARKER.search(content.text), "secret marker leaked"
    return body


def _free_json_schema(depth: int = 2) -> dict[str, Any]:
    """Bound arbitrary JSON containers at every level, including extra fields."""
    scalar = {"type": ["string", "number", "boolean", "null"], "maxLength": 80}
    if not depth:
        return scalar
    child = _free_json_schema(depth - 1)
    return {
        "anyOf": [
            scalar,
            {"type": "array", "items": child, "maxItems": 5},
            {
                "type": "object",
                "additionalProperties": child,
                "propertyNames": {"maxLength": 80},
                "maxProperties": 5,
            },
        ]
    }


def _bound_free_json(schema: dict[str, Any]) -> dict[str, Any]:
    # zenml-io/zenml-internal#139: keep arbitrary JSON shallow until fixed.
    # Preserve declared object types and public array bounds.
    if not set(schema) - {"title", "description", "default"}:
        return {**schema, **_free_json_schema()}
    result = dict(schema)
    for key in ("properties", "$defs"):
        if key in result:
            result[key] = {
                name: _bound_free_json(value) if isinstance(value, dict) else value
                for name, value in result[key].items()
            }
    for key in ("anyOf", "oneOf", "allOf"):
        if key in result:
            result[key] = [_bound_free_json(value) for value in result[key]]
    if isinstance(result.get("items"), dict):
        result["items"] = _bound_free_json(result["items"])
    additional = result.get("additionalProperties")
    if additional is True or additional == {}:
        result["additionalProperties"] = _free_json_schema()
        result["maxProperties"] = min(result.get("maxProperties", 5), 5)
    elif isinstance(additional, dict):
        result["additionalProperties"] = _bound_free_json(additional)
    return result


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


# `hypothesis_jsonschema` cannot follow a self-referencing `$ref`, so the boolean
# filter recursion is unrolled into this many levels of depth-indexed `$defs`
# copies. Two levels reach a boolean filter nested inside another one.
_RECURSION_DEPTH = 2


def _retarget_refs(node: Any, cyclic: set[str], level: int) -> Any:
    """Point every `$ref` to a cyclic `$defs` entry at that entry's level copy.

    Past the last level such a reference is dropped from the unions that offer
    it, which terminates the unrolled recursion.
    """
    if isinstance(node, list):
        return [_retarget_refs(item, cyclic, level) for item in node]
    if not isinstance(node, dict):
        return node
    name = _ref_name(node)
    if name in cyclic:
        assert level < _RECURSION_DEPTH, f"required reference to {name}"
        return {**node, "$ref": f"#/$defs/{name}__{level}"}
    retargeted: dict[str, Any] = {}
    for key, value in node.items():
        if key == "anyOf" and level >= _RECURSION_DEPTH:
            value = [option for option in value if _ref_name(option) not in cyclic]
            assert value, "every union branch pruned away"
        retargeted[key] = _retarget_refs(value, cyclic, level)
    return retargeted


def _unroll_recursion(schema: dict[str, Any]) -> dict[str, Any]:
    """Replace self-referencing `$defs` with a bounded chain of copies."""
    defs = schema.get("$defs", {})
    cyclic = _self_referencing_defs(defs)
    if not cyclic:
        return schema
    kept = {
        name: _retarget_refs(body, cyclic, 0)
        for name, body in defs.items()
        if name not in cyclic
    }
    for level in range(_RECURSION_DEPTH):
        for name in cyclic:
            kept[f"{name}__{level}"] = _retarget_refs(defs[name], cyclic, level + 1)
    body = _retarget_refs(
        {key: value for key, value in schema.items() if key != "$defs"}, cyclic, 0
    )
    return {**body, "$defs": kept}


def _is_model_valid(name: str, request: object) -> bool:
    # A request model's JSON schema cannot express its cross-field validators, so
    # drop the payloads the model itself would reject before the SDK sees them.
    try:
        request_adapter_for(name).validate_python(request)
    except ValidationError:
        return False
    return True


def _require_discriminator_tags(schema: dict[str, Any]) -> dict[str, Any]:
    """Require union tags even when their model fields have defaults."""
    defs = schema.get("$defs", {})

    def visit(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                visit(item)
        elif isinstance(node, dict):
            discriminator = node.get("discriminator", {})
            tag = discriminator.get("propertyName")
            for ref in discriminator.get("mapping", {}).values():
                definition = defs[ref.removeprefix("#/$defs/")]
                definition["required"] = sorted(
                    set(definition.get("required", [])) | {tag}
                )
            for value in node.values():
                visit(value)

    visit(schema)
    return schema


def _from_schema(
    schema: dict[str, Any], defs: dict[str, Any]
) -> st.SearchStrategy[Any]:
    return from_schema({**schema, "$defs": defs}, custom_formats=_FORMATS)


def _without_fields(schema: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    # False schemas also prevent free-form extra fields from reintroducing keys.
    return {
        **schema,
        "properties": {
            name: False if name in fields else value
            for name, value in schema["properties"].items()
        },
        "required": [name for name in schema.get("required", []) if name not in fields],
    }


def _non_null(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        **schema,
        "anyOf": [branch for branch in schema["anyOf"] if branch.get("type") != "null"],
    }


def _update_strategy(
    schema: dict[str, Any], defs: dict[str, Any]
) -> st.SearchStrategy[dict[str, Any]]:
    """Draw sparse updates with at least one consistent set or clear operation."""
    properties = schema["properties"]
    fields = sorted(set(properties) - set(schema["required"]))
    values = [name for name in fields if not name.startswith("clear_")]
    base = _from_schema(_without_fields(schema, set(fields)), defs)
    setters = {name: _from_schema(_non_null(properties[name]), defs) for name in values}

    @st.composite
    def updates(draw: st.DrawFn) -> dict[str, Any]:
        request = draw(base)
        changed = draw(st.sampled_from(values))
        for name in values:
            clear = f"clear_{name}"
            states = ["set", "clear"] if clear in properties else ["set"]
            state = draw(
                st.sampled_from(states + (["omit"] if name != changed else []))
            )
            if state == "set":
                request[name] = draw(setters[name])
            if state == "clear":
                request[clear] = True
                if draw(st.booleans()):
                    request[name] = None
            elif clear in properties and draw(st.booleans()):
                request[clear] = False
        return request

    return updates()


def _import_query_strategy(
    schema: dict[str, Any], defs: dict[str, Any]
) -> st.SearchStrategy[dict[str, Any]]:
    """Draw optional trace selection and an ordered, timezone-aware window."""
    base = _from_schema(_without_fields(schema, {"since", "until"}), defs)
    timestamp = _from_schema(_non_null(schema["properties"]["since"]), defs)

    @st.composite
    def queries(draw: st.DrawFn) -> dict[str, Any]:
        query = draw(base)
        if query.get("trace_ids") is None or draw(st.booleans()):
            query["since"] = draw(timestamp)
        elif draw(st.booleans()):
            query["since"] = None
        if draw(st.booleans()):
            query["until"] = draw(st.none() | timestamp)
        if (
            query.get("since") is not None
            and query.get("until") is not None
            and datetime.fromisoformat(query["until"])
            < datetime.fromisoformat(query["since"])
        ):
            query["since"], query["until"] = query["until"], query["since"]
        return query

    return queries()


def _session_import_strategy(
    schema: dict[str, Any], defs: dict[str, Any]
) -> st.SearchStrategy[dict[str, Any]]:
    base = _from_schema(_without_fields(schema, {"source"}), defs)
    api = _from_schema(_without_fields(defs["ApiImportSource"], {"query"}), defs)
    query = _import_query_strategy(defs["ImportQuery"], defs)
    blob = _from_schema(defs["BlobImportSource"], defs)

    @st.composite
    def imports(draw: st.DrawFn) -> dict[str, Any]:
        request = draw(base)
        request["source"] = (
            {**draw(api), "query": draw(query)} if draw(st.booleans()) else draw(blob)
        )
        return request

    return imports()


# Other model-only constraints, such as unique memberships and pinned plugin
# requirements, still use the validation filter. Keep the health check scoped.
_SCHEMA_SETTINGS = settings(
    deadline=None, suppress_health_check=[HealthCheck.filter_too_much]
)


@cache
def _schema_strategy(name: str) -> st.SearchStrategy[Any]:
    schema = _unroll_recursion(
        _bound_free_json(
            _require_discriminator_tags(request_adapter_for(name).json_schema())
        )
    )
    defs = schema.get("$defs", {})
    # PlainStr rejects these controls in model validation, not in its JSON schema.
    for model, field in (
        ("EvaluatorConfig", "evaluator"),
        ("AnalyzerConfig", "analyzer"),
    ):
        if model in defs:
            defs[model]["properties"][field]["pattern"] = (
                r"^[^\x00-\x08\x0b\x0c\x0e-\x1f]*$"
            )
    if name == "kitaru_session_import":
        strategy = _session_import_strategy(schema, defs)
    else:
        branches = schema.get("anyOf", [schema])
        strategies = []
        for branch in branches:
            definition = defs.get(_ref_name(branch), branch)
            properties = definition.get("properties", {})
            if (
                any(key.startswith("clear_") for key in properties)
                or definition.get("title") == "ConnectionUpdate"
            ):
                strategies.append(_update_strategy(definition, defs))
            else:
                strategies.append(_from_schema(branch, defs))
        strategy = st.one_of(strategies)
    return strategy.filter(lambda request: _is_model_valid(name, request))


@pytest.mark.mcp_fuzz
@pytest.mark.parametrize("spec", TOOL_SPECS, ids=lambda s: s.name)
@given(data=st.data())
@_SCHEMA_SETTINGS
def test_schema_valid_request_yields_envelope(
    spec: ToolSpec, data: st.DataObject
) -> None:
    request = data.draw(_schema_strategy(spec.name))
    request_adapter_for(spec.name).validate_python(request)
    server, context = server_context_for(CapabilityMode.DESTRUCTIVE)
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
        except ToolError:
            continue  # The SDK refuses unlisted tools by raising; that is also a pass.
        assert result.is_error, spec.name


def _marker() -> st.SearchStrategy[str]:
    return st.builds(
        lambda prefix, hexes: f"{prefix}FUZZ{hexes}",
        st.sampled_from(["KITKEY_", "Bearer "]),
        st.text(alphabet="0123456789abcdef", min_size=8, max_size=8),
    )


@st.composite
def _with_marker_in_string_field(
    draw: st.DrawFn, name: str, request: dict[str, Any]
) -> dict[str, Any]:
    """Put a secret-shaped marker into one free-form string field of a request."""
    marker = draw(_marker())
    # Most string fields are UUIDs or literals, so try each one and keep only the
    # substitutions the model still accepts: the request must stay schema-valid.
    keys = [
        key
        for key, value in request.items()
        if isinstance(value, str) and _is_model_valid(name, {**request, key: marker})
    ]
    if not keys:
        return request
    return {**request, draw(st.sampled_from(keys)): marker}


@st.composite
def _broken_request(draw: st.DrawFn, request: dict[str, Any]) -> dict[str, Any]:
    """Break schema validity in one of four ways."""
    kind = draw(
        st.sampled_from(["extra_key", "wrong_type", "bad_uuid", "bad_discriminator"])
    )
    broken = dict(request)
    if kind == "extra_key":
        broken[draw(st.text(min_size=1, max_size=12))] = draw(st.integers())
    elif kind == "wrong_type" and broken:
        key = draw(st.sampled_from(sorted(broken)))
        broken[key] = [broken[key]]
    elif kind == "bad_uuid":
        for key, value in broken.items():
            if isinstance(value, str) and len(value) == 36 and value.count("-") == 4:
                broken[key] = "not-a-uuid"
                break
    elif kind == "bad_discriminator" and "operation" in broken:
        broken["operation"] = "definitely_not_an_operation"
    return broken


@pytest.mark.mcp_fuzz
@pytest.mark.parametrize("spec", TOOL_SPECS, ids=lambda s: s.name)
@given(data=st.data())
@_SCHEMA_SETTINGS
def test_schema_valid_request_with_marker_never_leaks(
    spec: ToolSpec, data: st.DataObject
) -> None:
    request = data.draw(
        _with_marker_in_string_field(spec.name, data.draw(_schema_strategy(spec.name)))
    )
    request_adapter_for(spec.name).validate_python(request)
    server, context = server_context_for(CapabilityMode.DESTRUCTIVE, EchoClient)
    assert_envelope(call(server, context, spec.name, request))


_INTERNAL = "zenml-io/zenml-internal#139"


def _is_validation_rejection(exc: BaseException) -> bool:
    """Return whether every failure in `exc` is a tool call rejected at validation."""
    # Hypothesis raises an ExceptionGroup when one run finds several distinct bugs.
    if isinstance(exc, BaseExceptionGroup):
        return all(_is_validation_rejection(inner) for inner in exc.exceptions)
    return isinstance(exc, ToolError) and isinstance(exc.__cause__, ValidationError)


# Pin the known failures so a different exception, such as a crash, fails the run.
_REJECTED_AT_VALIDATION = pytest.RaisesExc(Exception, check=_is_validation_rejection)
_CRASHED_ON_RECURSION = pytest.RaisesExc(
    UnexpectedToolError, check=lambda exc: isinstance(exc.__cause__, RecursionError)
)


@pytest.mark.xfail(strict=True, raises=_REJECTED_AT_VALIDATION, reason=_INTERNAL)
@pytest.mark.mcp_fuzz
@pytest.mark.parametrize("spec", TOOL_SPECS, ids=lambda s: s.name)
@given(data=st.data())
@settings(
    deadline=None,
    # This failure is tracked already; retain generation and shrinking without
    # spending CI or nightly time explaining the known validation exception.
    phases=tuple(
        phase
        for phase in settings().phases
        if phase != Phase.explain
        or settings.get_current_profile_name() not in {"ci", "nightly"}
    ),
)
def test_schema_invalid_request_never_raises(
    spec: ToolSpec, data: st.DataObject
) -> None:
    """The SDK must turn validation failures into an error result, not an exception."""
    request = data.draw(_broken_request(data.draw(_schema_strategy(spec.name))))
    server, context = server_context_for(CapabilityMode.DESTRUCTIVE)
    result = call(server, context, spec.name, request)
    assert result.is_error or result.structured_content is not None


@pytest.mark.xfail(strict=True, raises=_REJECTED_AT_VALIDATION, reason=_INTERNAL)
def test_invalid_request_response_is_enveloped() -> None:
    server, context = server_context_for(CapabilityMode.READ_ONLY)
    request = {
        "operation": "list",
        "kind": "session",
        "agent_id": "Bearer FUZZdeadbeef",
    }
    assert_envelope(call(server, context, "kitaru_activity_read", request))


@pytest.mark.xfail(strict=True, raises=_CRASHED_ON_RECURSION, reason=_INTERNAL)
def test_deep_free_form_value_is_enveloped() -> None:
    value: dict[str, Any] = {}
    cursor = value
    # Twice the interpreter's default recursion limit: deep enough to be
    # interesting, cheap enough to build on every run.
    for _ in range(2_000):
        cursor["k"] = {}
        cursor = cursor["k"]
    request = {
        "operation": "create_annotation",
        "session_id": str(uuid.uuid4()),
        "value": value,
    }
    server, context = server_context_for(CapabilityMode.STANDARD)
    assert_envelope(call(server, context, "kitaru_review_manage", request))


_LEAK_MARKER = "KITKEY_FUZZdeadbeef"


def test_marker_in_an_echoed_name_comes_back_masked() -> None:
    """A stored value that a tool reads back must be masked on the way out."""
    server, context = server_context_for(CapabilityMode.STANDARD, EchoClient)
    body = assert_envelope(
        call(
            server,
            context,
            "kitaru_review_manage",
            {"operation": "create_tag", "name": _LEAK_MARKER},
        )
    )
    assert body["ok"] is True, body
    assert body["data"]["name"] == "KITKEY_***", body


def test_marker_in_an_echoed_annotation_value_comes_back_masked() -> None:
    """A marker buried in free-form JSON must be masked in the rendered payload."""
    server, context = server_context_for(CapabilityMode.STANDARD, EchoClient)
    body = assert_envelope(
        call(
            server,
            context,
            "kitaru_review_manage",
            {
                "operation": "create_annotation",
                "session_id": str(uuid.uuid4()),
                "value": {"note": f"use {_LEAK_MARKER} now"},
            },
        )
    )
    assert body["ok"] is True, body
    assert body["data"]["value"] == {"note": "use KITKEY_*** now"}, body


def test_boolean_filter_reaches_the_sdk() -> None:
    """A boolean `filter` is a valid request, not a server-side fault."""
    server, context = server_context_for(CapabilityMode.READ_ONLY)
    request = {
        "kind": "worker",
        "operation": "list",
        "filter": {"and": [{"field": "name", "op": "eq", "value": "a"}]},
    }
    assert_envelope(call(server, context, "kitaru_registry_read", request))
