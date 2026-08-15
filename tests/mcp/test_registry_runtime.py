#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Capability inventories, SDK schemas, and protocol result contracts."""

import json
import uuid
from datetime import UTC, datetime

from mcp.types import TextContent

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.mcp.errors import (
    MCPToolError,
    error_result,
    protocol_result,
    success_result,
)
from kitaru.mcp.models.common import RegistryReadResult, ToolResult
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings

EXPECTED = {
    CapabilityMode.READ_ONLY: [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_review_read",
    ],
    CapabilityMode.STANDARD: [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_review_read",
        "kitaru_cohorts_manage",
        "kitaru_experiments_manage",
        "kitaru_session_import",
        "kitaru_review_manage",
        "kitaru_workflow_start",
        "kitaru_evaluators_manage",
    ],
    CapabilityMode.DESTRUCTIVE: [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_review_read",
        "kitaru_cohorts_manage",
        "kitaru_experiments_manage",
        "kitaru_session_import",
        "kitaru_review_manage",
        "kitaru_workflow_start",
        "kitaru_evaluators_manage",
        "kitaru_workflow_cancel",
        "kitaru_delete",
    ],
}


async def test_exact_capability_filtered_inventories_and_annotations() -> None:
    for mode, names in EXPECTED.items():
        tools = await create_server(MCPSettings(mode=mode)).list_tools()
        assert [tool.name for tool in tools] == names
        for tool in tools:
            annotations = tool.annotations
            assert annotations is not None
            assert annotations.open_world_hint is True
            assert annotations.destructive_hint is (
                tool.name in {"kitaru_workflow_cancel", "kitaru_delete"}
            )
            assert annotations.read_only_hint is (
                tool.name
                in {
                    "kitaru_registry_read",
                    "kitaru_activity_read",
                    "kitaru_review_read",
                }
            )


async def test_actual_sdk_discovery_schemas_fit_budgets() -> None:
    tools = await create_server(
        MCPSettings(mode=CapabilityMode.DESTRUCTIVE)
    ).list_tools()
    dumped = [tool.model_dump(by_alias=True, exclude_none=True) for tool in tools]
    for tool in dumped:
        assert "request" in tool["inputSchema"]["properties"]
        request_schema = tool["inputSchema"]["properties"]["request"]
        assert "$ref" in request_schema or "oneOf" in request_schema
        combined = _compact_size(tool["inputSchema"]) + _compact_size(
            tool["outputSchema"]
        )
        assert combined < 33 * 1024
    assert _compact_size({"tools": dumped}) < 192 * 1024
    assert len(tools) <= 12


async def test_activity_child_schema_exposes_only_kind_specific_fields() -> None:
    tools = await create_server(MCPSettings()).list_tools()
    activity = next(tool for tool in tools if tool.name == "kitaru_activity_read")
    definitions = activity.input_schema["$defs"]

    session_fields = definitions["SessionNodesRequest"]["properties"]
    assert "include_payloads" in session_fields
    assert "sort" not in session_fields

    sorted_fields = definitions["SortedChildrenRequest"]["properties"]
    assert "sort" in sorted_fields
    assert "include_payloads" not in sorted_fields


async def test_registry_schema_exposes_only_supported_tag_and_worker_reads() -> None:
    tools = await create_server(MCPSettings()).list_tools()
    registry = next(tool for tool in tools if tool.name == "kitaru_registry_read")
    schema = json.dumps(registry.input_schema, sort_keys=True)

    assert '"tag"' in schema
    assert '"worker"' in schema
    assert '"get_worker"' in schema
    assert '"get_tag"' not in schema
    assert '"list_tag_links"' not in schema
    for unsupported in ("create_worker", "update_worker", "delete_worker"):
        assert f'"{unsupported}"' not in schema


async def test_capability_schemas_place_tag_mutations_by_risk() -> None:
    read_only = await create_server(
        MCPSettings(mode=CapabilityMode.READ_ONLY)
    ).list_tools()
    standard = await create_server(
        MCPSettings(mode=CapabilityMode.STANDARD)
    ).list_tools()
    destructive = await create_server(
        MCPSettings(mode=CapabilityMode.DESTRUCTIVE)
    ).list_tools()

    assert "kitaru_review_manage" not in {tool.name for tool in read_only}
    standard_by_name = {tool.name: tool for tool in standard}
    assert "kitaru_delete" not in standard_by_name
    review_schema = json.dumps(
        standard_by_name["kitaru_review_manage"].input_schema, sort_keys=True
    )
    for operation in ("create_tag", "update_tag", "link_tag"):
        assert f'"{operation}"' in review_schema

    destructive_by_name = {tool.name: tool for tool in destructive}
    delete_tool = destructive_by_name["kitaru_delete"]
    assert delete_tool.annotations is not None
    assert delete_tool.annotations.destructive_hint is True
    delete_schema = json.dumps(delete_tool.input_schema, sort_keys=True)
    assert '"tag"' in delete_schema
    assert '"tag_link"' in delete_schema


def test_success_and_error_structured_text_parity_and_redaction() -> None:
    now = datetime.now(UTC)
    agent = AgentResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name="agent",
        description=None,
        latest_version=1,
        created=now,
        updated=now,
    )
    success = protocol_result(success_result(RegistryReadResult, agent))
    assert isinstance(success.content[0], TextContent)
    assert json.loads(success.content[0].text) == success.structured_content
    assert success.is_error is False

    failure = protocol_result(
        error_result(
            RegistryReadResult,
            MCPToolError("conflict", "Bearer sensitive-token was rejected"),
        )
    )
    assert isinstance(failure.content[0], TextContent)
    assert json.loads(failure.content[0].text) == failure.structured_content
    assert failure.is_error is True
    assert "sensitive-token" not in failure.content[0].text


def test_nested_secret_containers_are_redacted_in_structured_and_text_results() -> None:
    secrets = {
        "integration": {
            "OPENAI_API_KEY": "leak-api-key",
            "oauth_access_token": "leak-access-token",
            "oauth_refresh_token": "leak-refresh-token",
            "session_token": "leak-token",
            "database_password": "leak-password",
            "signing_secret": "leak-secret",
            "client_secret": "leak-client-secret",
            "private_key": "leak-private-key",
            "secret_env": {"SAFE_NAME": "leak-container-value"},
            "diagnostic": "Bearer leak-bearer",
            "credential_hint": "KITKEY_leak-prefix",
        }
    }
    result = protocol_result(success_result(ToolResult, secrets))
    assert result.structured_content is not None
    serialized = json.dumps(result.structured_content, sort_keys=True)
    text = result.content[0]
    assert isinstance(text, TextContent)
    for leaked in (
        "leak-api-key",
        "leak-access-token",
        "leak-refresh-token",
        "leak-token",
        "leak-password",
        "leak-secret",
        "leak-client-secret",
        "leak-private-key",
        "leak-container-value",
        "leak-bearer",
        "leak-prefix",
    ):
        assert leaked not in serialized
        assert leaked not in text.text
    assert json.loads(text.text) == result.structured_content


def _compact_size(value: object) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
