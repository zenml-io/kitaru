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
from kitaru.mcp.models.common import RegistryReadResult
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings

EXPECTED = {
    CapabilityMode.READ_ONLY: [
        "kitaru_registry_read",
        "kitaru_activity_read",
    ],
    CapabilityMode.STANDARD: [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_cohorts_manage",
        "kitaru_experiments_manage",
        "kitaru_workflow_start",
    ],
    CapabilityMode.DESTRUCTIVE: [
        "kitaru_registry_read",
        "kitaru_activity_read",
        "kitaru_cohorts_manage",
        "kitaru_experiments_manage",
        "kitaru_workflow_start",
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
                tool.name in {"kitaru_registry_read", "kitaru_activity_read"}
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
        assert combined < 32 * 1024
    assert _compact_size({"tools": dumped}) < 192 * 1024
    assert len(tools) <= 12


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


def _compact_size(value: object) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
