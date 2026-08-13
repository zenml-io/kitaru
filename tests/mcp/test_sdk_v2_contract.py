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
"""Executable findings for the supported MCP SDK v2 public contract."""

from importlib.metadata import requires, version
from typing import Annotated, Literal, cast

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version
from pydantic import BaseModel, ConfigDict, Field

MCP_SDK_RANGE = SpecifierSet(">=2.0,<3")


class ReadInput(BaseModel):
    """First operation used to probe generated discriminated schemas."""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["read"]
    value: int


class ListInput(BaseModel):
    """Second operation used to probe generated discriminated schemas."""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["list"]
    name: str


ProbeInput = Annotated[ReadInput | ListInput, Field(discriminator="operation")]


class ProbeOutput(BaseModel):
    """Typed structured output used to keep an SDK output schema."""

    model_config = ConfigDict(extra="forbid")

    ok: bool


async def _probe_tool(request: ProbeInput, context: Context) -> ProbeOutput:
    """Return explicit canonical protocol content behind a typed annotation."""
    del request, context
    result = CallToolResult(
        content=[TextContent(type="text", text='{"ok":true}')],
        structured_content={"ok": True},
    )
    return cast(ProbeOutput, result)


async def test_supported_sdk_public_registry_and_schema_contract() -> None:
    """Guard required discovery behavior for the installed compatible v2 SDK."""
    requirement = next(
        Requirement(item)
        for item in requires("kitaru") or []
        if Requirement(item).name == "mcp"
    )
    assert requirement.specifier == MCP_SDK_RANGE
    assert Version(version("mcp")) in requirement.specifier
    server = MCPServer("schema-probe")
    server.add_tool(
        _probe_tool,
        name="probe",
        structured_output=True,
        annotations=ToolAnnotations(
            read_only_hint=True,
            destructive_hint=False,
            idempotent_hint=True,
            open_world_hint=True,
        ),
    )

    tools = await server.list_tools()

    assert len(tools) == 1
    tool = tools[0].model_dump(by_alias=True, exclude_none=True)
    assert tool["name"] == "probe"
    annotations = tool["annotations"]
    assert annotations["readOnlyHint"] is True
    assert annotations["destructiveHint"] is False
    assert annotations["idempotentHint"] is True
    assert annotations["openWorldHint"] is True
    assert "operation" not in tool["inputSchema"]["properties"]
    request_schema = tool["inputSchema"]["properties"]["request"]
    assert request_schema["discriminator"]["propertyName"] == "operation"
    assert tool["outputSchema"] == ProbeOutput.model_json_schema()


def test_supported_sdk_exposes_public_lifecycle_and_result_seams() -> None:
    """Guard the public APIs Item 3 relies on before building its runtime."""
    assert callable(MCPServer.run_stdio_async)
    assert callable(MCPServer.call_tool)

    result = CallToolResult(
        content=[TextContent(type="text", text='{"ok":true}')],
        structured_content={"ok": True},
        is_error=False,
    )
    dumped = result.model_dump(by_alias=True, exclude_none=True)
    assert dumped["content"] == [{"type": "text", "text": '{"ok":true}'}]
    assert dumped["structuredContent"] == {"ok": True}
    assert dumped["isError"] is False
    assert dumped["resultType"] == "complete"
