"""Public compatibility contracts for the Claude Agent SDK adapter."""

from dataclasses import fields, is_dataclass
from importlib.metadata import version
from typing import Any

import claude_agent_sdk
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    HookMatcher,
    ResultMessage,
    SdkMcpTool,
    SystemMessage,
    UserMessage,
    create_sdk_mcp_server,
    query,
)
from packaging.version import Version

from kitaru_claude_agent_sdk import ADAPTER_VERSION


def _field_names(value: Any) -> set[str]:
    return {field.name for field in fields(value)}


def test_adapter_version_matches_distribution() -> None:
    assert version("kitaru-claude-agent-sdk") == ADAPTER_VERSION


def test_supports_claude_agent_sdk_02_line() -> None:
    installed = Version(version("claude-agent-sdk"))

    assert installed >= Version("0.2.149")
    assert installed < Version("0.3")


def test_required_sdk_contract_is_publicly_exported() -> None:
    exports = {
        "query": query,
        "ClaudeAgentOptions": ClaudeAgentOptions,
        "AssistantMessage": AssistantMessage,
        "UserMessage": UserMessage,
        "SystemMessage": SystemMessage,
        "ResultMessage": ResultMessage,
        "HookMatcher": HookMatcher,
        "SdkMcpTool": SdkMcpTool,
        "create_sdk_mcp_server": create_sdk_mcp_server,
    }

    assert all(
        getattr(claude_agent_sdk, name) is value for name, value in exports.items()
    )
    assert all("._internal" not in value.__module__ for value in exports.values())


def test_typed_messages_expose_required_public_fields() -> None:
    assert is_dataclass(AssistantMessage)
    assert {"content", "parent_tool_use_id", "message_id", "session_id"} <= (
        _field_names(AssistantMessage)
    )
    assert is_dataclass(UserMessage)
    assert {"content", "uuid", "parent_tool_use_id"} <= _field_names(UserMessage)
    assert is_dataclass(SystemMessage)
    assert {"subtype", "data"} <= _field_names(SystemMessage)
    assert is_dataclass(ResultMessage)
    assert {"is_error", "session_id", "usage", "result"} <= _field_names(ResultMessage)


def test_options_expose_required_public_fields() -> None:
    assert is_dataclass(ClaudeAgentOptions)
    assert {
        "allowed_tools",
        "can_use_tool",
        "continue_conversation",
        "disallowed_tools",
        "fork_session",
        "hooks",
        "mcp_servers",
        "model",
        "permission_mode",
        "resume",
        "system_prompt",
    } <= _field_names(ClaudeAgentOptions)


def test_hook_matcher_exposes_composable_public_fields() -> None:
    matcher = HookMatcher(matcher="mcp__support__.*", hooks=[], timeout=1.0)

    assert matcher.matcher == "mcp__support__.*"
    assert matcher.hooks == []
    assert matcher.timeout == 1.0


async def test_sdk_mcp_tool_preserves_public_handler_contract() -> None:
    async def handler(arguments: dict[str, object]) -> dict[str, object]:
        return {"content": [{"type": "text", "text": str(arguments["query"])}]}

    tool = SdkMcpTool(
        name="lookup",
        description="Look up a support answer.",
        input_schema={"type": "object"},
        handler=handler,
    )

    assert is_dataclass(SdkMcpTool)
    assert {"name", "description", "input_schema", "handler", "annotations"} <= (
        _field_names(SdkMcpTool)
    )
    assert tool.handler is handler
    assert await tool.handler({"query": "refund"}) == {
        "content": [{"type": "text", "text": "refund"}]
    }


def test_public_mcp_factory_accepts_sdk_tools() -> None:
    async def handler(_: dict[str, object]) -> dict[str, object]:
        return {"content": [{"type": "text", "text": "ok"}]}

    server = create_sdk_mcp_server(
        name="support",
        version="1.0.0",
        tools=[
            SdkMcpTool(
                name="lookup",
                description="Look up a support answer.",
                input_schema={"type": "object"},
                handler=handler,
            )
        ],
    )

    assert server["type"] == "sdk"
    assert server["name"] == "support"
