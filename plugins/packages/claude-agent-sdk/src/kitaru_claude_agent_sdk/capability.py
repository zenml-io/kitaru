#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Public capability errors for the Claude Agent SDK adapter."""

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from claude_agent_sdk import ResultMessage, SdkMcpTool


class KitaruRecordingError(RuntimeError):
    """Report a Kitaru failure after Claude execution succeeded."""

    def __init__(
        self,
        *,
        terminal_message: ResultMessage | None,
        session_id: uuid.UUID | None,
        phase: str,
    ) -> None:
        self.terminal_message = terminal_message
        self.session_id = session_id
        self.phase = phase
        self.retry_safe = False
        self.side_effects_possible = True
        super().__init__(
            f"Kitaru recording failed during {phase} after Claude execution; "
            "automatic retry is unsafe because model or tool side effects may "
            "already have occurred."
        )


class UnsupportedReplayError(ValueError):
    """Reject a replay request that the public Claude boundary cannot enforce."""


class ToolPolicyError(RuntimeError):
    """Reject a tool replay policy that cannot be applied safely."""


class ToolPolicyMissError(ToolPolicyError):
    """Report a fail-closed static or history lookup miss."""


@dataclass(frozen=True)
class ReplayableSdkMcpServer:
    """Immutable definition used to materialize one fresh server per query."""

    name: str
    version: str
    tools: tuple[SdkMcpTool[Any], ...]


def replayable_sdk_mcp_server(
    *,
    name: str,
    tools: Iterable[SdkMcpTool[Any]],
    version: str = "1.0.0",
) -> ReplayableSdkMcpServer:
    """Define public SDK MCP tools that Kitaru may replay."""
    return ReplayableSdkMcpServer(name=name, version=version, tools=tuple(tools))


__all__ = [
    "KitaruRecordingError",
    "ReplayableSdkMcpServer",
    "ToolPolicyError",
    "ToolPolicyMissError",
    "UnsupportedReplayError",
    "replayable_sdk_mcp_server",
]
