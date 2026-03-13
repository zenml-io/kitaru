"""Phase 19 example: query Kitaru state through MCP tool functions.

This example demonstrates the same structured query payloads that the
`kitaru-mcp` server exposes:
- `kitaru_status`
- `kitaru_stacks_list`
- `kitaru_executions_list`
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any


def collect_query_snapshot(
    *,
    status_tool: Callable[[], dict[str, Any]] | None = None,
    stacks_tool: Callable[[], list[dict[str, Any]]] | None = None,
    executions_tool: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Collect a query snapshot using MCP tool call semantics.

    Args:
        status_tool: Optional override for status querying.
        stacks_tool: Optional override for stack listing.
        executions_tool: Optional override for execution listing.

    Returns:
        Structured snapshot data for status, stacks, and waiting executions.
    """
    if status_tool is None or stacks_tool is None or executions_tool is None:
        from kitaru.mcp.server import (
            kitaru_executions_list,
            kitaru_stacks_list,
            kitaru_status,
        )

        status_tool = status_tool or kitaru_status
        stacks_tool = stacks_tool or kitaru_stacks_list
        executions_tool = executions_tool or kitaru_executions_list

    return {
        "status": status_tool(),
        "stacks": stacks_tool(),
        "waiting_executions": executions_tool(status="waiting", limit=5),
    }


def main() -> None:
    """Run the MCP query snapshot example as a script."""
    snapshot = collect_query_snapshot()
    print(json.dumps(snapshot, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
