"""Query Kitaru state through MCP tool functions.

This example demonstrates the structured queries the kitaru-mcp server exposes:
- ``kitaru_status``
- ``kitaru_stacks_list``
- ``kitaru_executions_list``
"""

from __future__ import annotations

import json
from typing import Any


def collect_query_snapshot() -> dict[str, Any]:
    """Collect a query snapshot using MCP tool call semantics."""
    from kitaru.mcp.server import (
        kitaru_executions_list,
        kitaru_stacks_list,
        kitaru_status,
    )

    return {
        "status": kitaru_status(),
        "stacks": kitaru_stacks_list(),
        "waiting_executions": kitaru_executions_list(status="waiting", limit=5),
    }


def main() -> None:
    """Run the MCP query snapshot example as a script."""
    snapshot = collect_query_snapshot()
    print(json.dumps(snapshot, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
