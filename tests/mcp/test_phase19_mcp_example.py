"""Unit test for the MCP query tools example."""

from __future__ import annotations

from unittest.mock import patch

from examples.mcp.mcp_query_tools import collect_query_snapshot


def test_phase19_mcp_query_example_collects_expected_sections() -> None:
    """Example helper should aggregate status, stacks, and waiting executions."""
    with (
        patch(
            "kitaru.mcp.server.kitaru_status",
            return_value={"connection": "remote Kitaru server"},
        ),
        patch(
            "kitaru.mcp.server.kitaru_stacks_list",
            return_value=[{"name": "prod", "is_active": True}],
        ),
        patch(
            "kitaru.mcp.server.kitaru_executions_list",
            return_value=[{"exec_id": "kr-a8f3c2", "status": "waiting"}],
        ),
    ):
        snapshot = collect_query_snapshot()

    assert snapshot["status"]["connection"] == "remote Kitaru server"
    assert snapshot["stacks"][0]["name"] == "prod"
    assert snapshot["waiting_executions"][0]["exec_id"] == "kr-a8f3c2"
