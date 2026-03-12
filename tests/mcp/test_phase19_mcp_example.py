"""Unit test for the Phase 19 MCP query example."""

from __future__ import annotations

from examples.mcp.mcp_query_tools import collect_query_snapshot


def test_phase19_mcp_query_example_collects_expected_sections() -> None:
    """Example helper should aggregate status, runners, and waiting executions."""
    snapshot = collect_query_snapshot(
        status_tool=lambda: {"connection": "remote Kitaru server"},
        runners_tool=lambda: [{"name": "prod", "is_active": True}],
        executions_tool=lambda **_: [{"exec_id": "kr-a8f3c2", "status": "waiting"}],
    )

    assert snapshot["status"]["connection"] == "remote Kitaru server"
    assert snapshot["runners"][0]["name"] == "prod"
    assert snapshot["waiting_executions"][0]["exec_id"] == "kr-a8f3c2"
