#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Canonical MCP registry snapshots and schema budgets."""

import subprocess
import sys
from pathlib import Path


def test_committed_schema_report_matches_public_sdk_registry() -> None:
    """The checked-in discovery snapshots match the public SDK registry."""
    repository = Path(__file__).parents[2]
    result = subprocess.run(
        [sys.executable, "scripts/report_mcp_schema.py", "--check"],
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "read-only: 3 tools" in result.stdout
    assert "standard: 10 tools" in result.stdout
    assert "destructive: 12 tools" in result.stdout
