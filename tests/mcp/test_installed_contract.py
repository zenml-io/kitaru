#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Installed console contracts for the optional MCP server."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_built_wheel_base_and_mcp_contracts() -> None:
    """One wheel preserves base isolation and supports real MCP stdio."""
    artifact = os.getenv("KITARU_TEST_WHEEL")
    if artifact is None:
        pytest.skip("set KITARU_TEST_WHEEL to a wheel or artifact directory")
    repository = Path(__file__).parents[2]
    result = subprocess.run(
        [sys.executable, "scripts/smoke_mcp_wheel.py", artifact],
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MCP wheel smoke passed" in result.stdout


def test_development_console_serves_every_mode_over_stdio() -> None:
    """The source-installed `kitaru-mcp` console speaks MCP over real stdio."""
    scripts = Path(sys.executable).parent
    console = scripts / ("kitaru-mcp.exe" if os.name == "nt" else "kitaru-mcp")
    assert console.exists(), f"missing console script: {console}"
    repository = Path(__file__).parents[2]
    result = subprocess.run(
        [sys.executable, "scripts/probe_mcp_wheel.py", str(console)],
        cwd=repository,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "read-only, standard, and destructive" in result.stdout
