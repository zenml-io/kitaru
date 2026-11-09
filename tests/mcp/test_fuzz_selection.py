"""Verify split nightly MCP jobs preserve selection and example depth."""

import os
import shlex
import subprocess
import sys
from pathlib import Path

from hypothesis import settings

REPO_ROOT = Path(__file__).parents[2]


def _get_recipe_command(name: str) -> list[str]:
    lines = (REPO_ROOT / "Justfile").read_text().splitlines()
    start = lines.index(f"{name}:")
    return shlex.split(lines[start + 1].strip())


def _collect_recipe(name: str) -> set[str]:
    command = _get_recipe_command(name)
    assert "HYPOTHESIS_PROFILE=nightly" in command
    arguments = [
        argument
        for argument in command[command.index("pytest") + 1 :]
        if argument not in {"-v", "-vv", "-s"}
    ]
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "PYTEST_ADDOPTS",
            "KITARU_TEST_SHARD_INDEX",
            "KITARU_TEST_SHARD_COUNT",
        }
    }
    env["HYPOTHESIS_PROFILE"] = "nightly"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            *arguments,
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    node_ids = {
        line.strip()
        for line in result.stdout.splitlines()
        if line.startswith("tests/") and "::" in line and " " not in line
    }
    assert node_ids, result.stdout + result.stderr
    return node_ids


def test_mcp_nightly_jobs_partition_the_full_selection() -> None:
    combined = _collect_recipe("fuzz-mcp")
    boundary = _collect_recipe("fuzz-mcp-boundary")
    redaction = _collect_recipe("fuzz-mcp-redaction")

    assert boundary.isdisjoint(redaction)
    assert boundary | redaction == combined
    assert any(
        "test_schema_valid_request_yields_envelope[" in node for node in boundary
    )
    assert any("test_schema_invalid_request_never_raises[" in node for node in boundary)
    assert any(
        "test_schema_valid_request_with_marker_never_leaks[" in node
        for node in redaction
    )
    assert any("test_redaction_properties.py::" in node for node in redaction)
    assert settings.get_profile("nightly").max_examples == 2_000
