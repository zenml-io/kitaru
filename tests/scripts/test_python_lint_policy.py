"""Verify the actual Ruff policy from supported invocation directories."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("directory", "config", "filename", "requires_annotations"),
    [
        (".", "pyproject.toml", "src/kitaru/probe.py", True),
        (".", "pyproject.toml", "tests/probe.py", False),
        (
            ".",
            "plugins/pyproject.toml",
            "plugins/packages/example/src/example/probe.py",
            True,
        ),
        (".", "plugins/pyproject.toml", "plugins/tests/probe.py", False),
        ("plugins", "pyproject.toml", "packages/example/src/example/probe.py", True),
        ("plugins", "pyproject.toml", "tests/probe.py", False),
    ],
)
def test_signature_policy_respects_invocation_directory(
    directory: str, config: str, filename: str, requires_annotations: bool
) -> None:
    """Require production signatures without treating fixture arguments as debt."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--config",
            config,
            "--stdin-filename",
            filename,
            "--output-format",
            "json",
            "-",
        ],
        input='''"""Example module."""
def qualityprobe(value):
    """Return the input."""
    return value
''',
        cwd=ROOT / directory,
        capture_output=True,
        text=True,
        check=False,
    )
    codes = {diagnostic["code"] for diagnostic in json.loads(result.stdout)}
    if requires_annotations:
        assert codes == {"ANN001", "ANN201"}
        assert result.returncode == 1
    else:
        assert codes == set()
        assert result.returncode == 0


@pytest.mark.parametrize("config", ["pyproject.toml", "plugins/pyproject.toml"])
def test_postponed_annotations_are_rejected(config: str) -> None:
    """Catch the written runtime policy through the configured banned API."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--config",
            str(ROOT / config),
            "--stdin-filename",
            str(ROOT / "scripts/probe.py"),
            "--select",
            "TID251",
            "--output-format",
            "json",
            "-",
        ],
        input="from __future__ import annotations\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    diagnostics = json.loads(result.stdout)
    assert [diagnostic["code"] for diagnostic in diagnostics] == ["TID251"]
    assert "runtime model inspection" in diagnostics[0]["message"]
