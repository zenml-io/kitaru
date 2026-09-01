#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Base-install behavior for the optional MCP console boundary."""

import subprocess
import sys

from kitaru import mcp_entrypoint


def test_base_imports_do_not_load_mcp() -> None:
    """Importing Kitaru, the client, and the launcher stays MCP-free."""
    script = (
        "import sys; import kitaru; import kitaru.client; "
        "import kitaru.mcp_entrypoint; "
        "assert not any(name == 'mcp' or name.startswith('mcp.') "
        "for name in sys.modules)"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_help_and_version_do_not_load_optional_dependency(capsys, monkeypatch) -> None:
    """Base-safe informational flags exit successfully before lazy import."""
    imported: list[str] = []

    def unexpected_import(name: str):
        imported.append(name)
        raise AssertionError(name)

    monkeypatch.setattr(mcp_entrypoint.importlib, "import_module", unexpected_import)
    monkeypatch.setattr(mcp_entrypoint, "_get_package_version", lambda: "1.2.3")

    assert mcp_entrypoint.main(["--help"]) == 0
    assert "usage: kitaru-mcp" in capsys.readouterr().out
    assert mcp_entrypoint.main(["--version"]) == 0
    assert capsys.readouterr().out == "1.2.3\n"
    assert imported == []


def test_missing_extra_has_actionable_error_without_traceback(
    capsys, monkeypatch
) -> None:
    """Starting without MCP exits two and prints only installation guidance."""

    def missing_mcp(name: str):
        assert name == "mcp.server"
        raise ModuleNotFoundError("No module named 'mcp'", name="mcp")

    monkeypatch.setattr(mcp_entrypoint.importlib, "import_module", missing_mcp)

    assert mcp_entrypoint.main([]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Install the Kitaru MCP server with pip install 'kitaru[mcp]'\n"
    )
    assert "Traceback" not in captured.err


def test_shared_redaction_does_not_load_cli_or_server_dependencies() -> None:
    script = """
import sys
import kitaru.redaction
import kitaru.mcp.redaction
for prefix in ("cyclopts", "rich", "kitaru.cli", "kitaru.server"):
    assert not any(
        name == prefix or name.startswith(prefix + ".") for name in sys.modules
    ), prefix
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
