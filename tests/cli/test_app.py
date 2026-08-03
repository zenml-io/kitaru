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
"""CLI bootstrap, invocation, and lazy-extra behavior."""

import builtins
import json
from pathlib import Path

import kitaru.cli
from kitaru.cli import app as app_module


def test_help_version_schema_and_scaffold_skip_bootstrap(
    monkeypatch, capsys, tmp_path: Path
) -> None:
    """Offline bootstrap commands do not construct the local config store."""

    def fail_config_store():
        raise AssertionError("offline command read local config")

    monkeypatch.setattr(app_module, "ConfigStore", fail_config_store)

    assert app_module.main(["--help"]) == 0
    help_output = capsys.readouterr()
    assert "Connect to, inspect, and configure Kitaru" in help_output.out

    assert app_module.main(["version"]) == 0
    version_output = capsys.readouterr()
    payload = json.loads(version_output.out)
    assert payload["command"] == "version"
    assert payload["item"]["version"]
    assert version_output.err == ""

    assert app_module.main(["schema", "config"]) == 0
    schema_output = capsys.readouterr()
    schema_payload = json.loads(schema_output.out)
    assert schema_payload["command"] == "schema"
    assert {item["command"] for item in schema_payload["items"]} == {
        "config.get",
        "config.list",
        "config.path",
        "config.set",
    }

    scaffold_path = tmp_path / "importer.py"
    assert (
        app_module.main(
            [
                "importer",
                "scaffold",
                "demo",
                "--path",
                str(scaffold_path),
            ]
        )
        == 0
    )
    scaffold_output = capsys.readouterr()
    assert json.loads(scaffold_output.out)["command"] == "importer.scaffold"
    assert scaffold_path.is_file()


def test_non_finite_request_timeout_is_rejected(capsys) -> None:
    """Global request timeouts must remain finite positive bounds."""
    for value in ("nan", "inf"):
        assert app_module.main(["version", "--request-timeout", value]) == 2
        captured = capsys.readouterr()
        assert captured.out == ""
        payload = json.loads(captured.err)
        assert payload["error"]["kind"] == "invalid_arguments"


def test_parse_error_is_structured_on_stderr(capsys) -> None:
    """Argument failures never corrupt structured stdout."""
    assert app_module.main(["not-a-command"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["ok"] is False
    assert payload["error"]["kind"] == "invalid_arguments"


def test_nested_asset_parse_error_reports_full_command(capsys) -> None:
    """Early argument failures retain the complete nested command path."""
    assert app_module.main(["importer", "version", "get"]) == 2
    captured = capsys.readouterr()
    assert json.loads(captured.err)["command"] == "importer.version.get"


def test_output_context_resets_between_invocations(capsys) -> None:
    """An explicit text invocation does not leak into the next call."""
    assert app_module.main(["version", "--output", "text"]) == 0
    first = capsys.readouterr()
    assert first.out.startswith("version: ")

    assert app_module.main(["version"]) == 0
    second = capsys.readouterr()
    assert json.loads(second.out)["command"] == "version"


def test_lazy_entry_point_reports_missing_cli_extra(monkeypatch, capsys) -> None:
    """The package entry point turns an optional import failure into a hint."""
    real_import = builtins.__import__

    def missing_cli_extra(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "kitaru.cli.app":
            raise ModuleNotFoundError("No module named 'cyclopts'", name="cyclopts")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_cli_extra)
    assert kitaru.cli.main(["--help"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "kitaru[cli]" in captured.err
