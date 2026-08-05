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
"""Secure CLI-local config and environment-only target resolution."""

import json
import os
import stat

import pytest

from kitaru.cli.app import main
from kitaru.cli.config import ConfigStore, resolve_target, validate_server_url
from kitaru.cli.diagnostics import _check_mode
from kitaru.cli.output import CLIError


def test_context_configuration_is_removed_and_legacy_fields_are_dropped(
    tmp_path, monkeypatch, capsys
) -> None:
    """The CLI rejects context commands and never rewrites legacy targets."""
    config_path = tmp_path / "kitaru" / "config.json"
    monkeypatch.setenv("KITARU_CONFIG_PATH", str(config_path))
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "active_context": "Prod",
                "contexts": {"Prod": {"server_url": "https://example.com/api"}},
                "cli": {"machine_mode": False},
            }
        ),
        encoding="utf-8",
    )

    assert main(["context", "list"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "invalid_arguments"
    assert main(["version", "--context", "Prod"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "invalid_arguments"
    assert main(["config", "set", "cli.machine_mode", "true"]) == 0
    capsys.readouterr()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload == {"cli": {"machine_mode": True}, "schema_version": 1}

    if os.name == "posix":
        assert stat.S_IMODE(config_path.stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission behavior")
def test_explicit_config_override_preserves_existing_parent_mode(
    tmp_path, monkeypatch
) -> None:
    """Writing an override secures its file without taking over its parent."""
    parent = tmp_path / "shared"
    parent.mkdir(mode=0o755)
    os.chmod(parent, 0o755)
    config_path = parent / "config.json"
    monkeypatch.setenv("KITARU_CONFIG_PATH", str(config_path))

    store = ConfigStore()
    store.set_machine_mode(True)
    _check_mode(config_path, require_private_parent=store.manages_parent_directory)

    assert store.manages_parent_directory is False
    assert stat.S_IMODE(parent.stat().st_mode) == 0o755
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600


def test_config_is_allowlisted_and_malformed_state_is_refused(
    tmp_path, monkeypatch, capsys
) -> None:
    """Unknown preferences and malformed documents fail before mutation."""
    config_path = tmp_path / "config.json"
    monkeypatch.setenv("KITARU_CONFIG_PATH", str(config_path))

    assert main(["config", "set", "server_url", "true"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "invalid_arguments"
    assert not config_path.exists()

    config_path.write_text("{", encoding="utf-8")
    assert main(["config", "list"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "invalid_configuration"

    assert main(["config", "path"]) == 0
    path_result = json.loads(capsys.readouterr().out)
    assert path_result["item"] == {"path": str(config_path), "exists": True}


def test_server_resolution_uses_only_explicit_or_environment(monkeypatch) -> None:
    """An explicit URL wins, then the environment, with no persisted fallback."""
    monkeypatch.setenv("KITARU_API_URL", "https://env.example.com/")

    assert (
        resolve_target(explicit_server="https://explicit.example.com/").server_url
        == "https://explicit.example.com"
    )
    assert resolve_target().source == "environment"

    monkeypatch.delenv("KITARU_API_URL")
    with pytest.raises(CLIError, match="No Kitaru server was resolved"):
        resolve_target()


@pytest.mark.parametrize(
    "url",
    [
        "example.com",
        "ftp://example.com",
        "https://user@example.com",
        "https://example.com?secret=yes",
        "https://example.com/#fragment",
    ],
)
def test_invalid_server_urls_are_rejected(url: str) -> None:
    """Unsafe URL components are rejected before storage or requests."""
    with pytest.raises(CLIError):
        validate_server_url(url)
