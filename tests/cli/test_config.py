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
"""Secure local config, contexts, and resolution precedence."""

import json
import os
import stat

import pytest

from kitaru.cli.app import main
from kitaru.cli.config import ConfigStore, resolve_target, validate_server_url
from kitaru.cli.diagnostics import _check_mode
from kitaru.cli.output import CLIError


def test_context_crud_preserves_credentials_and_active_guard(
    tmp_path, monkeypatch, capsys
) -> None:
    """Context writes normalize URLs and active removal requires force."""
    config_path = tmp_path / "kitaru" / "config.json"
    credential_path = tmp_path / "kitaru" / "credentials.json"
    monkeypatch.setenv("KITARU_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("KITARU_CREDENTIALS_PATH", str(credential_path))
    monkeypatch.delenv("KITARU_API_URL", raising=False)

    assert main(["context", "add", "Prod", "https://example.com/api/"]) == 0
    assert json.loads(capsys.readouterr().out)["item"]["server_url"] == (
        "https://example.com/api"
    )
    assert main(["context", "use", "Prod"]) == 0
    capsys.readouterr()

    assert main(["context", "remove", "Prod"]) == 5
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "conflict"

    credential_path.write_text('{"sentinel": {}}', encoding="utf-8")
    assert main(["context", "remove", "Prod", "--force"]) == 0
    capsys.readouterr()
    assert credential_path.read_text(encoding="utf-8") == '{"sentinel": {}}'
    assert ConfigStore(config_path).load().active_context is None

    if os.name == "posix":
        assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
        assert stat.S_IMODE(config_path.parent.stat().st_mode) == 0o700


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


def test_server_resolution_precedence_and_missing_named_context(
    tmp_path, monkeypatch
) -> None:
    """Explicit URL, context, environment, and active context resolve in order."""
    store = ConfigStore(tmp_path / "config.json")
    store.add_context("Prod", "https://active.example.com", activate=True)
    store.add_context("Other", "https://other.example.com")
    monkeypatch.setenv("KITARU_API_URL", "https://env.example.com/")

    assert (
        resolve_target(
            store, explicit_server="https://explicit.example.com/", context_name="Other"
        ).server_url
        == "https://explicit.example.com"
    )
    selected = resolve_target(store, context_name="Other")
    assert selected.server_url == "https://other.example.com"
    assert selected.source == "context"
    assert resolve_target(store).source == "environment"

    monkeypatch.delenv("KITARU_API_URL")
    active = resolve_target(store)
    assert active.server_url == "https://active.example.com"
    assert active.source == "active_context"

    with pytest.raises(CLIError, match="does not exist"):
        resolve_target(store, context_name="Missing")


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
