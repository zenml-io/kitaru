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
"""Tests for the on-disk client configuration."""

import json
from pathlib import Path

import httpx
import pytest

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.auth import RenewingTokenAuth, StaticTokenAuth
from kitaru.client.client import KitaruClient
from kitaru.client.config import (
    DEFAULT_SERVER_URL,
    ENV_CONFIG_PATH,
    ClientConfig,
    get_active_server_url,
    load_config,
    set_active_server_url,
)

pytestmark = pytest.mark.usefixtures("isolated_config_directory")


@pytest.fixture(autouse=True)
def clean_client_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear the environment variables client construction reads."""
    for name in (
        ENV_CONFIG_PATH,
        "KITARU_API_URL",
        "KITARU_API_KEY",
        "KITARU_TASK_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)


def test_round_trip(tmp_path: Path) -> None:
    """Write the active server URL and read it back normalized."""
    set_active_server_url("https://kitaru.example.com/")

    assert (tmp_path / "config" / "kitaru" / "config.json").exists()
    assert get_active_server_url() == "https://kitaru.example.com"


def test_missing_file_reads_as_empty() -> None:
    """Read an empty configuration when the file does not exist."""
    assert load_config() == ClientConfig()


def test_malformed_file_is_ignored(tmp_path: Path) -> None:
    """Read an empty configuration when the file cannot be parsed."""
    path = tmp_path / "config" / "kitaru" / "config.json"
    path.parent.mkdir(parents=True)
    path.write_text("not-json", encoding="utf-8")

    assert load_config() == ClientConfig()


def test_environment_overrides_the_config_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read and write the file the environment names."""
    override = tmp_path / "elsewhere.json"
    monkeypatch.setenv(ENV_CONFIG_PATH, str(override))

    set_active_server_url("https://kitaru.example.com")

    assert override.exists()
    assert get_active_server_url() == "https://kitaru.example.com"


def test_clearing_the_active_server(tmp_path: Path) -> None:
    """Drop the active server URL from the file when cleared."""
    set_active_server_url("https://kitaru.example.com")
    set_active_server_url(None)

    path = tmp_path / "config" / "kitaru" / "config.json"
    assert get_active_server_url() is None
    assert json.loads(path.read_text(encoding="utf-8")) == {}


def test_from_config_prefers_the_environment_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the server URL the environment names over the stored one."""
    monkeypatch.setenv("KITARU_API_URL", "http://environment")
    set_active_server_url("http://stored")

    client = KitaruAPIClient.from_config()

    assert client._http.base_url == httpx.URL("http://environment")


def test_from_config_uses_the_active_server() -> None:
    """Use the stored active server when the environment names none."""
    set_active_server_url("http://stored")

    client = KitaruAPIClient.from_config()

    assert client._http.base_url == httpx.URL("http://stored")


def test_from_config_falls_back_to_the_local_default() -> None:
    """Use the local default when nothing is configured."""
    client = KitaruAPIClient.from_config()

    assert client._http.base_url == httpx.URL(DEFAULT_SERVER_URL)


def test_from_config_prefers_the_environment_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authenticate with the credential the environment names."""
    monkeypatch.setenv("KITARU_API_KEY", "kitaru-key")

    client = KitaruAPIClient.from_config()

    assert isinstance(client._auth, StaticTokenAuth)


def test_from_config_falls_back_to_stored_credentials() -> None:
    """Authenticate through the credential store without an environment credential."""
    client = KitaruAPIClient.from_config()

    assert isinstance(client._auth, RenewingTokenAuth)


def test_kitaru_client_defaults_to_the_resolved_server() -> None:
    """Build the default API client from the stored configuration."""
    set_active_server_url("http://stored")

    client = KitaruClient()

    assert client._api_client._http.base_url == httpx.URL("http://stored")
