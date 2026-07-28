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
"""Tests for the on-disk credential store."""

import stat
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from kitaru.client.credential_store import (
    DIRECTORY_MODE,
    ENV_DISABLE_CREDENTIALS_CACHE,
    FILE_MODE,
    CredentialStore,
    get_config_directory,
    normalize_server_url,
)
from kitaru.client.credentials import ApiToken, CredentialType

SERVER_URL = "https://kitaru.example.com"


def _expired_token(age: timedelta) -> ApiToken:
    """Build a token whose expiry lies in the past by the given age.

    Args:
        age: How far in the past the token expired.

    Returns:
        Token expired by the given age.
    """
    return ApiToken(
        access_token="stale-token",
        expires_at=datetime.now(UTC) - age,
        leeway_seconds=0,
    )


def test_round_trip(tmp_path: Path) -> None:
    """Read back an API key, a device authorization, and a token."""
    path = tmp_path / "credentials.json"
    store = CredentialStore(path=path)
    device_id = uuid.uuid4()
    token = ApiToken(access_token="session-token", leeway_seconds=30)

    store.set_api_key(SERVER_URL, "kitaru-key", type=CredentialType.CONTROL_PLANE)
    store.set_device(SERVER_URL, device_id, "device-code")
    store.set_token(SERVER_URL, token)

    reloaded = CredentialStore(path=path)
    credentials = reloaded.get(SERVER_URL)
    assert credentials is not None
    assert credentials.api_key == "kitaru-key"
    assert credentials.type == CredentialType.CONTROL_PLANE
    assert credentials.device_id == device_id
    assert credentials.device_code == "device-code"
    assert credentials.api_token == token


def test_control_plane_api_url_survives_a_reload(tmp_path: Path) -> None:
    """Persist which control plane a server delegates to alongside its token."""
    path = tmp_path / "credentials.json"
    store = CredentialStore(path=path)

    store.set_token(
        SERVER_URL,
        ApiToken(access_token="session-token", leeway_seconds=30),
        control_plane_api_url="https://control-plane.example.com/",
    )

    credentials = CredentialStore(path=path).get(SERVER_URL)
    assert credentials is not None
    assert credentials.control_plane_api_url == "https://control-plane.example.com"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes only")
def test_file_and_directory_modes(tmp_path: Path) -> None:
    """Create the credentials file and its directory with owner-only modes."""
    path = tmp_path / "nested" / "credentials.json"
    store = CredentialStore(path=path)
    store.set_api_key(SERVER_URL, "kitaru-key")

    assert stat.S_IMODE(path.stat().st_mode) == FILE_MODE
    assert stat.S_IMODE(path.parent.stat().st_mode) == DIRECTORY_MODE


def test_second_instance_picks_up_change(tmp_path: Path) -> None:
    """Reload another instance's write once the file's mtime moves."""
    path = tmp_path / "credentials.json"
    writer = CredentialStore(path=path)
    writer.set_api_key(SERVER_URL, "first-key")

    reader = CredentialStore(path=path)
    first = reader.get(SERVER_URL)
    assert first is not None
    assert first.api_key == "first-key"

    time.sleep(0.1)
    writer.set_api_key(SERVER_URL, "second-key")
    second = reader.get(SERVER_URL)
    assert second is not None
    assert second.api_key == "second-key"


def test_malformed_file_is_ignored(tmp_path: Path) -> None:
    """Ignore a credentials file that is not valid JSON."""
    path = tmp_path / "credentials.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json", encoding="utf-8")

    store = CredentialStore(path=path)

    assert store.list() == []


def test_malformed_entry_is_ignored(tmp_path: Path) -> None:
    """Ignore one malformed entry while keeping the rest of the file."""
    path = tmp_path / "credentials.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"https://good.example.com": {"url": "https://good.example.com", '
        '"api_key": "good-key"}, '
        '"https://bad.example.com": {"url": "https://bad.example.com", '
        '"api_key": 123}}',
        encoding="utf-8",
    )

    store = CredentialStore(path=path)

    assert store.get("https://bad.example.com") is None
    good = store.get("https://good.example.com")
    assert good is not None
    assert good.api_key == "good-key"


def test_get_token_hides_expired_unless_allowed(tmp_path: Path) -> None:
    """Hide an expired token by default and return it with allow_expired."""
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_api_key(SERVER_URL, "kitaru-key")
    store.set_token(SERVER_URL, _expired_token(timedelta(minutes=1)))

    assert store.get_token(SERVER_URL) is None
    assert store.get_token(SERVER_URL, allow_expired=True) is not None


def test_api_token_expired_respects_leeway() -> None:
    """Treat a token as expired once it enters its leeway window."""
    fresh = ApiToken(
        access_token="token",
        expires_at=datetime.now(UTC) + timedelta(seconds=10),
        leeway_seconds=1,
    )
    inside_leeway = ApiToken(
        access_token="token",
        expires_at=datetime.now(UTC) + timedelta(seconds=1),
        leeway_seconds=30,
    )
    no_expiry = ApiToken(access_token="token")

    assert fresh.expired is False
    assert inside_leeway.expired is True
    assert no_expiry.expired is False


def test_eviction_drops_token_only_entry(tmp_path: Path) -> None:
    """Drop an entry with only a long-expired token on the next save."""
    path = tmp_path / "credentials.json"
    store = CredentialStore(path=path)
    store.set_token(SERVER_URL, _expired_token(timedelta(days=8)))

    reloaded = CredentialStore(path=path)
    assert reloaded.get(SERVER_URL) is None


def test_eviction_keeps_entry_with_api_key(tmp_path: Path) -> None:
    """Keep an entry with an API key even once its token is long expired."""
    path = tmp_path / "credentials.json"
    store = CredentialStore(path=path)
    store.set_api_key(SERVER_URL, "kitaru-key")
    store.set_token(SERVER_URL, _expired_token(timedelta(days=8)))

    reloaded = CredentialStore(path=path)
    credentials = reloaded.get(SERVER_URL)
    assert credentials is not None
    assert credentials.api_key == "kitaru-key"


def test_disable_credentials_cache_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the store in-memory and write nothing to disk when disabled."""
    monkeypatch.setenv(ENV_DISABLE_CREDENTIALS_CACHE, "true")
    path = tmp_path / "credentials.json"

    store = CredentialStore(path=path)
    store.set_api_key(SERVER_URL, "kitaru-key")

    credentials = store.get(SERVER_URL)
    assert credentials is not None
    assert credentials.api_key == "kitaru-key"
    assert not path.exists()


def test_normalize_server_url_strips_trailing_slash() -> None:
    """Fold a URL with a trailing slash into the same entry as without."""
    assert normalize_server_url("https://kitaru.example.com/") == SERVER_URL

    store = CredentialStore(path=Path("unused"), persist=False)
    store.set_api_key(f"{SERVER_URL}/", "kitaru-key")
    credentials = store.get(SERVER_URL)
    assert credentials is not None
    assert credentials.api_key == "kitaru-key"


def test_get_config_directory_uses_xdg_config_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Resolve the config directory under XDG_CONFIG_HOME when set."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert get_config_directory() == tmp_path / "kitaru"


def test_get_config_directory_falls_back_to_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fall back to ~/.config/kitaru when XDG_CONFIG_HOME is unset."""
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    assert get_config_directory() == tmp_path / ".config" / "kitaru"
