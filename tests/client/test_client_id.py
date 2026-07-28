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
"""Tests for the client installation id."""

import uuid
from pathlib import Path

import pytest

from kitaru.client.client_id import ENV_CLIENT_ID, get_client_id

pytestmark = pytest.mark.usefixtures("isolated_config_directory")


def test_generated_id_is_reused_across_calls(tmp_path: Path) -> None:
    """Write a generated id once and read it back on the next call."""
    first = get_client_id()

    assert (tmp_path / "config" / "kitaru" / "client_id").exists()
    assert get_client_id() == first


def test_environment_overrides_the_stored_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prefer the id the environment names over the stored one."""
    stored = get_client_id()
    override = uuid.uuid4()
    monkeypatch.setenv(ENV_CLIENT_ID, str(override))

    assert get_client_id() == override
    assert override != stored


def test_malformed_stored_id_is_replaced(tmp_path: Path) -> None:
    """Generate a fresh id when the stored one cannot be parsed."""
    path = tmp_path / "config" / "kitaru" / "client_id"
    path.parent.mkdir(parents=True)
    path.write_text("not-a-uuid", encoding="utf-8")

    client_id = get_client_id()

    assert path.read_text(encoding="utf-8") == str(client_id)


def test_malformed_environment_id_falls_back_to_the_stored_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ignore an unparsable environment id rather than failing the login."""
    stored = get_client_id()
    monkeypatch.setenv(ENV_CLIENT_ID, "not-a-uuid")

    assert get_client_id() == stored
