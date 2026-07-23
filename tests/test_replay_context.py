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
"""Tests for replay environment accessors."""

from typing import Any

import httpx
import pytest

from kitaru.replay_context import replay_id, replay_inputs


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove the replay env contract variables."""
    for name in (
        "KITARU_REPLAY_ID",
        "KITARU_INPUTS",
        "KITARU_API_URL",
        "KITARU_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


def test_replay_id_outside_replay_mode() -> None:
    """Return None without KITARU_REPLAY_ID."""
    assert replay_id() is None


def test_replay_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the KITARU_REPLAY_ID value."""
    monkeypatch.setenv("KITARU_REPLAY_ID", "replay-1")
    assert replay_id() == "replay-1"


def test_replay_inputs_outside_replay_mode() -> None:
    """Return None without KITARU_REPLAY_ID."""
    assert replay_inputs() is None


def test_replay_inputs_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse KITARU_INPUTS when the runner set it."""
    monkeypatch.setenv("KITARU_REPLAY_ID", "replay-1")
    monkeypatch.setenv("KITARU_INPUTS", '{"question": "hi"}')
    assert replay_inputs() == {"question": "hi"}


def test_replay_inputs_fetches_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetch the replay spec when KITARU_INPUTS is absent."""
    monkeypatch.setenv("KITARU_REPLAY_ID", "replay-1")
    monkeypatch.setenv("KITARU_API_URL", "http://server/")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    requests: list[tuple[str, dict[str, str]]] = []

    class FakeResponse:
        """Fake HTTP response."""

        def raise_for_status(self) -> "FakeResponse":
            """Return the response."""
            return self

        def json(self) -> Any:
            """Return the spec payload."""
            return {"replay_id": "replay-1", "inputs": {"question": "hi"}}

    def fake_get(url: str, headers: dict[str, str]) -> FakeResponse:
        requests.append((url, headers))
        return FakeResponse()

    monkeypatch.setattr(httpx, "get", fake_get)
    assert replay_inputs() == {"question": "hi"}
    assert requests == [
        ("http://server/v1/replays/replay-1/spec", {"Authorization": "Bearer key"})
    ]


def test_replay_inputs_missing_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject a spec fetch without KITARU_API_URL."""
    monkeypatch.setenv("KITARU_REPLAY_ID", "replay-1")
    with pytest.raises(RuntimeError, match="KITARU_API_URL"):
        replay_inputs()
