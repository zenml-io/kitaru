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
"""Tests for the install event the one-line installer asks for."""

import json

import httpx
import pytest

from kitaru.analytics.client import AnalyticsClient
from kitaru.cli.install_report import report_install
from kitaru.client.config import get_analytics_id, get_client_id


def _recording_client() -> tuple[AnalyticsClient, list[httpx.Request]]:
    """Build an analytics client whose requests stay in memory."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    client = AnalyticsClient()
    client._http = httpx.AsyncClient(
        base_url="https://analytics.test", transport=httpx.MockTransport(handler)
    )
    return client, requests


async def test_installer_run_sends_one_event_joinable_to_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send the login client id with the install event."""
    monkeypatch.setenv("KITARU_INSTALL_METHOD", "script")
    client, requests = _recording_client()

    await report_install("project", client)

    assert len(requests) == 1
    assert requests[0].headers["Source-Context"] == "kitaru-cli"
    (message,) = json.loads(requests[0].content)
    assert message["event"] == "Install script completed"
    assert message["user_id"] == str(get_analytics_id())
    assert message["properties"]["client_id"] == str(get_client_id())
    assert message["properties"]["install_method"] == "script"
    assert message["properties"]["install_scope"] == "project"


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {"KITARU_INSTALL_METHOD": "script", "DO_NOT_TRACK": "1"},
        {"KITARU_INSTALL_METHOD": "script", "KITARU_DISABLE_CLIENT_ANALYTICS": "1"},
    ],
)
async def test_manual_setup_and_opt_out_send_nothing(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str]
) -> None:
    """Stay silent outside the installer and when analytics are disabled."""
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    client, requests = _recording_client()

    await report_install("user", client)
    await client.aclose()

    assert requests == []


async def test_unreachable_analytics_server_does_not_fail_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Swallow delivery failures."""
    monkeypatch.setenv("KITARU_INSTALL_METHOD", "script")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    client = AnalyticsClient()
    client._http = httpx.AsyncClient(
        base_url="https://analytics.test", transport=httpx.MockTransport(handler)
    )

    await report_install("user", client)
