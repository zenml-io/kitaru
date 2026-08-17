#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Dashboard URL resolution behavior."""

import uuid

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.client.dashboard_urls import (
    get_dashboard_base_url,
    get_investigation_review_url,
)

API_URL = "https://api.example.com/"


def _info(**overrides: str | None) -> ServerInfoResponse:
    return ServerInfoResponse(
        version="0.0.0", auth_scheme=AuthScheme.LOCAL, **overrides
    )


def test_stated_dashboard_url_wins() -> None:
    """A server-stated dashboard URL is used as-is, without its trailing slash."""
    info = _info(
        dashboard_url="https://cloud.example.com/kitaru-workspaces/ws-1/",
        ui_version="1.0.0",
        server_url="https://elsewhere.example.com",
    )
    base = get_dashboard_base_url(info, API_URL)
    assert base == "https://cloud.example.com/kitaru-workspaces/ws-1"


def test_bundled_ui_prefers_stated_server_url() -> None:
    """A bundled-UI server that states its own URL is reached through it."""
    info = _info(ui_version="1.0.0", server_url="https://kitaru.example.com/")
    assert get_dashboard_base_url(info, API_URL) == "https://kitaru.example.com"


def test_bundled_ui_falls_back_to_the_login_url() -> None:
    """A bundled-UI server with no stated URLs is reached where the client is."""
    info = _info(ui_version="1.0.0")
    assert get_dashboard_base_url(info, API_URL) == "https://api.example.com"


def test_api_only_server_has_no_dashboard() -> None:
    """A server with no dashboard URL and no UI resolves to no base."""
    assert get_dashboard_base_url(_info(), API_URL) is None


def test_review_url_appends_the_stable_route() -> None:
    """The review URL joins the base with the agent and investigation route."""
    agent_id = uuid.uuid4()
    investigation_id = uuid.uuid4()
    url = get_investigation_review_url(
        _info(ui_version="1.0.0"),
        API_URL,
        agent_id=agent_id,
        investigation_id=investigation_id,
    )
    assert url == (
        "https://api.example.com"
        f"/agents/{agent_id}/investigations/{investigation_id}/review"
    )


def test_review_url_is_none_without_a_dashboard() -> None:
    """No dashboard base means no review URL."""
    url = get_investigation_review_url(
        _info(), API_URL, agent_id=uuid.uuid4(), investigation_id=uuid.uuid4()
    )
    assert url is None
