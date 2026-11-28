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

import pytest

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.client.dashboard_urls import (
    get_agent_sessions_url,
    get_cohort_url,
    get_dashboard_base_url,
    get_experiment_run_url,
    get_experiment_url,
    get_investigation_review_page_url,
    get_investigation_review_url,
    get_job_url,
    get_session_url,
    validate_dashboard_base_url,
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


@pytest.mark.parametrize(
    "base",
    [
        "https://cloud.example.com",
        "https://cloud.example.com/kitaru-workspaces/ws-1",
        "http://localhost:8000",
    ],
)
def test_validate_dashboard_base_url_accepts_safe_http_bases(base: str) -> None:
    """Plain HTTP(S) base URLs with optional path prefixes are accepted."""
    assert validate_dashboard_base_url(base) == base


@pytest.mark.parametrize(
    "base",
    [
        "javascript:alert(1)",
        "file:///etc/passwd",
        "https://",
        "https://user:pass@cloud.example.com",
        "https://user@cloud.example.com",
        "https://cloud.example.com?token=secret",
        "https://cloud.example.com#section",
    ],
)
def test_validate_dashboard_base_url_rejects_unsafe_bases(base: str) -> None:
    """Non-HTTP schemes, credentials, queries, and fragments are rejected."""
    with pytest.raises(ValueError, match="invalid dashboard URL"):
        validate_dashboard_base_url(base)


def test_resource_url_builders_match_dashboard_routes() -> None:
    """Each resource builder joins the base with its exact dashboard route."""
    base = "https://cloud.example.com/kitaru-workspaces/ws-1"
    agent_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    session_id = uuid.uuid4()
    experiment_id = uuid.uuid4()
    cohort_id = uuid.uuid4()
    job_id = uuid.uuid4()
    investigation_id = uuid.uuid4()

    assert get_session_url(base, session_id) == f"{base}/sessions/{session_id}"
    assert (
        get_agent_sessions_url(base, agent_id) == f"{base}/agents/{agent_id}/sessions"
    )
    assert (
        get_agent_sessions_url(base, agent_id, agent_version_id=agent_version_id)
        == f"{base}/agents/{agent_id}/sessions?version={agent_version_id}"
    )
    assert (
        get_experiment_url(base, experiment_id) == f"{base}/experiments/{experiment_id}"
    )
    assert (
        get_experiment_run_url(base, experiment_id, 4)
        == f"{base}/experiments/{experiment_id}?run=4"
    )
    assert (
        get_cohort_url(base, agent_id=agent_id, cohort_id=cohort_id)
        == f"{base}/agents/{agent_id}/cohorts/{cohort_id}"
    )
    assert get_job_url(base, job_id) == f"{base}/jobs/{job_id}"
    assert (
        get_investigation_review_page_url(
            base, agent_id=agent_id, investigation_id=investigation_id
        )
        == f"{base}/agents/{agent_id}/investigations/{investigation_id}/review"
    )
