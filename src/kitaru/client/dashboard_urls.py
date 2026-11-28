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
"""Dashboard URL resolution from server info."""

import uuid
from urllib.parse import urlsplit

from kitaru.api_models.v1.info import ServerInfoResponse
from kitaru.client.config import normalize_server_url


def get_dashboard_base_url(info: ServerInfoResponse, api_url: str) -> str | None:
    """Resolve the base URL of the dashboard serving this server.

    Args:
        info: Server info reported by the selected server.
        api_url: URL the client reaches the server API at.

    Returns:
        The dashboard base URL without a trailing slash, or None when the
        deployment reports no dashboard and serves no UI itself.
    """
    if info.dashboard_url:
        return normalize_server_url(info.dashboard_url)
    # A server that reports a UI version serves the bundled UI at its own
    # origin, so the URL the client already reaches the API at is also the
    # dashboard base. The server cannot state that URL itself because behind
    # a proxy it only sees the untrusted Host header.
    if info.ui_version:
        return normalize_server_url(info.server_url or api_url)
    return None


def validate_dashboard_base_url(base: str) -> str:
    """Validate that a resolved dashboard base URL is safe to build links from.

    Args:
        base: Normalized dashboard base URL.

    Raises:
        ValueError: The URL is not a plain HTTP(S) base URL without embedded
            credentials, query parameters, or fragments.

    Returns:
        The validated dashboard base URL.
    """
    parsed = urlsplit(base)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("invalid dashboard URL")
    return base


def get_session_url(base: str, session_id: uuid.UUID) -> str:
    """Build the dashboard page URL for a session.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        session_id: Session identifier.

    Returns:
        Absolute session page URL.
    """
    return f"{base}/sessions/{session_id}"


def get_agent_sessions_url(
    base: str,
    agent_id: uuid.UUID,
    *,
    agent_version_id: uuid.UUID | None = None,
) -> str:
    """Build the dashboard sessions list URL for an agent.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        agent_id: Agent identifier.
        agent_version_id: Optional agent version to filter the sessions list.

    Returns:
        Absolute agent sessions page URL.
    """
    url = f"{base}/agents/{agent_id}/sessions"
    if agent_version_id is not None:
        return f"{url}?version={agent_version_id}"
    return url


def get_experiment_url(base: str, experiment_id: uuid.UUID) -> str:
    """Build the dashboard page URL for an experiment.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        experiment_id: Experiment identifier.

    Returns:
        Absolute experiment page URL.
    """
    return f"{base}/experiments/{experiment_id}"


def get_experiment_run_url(base: str, experiment_id: uuid.UUID, run_number: int) -> str:
    """Build the dashboard experiment URL with a specific run selected.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        experiment_id: Experiment identifier.
        run_number: 1-based run number within the experiment.

    Returns:
        Absolute experiment page URL selecting the run.
    """
    return f"{base}/experiments/{experiment_id}?run={run_number}"


def get_cohort_url(base: str, *, agent_id: uuid.UUID, cohort_id: uuid.UUID) -> str:
    """Build the dashboard page URL for a cohort.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        agent_id: Agent the cohort belongs to.
        cohort_id: Cohort identifier.

    Returns:
        Absolute cohort page URL.
    """
    return f"{base}/agents/{agent_id}/cohorts/{cohort_id}"


def get_job_url(base: str, job_id: uuid.UUID) -> str:
    """Build the dashboard page URL for a job.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        job_id: Job identifier.

    Returns:
        Absolute job page URL.
    """
    return f"{base}/jobs/{job_id}"


def get_investigation_review_page_url(
    base: str,
    *,
    agent_id: uuid.UUID,
    investigation_id: uuid.UUID,
) -> str:
    """Build the dashboard review page URL for an investigation.

    Args:
        base: Validated dashboard base URL without a trailing slash.
        agent_id: Agent the investigation belongs to.
        investigation_id: Investigation to review.

    Returns:
        Absolute investigation review page URL.
    """
    return f"{base}/agents/{agent_id}/investigations/{investigation_id}/review"


def get_investigation_review_url(
    info: ServerInfoResponse,
    api_url: str,
    *,
    agent_id: uuid.UUID,
    investigation_id: uuid.UUID,
) -> str | None:
    """Resolve the dashboard review page URL for an investigation.

    Args:
        info: Server info reported by the selected server.
        api_url: URL the client reaches the server API at.
        agent_id: Agent the investigation belongs to.
        investigation_id: Investigation to review.

    Returns:
        The absolute review page URL, or None when the deployment has no
        reachable dashboard.
    """
    base = get_dashboard_base_url(info, api_url)
    if base is None:
        return None
    return get_investigation_review_page_url(
        base, agent_id=agent_id, investigation_id=investigation_id
    )
