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
    return f"{base}/agents/{agent_id}/investigations/{investigation_id}/review"
