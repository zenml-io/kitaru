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
"""Best-effort dashboard link resolution for CLI command results."""

import asyncio
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

import httpx

from kitaru.api_models.v1.info import ServerInfoResponse
from kitaru.client.dashboard_urls import (
    get_dashboard_base_url,
    validate_dashboard_base_url,
)
from kitaru.client.exceptions import APIError, InvalidServerResponseError

_INFO_LOOKUP_TIMEOUT_SECONDS = 5.0


@runtime_checkable
class _DashboardInfoClient(Protocol):
    """Client interface capable of resolving server dashboard metadata."""

    base_url: str
    info: Any


async def get_dashboard_base(
    client: Any,
    *,
    warning: str,
    info: ServerInfoResponse | None = None,
    timeout: float = _INFO_LOOKUP_TIMEOUT_SECONDS,
) -> tuple[str | None, list[str]]:
    """Resolve a validated dashboard base URL without failing a CLI command.

    Args:
        client: Active API client.
        warning: Warning to return when server info lookup or validation fails.
        info: Optional already-fetched server info to reuse without a second
            network call.
        timeout: Maximum seconds to wait for the server info request.

    Returns:
        A pair of the validated dashboard base URL (or None) and any warnings.
    """
    if not isinstance(client, _DashboardInfoClient):
        return None, []
    try:
        resolved_info = info
        if resolved_info is None:
            bounded_timeout = max(0.0, min(timeout, _INFO_LOOKUP_TIMEOUT_SECONDS))
            async with asyncio.timeout(bounded_timeout):
                resolved_info = await client.info.get()
        if not isinstance(resolved_info, ServerInfoResponse):
            raise ValueError("invalid server info response")
        base = get_dashboard_base_url(resolved_info, client.base_url)
        if base is None:
            return None, []
        return validate_dashboard_base_url(base), []
    except (
        APIError,
        InvalidServerResponseError,
        httpx.HTTPError,
        TimeoutError,
        ValueError,
    ):
        return None, [warning]


async def resolve_dashboard_links(
    client: Any,
    build_links: Callable[[str], dict[str, str]],
    *,
    warning: str,
    info: ServerInfoResponse | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Build dashboard links for a CLI command result when a dashboard exists.

    Args:
        client: Active API client.
        build_links: Callback turning a validated dashboard base URL into the
            result ``links`` mapping.
        warning: Warning to return when server info lookup or validation fails.
        info: Optional already-fetched server info to reuse.

    Returns:
        A pair of the links mapping (empty when no dashboard is reachable) and
        any warnings.
    """
    base, warnings = await get_dashboard_base(client, warning=warning, info=info)
    if base is None:
        return {}, warnings
    return build_links(base), warnings
