#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Best-effort dashboard links for exact MCP results."""

import asyncio

import httpx

from kitaru.api_models.v1.info import ServerInfoResponse
from kitaru.client.dashboard_urls import (
    get_dashboard_base_url,
    get_experiment_run_url,
    get_session_url,
    validate_dashboard_base_url,
)
from kitaru.client.exceptions import APIError, InvalidServerResponseError
from kitaru.mcp.lifecycle import MCPServerState

__all__ = [
    "get_dashboard_info",
    "get_experiment_run_url",
    "get_session_url",
]

_INFO_LOOKUP_MAX_SECONDS = 5.0
_INFO_LOOKUP_HANDLER_FRACTION = 0.25
_INFO_LOOKUP_DEADLINE_MARGIN_SECONDS = 0.02


async def get_dashboard_info(
    state: MCPServerState, *, warning: str
) -> tuple[ServerInfoResponse | None, str | None, list[str]]:
    """Resolve a safe dashboard base without affecting a completed operation."""
    try:
        timeout = min(
            _INFO_LOOKUP_MAX_SECONDS,
            state.settings.handler_timeout * _INFO_LOOKUP_HANDLER_FRACTION,
        )
        remaining = state.get_remaining_handler_time()
        if remaining is not None:
            timeout = min(timeout, remaining - _INFO_LOOKUP_DEADLINE_MARGIN_SECONDS)
        if timeout <= 0:
            return None, None, [warning]
        async with asyncio.timeout(timeout):
            info = await state.client.info.get()
        base = get_dashboard_base_url(info, state.client.base_url)
        if base is None:
            return None, None, []
        return info, validate_dashboard_base_url(base), []
    except (
        APIError,
        InvalidServerResponseError,
        httpx.HTTPError,
        TimeoutError,
        ValueError,
    ):
        return None, None, [warning]
