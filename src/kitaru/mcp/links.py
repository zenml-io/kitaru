#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Best-effort dashboard links for exact MCP results."""

import asyncio
import uuid
from urllib.parse import urlsplit

import httpx

from kitaru.api_models.v1.info import ServerInfoResponse
from kitaru.client.dashboard_urls import get_dashboard_base_url
from kitaru.client.exceptions import APIError, InvalidServerResponseError
from kitaru.mcp.lifecycle import MCPServerState

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
        return info, base, []
    except (
        APIError,
        InvalidServerResponseError,
        httpx.HTTPError,
        TimeoutError,
        ValueError,
    ):
        return None, None, [warning]


def get_session_url(base: str, session_id: uuid.UUID) -> str:
    """Build the exact session inspection URL."""
    return f"{base}/sessions/{session_id}"


def get_experiment_run_url(base: str, experiment_id: uuid.UUID, run_number: int) -> str:
    """Build the experiment URL with its run selected."""
    return f"{base}/experiments/{experiment_id}?run={run_number}"
