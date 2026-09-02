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
"""Arize Phoenix API read layer."""

import asyncio
import json
from typing import Any

import httpx
from phoenix.client import AsyncClient
from phoenix.client.utils.config import get_env_project_name

__all__ = ["fetch_spans", "serialize_spans", "wait_for_spans"]

_POLL_INTERVAL = 2.0
_SPAN_LIMIT = 1000


def _has_root(spans: list[Any]) -> bool:
    """Return whether a root span is among the fetched spans."""
    ids = {span["context"]["span_id"] for span in spans}
    return any(
        span.get("parent_id") is None or span["parent_id"] not in ids for span in spans
    )


async def wait_for_spans(trace_id: str) -> list[Any]:
    """Poll the Phoenix span API until the trace is complete.

    Args:
        trace_id: Phoenix trace id.

    Returns:
        Fetched spans.
    """
    project = get_env_project_name()
    client = AsyncClient()
    # The trace is complete when it has spans, a root span is present,
    # and the span count is stable across two consecutive polls.
    previous_count: int | None = None
    while True:
        try:
            spans = await client.spans.get_spans(
                project_identifier=project,
                trace_ids=[trace_id],
                limit=_SPAN_LIMIT,
            )
        except httpx.HTTPStatusError as exc:
            # The project only exists once its first spans land, so a
            # missing project is a trace without spans.
            if exc.response.status_code != httpx.codes.NOT_FOUND:
                raise
            previous_count = None
        else:
            if len(spans) == previous_count and _has_root(spans):
                return spans
            previous_count = len(spans)
        await asyncio.sleep(_POLL_INTERVAL)


async def fetch_spans(trace_id: str) -> list[Any]:
    """Fetch the spans of a trace once from the Phoenix span API.

    Args:
        trace_id: Phoenix trace id.

    Returns:
        Fetched spans.
    """
    return await AsyncClient().spans.get_spans(
        project_identifier=get_env_project_name(),
        trace_ids=[trace_id],
        limit=_SPAN_LIMIT,
    )


def serialize_spans(spans: list[Any]) -> bytes:
    """Serialize fetched spans into the payload the parser accepts.

    Args:
        spans: Fetched spans.

    Returns:
        Trace payload bytes.
    """
    return json.dumps(spans).encode("utf-8")
