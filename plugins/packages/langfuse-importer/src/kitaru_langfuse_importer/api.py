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
"""Langfuse API read layer."""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import datetime
from functools import partial
from typing import Any

from langfuse import get_client
from langfuse.api import NotFoundError, ObservationV2
from langfuse.api.core import ApiError, RequestOptions
from pydantic import BaseModel, ConfigDict

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.task.importer import retry_rate_limited, stream_bounded

__all__ = ["FetchedTrace", "fetch", "fetch_trace", "serialize_trace", "wait_for_trace"]

_POLL_INTERVAL = 2.0
_REQUEST_OPTIONS: RequestOptions = {"timeout_in_seconds": 60}
_DEFAULT_RETRY_AFTER = 60.0
_OBSERVATION_FIELDS = "core,basic,time,io,metadata,model,usage,prompt,trace_context"
_OBSERVATION_LIMIT = 500
_EXPAND_METADATA = {"attributes", "resourceAttributes"}


def _get_retry_after(exc: Exception) -> float | None:
    """Return the rate-limit wait in seconds, or None for other errors."""
    if not isinstance(exc, ApiError) or exc.status_code != 429:
        return None
    body = exc.body if isinstance(exc.body, dict) else {}
    details = body.get("details")
    retry_after = (
        details.get("retryAfterSeconds") if isinstance(details, dict) else None
    )
    if retry_after is None:
        retry_after = (exc.headers or {}).get("retry-after")
    try:
        return float(retry_after) if retry_after is not None else _DEFAULT_RETRY_AFTER
    except (TypeError, ValueError):
        return _DEFAULT_RETRY_AFTER


def _is_root(observation: ObservationV2) -> bool:
    """Identify application roots without mistaking missing parents for roots."""
    return observation.is_root_observation is True or (
        observation.is_root_observation is None
        and observation.parent_observation_id is None
    )


def _decode_io(value: Any) -> Any:
    """Prepare v2 raw I/O for one JSON decode by the parser."""
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except ValueError:
            pass
        else:
            # The parser decodes quoted strings itself. Decoding them here
            # too would turn JSON-looking source text into an object or list.
            return value if isinstance(decoded, str) else decoded
    return value


class FetchedTrace(BaseModel):
    """A trace reconstructed from all pages of the observations API."""

    id: str
    observations: list[ObservationV2]

    @property
    def session_id(self) -> str | None:
        """Return the session id supplied by the trace's observations."""
        return next(
            (row.session_id for row in self.observations if row.session_id), None
        )

    @property
    def started_at(self) -> datetime | None:
        """Return the earliest available observation start time."""
        return min((row.start_time for row in self.observations), default=None)

    def to_payload(self) -> dict[str, Any]:
        """Build a parser trace record, using only actual roots for trace I/O."""
        roots = [row for row in self.observations if row.is_root_observation is True]
        roots = roots or [row for row in self.observations if _is_root(row)]
        root = roots[0] if len(roots) == 1 else None
        rows = []
        for observation in self.observations:
            row = observation.model_dump(mode="json", by_alias=True)
            row["input"] = _decode_io(row.get("input"))
            row["output"] = _decode_io(row.get("output"))
            row["traceTags"] = row.get("tags")
            row["traceRelease"] = row.get("release")
            rows.append(row)
        return {
            "id": self.id,
            "sessionId": self.session_id,
            "input": _decode_io(root.input) if root else None,
            "output": _decode_io(root.output) if root else None,
            "observations": rows,
        }


async def _list_observations(
    *,
    fields: str = _OBSERVATION_FIELDS,
    expand_metadata: set[str] | None = None,
    **filters: Any,
) -> list[ObservationV2]:
    """Read every cursor page matching the observation filters."""
    api = get_client().async_api
    observations: dict[tuple[str | None, str], ObservationV2] = {}
    cursor: str | None = None
    while True:
        response = await retry_rate_limited(
            partial(
                api.observations.get_many,
                fields=fields,
                expand_metadata=",".join(sorted(expand_metadata or _EXPAND_METADATA)),
                limit=_OBSERVATION_LIMIT,
                cursor=cursor,
                request_options=_REQUEST_OPTIONS,
                **filters,
            ),
            _get_retry_after,
        )
        observations.update(((row.trace_id, row.id), row) for row in response.data)
        cursor = response.meta.cursor
        if cursor is None:
            return sorted(
                observations.values(), key=lambda row: (row.start_time, row.id)
            )


async def fetch_trace(trace_id: str) -> FetchedTrace:
    """Fetch every observation of one exact trace, including all cursor pages.

    Args:
        trace_id: Langfuse trace id.

    Returns:
        Reconstructed trace, empty while no observations are available.
    """
    # An exact id has no known start/end bounds. Restrict by traceId instead
    # of imposing an arbitrary lookback that would silently truncate traces.
    observations = await _list_observations(trace_id=trace_id)
    metadata_keys = _EXPAND_METADATA | {
        key for row in observations for key in (row.metadata or {})
    }
    if metadata_keys != _EXPAND_METADATA:
        # v2 truncates values of unexpanded keys. Discover custom keys before
        # rereading so joins and selected provider metadata keep their values.
        observations = await _list_observations(
            trace_id=trace_id, expand_metadata=metadata_keys
        )
    return FetchedTrace(id=trace_id, observations=observations)


async def _fetch_trace_identity(trace_id: str) -> FetchedTrace:
    """Read complete grouping context without fetching I/O or metadata."""
    return FetchedTrace(
        id=trace_id,
        observations=await _list_observations(fields="core,basic", trace_id=trace_id),
    )


async def wait_for_trace(trace_id: str) -> FetchedTrace:
    """Poll until real roots have ended and two complete snapshots agree.

    Args:
        trace_id: Langfuse trace id.

    Returns:
        Reconstructed finished trace.
    """
    previous: FetchedTrace | None = None
    while True:
        try:
            trace = await fetch_trace(trace_id)
        except NotFoundError:
            previous = None
        else:
            roots = [row for row in trace.observations if _is_root(row)]
            if roots and all(row.end_time is not None for row in roots):
                if trace == previous:
                    return trace
                previous = trace
            else:
                previous = None
        await asyncio.sleep(_POLL_INTERVAL)


def serialize_trace(trace: FetchedTrace) -> bytes:
    """Serialize a reconstructed trace for the SDK-independent parser.

    Args:
        trace: Fetched trace.

    Returns:
        Trace payload bytes.
    """
    return json.dumps(trace.to_payload()).encode("utf-8")


class LangfuseImportQuery(ImportQuery):
    """Langfuse import query."""

    model_config = ConfigDict(extra="forbid")


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch complete traces and emit each selected session together once.

    Window discovery reads bounded observation pages, then restores oldest
    first order because v2 always lists newest first. Complete trace reads
    retain children beyond until; the earliest available start determines
    window membership. A known session is emitted when all its candidate
    traces have arrived. Exact-id sessions wait for every requested trace,
    since their session membership is unknown before fetching.

    Args:
        query: Exact trace ids or a since/until window, and concurrency.

    Yields:
        Parser trace-list payloads, each containing one complete session.
    """
    parsed = LangfuseImportQuery.model_validate(query)
    remaining: dict[str, int] = {}
    groups: dict[str, str] = {}
    window: tuple[datetime, datetime] | None = None
    if parsed.trace_ids is not None:
        trace_ids = list(dict.fromkeys(parsed.trace_ids))
    else:
        window = parsed.get_window()
        rows = await _list_observations(
            fields="core,basic", from_start_time=window[0], to_start_time=window[1]
        )
        # All discovery pages must be read before releasing any session, so
        # a later turn cannot split the same external session across payloads.
        trace_ids = list(dict.fromkeys(row.trace_id for row in rows if row.trace_id))
        for row in rows:
            if row.trace_id and row.session_id:
                groups.setdefault(row.trace_id, row.session_id)
        # Missing window context does not prove a trace is standalone: a
        # later child outside until may supply its session id. Resolve those
        # identities before any complete-session payload can be released.
        unresolved = (trace_id for trace_id in trace_ids if trace_id not in groups)
        async with aclosing(
            stream_bounded(
                (_fetch_trace_identity(trace_id) for trace_id in unresolved),
                parsed.concurrency,
            )
        ) as identities:
            async for trace in identities:
                groups[trace.id] = trace.session_id or trace.id
        for key in groups.values():
            remaining[key] = remaining.get(key, 0) + 1

    held: dict[str, list[dict[str, Any]]] = {}
    async with aclosing(
        stream_bounded(
            (fetch_trace(trace_id) for trace_id in trace_ids), parsed.concurrency
        )
    ) as traces:
        async for trace in traces:
            selected = bool(trace.observations) and (
                window is None
                or (
                    trace.started_at is not None
                    and window[0] <= trace.started_at < window[1]
                )
            )
            key = groups.get(trace.id, trace.session_id or trace.id)
            if selected:
                held.setdefault(key, []).append(trace.to_payload())
            if window is not None:
                remaining[key] -= 1
                if remaining[key] == 0 and key in held:
                    yield json.dumps(held.pop(key)).encode("utf-8")
            elif selected and not trace.session_id:
                yield json.dumps(held.pop(key)).encode("utf-8")
    for payloads in held.values():
        yield json.dumps(payloads).encode("utf-8")
