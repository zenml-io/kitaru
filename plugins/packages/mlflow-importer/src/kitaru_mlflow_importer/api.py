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
"""MLflow tracking server read layer."""

import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import datetime
from typing import Any

import mlflow
from mlflow.entities import Trace, TraceInfo
from mlflow.exceptions import MlflowException
from mlflow.tracing.client import TracingClient
from pydantic import ConfigDict, Field

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.env import get_required_env
from kitaru.task.importer import stream_bounded

from .importer import get_default_join_value

__all__ = ["fetch", "fetch_traces", "serialize_traces"]

# Traces fetched by one batch. Whole sessions are never split across batches.
_TRACES_PER_BATCH = 25


class MlflowImportQuery(ImportQuery):
    """MLflow import query."""

    model_config = ConfigDict(extra="forbid")

    experiment_ids: list[str] | None = Field(
        default=None,
        min_length=1,
        description=(
            "Experiments to search in a time window, defaulting to "
            "MLFLOW_EXPERIMENT_ID."
        ),
    )
    filter_string: str | None = Field(
        default=None,
        description="MLflow search filter combined with the time window.",
    )


def _require_tracking_uri() -> None:
    """Require the tracking server connection in the environment.

    MLflow reads ``MLFLOW_TRACKING_URI`` and ``MLFLOW_TRACKING_TOKEN`` or
    ``MLFLOW_TRACKING_USERNAME`` and ``MLFLOW_TRACKING_PASSWORD`` from the
    environment itself. Without a tracking URI it would silently search a
    local ``mlruns`` directory instead.
    """
    get_required_env("MLFLOW_TRACKING_URI")


def _get_experiment_ids(query: MlflowImportQuery) -> list[str]:
    """Return the experiments a time-window query searches.

    Args:
        query: Validated import query.

    Raises:
        ValueError: Neither the query nor the environment names an experiment.

    Returns:
        Experiment ids.
    """
    if query.experiment_ids:
        return query.experiment_ids
    configured = os.environ.get("MLFLOW_EXPERIMENT_ID", "").strip()
    if not configured:
        raise ValueError(
            "experiment_ids is required when MLFLOW_EXPERIMENT_ID is not set"
        )
    return [configured]


def _get_window_filter(since: datetime, until: datetime, extra: str | None) -> str:
    """Build a search filter selecting traces started within a time window.

    Args:
        since: Lower bound of trace start time.
        until: Upper bound of trace start time.
        extra: Additional MLflow filter, None for none.

    Returns:
        MLflow search filter string.
    """
    window = (
        f"trace.timestamp_ms >= {int(since.timestamp() * 1000)} "
        f"AND trace.timestamp_ms <= {int(until.timestamp() * 1000)}"
    )
    return f"{window} AND {extra}" if extra else window


def _get_session_key(info: TraceInfo) -> str | None:
    """Return a trace's session join value at the default join paths.

    Args:
        info: Trace info.

    Returns:
        Session join value, None when the trace carries no session metadata.
    """
    return get_default_join_value({"info": {"trace_metadata": info.trace_metadata}})


def _list_trace_infos(experiment_ids: list[str], filter_string: str) -> list[TraceInfo]:
    """List every trace info matching a filter, oldest first, without spans.

    Args:
        experiment_ids: Experiments to search.
        filter_string: MLflow search filter.

    Returns:
        Trace infos in start order.
    """
    traces = mlflow.search_traces(
        locations=experiment_ids,
        filter_string=filter_string,
        order_by=["timestamp_ms ASC"],
        return_type="list",
        include_spans=False,
    )
    return [trace.info for trace in traces]


def fetch_traces(trace_ids: list[str]) -> list[Trace]:
    """Fetch complete traces with their spans, in the given order.

    Args:
        trace_ids: Trace ids to fetch.

    Raises:
        MlflowException: The tracking server failed for a reason other than
            a missing trace.

    Returns:
        Traces in the given order. A trace the server does not find is
        omitted.
    """
    # `mlflow.get_trace` returns None for every server error, including
    # authentication and network failures, which would import a session
    # without some of its traces for good. The client it wraps raises, so
    # only a confirmed missing trace is skipped.
    client = TracingClient()
    traces: list[Trace] = []
    for trace_id in trace_ids:
        try:
            traces.append(client.get_trace(trace_id))
        except MlflowException as exc:
            if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise
    return traces


def serialize_traces(traces: list[Trace]) -> bytes:
    """Serialize traces into the ``mlflow traces search`` JSON page the parser reads.

    Args:
        traces: Traces to serialize.

    Returns:
        Parser payload bytes.
    """
    return ('{"traces": [' + ", ".join(t.to_json() for t in traces) + "]}").encode(
        "utf-8"
    )


def _split_batches(groups: dict[str, list[str]]) -> list[list[str]]:
    """Pack whole session groups into batches of at least _TRACES_PER_BATCH ids.

    Args:
        groups: Trace ids per session key, in first-appearance order.

    Returns:
        Batches of trace ids in listing order. A group is never split
        across batches.
    """
    batches: list[list[str]] = []
    current: list[str] = []
    for trace_ids in groups.values():
        current.extend(trace_ids)
        if len(current) >= _TRACES_PER_BATCH:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    return batches


async def fetch(query: dict[str, Any]) -> AsyncGenerator[bytes, None]:
    """Fetch parser payloads matching a query, one per batch of complete sessions.

    A time-window query lists the matching traces without their spans,
    oldest first, groups them by the parser's default session join value,
    falling back to the trace id, and packs whole groups into batches of at
    least _TRACES_PER_BATCH traces. Each batch fetches its complete traces
    and yields them as one payload, oldest batch first.

    A ``trace_ids`` query fetches those traces in chunks of the same size.
    A trace carrying session metadata is held until every chunk has been
    fetched, then yielded together with the other requested traces sharing
    its session. A trace without session metadata is yielded as soon as its
    chunk completes.

    Batches and chunks are fetched concurrently, up to the query's
    concurrency, and yielded in submission order. The blocking MLflow client
    runs in worker threads.

    Args:
        query: Fetch query. ``trace_ids`` fetches exactly those traces and
            ignores the time window. Otherwise ``since`` is required,
            ``until`` defaults to now, and ``experiment_ids`` defaults to
            ``MLFLOW_EXPERIMENT_ID``.

    Yields:
        One payload per batch of complete sessions in a time window, or one
        payload per chunk of standalone traces and one per session spanning
        several requested traces. Nothing when there is nothing to fetch.
    """
    parsed = MlflowImportQuery.model_validate(query)
    _require_tracking_uri()

    if parsed.trace_ids is not None:
        chunks = [
            parsed.trace_ids[index : index + _TRACES_PER_BATCH]
            for index in range(0, len(parsed.trace_ids), _TRACES_PER_BATCH)
        ]
        held: dict[str, list[Trace]] = {}
        async with aclosing(
            stream_bounded(
                (asyncio.to_thread(fetch_traces, chunk) for chunk in chunks),
                parsed.concurrency,
            )
        ) as results:
            async for traces in results:
                immediate: list[Trace] = []
                for trace in traces:
                    session_key = _get_session_key(trace.info)
                    if session_key:
                        held.setdefault(session_key, []).append(trace)
                    else:
                        immediate.append(trace)
                if immediate:
                    yield serialize_traces(immediate)
        for session_traces in held.values():
            yield serialize_traces(session_traces)
        return

    since, until = parsed.get_window()
    infos = await asyncio.to_thread(
        _list_trace_infos,
        _get_experiment_ids(parsed),
        _get_window_filter(since, until, parsed.filter_string),
    )
    groups: dict[str, list[str]] = {}
    for info in infos:
        groups.setdefault(_get_session_key(info) or info.trace_id, []).append(
            info.trace_id
        )
    batches = _split_batches(groups)
    async with aclosing(
        stream_bounded(
            (asyncio.to_thread(fetch_traces, batch) for batch in batches),
            parsed.concurrency,
        )
    ) as results:
        async for traces in results:
            if traces:
                yield serialize_traces(traces)
