#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Transition failure matrix and matrix-cell drill-down handlers."""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar

from kitaru.api_models.v1.annotation import AnnotationListParams
from kitaru.api_models.v1.base import ResponseModel
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import (
    Filter,
    FilterableListParams,
    FilterCondition,
    FilterOp,
)
from kitaru.api_models.v1.session import SessionListParams, SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeListParams, SessionNodeResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import ToolSuccessPayload
from kitaru.mcp.models.failure_matrix import (
    FailureCellData,
    FailureCellRequest,
    FailureMatrixData,
    FailureMatrixRequest,
    GroupRecords,
    GroupSummary,
    MatrixCell,
)
from kitaru.mcp.tools.transitions import (
    SessionOutcome,
    analyze_group,
    build_cells,
    build_labeler,
    cell_details,
    fold_rare_states,
    summarize_group,
)

IDS_PER_FILTER = 100
PAGE_SIZE = 500
TOP_CELLS_IN_TEXT = 5

ItemT = TypeVar("ItemT", bound=ResponseModel)
ParamsT = TypeVar("ParamsT", bound=FilterableListParams)


async def handle_failure_matrix(
    state: MCPServerState, request: FailureMatrixRequest
) -> ToolSuccessPayload:
    """Build the transition failure matrix for one or two session groups."""
    # Read fresh: a reviewer who just marked a first failure expects to see it.
    groups = await _analyze(state, request, reuse_records=False)
    base, compare = groups[0], groups[1] if len(groups) > 1 else None
    rows, cols, cells = build_cells(
        base.outcomes, compare.outcomes if compare else None, request.sources
    )
    data = FailureMatrixData(
        state_by=request.state_by,
        rows=rows,
        cols=cols,
        cells=cells,
        base=summarize_group(
            request.label or "Selected sessions",
            base.outcomes,
            request.sources,
            base.truncated,
        ),
        compare=summarize_group(
            request.compare_label or "Comparison",
            compare.outcomes,
            request.sources,
            compare.truncated,
        )
        if compare is not None
        else None,
    )
    return ToolSuccessPayload(data=data, text=describe_matrix(data))


async def handle_failure_cell(
    state: MCPServerState, request: FailureCellRequest
) -> FailureCellData:
    """List the sessions and repeated notes behind one matrix cell."""
    # Cell clicks follow the matrix call within seconds, so they reuse its records.
    groups = await _analyze(state, request, reuse_records=True)
    index = 1 if request.side == "compare" else 0
    if index >= len(groups):
        raise MCPToolError(
            "invalid_arguments",
            "side='compare' needs compare_filter.",
            recovery="Pass the same compare_filter the matrix was built with.",
        )
    return cell_details(
        groups[index].outcomes,
        (request.from_state, request.to_state),
        request.sources,
        request.limit,
    )


@dataclass(frozen=True, slots=True)
class _AnalyzedGroup:
    outcomes: list[SessionOutcome]
    truncated: bool


async def _analyze(
    state: MCPServerState, request: FailureMatrixRequest, *, reuse_records: bool
) -> list[_AnalyzedGroup]:
    filters = [request.filter]
    if request.compare_filter is not None:
        filters.append(request.compare_filter)
    records = await asyncio.gather(
        *(
            _load_group(state, session_filter, request.max_sessions, reuse_records)
            for session_filter in filters
        )
    )
    labeler = build_labeler(request.state_by, request.state_map)
    folded = fold_rare_states(
        [analyze_group(group, labeler, request.state_map) for group in records],
        request.sources,
    )
    return [
        _AnalyzedGroup(outcomes, group.truncated)
        for outcomes, group in zip(folded, records, strict=True)
    ]


async def _load_group(
    state: MCPServerState,
    session_filter: Filter | None,
    max_sessions: int,
    reuse: bool,
) -> GroupRecords:
    dumped = (
        session_filter.model_dump(by_alias=True, mode="json")
        if session_filter
        else None
    )
    key = json.dumps({"filter": dumped, "max": max_sessions}, sort_keys=True)
    cached = state.group_cache.get(key) if reuse else None
    if cached is not None:
        return cached
    records = await fetch_group(
        state.client, session_filter, max_sessions, state.settings.pool_size
    )
    state.group_cache.put(key, records)
    return records


async def fetch_group(
    client: KitaruAPIClient,
    session_filter: Filter | None,
    max_sessions: int,
    concurrency: int,
) -> GroupRecords:
    """Fetch the newest matching sessions with their nodes, marks, and evaluations."""
    params = SessionListParams(
        filter=session_filter,
        size=min(max_sessions + 1, PAGE_SIZE),
        sort="created:desc",
    )
    sessions: list[SessionResponse] = []
    while len(sessions) <= max_sessions:
        page = await client.sessions.list(params)
        sessions.extend(page.items)
        if page.next_cursor is None:
            break
        params = params.model_copy(update={"cursor": page.next_cursor})
    truncated = len(sessions) > max_sessions
    sessions = sessions[:max_sessions]
    ids = [session.id for session in sessions]
    limiter = asyncio.Semaphore(concurrency)

    async def nodes_of(session_id: uuid.UUID) -> tuple[SessionNodeResponse, ...]:
        async with limiter:
            iterator = client.sessions.iter_nodes(
                session_id, SessionNodeListParams(size=PAGE_SIZE)
            )
            return tuple([node async for node in iterator])

    node_lists, annotations, evaluations = await asyncio.gather(
        asyncio.gather(*(nodes_of(session_id) for session_id in ids)),
        _collect_by_session(client.annotations.iter, AnnotationListParams, ids),
        _collect_by_session(client.evaluations.iter, EvaluationListParams, ids),
    )
    return GroupRecords(
        sessions=tuple(sessions),
        nodes=dict(zip(ids, node_lists, strict=True)),
        annotations=tuple(annotations),
        evaluations=tuple(evaluations),
        truncated=truncated,
    )


async def _collect_by_session(
    iterate: Callable[[ParamsT], AsyncIterator[ItemT]],
    params_type: type[ParamsT],
    session_ids: Sequence[uuid.UUID],
) -> list[ItemT]:
    async def chunk_items(start: int) -> list[ItemT]:
        chunk = [str(i) for i in session_ids[start : start + IDS_PER_FILTER]]
        params = params_type(
            filter=FilterCondition(field="session_id", op=FilterOp.IN, value=chunk),
            size=PAGE_SIZE,
        )
        return [item async for item in iterate(params)]

    chunks = await asyncio.gather(
        *(chunk_items(start) for start in range(0, len(session_ids), IDS_PER_FILTER))
    )
    return [item for chunk in chunks for item in chunk]


def describe_matrix(data: FailureMatrixData) -> str:
    """Summarize a matrix in plain language for the model and for hosts without UI."""
    base, compare = data.base, data.compare
    lines = [
        f"Transition failure matrix: rows are the last step that went right, "
        f"columns the first step that went wrong (states: {data.state_by}).",
        _describe_group(base),
    ]
    if compare is not None:
        lines.append(_describe_group(compare))
        changed = sorted(
            (c for c in data.cells if (c.compare_count or 0) != c.count),
            key=lambda c: -abs((c.compare_count or 0) - c.count),
        )
        if changed:
            lines.append(f"Changed transitions ({base.label} -> {compare.label}):")
            lines.extend(
                f"  {_pair(c)}: {c.count} -> {c.compare_count or 0}"
                for c in changed[: TOP_CELLS_IN_TEXT * 2]
            )
    else:
        top = sorted((c for c in data.cells if c.count), key=lambda c: -c.count)
        if top:
            lines.append("Top transitions by failures:")
            lines.extend(
                f"  {_pair(c)}: {c.count}{_rate(c)}" for c in top[:TOP_CELLS_IN_TEXT]
            )
    lines.append(
        "To look at it another way, call again with state_by ('tool', 'span', "
        "'node'), state_map to merge states, sources to count only errors or "
        "annotations, or compare_filter to compare with a replay or another group."
    )
    return "\n".join(lines)


def _describe_group(group: GroupSummary) -> str:
    text = (
        f"{group.label}: {group.session_count} sessions, {group.failed_count} failed, "
        f"{group.shown_count} placed in the matrix"
    )
    if group.unlocated_count:
        evaluations = ", ".join(
            f"{item.name} x{item.count}" for item in group.unlocated_evaluations
        )
        text += (
            f"; {group.unlocated_count} failed without a located step"
            + (f" (failed evaluations: {evaluations})" if evaluations else "")
            + ". Mark the step with an annotation on the node, value "
            '{"first_failure": true, "note": "..."}, to place them'
        )
    if group.truncated:
        text += "; only the newest sessions were read (raise max_sessions for more)"
    return text + "."


def _pair(cell: MatrixCell) -> str:
    return f"{cell.from_state} -> {cell.to_state}"


def _rate(cell: MatrixCell) -> str:
    if not cell.attempts:
        return ""
    return f" of {cell.attempts} tries ({round(100 * cell.count / cell.attempts)}%)"
