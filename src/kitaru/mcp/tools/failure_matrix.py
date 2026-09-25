#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Transition failure matrix and matrix-cell drill-down handlers."""

import asyncio
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
MAX_NODES_PER_SESSION = 2000
MAX_NODES_PER_GROUP = 100_000
MAX_RECORDS_PER_BATCH = 2000

ItemT = TypeVar("ItemT", bound=ResponseModel)
ParamsT = TypeVar("ParamsT", bound=FilterableListParams)


async def handle_failure_matrix(
    state: MCPServerState, request: FailureMatrixRequest
) -> ToolSuccessPayload:
    """Build the transition failure matrix for one or two session groups."""
    # Read fresh: a reviewer who just marked a first failure expects to see it.
    records = await _fetch_groups(state, request)
    snapshot_id = uuid.uuid4().hex
    state.matrix_snapshots.put(snapshot_id, records)
    groups = _analyze(request, records)
    base, compare = groups[0], groups[1] if len(groups) > 1 else None
    rows, cols, cells = build_cells(
        base.outcomes, compare.outcomes if compare else None, request.sources
    )
    data = FailureMatrixData(
        snapshot_id=snapshot_id,
        state_by=request.state_by,
        rows=rows,
        cols=cols,
        cells=cells,
        base=summarize_group(
            request.label or "Selected sessions",
            base.outcomes,
            request.sources,
            base.truncated,
            base.records_capped,
        ),
        compare=summarize_group(
            request.compare_label or "Comparison",
            compare.outcomes,
            request.sources,
            compare.truncated,
            compare.records_capped,
        )
        if compare is not None
        else None,
    )
    return ToolSuccessPayload(data=data, text=describe_matrix(data))


async def handle_failure_cell(
    state: MCPServerState, request: FailureCellRequest
) -> FailureCellData:
    """List the sessions and repeated notes behind one matrix cell."""
    if request.snapshot_id is None:
        records = await _fetch_groups(state, request)
    else:
        # Drill into exactly the records the visible matrix was built from;
        # refetching could silently return a different membership.
        cached = state.matrix_snapshots.get(request.snapshot_id)
        if cached is None:
            raise MCPToolError(
                "snapshot_expired",
                "The matrix this cell belongs to is no longer cached.",
                recovery="Build the transition failure matrix again, then open "
                "the cell.",
            )
        records = cached
    groups = _analyze(request, records)
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
    records_capped: bool


async def _fetch_groups(
    state: MCPServerState, request: FailureMatrixRequest
) -> tuple[GroupRecords, ...]:
    filters = [request.filter]
    if request.compare_filter is not None:
        filters.append(request.compare_filter)
    return tuple(
        await asyncio.gather(
            *(
                fetch_group(
                    state.client,
                    session_filter,
                    request.max_sessions,
                    state.settings.pool_size,
                )
                for session_filter in filters
            )
        )
    )


def _analyze(
    request: FailureMatrixRequest, records: Sequence[GroupRecords]
) -> list[_AnalyzedGroup]:
    labeler = build_labeler(request.state_by, request.state_map)
    folded = fold_rare_states(
        [analyze_group(group, labeler, request.state_map) for group in records],
        request.sources,
    )
    return [
        _AnalyzedGroup(outcomes, group.truncated, group.records_capped)
        for outcomes, group in zip(folded, records, strict=True)
    ]


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
    # Split one group-wide budget evenly so snapshot memory stays bounded however
    # many sessions are read, without sessions racing for a shared counter.
    node_limit = min(MAX_NODES_PER_SESSION, MAX_NODES_PER_GROUP // max(len(ids), 1))

    async def nodes_of(session_id: uuid.UUID) -> tuple[list[SessionNodeResponse], bool]:
        async with limiter:
            iterator = client.sessions.iter_nodes(
                session_id, SessionNodeListParams(size=PAGE_SIZE)
            )
            return await _read_up_to(iterator, node_limit)

    (
        node_reads,
        (annotations, annotations_capped),
        (evaluations, evaluations_capped),
    ) = await asyncio.gather(
        asyncio.gather(*(nodes_of(session_id) for session_id in ids)),
        _collect_by_session(client.annotations.iter, AnnotationListParams, ids),
        _collect_by_session(client.evaluations.iter, EvaluationListParams, ids),
    )
    return GroupRecords(
        sessions=tuple(sessions),
        nodes={
            session_id: tuple(nodes)
            for session_id, (nodes, _) in zip(ids, node_reads, strict=True)
        },
        annotations=tuple(annotations),
        evaluations=tuple(evaluations),
        truncated=truncated,
        records_capped=annotations_capped
        or evaluations_capped
        or any(capped for _, capped in node_reads),
    )


async def _read_up_to(
    iterator: AsyncIterator[ItemT], limit: int
) -> tuple[list[ItemT], bool]:
    items: list[ItemT] = []
    async for item in iterator:
        if len(items) == limit:
            return items, True
        items.append(item)
    return items, False


async def _collect_by_session(
    iterate: Callable[[ParamsT], AsyncIterator[ItemT]],
    params_type: type[ParamsT],
    session_ids: Sequence[uuid.UUID],
) -> tuple[list[ItemT], bool]:
    async def chunk_items(start: int) -> tuple[list[ItemT], bool]:
        chunk = [str(i) for i in session_ids[start : start + IDS_PER_FILTER]]
        params = params_type(
            filter=FilterCondition(field="session_id", op=FilterOp.IN, value=chunk),
            size=PAGE_SIZE,
        )
        return await _read_up_to(iterate(params), MAX_RECORDS_PER_BATCH)

    chunks = await asyncio.gather(
        *(chunk_items(start) for start in range(0, len(session_ids), IDS_PER_FILTER))
    )
    return [item for items, _ in chunks for item in items], any(
        capped for _, capped in chunks
    )


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
    if group.records_capped:
        text += "; some very large sessions were only partly read"
    return text + "."


def _pair(cell: MatrixCell) -> str:
    return f"{cell.from_state} -> {cell.to_state}"


def _rate(cell: MatrixCell) -> str:
    if not cell.attempts:
        return ""
    return f" of {cell.attempts} tries ({round(100 * cell.count / cell.attempts)}%)"
