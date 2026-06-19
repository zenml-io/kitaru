from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from kitaru._replay_verify_imported_models import ImportedReplayCase, RecordedCall
from kitaru._replay_verify_imported_sources.langfuse import (
    cases_from_langfuse_observations,
)


def _node_of(call: RecordedCall) -> str:
    node = call.metadata.get("langgraph_node") if call.metadata else None
    if isinstance(node, str) and node:
        return node
    return call.name


def key_calls_by_node(case: ImportedReplayCase) -> ImportedReplayCase:
    """Return a copy whose recorded_calls carry node + call_index.

    call_index is the 0-based position within a node, in recorded order
    (recorded_calls already arrive sorted by observation start time).
    """
    per_node_counter: dict[str, int] = {}
    keyed: list[RecordedCall] = []
    for call in case.recorded_calls:
        node = _node_of(call)
        index = per_node_counter.get(node, 0)
        per_node_counter[node] = index + 1
        keyed.append(replace(call, node=node, call_index=index))
    return replace(case, recorded_calls=keyed)


def import_trace(
    rows: Iterable[Mapping[str, Any]],
    *,
    trace_id: str | None = None,
) -> ImportedReplayCase:
    """Import one trace (rich per-observation Langfuse rows) into a keyed Case."""
    cases = cases_from_langfuse_observations(rows)
    if not cases:
        raise ValueError("No cases could be imported from the provided rows.")
    if trace_id is not None:
        cases = [c for c in cases if c.source_ref.source_id == trace_id]
        if not cases:
            raise ValueError(f"Trace id {trace_id!r} not found in rows.")
    if len(cases) > 1:
        raise ValueError(
            "Multiple traces found; pass trace_id= to select one "
            f"({', '.join(c.source_ref.source_id for c in cases)})."
        )
    return key_calls_by_node(cases[0])
