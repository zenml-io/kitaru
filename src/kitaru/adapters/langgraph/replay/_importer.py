from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from kitaru._replay_verify_imported_models import ImportedReplayCase, RecordedCall
from kitaru._replay_verify_imported_sources.langfuse import (
    cases_from_langfuse_observations,
)

_NODE_METADATA_KEY = "langgraph_node"


def _first_str(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _row_id(row: Mapping[str, Any]) -> str | None:
    return _first_str(row, "id", "observationId", "observation_id")


def _row_parent(row: Mapping[str, Any]) -> str | None:
    return _first_str(row, "parentObservationId", "parent_observation_id")


def _row_name(row: Mapping[str, Any]) -> str | None:
    return _first_str(row, "name")


def _row_metadata(row: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = row.get("metadata")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            return {}
    return metadata if isinstance(metadata, Mapping) else {}


def _own_node(row: Mapping[str, Any]) -> str | None:
    node = _row_metadata(row).get(_NODE_METADATA_KEY)
    return node if isinstance(node, str) and node else None


def build_node_map(rows: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    """Map each observation id to its enclosing LangGraph node, via the trace tree.

    Real LangChain/Langfuse call observations (GENERATION/TOOL) do NOT carry
    ``langgraph_node`` on themselves — the agent threads an explicit
    ``config={'metadata': ...}`` that drops the inherited node metadata, so the
    node name survives only on the node-level span. Resolution per observation:

    1. the observation's own metadata ``langgraph_node`` (synthetic / propagated);
    2. the nearest ancestor whose metadata carries ``langgraph_node``;
    3. the ``name`` of the nearest ancestor that is a direct child of the trace
       root (the node-level span).

    Root-level observations with no enclosing node are omitted.
    """
    by_id: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        row_id = _row_id(row)
        if row_id:
            by_id[row_id] = row

    root_ids = {
        row_id
        for row_id, row in by_id.items()
        if _row_parent(row) is None or _row_parent(row) not in by_id
    }

    node_map: dict[str, str] = {}
    for row_id, row in by_id.items():
        own = _own_node(row)
        if own:
            node_map[row_id] = own
            continue

        current = row
        # Bounded walk up the parent chain (guard against malformed cycles).
        for _ in range(len(by_id) + 1):
            parent_id = _row_parent(current)
            if parent_id is None or parent_id not in by_id:
                break  # reached a root-level observation: no enclosing node
            parent = by_id[parent_id]
            parent_node = _own_node(parent)
            if parent_node:
                node_map[row_id] = parent_node
                break
            if parent_id in root_ids:
                # `current` is a direct child of the root: the node-level span.
                label = _own_node(current) or _row_name(current)
                if label:
                    node_map[row_id] = label
                break
            current = parent

    return node_map


def _call_own_node(call: RecordedCall) -> str | None:
    node = call.metadata.get(_NODE_METADATA_KEY) if call.metadata else None
    return node if isinstance(node, str) and node else None


def key_calls_by_node(
    case: ImportedReplayCase,
    node_by_observation: Mapping[str, str] | None = None,
) -> ImportedReplayCase:
    """Return a copy whose recorded_calls carry node + call_index.

    Node attribution prefers ``node_by_observation`` (the trace-tree map keyed by
    observation id), then the call's own ``langgraph_node`` metadata, then the
    call name as a last resort. ``call_index`` is the 0-based position within a
    node, in recorded order (recorded_calls arrive sorted by start time).
    """
    node_by_observation = node_by_observation or {}
    per_node_counter: dict[str, int] = {}
    keyed: list[RecordedCall] = []
    for call in case.recorded_calls:
        node: str | None = None
        if call.observation_id:
            node = node_by_observation.get(call.observation_id)
        if not node:
            node = _call_own_node(call) or call.name
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
    rows = list(rows)
    node_map = build_node_map(rows)
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
    return key_calls_by_node(cases[0], node_map)
