"""Replay planning utilities.

This module translates Kitaru replay semantics (``from_`` + overrides) into a
backend-neutral ``ReplayPlan``.  The plan is consumed by whichever backend
actually executes the replay (ZenML ``Pipeline.replay(...)`` today, Dapr
workflow restart in the future).

Ordering uses DAG topology derived from ``CheckpointGraphNode.upstream_invocation_ids``.
Timestamps are used only as a last-resort fallback for legacy runs missing
topology metadata.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from math import inf
from typing import Any

from kitaru.engines._types import CheckpointGraphNode, ExecutionGraphSnapshot
from kitaru.errors import KitaruRuntimeError, KitaruStateError, KitaruUsageError

_CHECKPOINT_OVERRIDE_PREFIX = "checkpoint."


@dataclass(frozen=True)
class ReplayPlan:
    """Resolved replay parameters ready for backend execution."""

    original_run_id: str
    steps_to_skip: set[str]
    input_overrides: dict[str, Any]
    step_input_overrides: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class _OrderedCheckpoint:
    """One checkpoint invocation in DAG-topological order."""

    index: int
    node: CheckpointGraphNode

    @property
    def invocation_id(self) -> str:
        return self.node.invocation_id

    @property
    def call_id(self) -> str:
        return self.node.call_id

    @property
    def name(self) -> str:
        return self.node.name


def _timestamp(value: datetime | None) -> float:
    if value is None:
        return inf
    try:
        return value.timestamp()
    except Exception:
        return inf


def _topo_sort_nodes(
    nodes: Sequence[CheckpointGraphNode],
) -> list[CheckpointGraphNode]:
    """Topologically sort checkpoint nodes using upstream edges.

    Within a topological layer, nodes are sorted by invocation ID for
    determinism. Falls back to timestamp ordering when upstream metadata
    is missing from all nodes.
    """
    if not nodes:
        return []

    by_invocation: dict[str, CheckpointGraphNode] = {}
    children: dict[str, list[str]] = defaultdict(list)
    parent_count: dict[str, int] = {}
    has_any_upstream = False

    for node in nodes:
        by_invocation[node.invocation_id] = node

    for inv_id, node in by_invocation.items():
        valid_parents = [p for p in node.upstream_invocation_ids if p in by_invocation]
        parent_count[inv_id] = len(valid_parents)
        if valid_parents:
            has_any_upstream = True
        for parent in valid_parents:
            children[parent].append(inv_id)

    if not has_any_upstream:
        return sorted(
            nodes,
            key=lambda n: (
                _timestamp(n.start_time or n.end_time),
                n.invocation_id,
            ),
        )

    # Kahn's algorithm with deterministic layer ordering.
    sorted_nodes: list[CheckpointGraphNode] = []
    layer = sorted(
        [inv_id for inv_id, count in parent_count.items() if count == 0],
    )

    while layer:
        for inv_id in layer:
            sorted_nodes.append(by_invocation[inv_id])

        next_layer: list[str] = []
        for inv_id in layer:
            for child in children.get(inv_id, []):
                parent_count[child] -= 1
                if parent_count[child] == 0:
                    next_layer.append(child)
        layer = sorted(next_layer)

    if len(sorted_nodes) < len(by_invocation):
        raise KitaruRuntimeError(
            "Step dependency graph contains a cycle; cannot determine replay order."
        )

    return sorted_nodes


def _ordered_checkpoints(
    snapshot: ExecutionGraphSnapshot,
) -> list[_OrderedCheckpoint]:
    """Build checkpoint list in DAG-topological order."""
    sorted_nodes = _topo_sort_nodes(snapshot.checkpoints)

    return [
        _OrderedCheckpoint(index=index, node=node)
        for index, node in enumerate(sorted_nodes)
    ]


def _available_checkpoint_selectors(checkpoints: Sequence[_OrderedCheckpoint]) -> str:
    names = sorted({checkpoint.name for checkpoint in checkpoints})
    if not names:
        return "none"
    return ", ".join(names)


def _resolve_checkpoint_selector(
    selector: str,
    checkpoints: Sequence[_OrderedCheckpoint],
) -> _OrderedCheckpoint:
    matches = [
        checkpoint
        for checkpoint in checkpoints
        if selector
        in {
            checkpoint.name,
            checkpoint.invocation_id,
            checkpoint.call_id,
        }
    ]

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise KitaruStateError(
            "Replay selector is ambiguous for checkpoint "
            f"'{selector}'. Use a checkpoint call ID instead."
        )

    raise KitaruStateError(
        f"Unknown checkpoint selector '{selector}'. Available checkpoints: "
        f"{_available_checkpoint_selectors(checkpoints)}."
    )


def _single_checkpoint_output_name(checkpoint: _OrderedCheckpoint) -> str:
    output_names = list(checkpoint.node.output_names)

    if not output_names:
        raise KitaruStateError(
            f"Checkpoint '{checkpoint.name}' does not expose replayable outputs."
        )
    if len(output_names) > 1:
        raise KitaruUsageError(
            "Checkpoint overrides currently require single-output checkpoints. "
            f"Checkpoint '{checkpoint.name}' has outputs: {', '.join(output_names)}."
        )
    return output_names[0]


def _find_downstream_consumers(
    *,
    source: _OrderedCheckpoint,
    checkpoints: Sequence[_OrderedCheckpoint],
) -> tuple[list[tuple[str, str]], list[int]]:
    output_name = _single_checkpoint_output_name(source)

    consumers: list[tuple[str, str]] = []
    consumer_indices: list[int] = []
    for checkpoint in checkpoints:
        if checkpoint.index <= source.index:
            continue

        for binding in checkpoint.node.input_bindings:
            if binding.upstream_invocation_id != source.invocation_id:
                continue
            if binding.upstream_output_name != output_name:
                continue

            consumers.append((checkpoint.invocation_id, binding.input_name))
            consumer_indices.append(checkpoint.index)

    if not consumers:
        raise KitaruStateError(
            "Checkpoint override has no downstream consumer in this execution: "
            f"{source.name}."
        )

    return consumers, consumer_indices


def _split_overrides(
    overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    checkpoint_overrides: dict[str, Any] = {}

    if not overrides:
        return checkpoint_overrides

    for key, value in overrides.items():
        if key.startswith(_CHECKPOINT_OVERRIDE_PREFIX):
            selector = key.removeprefix(_CHECKPOINT_OVERRIDE_PREFIX).strip()
            if not selector:
                raise KitaruUsageError(
                    "Checkpoint override keys must include a selector after "
                    "`checkpoint.`."
                )
            checkpoint_overrides[selector] = value
            continue

        if key.startswith("wait."):
            raise KitaruUsageError(
                "Wait overrides (`wait.*`) are not supported in replay. "
                "If the replayed execution reaches a wait, resolve it "
                "via `client.executions.input(...)` or "
                "`kitaru executions input`."
            )

        raise KitaruUsageError(
            f"Override keys must start with `checkpoint.`. Received: {key!r}."
        )

    return checkpoint_overrides


def build_replay_plan(
    *,
    snapshot: ExecutionGraphSnapshot,
    from_: str,
    overrides: Mapping[str, Any] | None = None,
    flow_inputs: Mapping[str, Any] | None = None,
) -> ReplayPlan:
    """Build a replay plan for a completed/paused execution.

    Args:
        snapshot: Backend-neutral execution graph to replay from.
        from_: Checkpoint selector (checkpoint name, invocation ID, or call ID).
        overrides: Optional checkpoint override map (``checkpoint.*`` keys).
        flow_inputs: Optional flow input overrides.

    Returns:
        A resolved replay plan.

    Raises:
        KitaruStateError: If the plan would place a step in both
            ``steps_to_skip`` and ``step_input_overrides``.
    """
    checkpoints = _ordered_checkpoints(snapshot)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{snapshot.exec_id}' has no checkpoint history to replay."
        )

    if not from_.strip():
        raise KitaruUsageError("`from_` must be a non-empty selector.")

    checkpoint_overrides = _split_overrides(overrides)

    explicit_checkpoint = _resolve_checkpoint_selector(from_, checkpoints)

    step_input_overrides: dict[str, dict[str, Any]] = {}
    frontier_candidates = [explicit_checkpoint.index]

    for selector, value in checkpoint_overrides.items():
        source = _resolve_checkpoint_selector(selector, checkpoints)
        consumers, _consumer_indexes = _find_downstream_consumers(
            source=source,
            checkpoints=checkpoints,
        )
        for invocation_id, input_name in consumers:
            step_input_overrides.setdefault(invocation_id, {})[input_name] = value
        frontier_candidates.append(source.index)

    replay_frontier = min(frontier_candidates)
    steps_to_skip = {
        checkpoint.invocation_id
        for checkpoint in checkpoints
        if checkpoint.index < replay_frontier
    }

    # Safety: if a step has input overrides it must not be skipped, or the
    # override would be silently discarded by the backend.
    overlap = steps_to_skip & set(step_input_overrides)
    if overlap:
        steps_to_skip -= overlap

    return ReplayPlan(
        original_run_id=snapshot.exec_id,
        steps_to_skip=steps_to_skip,
        input_overrides=dict(flow_inputs or {}),
        step_input_overrides=step_input_overrides,
    )


__all__ = ["ReplayPlan", "build_replay_plan"]
