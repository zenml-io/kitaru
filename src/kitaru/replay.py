"""Replay planning utilities.

This module translates Kitaru replay semantics (`from_` + overrides) into the
ZenML replay inputs consumed by `Pipeline.replay(...)`.

Ordering uses DAG topology derived from ``StepSpec.upstream_steps``, matching
ZenML's own ``Compiler._get_sorted_invocations()`` strategy. Timestamps are
used only as a last-resort fallback for legacy runs missing topology metadata.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from math import inf
from typing import Any

from zenml.models import PipelineRunResponse, StepRunResponse

from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.errors import KitaruRuntimeError, KitaruStateError, KitaruUsageError

_CHECKPOINT_OVERRIDE_PREFIX = "checkpoint."


@dataclass(frozen=True)
class ReplayPlan:
    """Resolved replay parameters ready for `Pipeline.replay(...)`."""

    original_run_id: str
    steps_to_skip: set[str]
    input_overrides: dict[str, Any]
    step_input_overrides: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class _OrderedCheckpoint:
    """One checkpoint invocation in DAG-topological order."""

    index: int
    invocation_id: str
    call_id: str
    name: str
    step: StepRunResponse


def _timestamp(value: datetime | None) -> float:
    if value is None:
        return inf
    try:
        return value.timestamp()
    except Exception:
        return inf


def _topo_sort_steps(
    steps: Mapping[str, StepRunResponse],
) -> list[StepRunResponse]:
    """Topologically sort steps using ``upstream_steps`` from step specs.

    Within a topological layer, steps are sorted by invocation ID for
    determinism. Falls back to timestamp ordering when upstream metadata
    is missing from all steps.
    """
    step_list = list(steps.values())
    if not step_list:
        return []

    # Build an invocation-keyed lookup. Steps are keyed by name in the run
    # dict, but their spec carries the invocation_id used in upstream_steps.
    by_invocation: dict[str, StepRunResponse] = {}
    children: dict[str, list[str]] = defaultdict(list)
    parent_count: dict[str, int] = {}
    has_any_upstream = False

    for step in step_list:
        spec = getattr(step, "spec", None)
        inv_id = getattr(spec, "invocation_id", None)
        if not isinstance(inv_id, str) or not inv_id:
            inv_id = step.name
        by_invocation[inv_id] = step

    for inv_id, step in by_invocation.items():
        spec = getattr(step, "spec", None)
        upstream: Sequence[str] = getattr(spec, "upstream_steps", None) or ()
        # Only count parents that are actually present in this run.
        valid_parents = [p for p in upstream if p in by_invocation]
        parent_count[inv_id] = len(valid_parents)
        if valid_parents:
            has_any_upstream = True
        for parent in valid_parents:
            children[parent].append(inv_id)

    if not has_any_upstream:
        # No topology metadata — fall back to timestamp ordering.
        return sorted(
            step_list,
            key=lambda s: (
                _timestamp(s.start_time or s.end_time),
                getattr(getattr(s, "spec", None), "invocation_id", s.name),
            ),
        )

    # Kahn's algorithm with deterministic layer ordering.
    sorted_steps: list[StepRunResponse] = []
    layer = sorted(
        [inv_id for inv_id, count in parent_count.items() if count == 0],
    )

    while layer:
        for inv_id in layer:
            sorted_steps.append(by_invocation[inv_id])

        next_layer: list[str] = []
        for inv_id in layer:
            for child in children.get(inv_id, []):
                parent_count[child] -= 1
                if parent_count[child] == 0:
                    next_layer.append(child)
        layer = sorted(next_layer)

    if len(sorted_steps) < len(by_invocation):
        raise KitaruRuntimeError(
            "Step dependency graph contains a cycle; cannot determine replay order."
        )

    return sorted_steps


def _ordered_checkpoints(run: PipelineRunResponse) -> list[_OrderedCheckpoint]:
    """Build checkpoint list in DAG-topological order."""
    sorted_steps = _topo_sort_steps(run.steps)

    ordered: list[_OrderedCheckpoint] = []
    for index, step in enumerate(sorted_steps):
        invocation_id = getattr(getattr(step, "spec", None), "invocation_id", None)
        if not isinstance(invocation_id, str) or not invocation_id:
            invocation_id = step.name

        ordered.append(
            _OrderedCheckpoint(
                index=index,
                invocation_id=invocation_id,
                call_id=str(step.id),
                name=_normalize_checkpoint_name(step.name),
                step=step,
            )
        )
    return ordered


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


def _iter_step_input_specs(step: StepRunResponse) -> Iterator[tuple[str, Any]]:
    step_spec = getattr(step, "spec", None)
    if step_spec is None:
        return

    inputs_v2 = getattr(step_spec, "inputs_v2", None)
    if isinstance(inputs_v2, Mapping):
        for input_name, input_specs in inputs_v2.items():
            for input_spec in input_specs:
                yield input_name, input_spec
        return

    legacy_inputs = getattr(step_spec, "inputs", None)
    if not isinstance(legacy_inputs, Mapping):
        return

    for input_name, raw_input_specs in legacy_inputs.items():
        if isinstance(raw_input_specs, Sequence) and not isinstance(
            raw_input_specs, (str, bytes)
        ):
            iterable: Iterable[Any] = raw_input_specs
        else:
            iterable = [raw_input_specs]
        for input_spec in iterable:
            yield input_name, input_spec


def _single_checkpoint_output_name(checkpoint: _OrderedCheckpoint) -> str:
    try:
        output_names = list(checkpoint.step.regular_outputs)
    except Exception:
        outputs = getattr(checkpoint.step, "outputs", None)
        output_names = list(outputs or {})

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

        for input_name, input_spec in _iter_step_input_specs(checkpoint.step):
            upstream_name = getattr(input_spec, "step_name", None)
            upstream_output_name = getattr(input_spec, "output_name", None)
            if upstream_name != source.invocation_id:
                continue
            if upstream_output_name != output_name:
                continue

            consumers.append((checkpoint.invocation_id, input_name))
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
    run: PipelineRunResponse,
    from_: str,
    overrides: Mapping[str, Any] | None = None,
    flow_inputs: Mapping[str, Any] | None = None,
) -> ReplayPlan:
    """Build a replay plan for a completed/paused execution.

    Args:
        run: Source execution to replay from.
        from_: Checkpoint selector (checkpoint name, invocation ID, or call ID).
        overrides: Optional checkpoint override map (`checkpoint.*` keys).
        flow_inputs: Optional flow input overrides.

    Returns:
        A resolved replay plan.

    Raises:
        KitaruStateError: If the plan would place a step in both
            ``steps_to_skip`` and ``step_input_overrides``. ZenML's explicit
            skip wins unconditionally and would silently discard the override.
    """
    checkpoints = _ordered_checkpoints(run)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{run.id}' has no checkpoint history to replay."
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
        # Anchor frontier at the source checkpoint, not the first consumer.
        # This keeps the source out of steps_to_skip so ZenML re-executes it.
        frontier_candidates.append(source.index)

    replay_frontier = min(frontier_candidates)
    steps_to_skip = {
        checkpoint.invocation_id
        for checkpoint in checkpoints
        if checkpoint.index < replay_frontier
    }

    # Safety check: ZenML's explicit steps_to_skip wins unconditionally — it
    # does NOT check for step_input_overrides. If a step appears in both sets,
    # the override would be silently discarded.
    overlap = steps_to_skip & set(step_input_overrides)
    if overlap:
        steps_to_skip -= overlap

    return ReplayPlan(
        original_run_id=str(run.id),
        steps_to_skip=steps_to_skip,
        input_overrides=dict(flow_inputs or {}),
        step_input_overrides=step_input_overrides,
    )


__all__ = ["ReplayPlan", "build_replay_plan"]
