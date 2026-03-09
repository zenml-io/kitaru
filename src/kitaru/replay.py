"""Replay planning utilities.

This module translates Kitaru replay semantics (`from_` + overrides) into the
ZenML replay inputs consumed by `Pipeline.replay(...)`.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from math import inf
from typing import Any, Literal

from zenml.models import PipelineRunResponse, StepRunResponse

from kitaru.errors import KitaruRuntimeError, KitaruStateError, KitaruUsageError

_CHECKPOINT_SOURCE_ALIAS_PREFIX = "__kitaru_checkpoint_source_"
_CHECKPOINT_OVERRIDE_PREFIX = "checkpoint."
_WAIT_OVERRIDE_PREFIX = "wait."


@dataclass(frozen=True)
class ReplayPlan:
    """Resolved replay parameters ready for `Pipeline.replay(...)`."""

    original_run_id: str
    steps_to_skip: set[str]
    input_overrides: dict[str, Any]
    step_input_overrides: dict[str, dict[str, Any]]
    wait_overrides: dict[str, Any]


@dataclass(frozen=True)
class _OrderedCheckpoint:
    """One checkpoint invocation in durable execution order."""

    index: int
    invocation_id: str
    call_id: str
    name: str
    step: StepRunResponse


@dataclass(frozen=True)
class _WaitRecord:
    """One wait condition candidate for replay selection."""

    wait_id: str
    key: str
    created: datetime | None
    upstream_step_names: tuple[str, ...]
    downstream_step_names: tuple[str, ...]


def _normalize_checkpoint_name(step_name: str) -> str:
    if step_name.startswith(_CHECKPOINT_SOURCE_ALIAS_PREFIX):
        return step_name.removeprefix(_CHECKPOINT_SOURCE_ALIAS_PREFIX)
    return step_name


def _timestamp(value: datetime | None) -> float:
    if value is None:
        return inf
    try:
        return value.timestamp()
    except Exception:
        return inf


def _ordered_checkpoints(run: PipelineRunResponse) -> list[_OrderedCheckpoint]:
    ordered_items = sorted(
        enumerate(run.steps.values()),
        key=lambda item: (
            _timestamp(item[1].start_time or item[1].end_time),
            item[0],
        ),
    )

    ordered: list[_OrderedCheckpoint] = []
    for index, (_, step) in enumerate(ordered_items):
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


def _wait_records(wait_conditions: Sequence[Any] | None) -> list[_WaitRecord]:
    if not wait_conditions:
        return []

    records: list[_WaitRecord] = []
    for condition in wait_conditions:
        key = getattr(condition, "wait_condition_key", None)
        if not isinstance(key, str) or not key:
            continue
        wait_id = str(getattr(condition, "id", key))
        upstream = tuple(getattr(condition, "upstream_step_names", None) or ())
        downstream = tuple(getattr(condition, "downstream_step_names", None) or ())
        records.append(
            _WaitRecord(
                wait_id=wait_id,
                key=key,
                created=getattr(condition, "created", None),
                upstream_step_names=upstream,
                downstream_step_names=downstream,
            )
        )

    return sorted(records, key=lambda record: _timestamp(record.created))


def _available_checkpoint_selectors(checkpoints: Sequence[_OrderedCheckpoint]) -> str:
    names = sorted({checkpoint.name for checkpoint in checkpoints})
    if not names:
        return "none"
    return ", ".join(names)


def _available_wait_selectors(waits: Sequence[_WaitRecord]) -> str:
    names = sorted({wait.key for wait in waits})
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


def _wait_prefix(wait_key: str) -> str:
    return wait_key.split(":", maxsplit=1)[0]


def _resolve_wait_selector(selector: str, waits: Sequence[_WaitRecord]) -> _WaitRecord:
    exact_matches = [
        wait
        for wait in waits
        if selector
        in {
            wait.key,
            wait.wait_id,
        }
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise KitaruStateError(
            f"Replay selector '{selector}' matches multiple waits; use wait ID."
        )

    prefix_matches = [wait for wait in waits if _wait_prefix(wait.key) == selector]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    if len(prefix_matches) > 1:
        raise KitaruStateError(
            f"Replay selector '{selector}' matches multiple waits; use full wait key."
        )

    raise KitaruStateError(
        f"Unknown wait selector '{selector}'. Available waits: "
        f"{_available_wait_selectors(waits)}."
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
) -> tuple[dict[str, Any], dict[str, Any]]:
    checkpoint_overrides: dict[str, Any] = {}
    wait_overrides: dict[str, Any] = {}

    if not overrides:
        return checkpoint_overrides, wait_overrides

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

        if key.startswith(_WAIT_OVERRIDE_PREFIX):
            selector = key.removeprefix(_WAIT_OVERRIDE_PREFIX).strip()
            if not selector:
                raise KitaruUsageError(
                    "Wait override keys must include a selector after `wait.`."
                )
            wait_overrides[selector] = value
            continue

        raise KitaruUsageError(
            "Override keys must start with `checkpoint.` or `wait.`. "
            f"Received: {key!r}."
        )

    return checkpoint_overrides, wait_overrides


def _frontier_index_for_wait(
    *,
    wait: _WaitRecord,
    checkpoints: Sequence[_OrderedCheckpoint],
    index_by_invocation_id: Mapping[str, int],
) -> int:
    created_ts = _timestamp(wait.created)
    if created_ts != inf:
        for checkpoint in checkpoints:
            checkpoint_ts = _timestamp(
                checkpoint.step.start_time or checkpoint.step.end_time
            )
            if checkpoint_ts >= created_ts:
                return checkpoint.index
        return len(checkpoints)

    downstream_indexes = [
        index_by_invocation_id[name]
        for name in wait.downstream_step_names
        if name in index_by_invocation_id
    ]
    if downstream_indexes:
        return min(downstream_indexes)

    upstream_indexes = [
        index_by_invocation_id[name]
        for name in wait.upstream_step_names
        if name in index_by_invocation_id
    ]
    if upstream_indexes:
        return min(max(upstream_indexes) + 1, len(checkpoints))

    raise KitaruRuntimeError(
        "Unable to resolve replay order for wait "
        f"'{wait.key}': wait timing metadata is unavailable."
    )


def _resolve_replay_selector(
    *,
    selector: str,
    checkpoints: Sequence[_OrderedCheckpoint],
    waits: Sequence[_WaitRecord],
    index_by_invocation_id: Mapping[str, int],
) -> tuple[Literal["checkpoint", "wait"], int]:
    if not selector.strip():
        raise KitaruUsageError("`from_` must be a non-empty selector.")

    checkpoint_match: _OrderedCheckpoint | None = None
    wait_match: _WaitRecord | None = None

    try:
        checkpoint_match = _resolve_checkpoint_selector(selector, checkpoints)
    except KitaruStateError:
        checkpoint_match = None

    try:
        wait_match = _resolve_wait_selector(selector, waits)
    except KitaruStateError:
        wait_match = None

    if checkpoint_match and wait_match:
        raise KitaruStateError(
            "Replay selector matches both a checkpoint and a wait. "
            f"Use a more specific selector: {selector!r}."
        )
    if checkpoint_match:
        return "checkpoint", checkpoint_match.index
    if wait_match:
        return "wait", _frontier_index_for_wait(
            wait=wait_match,
            checkpoints=checkpoints,
            index_by_invocation_id=index_by_invocation_id,
        )

    raise KitaruStateError(
        f"Replay selector '{selector}' was not found. Available checkpoints: "
        f"{_available_checkpoint_selectors(checkpoints)}. Available waits: "
        f"{_available_wait_selectors(waits)}."
    )


def build_replay_plan(
    *,
    run: PipelineRunResponse,
    from_: str,
    overrides: Mapping[str, Any] | None = None,
    flow_inputs: Mapping[str, Any] | None = None,
    wait_conditions: Sequence[Any] | None = None,
) -> ReplayPlan:
    """Build a replay plan for a completed/paused execution.

    Args:
        run: Source execution to replay from.
        from_: Replay selector (`checkpoint`, invocation ID, call ID, wait key).
        overrides: Optional checkpoint/wait override map.
        flow_inputs: Optional flow input overrides.
        wait_conditions: Optional run wait conditions from backend listing.

    Returns:
        A resolved replay plan.
    """
    checkpoints = _ordered_checkpoints(run)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{run.id}' has no checkpoint history to replay."
        )

    waits = _wait_records(wait_conditions)
    checkpoint_overrides, wait_override_selectors = _split_overrides(overrides)

    index_by_invocation_id = {
        checkpoint.invocation_id: checkpoint.index for checkpoint in checkpoints
    }

    _, explicit_frontier = _resolve_replay_selector(
        selector=from_,
        checkpoints=checkpoints,
        waits=waits,
        index_by_invocation_id=index_by_invocation_id,
    )

    step_input_overrides: dict[str, dict[str, Any]] = {}
    frontier_candidates = [explicit_frontier]

    for selector, value in checkpoint_overrides.items():
        source = _resolve_checkpoint_selector(selector, checkpoints)
        consumers, consumer_indexes = _find_downstream_consumers(
            source=source,
            checkpoints=checkpoints,
        )
        for invocation_id, input_name in consumers:
            step_input_overrides.setdefault(invocation_id, {})[input_name] = value
        frontier_candidates.append(min(consumer_indexes))

    wait_overrides: dict[str, Any] = {}
    for selector, value in wait_override_selectors.items():
        wait_record = _resolve_wait_selector(selector, waits)
        wait_overrides[wait_record.key] = value
        frontier_candidates.append(
            _frontier_index_for_wait(
                wait=wait_record,
                checkpoints=checkpoints,
                index_by_invocation_id=index_by_invocation_id,
            )
        )

    replay_frontier = min(frontier_candidates)
    steps_to_skip = {
        checkpoint.invocation_id
        for checkpoint in checkpoints
        if checkpoint.index < replay_frontier
    }

    return ReplayPlan(
        original_run_id=str(run.id),
        steps_to_skip=steps_to_skip,
        input_overrides=dict(flow_inputs or {}),
        step_input_overrides=step_input_overrides,
        wait_overrides=wait_overrides,
    )


__all__ = ["ReplayPlan", "build_replay_plan"]
