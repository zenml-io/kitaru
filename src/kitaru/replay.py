"""Replay planning utilities.

This module translates Kitaru replay semantics (`from_` + overrides) into the
ZenML replay inputs consumed by `Pipeline.replay(...)`.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from zenml.models import PipelineRunResponse, StepRunResponse

from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.errors import KitaruStateError, KitaruUsageError

_CHECKPOINT_OVERRIDE_PREFIX = "checkpoint."


@dataclass(frozen=True)
class ReplayPlan:
    """Resolved replay parameters ready for `Pipeline.replay(...)`."""

    original_run_id: str
    steps_to_skip: set[str]
    input_overrides: dict[str, Any]
    step_input_overrides: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class _Checkpoint:
    """Checkpoint metadata used during replay planning."""

    invocation_id: str
    call_id: str
    name: str
    step: StepRunResponse


def _checkpoint_invocation_id(step: StepRunResponse) -> str:
    """Return invocation ID for a step, falling back to the step name."""
    invocation_id = getattr(getattr(step, "spec", None), "invocation_id", None)
    if not isinstance(invocation_id, str) or not invocation_id:
        return step.name
    return invocation_id


def _checkpoints(run: PipelineRunResponse) -> list[_Checkpoint]:
    """Build checkpoint metadata list from run steps."""
    checkpoints: list[_Checkpoint] = []
    for step in run.steps.values():
        checkpoints.append(
            _Checkpoint(
                invocation_id=_checkpoint_invocation_id(step),
                call_id=str(step.id),
                name=_normalize_checkpoint_name(step.name),
                step=step,
            )
        )
    return checkpoints


def _available_checkpoint_selectors(checkpoints: Sequence[_Checkpoint]) -> str:
    names = sorted({checkpoint.name for checkpoint in checkpoints})
    if not names:
        return "none"
    return ", ".join(names)


def _resolve_checkpoint_selector(
    selector: str,
    checkpoints: Sequence[_Checkpoint],
) -> _Checkpoint:
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


def _single_checkpoint_output_name(checkpoint: _Checkpoint) -> str:
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
    source: _Checkpoint,
    checkpoints: Sequence[_Checkpoint],
) -> list[tuple[str, str]]:
    output_name = _single_checkpoint_output_name(source)

    consumers: list[tuple[str, str]] = []
    for checkpoint in checkpoints:
        for input_name, input_spec in _iter_step_input_specs(checkpoint.step):
            upstream_name = getattr(input_spec, "step_name", None)
            upstream_output_name = getattr(input_spec, "output_name", None)
            if upstream_name != source.invocation_id:
                continue
            if upstream_output_name != output_name:
                continue

            consumers.append((checkpoint.invocation_id, input_name))

    if not consumers:
        raise KitaruStateError(
            "Checkpoint override has no downstream consumer in this execution: "
            f"{source.name}."
        )

    return consumers


def _build_children_by_invocation(
    checkpoints: Sequence[_Checkpoint],
) -> dict[str, set[str]]:
    """Build parent->children adjacency for checkpoints in this run."""
    checkpoints_by_invocation = {
        checkpoint.invocation_id: checkpoint for checkpoint in checkpoints
    }
    children_by_invocation: dict[str, set[str]] = {
        invocation_id: set() for invocation_id in checkpoints_by_invocation
    }

    for checkpoint in checkpoints:
        upstream_steps: Sequence[str] = (
            getattr(getattr(checkpoint.step, "spec", None), "upstream_steps", None)
            or ()
        )
        for upstream_invocation_id in upstream_steps:
            if upstream_invocation_id not in checkpoints_by_invocation:
                continue
            children_by_invocation[upstream_invocation_id].add(checkpoint.invocation_id)

    return children_by_invocation


def _collect_descendants(
    *,
    roots: set[str],
    children_by_invocation: Mapping[str, set[str]],
) -> set[str]:
    """Collect all transitive descendants for the provided roots."""
    descendants: set[str] = set()
    to_visit = list(roots)

    while to_visit:
        invocation_id = to_visit.pop()
        for child_invocation_id in children_by_invocation.get(invocation_id, set()):
            if child_invocation_id in descendants:
                continue
            descendants.add(child_invocation_id)
            to_visit.append(child_invocation_id)

    return descendants


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
    from_: str | None = None,
    skip: Sequence[str] | None = None,
    overrides: Mapping[str, Any] | None = None,
    flow_inputs: Mapping[str, Any] | None = None,
) -> ReplayPlan:
    """Build a replay plan for a completed/paused execution.

    Exactly one of ``from_`` or ``skip`` must be provided.

    When ``from_`` is given, replay starts from the explicit checkpoint.
    For checkpoint overrides, the direct consumers of each overridden source
    are added as replay roots (the source itself is not forced to re-execute).

    When ``skip`` is given, the named checkpoints are frozen (kept cached)
    and everything else re-executes.

    Args:
        run: Source execution to replay from.
        from_: Checkpoint selector (checkpoint name, invocation ID, or call ID).
            Mutually exclusive with ``skip``.
        skip: List of checkpoint selectors to keep cached (freeze). Everything
            else re-executes. Mutually exclusive with ``from_``.
        overrides: Optional checkpoint override map (`checkpoint.*` keys).
        flow_inputs: Optional flow input overrides.

    Returns:
        A resolved replay plan.

    Raises:
        KitaruStateError: If replay planning fails due to invalid run state.
        KitaruUsageError: If replay planning fails due to invalid usage.
    """
    checkpoints = _checkpoints(run)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{run.id}' has no checkpoint history to replay."
        )

    if (from_ is None) == (skip is None):
        raise KitaruUsageError("Provide exactly one of `from_` or `skip`.")

    checkpoint_overrides = _split_overrides(overrides)

    if skip is not None:
        if checkpoint_overrides:
            raise KitaruUsageError(
                "`overrides` is only supported with `from_`, not `skip`."
            )
        frozen = {
            _resolve_checkpoint_selector(sel, checkpoints).invocation_id for sel in skip
        }
        all_steps = {cp.invocation_id for cp in checkpoints}
        steps_to_skip = frozen & all_steps
        return ReplayPlan(
            original_run_id=str(run.id),
            steps_to_skip=steps_to_skip,
            input_overrides=dict(flow_inputs or {}),
            step_input_overrides={},
        )

    if not from_ or not from_.strip():
        raise KitaruUsageError("`from_` must be a non-empty selector.")

    explicit_checkpoint = _resolve_checkpoint_selector(from_, checkpoints)

    step_input_overrides: dict[str, dict[str, Any]] = {}
    replay_roots = {explicit_checkpoint.invocation_id}

    for selector, value in checkpoint_overrides.items():
        source = _resolve_checkpoint_selector(selector, checkpoints)
        consumers = _find_downstream_consumers(
            source=source,
            checkpoints=checkpoints,
        )
        for invocation_id, input_name in consumers:
            step_input_overrides.setdefault(invocation_id, {})[input_name] = value
            replay_roots.add(invocation_id)

    children_by_invocation = _build_children_by_invocation(checkpoints)
    steps_to_reexecute = replay_roots | _collect_descendants(
        roots=replay_roots,
        children_by_invocation=children_by_invocation,
    )
    all_steps = {checkpoint.invocation_id for checkpoint in checkpoints}
    steps_to_skip = all_steps - steps_to_reexecute

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
