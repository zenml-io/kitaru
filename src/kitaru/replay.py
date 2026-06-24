"""Replay planning utilities.

Translates Kitaru replay semantics (`at`, `input`, `output`, `tool`, `llm_model`)
into ZenML replay inputs consumed by ``Pipeline.replay(...)``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from zenml.models import PipelineRunResponse, StepRunResponse

from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.replay_context import ReplayRuntimeContext

_TOOL_CHECKPOINT_SUFFIX = re.compile(r"_tool(?:_\d+)?$")

REPLAY_RESERVED_KWARGS = frozenset(
    {
        "at",
        "input",
        "output",
        "tool",
        "llm_model",
        "skip",
        "stack",
        "image",
        "cache",
        "retries",
    }
)


@dataclass(frozen=True)
class ReplayPlan:
    """Resolved replay parameters ready for ``Pipeline.replay(...)``."""

    original_run_id: str
    steps_to_skip: set[str]
    input_overrides: dict[str, Any]
    step_input_overrides: dict[str, dict[str, Any]]
    runtime_context: ReplayRuntimeContext


ReplayAtStatus = Literal["present", "missing", "ambiguous", "no_checkpoints"]


@dataclass(frozen=True)
class ReplayManyResult:
    """Batch replay outcome for ``flow.replay_many(...)``."""

    at: str
    successes: list[tuple[str, Any]]
    failures: list[tuple[str, str]]
    skipped: list[tuple[str, str]]

    def wait(self) -> ReplayManyResult:
        """Block until every successful replay handle completes."""
        for _, handle in self.successes:
            handle.wait()
        return self


@dataclass(frozen=True)
class _Checkpoint:
    """Checkpoint metadata used during replay planning."""

    invocation_id: str
    call_id: str
    name: str
    step: StepRunResponse
    started_at: datetime | None
    checkpoint_type: str | None


def _checkpoint_invocation_id(step: StepRunResponse) -> str:
    invocation_id = getattr(getattr(step, "spec", None), "invocation_id", None)
    if not isinstance(invocation_id, str) or not invocation_id:
        return step.name
    return invocation_id


def _checkpoint_type(step: StepRunResponse) -> str | None:
    step_type = getattr(step, "type", None)
    if step_type is None:
        return None
    value = getattr(step_type, "value", step_type)
    return str(value) if value is not None else None


def _checkpoints(run: PipelineRunResponse) -> list[_Checkpoint]:
    checkpoints: list[_Checkpoint] = []
    for step in run.steps.values():
        checkpoints.append(
            _Checkpoint(
                invocation_id=_checkpoint_invocation_id(step),
                call_id=str(step.id),
                name=_normalize_checkpoint_name(step.name),
                step=step,
                started_at=getattr(step, "start_time", None),
                checkpoint_type=_checkpoint_type(step),
            )
        )
    return checkpoints


def _available_checkpoint_selectors(checkpoints: Sequence[_Checkpoint]) -> str:
    names = sorted({checkpoint.name for checkpoint in checkpoints})
    if not names:
        return "none"
    return ", ".join(names)


def replay_at_status(
    *,
    run: PipelineRunResponse,
    at: str,
) -> ReplayAtStatus:
    """Return whether ``at`` resolves for a source execution."""
    checkpoints = _checkpoints(run)
    if not checkpoints:
        return "no_checkpoints"

    matches = [
        checkpoint
        for checkpoint in checkpoints
        if at
        in {
            checkpoint.name,
            checkpoint.invocation_id,
            checkpoint.call_id,
        }
    ]
    if len(matches) == 1:
        return "present"
    if len(matches) > 1:
        return "ambiguous"
    return "missing"


def replay_at_skip_reason(*, run: PipelineRunResponse, at: str) -> str:
    """Return a human-readable skip/failure reason for a replay ``at`` selector."""
    status = replay_at_status(run=run, at=at)
    if status == "no_checkpoints":
        return f"Execution '{run.id}' has no checkpoint history to replay."
    if status == "missing":
        checkpoints = _checkpoints(run)
        return (
            f"Unknown checkpoint selector '{at}'. Available checkpoints: "
            f"{_available_checkpoint_selectors(checkpoints)}."
        )
    call_ids = ", ".join(
        sorted(
            {
                checkpoint.call_id
                for checkpoint in _checkpoints(run)
                if at
                in {
                    checkpoint.name,
                    checkpoint.invocation_id,
                    checkpoint.call_id,
                }
            }
        )
    )
    return (
        f"Replay selector '{at}' is ambiguous. Use a checkpoint call ID instead. "
        f"Matching call IDs: {call_ids}."
    )


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
        call_ids = ", ".join(sorted({match.call_id for match in matches}))
        raise KitaruStateError(
            "Replay selector is ambiguous for checkpoint "
            f"'{selector}'. Use a checkpoint call ID instead. "
            f"Matching call IDs: {call_ids}."
        )

    raise KitaruStateError(
        f"Unknown checkpoint selector '{selector}'. Available checkpoints: "
        f"{_available_checkpoint_selectors(checkpoints)}."
    )


def _normalized_tool_name(name: str) -> str:
    """Normalize adapter tool checkpoint names for replay selectors."""
    if _TOOL_CHECKPOINT_SUFFIX.search(name):
        return _TOOL_CHECKPOINT_SUFFIX.sub("", name)
    return name.removesuffix("_tool")


def _tool_key_matches_checkpoint(tool_key: str, checkpoint: _Checkpoint) -> bool:
    normalized_key = _normalized_tool_name(tool_key.removesuffix("_tool"))
    normalized_name = _normalized_tool_name(checkpoint.name)
    return normalized_key == normalized_name or tool_key in {
        checkpoint.name,
        checkpoint.invocation_id,
        checkpoint.call_id,
    }


def _checkpoints_for_tool_key(
    tool_key: str,
    checkpoints: Sequence[_Checkpoint],
) -> list[_Checkpoint]:
    matches = [
        checkpoint
        for checkpoint in checkpoints
        if _tool_key_matches_checkpoint(tool_key, checkpoint)
    ]
    return matches


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
            "Checkpoint output overrides currently require single-output "
            f"checkpoints. Checkpoint '{checkpoint.name}' has outputs: "
            f"{', '.join(output_names)}."
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
            "Checkpoint output override has no downstream consumer in this "
            f"execution: {source.name}."
        )

    return consumers


def _build_children_by_invocation(
    checkpoints: Sequence[_Checkpoint],
) -> dict[str, set[str]]:
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


def _checkpoint_input_slot_names(checkpoint: _Checkpoint) -> set[str]:
    if checkpoint.checkpoint_type == "tool_call":
        return {"tool_args"}
    if checkpoint.checkpoint_type == "llm_call":
        return {"messages", "input", "user_prompt", "message_history"}
    names: set[str] = set()
    for input_name, _ in _iter_step_input_specs(checkpoint.step):
        names.add(input_name)
    return names or {"input"}


def _normalize_checkpoint_input_value(
    checkpoint: _Checkpoint,
    value: Any,
) -> dict[str, Any]:
    if isinstance(value, Mapping):
        slot_names = _checkpoint_input_slot_names(checkpoint)
        if slot_names.intersection(value.keys()):
            return dict(value)
        if checkpoint.checkpoint_type == "tool_call":
            return {"tool_args": value}
        if checkpoint.checkpoint_type == "llm_call":
            return {"messages": value}
        if len(slot_names) == 1:
            slot_name = next(iter(slot_names))
            return {slot_name: value}
        return dict(value)
    slot_names = _checkpoint_input_slot_names(checkpoint)
    if len(slot_names) == 1:
        return {next(iter(slot_names)): value}
    raise KitaruUsageError(
        f"Checkpoint input override for '{checkpoint.name}' must be a mapping "
        f"with one of: {', '.join(sorted(slot_names))}."
    )


def _resolve_skip_invocation_ids(
    skip: Sequence[str] | None,
    checkpoints: Sequence[_Checkpoint],
) -> set[str]:
    if not skip:
        return set()

    invocation_ids: set[str] = set()
    for selector in skip:
        if not selector or not str(selector).strip():
            raise KitaruUsageError(
                "Every entry in `skip` must be a non-empty checkpoint selector."
            )
        checkpoint = _resolve_checkpoint_selector(str(selector).strip(), checkpoints)
        invocation_ids.add(checkpoint.invocation_id)
    return invocation_ids


def _resolve_input_targets(
    input_overrides: Mapping[str, Any] | None,
    checkpoints: Sequence[_Checkpoint],
) -> list[tuple[_Checkpoint, dict[str, Any]]]:
    if not input_overrides:
        return []

    targets: list[tuple[_Checkpoint, dict[str, Any]]] = []
    for selector, value in input_overrides.items():
        checkpoint = _resolve_checkpoint_selector(selector, checkpoints)
        targets.append(
            (checkpoint, _normalize_checkpoint_input_value(checkpoint, value))
        )
    return targets


def _resolve_output_targets(
    output_overrides: Mapping[str, Any] | None,
    *,
    at_checkpoint: _Checkpoint,
    checkpoints: Sequence[_Checkpoint],
) -> list[tuple[_Checkpoint, Any]]:
    if not output_overrides:
        return []

    targets: list[tuple[_Checkpoint, Any]] = []
    for tool_key, value in output_overrides.items():
        if _tool_key_matches_checkpoint(tool_key, at_checkpoint):
            targets.append((at_checkpoint, value))
            continue

        matches = _checkpoints_for_tool_key(tool_key, checkpoints)
        if not matches:
            raise KitaruStateError(
                f"Unknown output override target '{tool_key}'. Available "
                f"checkpoints: {_available_checkpoint_selectors(checkpoints)}."
            )
        if len(matches) > 1:
            call_ids = ", ".join(sorted({match.call_id for match in matches}))
            raise KitaruStateError(
                f"Output override target '{tool_key}' is ambiguous. Scope with "
                f"`at=` on the target invocation or use a checkpoint call ID. "
                f"Matching call IDs: {call_ids}."
            )
        targets.append((matches[0], value))

    return targets


def _compute_live_steps(
    *,
    at_checkpoint: _Checkpoint,
    checkpoints: Sequence[_Checkpoint],
    children_by_invocation: Mapping[str, set[str]],
) -> set[str]:
    live_roots = {at_checkpoint.invocation_id}
    live = live_roots | _collect_descendants(
        roots=live_roots,
        children_by_invocation=children_by_invocation,
    )

    at_started_at = at_checkpoint.started_at
    if at_started_at is None:
        return live

    skipped_before_at = {
        checkpoint.invocation_id
        for checkpoint in checkpoints
        if checkpoint.started_at is not None and checkpoint.started_at < at_started_at
    }

    for checkpoint in checkpoints:
        if checkpoint.invocation_id in live:
            continue
        if checkpoint.started_at is None:
            continue
        if checkpoint.started_at < at_started_at:
            continue

        upstream_steps: Sequence[str] = (
            getattr(
                getattr(checkpoint.step, "spec", None),
                "upstream_steps",
                None,
            )
            or ()
        )
        if upstream_steps:
            if all(upstream in skipped_before_at for upstream in upstream_steps):
                continue
            if not any(
                upstream in live or upstream == at_checkpoint.invocation_id
                for upstream in upstream_steps
            ):
                continue

        live.add(checkpoint.invocation_id)
        live |= _collect_descendants(
            roots={checkpoint.invocation_id},
            children_by_invocation=children_by_invocation,
        )

    return live


def build_replay_plan(
    *,
    run: PipelineRunResponse,
    at: str,
    input: Mapping[str, Any] | None = None,
    output: Mapping[str, Any] | None = None,
    tool: Mapping[str, str] | None = None,
    llm_model: str | None = None,
    skip: Sequence[str] | None = None,
    flow_inputs: Mapping[str, Any] | None = None,
) -> ReplayPlan:
    """Build a replay plan for a completed or paused execution.

    Checkpoints before ``at`` are skipped (playback). ``at`` and its downstream
    descendants re-execute unless mocked via ``output=`` or listed in ``skip=``.
    """
    checkpoints = _checkpoints(run)
    if not checkpoints:
        raise KitaruStateError(
            f"Execution '{run.id}' has no checkpoint history to replay."
        )

    if not at or not at.strip():
        raise KitaruUsageError("`at` must be a non-empty checkpoint selector.")

    at_checkpoint = _resolve_checkpoint_selector(at, checkpoints)
    children_by_invocation = _build_children_by_invocation(checkpoints)
    all_steps = {checkpoint.invocation_id for checkpoint in checkpoints}

    live = _compute_live_steps(
        at_checkpoint=at_checkpoint,
        checkpoints=checkpoints,
        children_by_invocation=children_by_invocation,
    )
    steps_to_skip = all_steps - live

    step_input_overrides: dict[str, dict[str, Any]] = {}
    runtime_input_overrides: dict[str, dict[str, Any]] = {}

    for checkpoint, normalized in _resolve_input_targets(input, checkpoints):
        steps_to_skip.discard(checkpoint.invocation_id)
        live.add(checkpoint.invocation_id)
        live |= _collect_descendants(
            roots={checkpoint.invocation_id},
            children_by_invocation=children_by_invocation,
        )
        step_input_overrides.setdefault(checkpoint.invocation_id, {}).update(normalized)
        runtime_input_overrides[checkpoint.call_id] = normalized

    steps_to_skip = all_steps - live

    output_mocks: dict[str, Any] = {}
    for source, value in _resolve_output_targets(
        output,
        at_checkpoint=at_checkpoint,
        checkpoints=checkpoints,
    ):
        steps_to_skip.add(source.invocation_id)
        live.discard(source.invocation_id)
        for invocation_id, input_name in _find_downstream_consumers(
            source=source,
            checkpoints=checkpoints,
        ):
            live.add(invocation_id)
            live |= _collect_descendants(
                roots={invocation_id},
                children_by_invocation=children_by_invocation,
            )
            step_input_overrides.setdefault(invocation_id, {})[input_name] = value
        output_mocks[source.call_id] = value

    steps_to_skip = all_steps - live

    overlap = steps_to_skip & set(step_input_overrides)
    if overlap:
        steps_to_skip -= overlap

    explicit_skip = _resolve_skip_invocation_ids(skip, checkpoints)
    if explicit_skip:
        input_overlap = explicit_skip & set(step_input_overrides)
        if input_overlap:
            joined = ", ".join(sorted(input_overlap))
            raise KitaruUsageError(
                f"Cannot skip and override inputs for the same checkpoint: {joined}."
            )
        live -= explicit_skip
        steps_to_skip = all_steps - live

    runtime_context = ReplayRuntimeContext(
        at=at,
        output_mocks=output_mocks,
        tool_overrides=dict(tool or {}),
        llm_model=llm_model,
        llm_model_at=at if llm_model and at else None,
        input_overrides=runtime_input_overrides,
    )

    return ReplayPlan(
        original_run_id=str(run.id),
        steps_to_skip=steps_to_skip,
        input_overrides=dict(flow_inputs or {}),
        step_input_overrides=step_input_overrides,
        runtime_context=runtime_context,
    )


__all__ = [
    "REPLAY_RESERVED_KWARGS",
    "ReplayManyResult",
    "ReplayPlan",
    "build_replay_plan",
    "replay_at_skip_reason",
    "replay_at_status",
]
