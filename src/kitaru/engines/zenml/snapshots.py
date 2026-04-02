"""ZenML → ExecutionGraphSnapshot mapper.

Converts a ZenML ``PipelineRunResponse`` into the backend-neutral
``ExecutionGraphSnapshot`` consumed by Kitaru's replay planner.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from zenml.models import PipelineRunResponse, StepRunResponse

from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.engines._types import (
    CheckpointGraphNode,
    CheckpointInputBinding,
    ExecutionGraphSnapshot,
)


def _extract_invocation_id(step: StepRunResponse) -> str:
    spec = getattr(step, "spec", None)
    inv_id = getattr(spec, "invocation_id", None)
    if isinstance(inv_id, str) and inv_id:
        return inv_id
    return step.name


def _extract_upstream_ids(step: StepRunResponse) -> tuple[str, ...]:
    spec = getattr(step, "spec", None)
    upstream: Sequence[str] = getattr(spec, "upstream_steps", None) or ()
    return tuple(upstream)


def _try_make_binding(
    input_name: str, input_spec: Any
) -> CheckpointInputBinding | None:
    step_name = getattr(input_spec, "step_name", None)
    output_name = getattr(input_spec, "output_name", None)
    if isinstance(step_name, str) and isinstance(output_name, str):
        return CheckpointInputBinding(
            input_name=input_name,
            upstream_invocation_id=step_name,
            upstream_output_name=output_name,
        )
    return None


def _extract_input_bindings(
    step: StepRunResponse,
) -> tuple[CheckpointInputBinding, ...]:
    """Extract input bindings from step spec (v2 or legacy format)."""
    spec = getattr(step, "spec", None)
    if spec is None:
        return ()

    bindings: list[CheckpointInputBinding] = []

    inputs_v2 = getattr(spec, "inputs_v2", None)
    if isinstance(inputs_v2, Mapping):
        for input_name, input_specs in inputs_v2.items():
            for input_spec in input_specs:
                binding = _try_make_binding(input_name, input_spec)
                if binding is not None:
                    bindings.append(binding)
        return tuple(bindings)

    legacy_inputs = getattr(spec, "inputs", None)
    if not isinstance(legacy_inputs, Mapping):
        return ()

    for input_name, raw_input_specs in legacy_inputs.items():
        if isinstance(raw_input_specs, Sequence) and not isinstance(
            raw_input_specs, (str, bytes)
        ):
            iterable: Iterable[Any] = raw_input_specs
        else:
            iterable = [raw_input_specs]
        for input_spec in iterable:
            binding = _try_make_binding(input_name, input_spec)
            if binding is not None:
                bindings.append(binding)

    return tuple(bindings)


def _extract_output_names(step: StepRunResponse) -> tuple[str, ...]:
    """Extract output names, preferring regular_outputs over legacy outputs."""
    regular = getattr(step, "regular_outputs", None)
    if isinstance(regular, Mapping):
        return tuple(regular)
    outputs = getattr(step, "outputs", None)
    if isinstance(outputs, Mapping):
        return tuple(outputs)
    return ()


def execution_graph_from_run(
    run: PipelineRunResponse,
) -> ExecutionGraphSnapshot:
    """Convert a ZenML pipeline run into a backend-neutral execution graph.

    Args:
        run: A ZenML pipeline run response (hydrated or not).

    Returns:
        An ``ExecutionGraphSnapshot`` suitable for ``build_replay_plan()``.
    """
    nodes: list[CheckpointGraphNode] = []

    for step in run.steps.values():
        nodes.append(
            CheckpointGraphNode(
                call_id=str(step.id),
                invocation_id=_extract_invocation_id(step),
                name=_normalize_checkpoint_name(step.name),
                upstream_invocation_ids=_extract_upstream_ids(step),
                input_bindings=_extract_input_bindings(step),
                output_names=_extract_output_names(step),
                start_time=step.start_time,
                end_time=step.end_time,
            )
        )

    flow_name: str | None = None
    if run.pipeline is not None:
        flow_name = getattr(run.pipeline, "name", None)

    return ExecutionGraphSnapshot(
        exec_id=str(run.id),
        flow_name=flow_name,
        checkpoints=tuple(nodes),
    )
