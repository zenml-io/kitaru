"""Backend-neutral types for the engine abstraction layer.

These types decouple Kitaru's replay planning and execution inspection from
any specific backend (ZenML, Dapr, etc.). Each backend provides a mapper
that converts its native models into these types.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class CheckpointInputBinding:
    """One input edge on a checkpoint: which upstream checkpoint and output it reads."""

    input_name: str
    upstream_invocation_id: str
    upstream_output_name: str


@dataclass(frozen=True)
class CheckpointGraphNode:
    """One checkpoint invocation in an execution graph.

    Carries exactly the information the replay planner needs: identity,
    ordering edges, input bindings, and available output names.
    """

    call_id: str
    invocation_id: str
    name: str
    upstream_invocation_ids: tuple[str, ...]
    input_bindings: tuple[CheckpointInputBinding, ...]
    output_names: tuple[str, ...]
    start_time: datetime | None = None
    end_time: datetime | None = None


@dataclass(frozen=True)
class ExecutionGraphSnapshot:
    """Backend-neutral execution graph for replay planning.

    This is the single input type consumed by ``build_replay_plan()``.
    Each backend converts its native execution model into this snapshot.
    """

    exec_id: str
    flow_name: str | None = None
    checkpoints: tuple[CheckpointGraphNode, ...] = ()
