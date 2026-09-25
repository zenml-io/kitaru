#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Transition failure matrix tool inputs, results, and fetched cohort data."""

import uuid
from dataclasses import dataclass
from typing import Literal, get_args

from pydantic import Field, field_validator

from kitaru.api_models.v1.annotation import AnnotationResponse
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.filter import Filter
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.mcp.models.common import MCPModel, ToolResult

StateBy = Literal["tool", "span", "node"]
FailureSource = Literal["error", "annotation"]
MatrixSide = Literal["base", "compare"]
START = "(start)"
OTHER = "(other)"


class FailureMatrixRequest(MCPModel):
    """Build a transition failure matrix over a group of sessions.

    A failed session contributes one count to the cell (last step that went right,
    first step that went wrong). A failed session is one whose status is failed,
    that failed an evaluation, or that carries a first-failure annotation. The
    failing step is the node a reviewer marked with a manual annotation whose
    value is `{"first_failure": true, "note": "..."}` and whose selector names
    the node, otherwise the deepest failed node.
    """

    filter: Filter | None = Field(
        default=None,
        description="Session filter selecting the group, e.g. agent_id, "
        "cohort_version_id, experiment_run_id, or a started_at range.",
    )
    label: str | None = Field(
        default=None, max_length=80, description="Display name for the group."
    )
    compare_filter: Filter | None = Field(
        default=None,
        description="Second session group to compare against, e.g. a replay's "
        "experiment_run_id.",
    )
    compare_label: str | None = Field(
        default=None, max_length=80, description="Display name for the second group."
    )
    state_by: StateBy = Field(
        default="tool",
        description="Which nodes count as states: tool and subagent calls, "
        "non-root spans, or both plus LLM calls.",
    )
    state_map: dict[str, str] | None = Field(
        default=None,
        description="Merge states: glob pattern of a state name -> group name, "
        "e.g. {'sql_*': 'SQL'}. The first matching pattern wins.",
    )

    @field_validator("state_map")
    @classmethod
    def _check_group_names(
        cls, state_map: dict[str, str] | None
    ) -> dict[str, str] | None:
        """Reject group names the matrix cannot show or confuses with its own rows.

        Raises:
            ValueError: A group name is blank or reserved.
        """
        for name in (state_map or {}).values():
            if not name.strip() or name in (START, OTHER):
                raise ValueError(
                    f"state_map group names must be non-blank and not {START!r} "
                    f"or {OTHER!r}, got {name!r}"
                )
        return state_map

    sources: list[FailureSource] = Field(
        default_factory=lambda: list(get_args(FailureSource)),
        min_length=1,
        description="Failure kinds to count.",
    )
    max_sessions: int = Field(
        default=200, ge=1, le=500, description="Most recent sessions per group."
    )


class FailureCellRequest(FailureMatrixRequest):
    """List the sessions behind one transition failure matrix cell."""

    snapshot_id: str | None = Field(
        default=None,
        max_length=64,
        description="snapshot_id of the matrix being drilled into; without it "
        "the sessions are read again.",
    )
    from_state: str = Field(min_length=1, description="Last step that went right.")
    to_state: str = Field(min_length=1, description="First step that went wrong.")
    side: MatrixSide = Field(default="base", description="Which session group.")
    limit: int = Field(default=10, ge=1, le=50)


class MatrixCell(MCPModel):
    """Failures and attempts for one transition."""

    from_state: str
    to_state: str
    count: int
    attempts: int | None = Field(
        description="Times the transition happened; null when the failing step "
        "is not itself a state (for example an LLM call)."
    )
    error_count: int
    annotation_count: int
    compare_count: int | None = None
    compare_attempts: int | None = None


class EvaluationCount(MCPModel):
    """A failed evaluation and how many sessions failed it."""

    name: str
    count: int


class GroupSummary(MCPModel):
    """Headline numbers for one session group."""

    label: str
    session_count: int
    failed_count: int
    located_count: int
    shown_count: int = Field(description="Located failures matching the sources.")
    unlocated_count: int
    # Names are values, not keys: redaction masks the value under any key that
    # looks sensitive, which would turn an evaluation named `api_key` into "***".
    unlocated_evaluations: list[EvaluationCount] = Field(
        description="Most common failed evaluations among unlocated sessions."
    )
    truncated: bool = Field(description="More sessions matched than max_sessions.")
    records_capped: bool = Field(
        description="A per-session read limit on nodes, annotations, or "
        "evaluations was reached, so some sessions were analyzed partially."
    )


class FailureMatrixData(MCPModel):
    """Transition failure matrix over one group, optionally compared to a second."""

    snapshot_id: str = Field(description="Identifies the records behind this matrix.")
    state_by: StateBy
    rows: list[str]
    cols: list[str]
    cells: list[MatrixCell]
    base: GroupSummary
    compare: GroupSummary | None = None


class CellSession(MCPModel):
    """One session that failed at the requested transition."""

    session_id: uuid.UUID
    number: int
    name: str | None
    source: FailureSource
    failing_node: str
    note: str | None
    path: list[str] = Field(description="States up to and including the failure.")
    path_truncated: bool


class CellPattern(MCPModel):
    """A repeated failure note and how often it appears."""

    note: str
    count: int


class FailureCellData(MCPModel):
    """Sessions and repeated notes behind one matrix cell."""

    total: int
    attempts: int | None
    patterns: list[CellPattern]
    sessions: list[CellSession]


class FailureMatrixResult(ToolResult):
    """Transition failure matrix envelope."""

    data: FailureMatrixData | None = None


class FailureCellResult(ToolResult):
    """Matrix cell drill-down envelope."""

    data: FailureCellData | None = None


@dataclass(frozen=True, slots=True)
class GroupRecords:
    """Sessions of one group with their nodes, annotations, and evaluations."""

    sessions: tuple[SessionResponse, ...]
    nodes: dict[uuid.UUID, tuple[SessionNodeResponse, ...]]
    annotations: tuple[AnnotationResponse, ...]
    evaluations: tuple[EvaluationResponse, ...]
    truncated: bool
    records_capped: bool
