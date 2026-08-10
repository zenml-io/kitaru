"""Models shared by experiment export formats."""

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.agent_version import AgentVersionResponse
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.session_node import SessionWithNodesResponse


class ExportError(Exception):
    """Report a stable experiment export failure."""

    def __init__(self, code: str, message: str) -> None:
        """Initialize the failure.

        Args:
            code: Stable machine-readable error code.
            message: Human-readable failure detail.
        """
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def get_export_error_kind(error: ExportError) -> str:
    """Map a private export failure to the shared public error kinds."""
    if error.code in {"destination_conflict", "archive_conflict"}:
        return "conflict"
    if error.code.endswith("not_found"):
        return "not_found"
    return "invalid_arguments"


@dataclass(frozen=True)
class RewardSelector:
    """Identify the evaluator result used as the primary reward."""

    evaluator: str
    result: str
    field: Literal["score", "passed"]

    @classmethod
    def parse(cls, value: str) -> "RewardSelector":
        """Parse an evaluator, result name, and supported result field.

        Args:
            value: Selector written as ``evaluator:result:score`` or
                ``evaluator:result:passed``.

        Raises:
            ExportError: The selector is malformed or selects an unsupported
                result field.

        Returns:
            Parsed selector.
        """
        parts = value.split(":")
        if (
            len(parts) != 3
            or not parts[0]
            or not parts[1]
            or parts[2] not in {"score", "passed"}
        ):
            raise ExportError(
                "invalid_reward_selector",
                "Primary reward must be EVALUATOR:RESULT:score or "
                "EVALUATOR:RESULT:passed.",
            )
        evaluator, result, raw_field = parts
        field = cast(Literal["score", "passed"], raw_field)
        return cls(
            evaluator=evaluator,
            result=result,
            field=field,
        )


@dataclass(frozen=True)
class SourceFile:
    """Record one safe source file."""

    path: str
    size: int
    sha256: str
    mode: Literal[0o644, 0o755]
    link_target: str | None = None


@dataclass(frozen=True)
class SourceInventory:
    """Record the deterministic contents of a local source root."""

    root: Path
    files: tuple[SourceFile, ...]
    excluded: tuple[str, ...]
    digest: str


@dataclass(frozen=True)
class MaterializedEvaluator:
    """Record one exact evaluator version and its exportable source."""

    name: str
    version: EvaluatorVersionResponse
    params: dict[str, Any]
    script: bytes | None
    source_sha256: str


@dataclass(frozen=True)
class ResolvedExport:
    """Hold the exact read-only inputs consumed by both renderers."""

    experiment: ExperimentResponse
    cohort_version: CohortVersionResponse
    agent_version: AgentVersionResponse
    sessions: tuple[SessionWithNodesResponse, ...]
    evaluators: tuple[MaterializedEvaluator, ...]
    reward: RewardSelector
    source: SourceInventory


class ValidationReceipt(BaseModel):
    """Record how far an exported artifact was validated."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    level: Literal["structural", "executable"]
    status: Literal["passed", "failed"]
    target_version: str


class ExportManifest(BaseModel):
    """Describe the provenance and contents of one exported artifact."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    format: Literal["harbor", "verifiers-v1"]
    target_version: str
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    evaluator_version_ids: tuple[uuid.UUID, ...]
    primary_reward: str
    source_digest: str
    generated_files: dict[str, str] = Field(default_factory=dict)
    required_environment_names: tuple[str, ...] = ()
    exclusions: tuple[str, ...] = ()
    validation: ValidationReceipt


@dataclass(frozen=True)
class PublishedBundle:
    """Record the paths and digest of a published artifact."""

    destination: Path
    archive_path: Path | None
    digest: str
