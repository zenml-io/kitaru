"""Models shared by experiment export formats."""

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.api_models.v1.agent_version import AgentVersionResponse
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.session_node import SessionWithNodesResponse

ContentCategory = Literal[
    "session_outputs",
    "model_payloads",
    "tool_payloads",
    "subagent_payloads",
    "span_payloads",
    "visible_reasoning",
    "metadata",
    "diagnostic_details",
    "usage_and_cost",
]
CONTENT_CATEGORIES: tuple[ContentCategory, ...] = (
    "session_outputs",
    "model_payloads",
    "tool_payloads",
    "subagent_payloads",
    "span_payloads",
    "visible_reasoning",
    "metadata",
    "diagnostic_details",
    "usage_and_cost",
)
_CONTENT_CATEGORY_ORDER = {
    category: index for index, category in enumerate(CONTENT_CATEGORIES)
}


class ExportBudgets(BaseModel):
    """Fixed resource limits for the v1 export contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_sessions: Literal[1000] = 1_000
    max_session_bytes: Literal[16777216] = 16 * 1024 * 1024
    max_total_session_bytes: Literal[268435456] = 256 * 1024 * 1024
    max_evaluator_bytes: Literal[10485760] = 10 * 1024 * 1024
    max_total_evaluator_bytes: Literal[104857600] = 100 * 1024 * 1024
    max_attached_secrets: Literal[100] = 100
    max_protected_value_bytes: Literal[1048576] = 1024 * 1024
    max_source_files: Literal[100000] = 100_000
    max_generated_files: Literal[100000] = 100_000
    max_relative_path_bytes: Literal[1024] = 1_024
    max_source_file_bytes: Literal[104857600] = 100 * 1024 * 1024
    max_total_source_bytes: Literal[1073741824] = 1024 * 1024 * 1024
    max_receipt_path_samples: Literal[100] = 100
    max_receipt_path_characters: Literal[512] = 512
    max_artifact_bytes: Literal[2147483648] = 2 * 1024 * 1024 * 1024


V1_EXPORT_BUDGETS = ExportBudgets()


class ExportWarning(BaseModel):
    """Record one stable, user-actionable export warning."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_]*$")
    message: str = Field(min_length=1)


class ContentPolicy(BaseModel):
    """Select the allowlisted optional experimental content to omit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    omit: tuple[ContentCategory, ...] = ()

    @field_validator("omit")
    @classmethod
    def _normalize_omissions(
        cls, value: tuple[ContentCategory, ...]
    ) -> tuple[ContentCategory, ...]:
        if len(set(value)) != len(value):
            raise ValueError("content policy omissions must be unique")
        return tuple(sorted(value, key=_CONTENT_CATEGORY_ORDER.__getitem__))

    def is_included(self, category: str) -> bool:
        """Return whether an optional content category remains included."""
        return category not in self.omit

    @property
    def warnings(self) -> tuple[ExportWarning, ...]:
        """Return warnings required by this effective policy."""
        if not self.omit:
            return ()
        return (
            ExportWarning(
                code="content_policy_changes_evaluation",
                message=(
                    "Omitting experimental content can change evaluator behavior."
                ),
            ),
        )


class EnvironmentPolicy(BaseModel):
    """Select how registered environment values enter an artifact."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["include", "runtime_only"] = "include"


class SourcePolicy(BaseModel):
    """Select explicit source-path overrides for one export."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @field_validator("include", "exclude")
    @classmethod
    def _normalize_paths(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized: list[str] = []
        for raw_path in value:
            if (
                len(raw_path.encode("utf-8"))
                > V1_EXPORT_BUDGETS.max_relative_path_bytes
            ):
                raise ValueError(
                    "source policy paths must be at most 1,024 UTF-8 bytes"
                )
            path = PurePosixPath(raw_path)
            if (
                not raw_path
                or raw_path == "."
                or path.is_absolute()
                or ".." in path.parts
                or "\\" in raw_path
            ):
                raise ValueError("source policy paths must be relative POSIX paths")
            normalized.append(path.as_posix())
        if len(set(normalized)) != len(normalized):
            raise ValueError("source policy paths must be unique")
        return tuple(sorted(normalized))

    @model_validator(mode="after")
    def _reject_contradictory_rules(self) -> "SourcePolicy":
        for included in self.include:
            include_path = PurePosixPath(included)
            for excluded in self.exclude:
                exclude_path = PurePosixPath(excluded)
                if (
                    include_path == exclude_path
                    or include_path in exclude_path.parents
                    or exclude_path in include_path.parents
                ):
                    raise ValueError(
                        f"source path {included!r} cannot be both included and excluded"
                    )
        return self


class BoundedPathSummary(BaseModel):
    """Summarize path decisions without allowing an unbounded receipt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    samples: tuple[str, ...] = Field(
        default=(), max_length=V1_EXPORT_BUDGETS.max_receipt_path_samples
    )
    total_count: int = Field(ge=0)
    truncated: bool

    @field_validator("samples")
    @classmethod
    def _validate_samples(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(
            len(sample) > V1_EXPORT_BUDGETS.max_receipt_path_characters
            for sample in value
        ):
            raise ValueError("receipt path samples must be at most 512 characters")
        return value

    @model_validator(mode="after")
    def _validate_count(self) -> "BoundedPathSummary":
        if self.total_count < len(self.samples):
            raise ValueError("receipt path total cannot be smaller than its samples")
        if self.total_count > len(self.samples) and not self.truncated:
            raise ValueError("a partial receipt path summary must be marked truncated")
        return self

    @classmethod
    def from_paths(cls, paths: Iterable[str]) -> "BoundedPathSummary":
        """Build a deterministic bounded summary from relative paths."""
        ordered = tuple(sorted(paths))
        selected = ordered[: V1_EXPORT_BUDGETS.max_receipt_path_samples]
        samples = tuple(
            path[: V1_EXPORT_BUDGETS.max_receipt_path_characters] for path in selected
        )
        truncated = len(ordered) > len(selected) or any(
            len(path) > V1_EXPORT_BUDGETS.max_receipt_path_characters
            for path in selected
        )
        return cls(
            samples=samples,
            total_count=len(ordered),
            truncated=truncated,
        )


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
class DependencyRequirement:
    """Record one normalized agent-runtime dependency declaration."""

    project: str
    requirement: str
    requirement_digest: str
    source_path: str | None = None


@dataclass(frozen=True)
class DependencyPlan:
    """Record the exact dependency inputs and their reproducibility status."""

    status: Literal["locked", "declared"]
    manifests: tuple[str, ...]
    requirements: tuple[DependencyRequirement, ...]
    requirement_digest: str


@dataclass(frozen=True)
class RuntimeEnvironmentRequirement:
    """Record why one environment variable must be supplied at runtime."""

    name: str
    owner: Literal["agent"]
    source: Literal["attached_secret", "registered_environment"]


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
    command_argv: tuple[str, ...] = ()
    required_environment_names: tuple[str, ...] = ()
    runtime_environment: tuple[RuntimeEnvironmentRequirement, ...] = ()
    dependency_plan: DependencyPlan | None = None


class ValidationReceipt(BaseModel):
    """Record how far an exported artifact was validated."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    level: Literal["preflight", "structural", "executable", "release"]
    status: Literal["passed", "failed", "not_performed"]
    target_version: str


class ExportAssurance(BaseModel):
    """Separate request, artifact, and release-level export assurance."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    preflight: ValidationReceipt
    structural_validation: ValidationReceipt
    release_compatibility: ValidationReceipt

    @model_validator(mode="after")
    def _validate_levels(self) -> "ExportAssurance":
        expected = (
            (self.preflight, "preflight"),
            (self.structural_validation, "structural"),
            (self.release_compatibility, "release"),
        )
        for receipt, level in expected:
            if receipt.level != level:
                raise ValueError(f"{level} assurance must use the {level!r} level")
        return self

    @classmethod
    def preflight_only(cls, target_version: str) -> "ExportAssurance":
        """Record a dry run that did not render or execute an artifact."""
        return cls(
            preflight=ValidationReceipt(
                level="preflight", status="passed", target_version=target_version
            ),
            structural_validation=ValidationReceipt(
                level="structural",
                status="not_performed",
                target_version=target_version,
            ),
            release_compatibility=ValidationReceipt(
                level="release",
                status="not_performed",
                target_version=target_version,
            ),
        )

    @classmethod
    def for_artifact(cls, validation: ValidationReceipt) -> "ExportAssurance":
        """Record preflight and structural validation for one artifact."""
        return cls(
            preflight=ValidationReceipt(
                level="preflight",
                status="passed",
                target_version=validation.target_version,
            ),
            structural_validation=validation,
            release_compatibility=ValidationReceipt(
                level="release",
                status="not_performed",
                target_version=validation.target_version,
            ),
        )


class DependencyReceipt(BaseModel):
    """Record the dependency reproducibility proved during export."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["locked", "declared", "not_assessed"] = "not_assessed"
    requirement_digest: str | None = None

    @field_validator("requirement_digest")
    @classmethod
    def _validate_digest(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError("requirement digest must be a lowercase SHA-256 digest")
        return value


class ArtifactProvenance(BaseModel):
    """Record deterministic component identities and native package names."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_digest: str
    benchmark_digest: str
    default_harness_digest: str
    runtime_bundle_digest: str
    plugin_id: str
    distribution_name: str
    module_name: str

    @field_validator(
        "artifact_digest",
        "benchmark_digest",
        "default_harness_digest",
        "runtime_bundle_digest",
    )
    @classmethod
    def _validate_digests(cls, value: str) -> str:
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("provenance digests must be lowercase SHA-256 digests")
        return value


class RuntimeRequirements(BaseModel):
    """Separate Task-private and bundled-Harness runtime requirements."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_private: tuple[str, ...] = ()
    bundled_harness: tuple[str, ...] = ()
    all: tuple[str, ...] = ()

    @field_validator("task_private", "bundled_harness", "all")
    @classmethod
    def _normalize_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("runtime requirement names must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _derive_union(self) -> "RuntimeRequirements":
        expected = tuple(sorted(set(self.task_private) | set(self.bundled_harness)))
        if self.all and self.all != expected:
            raise ValueError(
                "combined runtime requirements must equal the ownership union"
            )
        if not self.all:
            object.__setattr__(self, "all", expected)
        return self


class TaskProvenance(BaseModel):
    """Identify one immutable exported task and its source content."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    session_id: uuid.UUID
    content_digest: str

    @field_validator("content_digest")
    @classmethod
    def _validate_content_digest(cls, value: str) -> str:
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("task content digest must be a lowercase SHA-256 digest")
        return value


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
    content_policy: ContentPolicy = Field(default_factory=ContentPolicy)
    environment_policy: EnvironmentPolicy = Field(default_factory=EnvironmentPolicy)
    source_policy: SourcePolicy = Field(default_factory=SourcePolicy)
    warnings: tuple[ExportWarning, ...] = ()
    assurance: ExportAssurance | None = None
    dependencies: DependencyReceipt = Field(default_factory=DependencyReceipt)
    provenance: ArtifactProvenance | None = None
    runtime_requirements: RuntimeRequirements = Field(
        default_factory=RuntimeRequirements
    )
    task_provenance: tuple[TaskProvenance, ...] = ()
    validation: ValidationReceipt

    @model_validator(mode="after")
    def _derive_policy_receipts(self) -> "ExportManifest":
        if self.assurance is None:
            object.__setattr__(
                self, "assurance", ExportAssurance.for_artifact(self.validation)
            )
        warnings_by_code = {
            warning.code: warning
            for warning in (*self.warnings, *self.content_policy.warnings)
        }
        object.__setattr__(
            self,
            "warnings",
            tuple(warnings_by_code[code] for code in sorted(warnings_by_code)),
        )
        return self


@dataclass(frozen=True)
class PublishedBundle:
    """Record the paths and digest of a published artifact."""

    destination: Path
    archive_path: Path | None
    digest: str
