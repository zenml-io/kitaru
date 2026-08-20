"""Public contract and installed discovery for experiment exporters."""

import importlib.metadata
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from packaging.utils import canonicalize_name
from pydantic import BaseModel, ConfigDict, Field

from kitaru.exports.config import ExportFormat, TraceFormat
from kitaru.exports.models import (
    ExporterProvenance,
    ExportError,
    ExportManifest,
    ResolvedExport,
)

EXPORTER_CONTRACT_VERSION = 1
EXPORTER_ENTRY_POINT_PREFIX = "kitaru.exporters."
EXPORTER_ENTRY_POINT_GROUP = (
    f"{EXPORTER_ENTRY_POINT_PREFIX}v{EXPORTER_CONTRACT_VERSION}"
)
EXPECTED_EXPORTER_DISTRIBUTIONS: dict[ExportFormat, str] = {
    "harbor": "kitaru-harbor-exporter",
    "verifiers-v1": "kitaru-verifiers-exporter",
}


class ExporterMetadata(ExporterProvenance):
    """Declare one exporter's public identity and target compatibility."""


class ExporterOptions(BaseModel):
    """Pass the fixed target options from a validated export request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    required_environment_names: tuple[str, ...] = Field(default=(), max_length=100)
    trace_format: TraceFormat | None = None
    trace_path: str | None = Field(default=None, max_length=1_024)


@dataclass(frozen=True)
class ExporterContext:
    """Provide trusted exporter identity and cooperative cancellation."""

    exporter: ExporterProvenance
    cancellation_checkpoint: Callable[[], None]

    def checkpoint(self) -> None:
        """Stop plugin work when the caller has revoked the operation."""
        self.cancellation_checkpoint()


@runtime_checkable
class ExperimentExporter(Protocol):
    """Render one maintained target from resolved, sanitized export data."""

    metadata: ExporterMetadata

    def preflight(
        self,
        resolved: ResolvedExport,
        *,
        options: ExporterOptions,
        context: ExporterContext,
    ) -> None:
        """Validate target-specific inputs without creating an artifact."""
        ...

    def render(
        self,
        resolved: ResolvedExport,
        staging_root: Path,
        *,
        options: ExporterOptions,
        context: ExporterContext,
    ) -> ExportManifest:
        """Render and structurally validate an artifact in core-owned staging."""
        ...


@dataclass(frozen=True)
class LoadedExporter:
    """Pair one validated implementation with metadata trusted by core."""

    implementation: ExperimentExporter
    metadata: ExporterMetadata

    @property
    def provenance(self) -> ExporterProvenance:
        """Return the identity embedded in manifests and receipts."""
        return ExporterProvenance.model_validate(self.metadata.model_dump())


def _get_distribution_identity(entry_point: Any) -> tuple[str, str]:
    distribution = getattr(entry_point, "dist", None)
    if distribution is None:
        raise ExportError(
            "exporter_load_failed",
            "The installed exporter has no distribution metadata. Reinstall its "
            "package and try again.",
        )
    raw_name = getattr(distribution, "name", None)
    if raw_name is None:
        metadata = getattr(distribution, "metadata", {})
        raw_name = metadata.get("Name")
    raw_version = getattr(distribution, "version", None)
    if not isinstance(raw_name, str) or not isinstance(raw_version, str):
        raise ExportError(
            "exporter_load_failed",
            "The installed exporter has invalid distribution metadata. Reinstall "
            "its package and try again.",
        )
    name = canonicalize_name(raw_name)
    if (
        not name
        or len(name) > 128
        or len(raw_version) > 128
        or not raw_version
        or any(character in raw_version for character in "\r\n\0")
    ):
        raise ExportError(
            "exporter_load_failed",
            "The installed exporter has invalid distribution metadata. Reinstall "
            "its package and try again.",
        )
    return name, raw_version


@cache
def _iter_installed_exporter_entry_points() -> tuple[Any, ...]:
    discovered: list[Any] = []
    for distribution in importlib.metadata.distributions():
        for entry_point in distribution.entry_points:
            if entry_point.group.startswith(EXPORTER_ENTRY_POINT_PREFIX):
                discovered.append(entry_point)
    return tuple(discovered)


def _describe_entry_point(entry_point: Any) -> str:
    try:
        name, version = _get_distribution_identity(entry_point)
    except ExportError:
        return "unknown distribution"
    return f"{name}=={version}"


def _describe_group(group: str) -> str:
    if len(group) > 128 or any(character in group for character in "\r\n\0"):
        return "unknown exporter contract"
    return group


def _get_kitaru_version() -> str:
    try:
        return importlib.metadata.version("kitaru")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _validate_loaded_exporter(
    entry_point: Any, format: ExportFormat, loaded: Any
) -> LoadedExporter:
    distribution_name, distribution_version = _get_distribution_identity(entry_point)
    if not callable(loaded):
        raise ExportError(
            "exporter_load_failed",
            f"{distribution_name} does not expose a callable exporter factory.",
        )
    try:
        implementation = loaded()
    except Exception as error:
        raise ExportError(
            "exporter_load_failed",
            f"{distribution_name} could not initialize its exporter.",
        ) from error
    try:
        metadata = ExporterMetadata.model_validate(implementation.metadata)
    except Exception as error:
        raise ExportError(
            "exporter_load_failed",
            f"{distribution_name} returned invalid exporter metadata.",
        ) from error
    expected_name = EXPECTED_EXPORTER_DISTRIBUTIONS[format]
    compatible = (
        metadata.contract_version == EXPORTER_CONTRACT_VERSION
        and canonicalize_name(metadata.distribution_name) == distribution_name
        and metadata.distribution_version == distribution_version
        and metadata.format == format
        and distribution_name == canonicalize_name(expected_name)
    )
    if not compatible:
        raise ExportError(
            "exporter_incompatible",
            f"{distribution_name}=={distribution_version} is not compatible with "
            f"Kitaru {_get_kitaru_version()} exporter contract "
            f"v{EXPORTER_CONTRACT_VERSION} "
            f"for {format}. Upgrade kitaru and {expected_name} together.",
        )
    if not isinstance(implementation, ExperimentExporter):
        raise ExportError(
            "exporter_load_failed",
            f"{distribution_name} does not implement the Kitaru exporter contract.",
        )
    trusted_metadata = ExporterMetadata(
        contract_version=EXPORTER_CONTRACT_VERSION,
        distribution_name=distribution_name,
        distribution_version=distribution_version,
        format=format,
        target_version=metadata.target_version,
    )
    return LoadedExporter(implementation=implementation, metadata=trusted_metadata)


def resolve_exporter(
    format: ExportFormat, *, entry_points: Iterable[Any] | None = None
) -> LoadedExporter:
    """Resolve exactly one compatible installed exporter for a maintained format."""
    candidates = tuple(
        entry_points
        if entry_points is not None
        else _iter_installed_exporter_entry_points()
    )
    named = tuple(
        entry_point for entry_point in candidates if entry_point.name == format
    )
    compatible = tuple(
        entry_point
        for entry_point in named
        if entry_point.group == EXPORTER_ENTRY_POINT_GROUP
    )
    expected_distribution = EXPECTED_EXPORTER_DISTRIBUTIONS[format]
    if not compatible:
        incompatible_groups = sorted(
            {
                _describe_group(entry_point.group)
                for entry_point in named
                if entry_point.group.startswith(EXPORTER_ENTRY_POINT_PREFIX)
            }
        )
        if incompatible_groups:
            groups = ", ".join(incompatible_groups)
            providers = ", ".join(
                sorted(_describe_entry_point(entry_point) for entry_point in named)
            )
            raise ExportError(
                "exporter_incompatible",
                f"{format} providers ({providers}) are installed for unsupported "
                f"exporter contracts ({groups}) with Kitaru {_get_kitaru_version()}. "
                f"Upgrade kitaru and {expected_distribution} together.",
            )
        raise ExportError(
            "exporter_not_installed",
            f"Export format {format} requires {expected_distribution}. Install it "
            f"in this Python environment with `uv add {expected_distribution}`.",
        )
    if len(compatible) > 1:
        providers = ", ".join(
            sorted(_describe_entry_point(item) for item in compatible)
        )
        raise ExportError(
            "exporter_ambiguous",
            f"Multiple providers are installed for {format}: {providers}. Remove "
            "the duplicate exporter packages before retrying.",
        )
    selected = compatible[0]
    try:
        loaded = selected.load()
    except Exception as error:
        distribution = _describe_entry_point(selected)
        raise ExportError(
            "exporter_load_failed",
            f"{distribution} could not be loaded. Reinstall or upgrade the exporter "
            "package and try again.",
        ) from error
    return _validate_loaded_exporter(selected, format, loaded)


__all__ = [
    "EXPECTED_EXPORTER_DISTRIBUTIONS",
    "EXPORTER_CONTRACT_VERSION",
    "EXPORTER_ENTRY_POINT_GROUP",
    "ExperimentExporter",
    "ExporterContext",
    "ExporterMetadata",
    "ExporterOptions",
    "LoadedExporter",
    "resolve_exporter",
]
