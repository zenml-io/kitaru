"""Structural validation shared by generated export formats."""

import hashlib
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from kitaru.exports.models import (
    V1_EXPORT_BUDGETS,
    ExportError,
    RuntimeBridgeReceipt,
)
from kitaru.exports.writer import file_digest


def validate_kitaru_requirement(
    requirement: str, version: str, *, subject: str
) -> None:
    """Require a declared Kitaru dependency to include one runtime version."""
    try:
        parsed = Requirement(requirement)
    except InvalidRequirement as error:
        raise ExportError(
            "invalid_dependency_metadata", f"{subject} requirement is invalid."
        ) from error
    if canonicalize_name(parsed.name) != "kitaru":
        return
    if parsed.url is not None or not parsed.specifier.contains(
        version, prereleases=True
    ):
        raise ExportError(
            "dependency_conflict",
            f"Generated evaluation requires Kitaru {version}; {subject.lower()} "
            "requirement excludes that version.",
        )


def validate_generated_resources(root: Path) -> None:
    """Reject generated bundles that exceed the shared v1 resource limits."""
    file_count = 0
    total_bytes = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ExportError(
                "unsupported_bundle_symlink",
                "Generated bundles cannot contain symlinks.",
            )
        if not path.is_file():
            continue
        file_count += 1
        if file_count > V1_EXPORT_BUDGETS.max_generated_files:
            raise ExportError(
                "generated_files_limit",
                f"Generated file count {file_count} exceeds limit "
                f"{V1_EXPORT_BUDGETS.max_generated_files}.",
            )
        path_bytes = len(path.relative_to(root).as_posix().encode("utf-8"))
        if path_bytes > V1_EXPORT_BUDGETS.max_relative_path_bytes:
            raise ExportError(
                "generated_path_limit",
                f"Generated path length {path_bytes} exceeds limit "
                f"{V1_EXPORT_BUDGETS.max_relative_path_bytes} bytes.",
            )
        total_bytes += path.stat().st_size
        if total_bytes > V1_EXPORT_BUDGETS.max_artifact_bytes:
            raise ExportError(
                "artifact_size_limit",
                f"Generated artifact bytes {total_bytes} exceeds limit "
                f"{V1_EXPORT_BUDGETS.max_artifact_bytes}.",
            )


def validate_runtime_bridge(
    bridge_root: Path,
    receipt_data: object,
    *,
    error_code: str,
) -> None:
    """Verify bridge receipt structure, member bytes, and aggregate digest."""
    try:
        receipt = RuntimeBridgeReceipt.model_validate(receipt_data)
    except ValueError as error:
        raise ExportError(error_code, "Runtime bridge receipt is invalid.") from error
    aggregate = hashlib.sha256()
    for relative, expected in receipt.files.items():
        path = bridge_root / relative
        if not path.is_file() or file_digest(path) != expected:
            raise ExportError(error_code, "Runtime bridge bytes do not match manifest.")
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(path.read_bytes())
        aggregate.update(b"\n")
    if aggregate.hexdigest() != receipt.sha256:
        raise ExportError(error_code, "Runtime bridge digest does not match manifest.")
