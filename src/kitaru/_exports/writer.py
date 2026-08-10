"""Deterministic bundle writing and atomic publication."""

import hashlib
import json
import os
import shutil
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kitaru._exports.models import ExportError, PublishedBundle

_COPY_BUFFER_BYTES = 1024 * 1024


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a JSON value in the exporter's canonical form."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def write_canonical_json(path: Path, value: Any) -> None:
    """Write canonical JSON, creating its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value))


def file_digest(path: Path) -> str:
    """Hash one file without loading it into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(_COPY_BUFFER_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def file_digests(root: Path) -> dict[str, str]:
    """Return stable SHA-256 hashes for every regular file below a root."""
    result: dict[str, str] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.is_symlink():
            raise ExportError(
                "unsupported_bundle_symlink",
                f"Generated bundles cannot contain symlinks: {path.relative_to(root)}",
            )
        relative = path.relative_to(root).as_posix()
        result[relative] = file_digest(path)
    return result


def directory_digest(root: Path) -> str:
    """Hash a directory's relative paths and file contents deterministically."""
    digest = hashlib.sha256()
    for path, content_hash in file_digests(root).items():
        mode = 0o755 if (root / path).stat().st_mode & 0o111 else 0o644
        digest.update(path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content_hash.encode("ascii"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _write_deterministic_zip(root: Path, archive_path: Path) -> None:
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
            info.external_attr = (0o100000 | mode) << 16
            with path.open("rb") as source, archive.open(info, "w") as target:
                shutil.copyfileobj(source, target, length=_COPY_BUFFER_BYTES)


def publish_bundle(
    destination: Path,
    render: Callable[[Path], None],
    *,
    archive: bool = False,
) -> PublishedBundle:
    """Render and publish a complete bundle beside its destination.

    Args:
        destination: New canonical artifact directory.
        render: Function that writes the complete staging tree.
        archive: Also publish a deterministic sibling ZIP.

    Raises:
        ExportError: A destination conflicts or its parent is invalid.
        Exception: Rendering or filesystem publication fails.

    Returns:
        Published paths and canonical directory digest.
    """
    destination = destination.expanduser().absolute()
    parent = destination.parent
    if not parent.is_dir():
        raise ExportError(
            "invalid_destination", "Destination parent must already be a directory."
        )
    archive_path = destination.with_name(f"{destination.name}.zip") if archive else None
    if destination.exists():
        raise ExportError("destination_conflict", f"Destination exists: {destination}")
    if archive_path is not None and archive_path.exists():
        raise ExportError("archive_conflict", f"Archive exists: {archive_path}")

    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.kitaru-", suffix=".tmp", dir=parent
        )
    )
    staging_archive = staging.with_suffix(".zip") if archive_path is not None else None
    published_archive = False
    try:
        render(staging)
        digest = directory_digest(staging)
        if staging_archive is not None:
            _write_deterministic_zip(staging, staging_archive)

        if destination.exists():
            raise ExportError(
                "destination_conflict", f"Destination exists: {destination}"
            )
        if archive_path is not None and archive_path.exists():
            raise ExportError("archive_conflict", f"Archive exists: {archive_path}")
        if staging_archive is not None and archive_path is not None:
            os.rename(staging_archive, archive_path)
            published_archive = True
        try:
            os.rename(staging, destination)
        except FileExistsError as error:
            raise ExportError(
                "destination_conflict", f"Destination exists: {destination}"
            ) from error
        return PublishedBundle(
            destination=destination, archive_path=archive_path, digest=digest
        )
    except Exception:
        if published_archive and archive_path is not None:
            archive_path.unlink(missing_ok=True)
        raise
    finally:
        if staging.exists():
            shutil.rmtree(staging)
        if staging_archive is not None:
            staging_archive.unlink(missing_ok=True)
