"""Deterministic bundle staging and conflict-safe publication."""

import ctypes
import errno
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
import uuid
import zipfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kitaru.exports._cancellation import (
    CancellationCheckpoint,
    export_cancellation_scope,
    get_cancellation_checkpoint,
)
from kitaru.exports.models import ExportError, PublishedBundle

_COPY_BUFFER_BYTES = 1024 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_RENAME_NOREPLACE = 1
_RENAME_EXCL = 0x00000004
_AT_FDCWD = -100

_PathIdentity = tuple[int, int]


@dataclass(frozen=True, slots=True)
class _OwnedFile:
    path: Path
    identity: _PathIdentity


@dataclass(frozen=True, slots=True)
class StagedBundle:
    """An exporter-owned bundle that is validated but not yet published."""

    destination: Path
    archive_path: Path | None
    staging: Path
    staging_identity: _PathIdentity
    staging_archive: _OwnedFile | None
    owner_marker: _OwnedFile
    reservation_marker: _OwnedFile
    marker_content: bytes
    parent_identity: _PathIdentity
    digest: str


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a JSON value in the exporter's canonical form."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def write_canonical_json(path: Path, value: Any) -> None:
    """Write canonical JSON, creating its parent directories."""
    get_cancellation_checkpoint(None)()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value))
    get_cancellation_checkpoint(None)()


def file_digest(
    path: Path, *, cancellation_checkpoint: CancellationCheckpoint | None = None
) -> str:
    """Hash one file without loading it into memory."""
    checkpoint = get_cancellation_checkpoint(cancellation_checkpoint)
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(_COPY_BUFFER_BYTES):
            checkpoint()
            digest.update(chunk)
    checkpoint()
    return digest.hexdigest()


def file_digests(
    root: Path, *, cancellation_checkpoint: CancellationCheckpoint | None = None
) -> dict[str, str]:
    """Return stable SHA-256 hashes for every regular file below a root."""
    checkpoint = get_cancellation_checkpoint(cancellation_checkpoint)
    result: dict[str, str] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        checkpoint()
        if path.is_symlink():
            raise ExportError(
                "unsupported_bundle_symlink",
                f"Generated bundles cannot contain symlinks: {path.relative_to(root)}",
            )
        relative = path.relative_to(root).as_posix()
        result[relative] = file_digest(
            path, cancellation_checkpoint=cancellation_checkpoint
        )
    checkpoint()
    return result


def directory_digest(
    root: Path, *, cancellation_checkpoint: CancellationCheckpoint | None = None
) -> str:
    """Hash a directory's relative paths and file contents deterministically."""
    checkpoint = get_cancellation_checkpoint(cancellation_checkpoint)
    digest = hashlib.sha256()
    for path, content_hash in file_digests(
        root, cancellation_checkpoint=cancellation_checkpoint
    ).items():
        checkpoint()
        mode = 0o755 if (root / path).stat().st_mode & 0o111 else 0o644
        digest.update(path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content_hash.encode("ascii"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode("ascii"))
        digest.update(b"\n")
    checkpoint()
    return digest.hexdigest()


def _identity(path: Path) -> _PathIdentity:
    value = path.stat(follow_symlinks=False)
    return value.st_dev, value.st_ino


def _path_exists(path: Path) -> bool:
    return os.path.lexists(path)


def _open_private_file(path: Path) -> int:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    return os.open(path, flags, _PRIVATE_FILE_MODE)


def _create_marker(path: Path, content: bytes, *, conflict_code: str) -> _OwnedFile:
    try:
        descriptor = _open_private_file(path)
    except FileExistsError as error:
        raise ExportError(
            conflict_code,
            f"An incomplete export reservation exists: {path}",
        ) from error
    value = os.fstat(descriptor)
    owned = _OwnedFile(path=path, identity=(value.st_dev, value.st_ino))
    try:
        with os.fdopen(descriptor, "wb") as marker:
            marker.write(content)
            marker.flush()
            os.fsync(marker.fileno())
    except Exception as error:
        try:
            _unlink_owned_file(owned)
        except ExportError as cleanup_error:
            raise cleanup_error from error
        raise
    _validate_owned_file(owned, content)
    return owned


def _validate_parent(parent: Path, expected: _PathIdentity) -> None:
    try:
        value = parent.stat(follow_symlinks=False)
    except OSError as error:
        raise ExportError(
            "destination_parent_changed",
            "Destination parent changed during export.",
        ) from error
    if not stat.S_ISDIR(value.st_mode) or (value.st_dev, value.st_ino) != expected:
        raise ExportError(
            "destination_parent_changed",
            "Destination parent changed during export.",
        )


def _validate_owned_file(owned: _OwnedFile, content: bytes | None = None) -> None:
    try:
        value = owned.path.stat(follow_symlinks=False)
    except OSError as error:
        raise ExportError(
            "cleanup_ownership_lost",
            f"Exporter ownership cannot be verified for {owned.path}.",
        ) from error
    if (
        not stat.S_ISREG(value.st_mode)
        or (value.st_dev, value.st_ino) != owned.identity
    ):
        raise ExportError(
            "cleanup_ownership_lost",
            f"Exporter ownership cannot be verified for {owned.path}.",
        )
    if content is not None:
        try:
            actual = owned.path.read_bytes()
        except OSError as error:
            raise ExportError(
                "cleanup_ownership_lost",
                f"Exporter ownership cannot be verified for {owned.path}.",
            ) from error
        if actual != content:
            raise ExportError(
                "cleanup_ownership_lost",
                f"Exporter ownership cannot be verified for {owned.path}.",
            )


def _unlink_owned_file(owned: _OwnedFile, content: bytes | None = None) -> None:
    _validate_owned_file(owned, content)
    owned.path.unlink()


def _validate_owned_staging(staged: StagedBundle) -> None:
    _validate_parent(staged.destination.parent, staged.parent_identity)
    try:
        value = staged.staging.stat(follow_symlinks=False)
    except OSError as error:
        raise ExportError(
            "cleanup_ownership_lost",
            f"Exporter ownership cannot be verified for {staged.staging}.",
        ) from error
    if (
        not stat.S_ISDIR(value.st_mode)
        or (
            value.st_dev,
            value.st_ino,
        )
        != staged.staging_identity
    ):
        raise ExportError(
            "cleanup_ownership_lost",
            f"Exporter ownership cannot be verified for {staged.staging}.",
        )
    _validate_owned_file(staged.owner_marker, staged.marker_content)
    _validate_owned_file(staged.reservation_marker, staged.marker_content)


def _cleanup_staged_bundle(staged: StagedBundle, *, keep_reservation: bool) -> None:
    _validate_owned_staging(staged)
    if staged.staging_archive is not None:
        _unlink_owned_file(staged.staging_archive)
    shutil.rmtree(staged.staging)
    _unlink_owned_file(staged.owner_marker, staged.marker_content)
    if not keep_reservation:
        _unlink_owned_file(staged.reservation_marker, staged.marker_content)


def discard_staged_bundle(staged: StagedBundle) -> None:
    """Remove an unpublished bundle after verifying exporter ownership."""
    _cleanup_staged_bundle(staged, keep_reservation=False)


def _write_deterministic_zip(
    root: Path,
    archive_path: Path,
    *,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> _OwnedFile:
    checkpoint = get_cancellation_checkpoint(cancellation_checkpoint)
    descriptor = _open_private_file(archive_path)
    value = os.fstat(descriptor)
    owned = _OwnedFile(path=archive_path, identity=(value.st_dev, value.st_ino))
    try:
        with os.fdopen(descriptor, "w+b") as archive_file:
            with zipfile.ZipFile(
                archive_file, "w", compression=zipfile.ZIP_STORED
            ) as archive:
                for path in sorted(item for item in root.rglob("*") if item.is_file()):
                    checkpoint()
                    relative = path.relative_to(root).as_posix()
                    info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
                    info.compress_type = zipfile.ZIP_STORED
                    info.create_system = 3
                    mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                    info.external_attr = (0o100000 | mode) << 16
                    with path.open("rb") as source, archive.open(info, "w") as target:
                        while chunk := source.read(_COPY_BUFFER_BYTES):
                            checkpoint()
                            target.write(chunk)
            archive_file.flush()
            os.fsync(archive_file.fileno())
    except Exception as error:
        try:
            _unlink_owned_file(owned)
        except ExportError as cleanup_error:
            raise cleanup_error from error
        raise
    checkpoint()
    _validate_owned_file(owned)
    return owned


def _publish_file_noreplace(source: _OwnedFile, destination: Path) -> _OwnedFile:
    _validate_owned_file(source)
    try:
        os.link(source.path, destination, follow_symlinks=False)
    except FileExistsError as error:
        raise ExportError(
            "archive_conflict", f"Archive exists: {destination}"
        ) from error
    published = _OwnedFile(path=destination, identity=source.identity)
    _validate_owned_file(published)
    try:
        source.path.unlink()
    except OSError as error:
        try:
            _unlink_owned_file(published)
        except ExportError as cleanup_error:
            raise cleanup_error from error
        raise
    return published


def _publish_directory_noreplace(source: Path, destination: Path) -> None:
    """Atomically publish a directory without replacing another filesystem name."""
    if sys.platform == "darwin":
        library = ctypes.CDLL(None, use_errno=True)
        renamex_np = library.renamex_np
        renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        renamex_np.restype = ctypes.c_int
        result = renamex_np(os.fsencode(source), os.fsencode(destination), _RENAME_EXCL)
    elif sys.platform.startswith("linux"):
        library = ctypes.CDLL(None, use_errno=True)
        renameat2 = library.renameat2
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(
            _AT_FDCWD,
            os.fsencode(source),
            _AT_FDCWD,
            os.fsencode(destination),
            _RENAME_NOREPLACE,
        )
    elif os.name == "nt":
        os.rename(source, destination)
        return
    else:
        raise ExportError(
            "unsupported_publication_platform",
            f"Exclusive directory publication is unsupported on {sys.platform}.",
        )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, os.strerror(error_number), destination)
    raise OSError(error_number, os.strerror(error_number), destination)


def _reservation_path(destination: Path) -> Path:
    return destination.with_name(f".{destination.name}.kitaru-reservation")


def stage_bundle(
    destination: Path,
    render: Callable[[Path], None],
    *,
    archive: bool = False,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> StagedBundle:
    """Render, validate, hash, and optionally archive without publishing outputs."""
    checkpoint = get_cancellation_checkpoint(cancellation_checkpoint)
    destination = destination.expanduser().absolute()
    parent = destination.parent
    try:
        parent_value = parent.stat(follow_symlinks=False)
    except OSError as error:
        raise ExportError(
            "invalid_destination", "Destination parent must already be a directory."
        ) from error
    if not stat.S_ISDIR(parent_value.st_mode):
        raise ExportError(
            "invalid_destination", "Destination parent must already be a directory."
        )
    parent_identity = (parent_value.st_dev, parent_value.st_ino)
    archive_path = destination.with_name(f"{destination.name}.zip") if archive else None
    if _path_exists(destination):
        raise ExportError("destination_conflict", f"Destination exists: {destination}")
    if archive_path is not None and _path_exists(archive_path):
        raise ExportError("archive_conflict", f"Archive exists: {archive_path}")

    token = uuid.uuid4().hex
    marker_content = canonical_json_bytes(
        {"destination": str(destination), "owner": token, "state": "staging"}
    )
    reservation = _create_marker(
        _reservation_path(destination),
        marker_content,
        conflict_code="destination_conflict",
    )
    staging: Path | None = None
    staging_identity: _PathIdentity | None = None
    owner_marker: _OwnedFile | None = None
    staging_archive: _OwnedFile | None = None
    staged: StagedBundle | None = None
    try:
        _validate_parent(parent, parent_identity)
        staging = Path(
            tempfile.mkdtemp(
                prefix=f".{destination.name}.kitaru-", suffix=".tmp", dir=parent
            )
        )
        staging.chmod(_PRIVATE_DIRECTORY_MODE)
        staging_identity = _identity(staging)
        owner_marker = _create_marker(
            staging.with_name(f"{staging.name}.owner"),
            marker_content,
            conflict_code="destination_conflict",
        )
        checkpoint()
        with export_cancellation_scope(cancellation_checkpoint):
            render(staging)
        checkpoint()
        digest = directory_digest(
            staging, cancellation_checkpoint=cancellation_checkpoint
        )
        checkpoint()
        if archive_path is not None:
            staging_archive = _write_deterministic_zip(
                staging,
                staging.with_name(f"{staging.name}.zip"),
                cancellation_checkpoint=cancellation_checkpoint,
            )
        checkpoint()
        staged = StagedBundle(
            destination=destination,
            archive_path=archive_path,
            staging=staging,
            staging_identity=staging_identity,
            staging_archive=staging_archive,
            owner_marker=owner_marker,
            reservation_marker=reservation,
            marker_content=marker_content,
            parent_identity=parent_identity,
            digest=digest,
        )
        _validate_owned_staging(staged)
        return staged
    except BaseException as error:
        if (
            staged is None
            and staging is not None
            and staging_identity is not None
            and owner_marker is not None
        ):
            staged = StagedBundle(
                destination=destination,
                archive_path=archive_path,
                staging=staging,
                staging_identity=staging_identity,
                staging_archive=staging_archive,
                owner_marker=owner_marker,
                reservation_marker=reservation,
                marker_content=marker_content,
                parent_identity=parent_identity,
                digest="",
            )
        try:
            if staged is not None:
                discard_staged_bundle(staged)
            else:
                _validate_parent(parent, parent_identity)
                _unlink_owned_file(reservation, marker_content)
        except ExportError as cleanup_error:
            raise cleanup_error from error
        raise


def commit_staged_bundle(staged: StagedBundle) -> PublishedBundle:
    """Publish a validated bundle with genuine no-replace filesystem operations."""
    _validate_owned_staging(staged)
    if _path_exists(staged.destination):
        discard_staged_bundle(staged)
        raise ExportError(
            "destination_conflict", f"Destination exists: {staged.destination}"
        )
    published_archive: _OwnedFile | None = None
    try:
        if staged.archive_path is not None:
            if staged.staging_archive is None:
                raise ExportError(
                    "invalid_staged_bundle", "The staged archive is missing."
                )
            published_archive = _publish_file_noreplace(
                staged.staging_archive, staged.archive_path
            )
        try:
            _publish_directory_noreplace(staged.staging, staged.destination)
        except FileExistsError as error:
            raise ExportError(
                "destination_conflict", f"Destination exists: {staged.destination}"
            ) from error
    except BaseException as error:
        try:
            if published_archive is not None:
                _unlink_owned_file(published_archive)
            unpublished = StagedBundle(
                destination=staged.destination,
                archive_path=staged.archive_path,
                staging=staged.staging,
                staging_identity=staged.staging_identity,
                staging_archive=(
                    None if published_archive is not None else staged.staging_archive
                ),
                owner_marker=staged.owner_marker,
                reservation_marker=staged.reservation_marker,
                marker_content=staged.marker_content,
                parent_identity=staged.parent_identity,
                digest=staged.digest,
            )
            discard_staged_bundle(unpublished)
        except ExportError as cleanup_error:
            raise cleanup_error from error
        raise

    # Publication is complete. Marker cleanup is best-effort because returning a
    # failure now would contradict the two already published filesystem names.
    for marker in (staged.owner_marker, staged.reservation_marker):
        with suppress(ExportError, OSError):
            _unlink_owned_file(marker, staged.marker_content)
    return PublishedBundle(
        destination=staged.destination,
        archive_path=staged.archive_path,
        digest=staged.digest,
    )


def publish_bundle(
    destination: Path,
    render: Callable[[Path], None],
    *,
    archive: bool = False,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> PublishedBundle:
    """Stage and publish a complete bundle beside its destination."""
    staged = stage_bundle(
        destination,
        render,
        archive=archive,
        cancellation_checkpoint=cancellation_checkpoint,
    )
    return commit_staged_bundle(staged)
