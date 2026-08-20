"""Safe deterministic agent source snapshots."""

import errno
import hashlib
import os
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import BinaryIO, Literal

from kitaru.exports.models import (
    V1_EXPORT_BUDGETS,
    ExportError,
    SourceFile,
    SourceInventory,
    SourcePolicy,
)

DEFAULT_MAX_FILE_BYTES = V1_EXPORT_BUDGETS.max_source_file_bytes
DEFAULT_MAX_TOTAL_BYTES = V1_EXPORT_BUDGETS.max_total_source_bytes
DEFAULT_MAX_FILES = V1_EXPORT_BUDGETS.max_source_files
DEFAULT_MAX_PATH_BYTES = V1_EXPORT_BUDGETS.max_relative_path_bytes

_GENERATED_DIRECTORIES = frozenset(
    {
        ".git",
        ".hg",
        ".mypy_cache",
        ".nox",
        ".pytest_cache",
        ".ruff_cache",
        ".svn",
        ".tox",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "env",
        "node_modules",
        "venv",
    }
)
_PROTECTED_DIRECTORIES = frozenset({".kitaru", ".state", ".zen"})
_PROTECTED_FILENAMES = frozenset(
    {
        ".netrc",
        ".npmrc",
        ".pypirc",
        "credentials",
        "credentials.json",
        "secrets.json",
    }
)
_PRIVATE_KEY_PREFIXES = ("id_dsa", "id_ecdsa", "id_ed25519", "id_rsa")
_PRIVATE_KEY_SUFFIXES = (".key", ".p12", ".pem", ".pfx")
_COPY_BUFFER_BYTES = 1024 * 1024
CancellationCheckpoint = Callable[[], None]
_ACTIVE_CANCELLATION_CHECKPOINT: ContextVar[CancellationCheckpoint | None] = ContextVar(
    "kitaru_export_source_cancellation_checkpoint", default=None
)


def _noop_checkpoint() -> None:
    return None


def _get_checkpoint(
    cancellation_checkpoint: CancellationCheckpoint | None,
) -> CancellationCheckpoint:
    return (
        cancellation_checkpoint
        or _ACTIVE_CANCELLATION_CHECKPOINT.get()
        or _noop_checkpoint
    )


@contextmanager
def export_cancellation_scope(
    cancellation_checkpoint: CancellationCheckpoint | None,
) -> Iterator[None]:
    """Make one checkpoint available to nested source-copy calls."""
    token = _ACTIVE_CANCELLATION_CHECKPOINT.set(cancellation_checkpoint)
    try:
        yield
    finally:
        _ACTIVE_CANCELLATION_CHECKPOINT.reset(token)


@dataclass(frozen=True)
class _SnapshotSourceInventory(SourceInventory):
    """Retain the exact bytes used to construct one source inventory."""

    contents: Mapping[str, bytes] = field(repr=False, kw_only=True)


def _is_protected_filename(name: str) -> bool:
    lowered = name.lower()
    return (
        lowered == ".env"
        or lowered == ".envrc"
        or lowered.startswith(".env.")
        or lowered in _PROTECTED_FILENAMES
        or lowered.startswith(_PRIVATE_KEY_PREFIXES)
        or lowered.endswith(_PRIVATE_KEY_SUFFIXES)
    )


def _is_protected_path(path: PurePosixPath) -> bool:
    return any(part.lower() in _PROTECTED_DIRECTORIES for part in path.parts) or (
        bool(path.name) and _is_protected_filename(path.name)
    )


def _path_is_at_or_below(path: PurePosixPath, parent: PurePosixPath) -> bool:
    return path == parent or parent in path.parents


def _rule_affects_subtree(rule: PurePosixPath, directory: PurePosixPath) -> bool:
    return _path_is_at_or_below(rule, directory) or _path_is_at_or_below(
        directory, rule
    )


def _stat_identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _stat_version(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_source_file(path: Path) -> BinaryIO:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    return os.fdopen(descriptor, "rb")


def _read_opened_file(
    source: BinaryIO,
    *,
    max_bytes: int,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> bytes:
    checkpoint = _get_checkpoint(cancellation_checkpoint)
    content = bytearray()
    while chunk := source.read(min(_COPY_BUFFER_BYTES, max_bytes + 1 - len(content))):
        checkpoint()
        content.extend(chunk)
        if len(content) > max_bytes:
            break
    checkpoint()
    return bytes(content)


def _validate_regular_file(value: os.stat_result, relative: str) -> None:
    if stat.S_ISLNK(value.st_mode):
        raise ExportError(
            "unsupported_source_symlink",
            f"Source symlinks are not supported: {relative}",
        )
    if not stat.S_ISREG(value.st_mode):
        raise ExportError(
            "unsupported_source_file",
            f"Source entries must be regular files or directories: {relative}",
        )
    if value.st_nlink != 1:
        raise ExportError(
            "unsupported_source_hardlink",
            f"Source files with multiple hard links are not supported: {relative}",
        )


def _snapshot_file(
    path: Path,
    relative: str,
    *,
    max_file_bytes: int,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> tuple[SourceFile, bytes]:
    try:
        path_before = path.stat(follow_symlinks=False)
        _validate_regular_file(path_before, relative)
        if path_before.st_size > max_file_bytes:
            raise ExportError(
                "source_file_too_large",
                f"Source file exceeds {max_file_bytes} bytes: {relative}",
            )
        with _open_source_file(path) as source:
            handle_before = os.fstat(source.fileno())
            _validate_regular_file(handle_before, relative)
            if _stat_identity(handle_before) != _stat_identity(path_before):
                raise ExportError(
                    "source_changed",
                    f"Source file changed during export: {relative}",
                )
            token = _ACTIVE_CANCELLATION_CHECKPOINT.set(cancellation_checkpoint)
            try:
                content = _read_opened_file(source, max_bytes=max_file_bytes)
            finally:
                _ACTIVE_CANCELLATION_CHECKPOINT.reset(token)
            handle_after = os.fstat(source.fileno())
    except ExportError:
        raise
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise ExportError(
                "unsupported_source_symlink",
                f"Source symlinks are not supported: {relative}",
            ) from error
        raise ExportError(
            "source_read_failed", f"Cannot read source file {relative}: {error}"
        ) from error
    try:
        path_after = path.stat(follow_symlinks=False)
    except OSError as error:
        raise ExportError(
            "source_changed", f"Source file changed during export: {relative}"
        ) from error

    if (
        not stat.S_ISREG(handle_after.st_mode)
        or not stat.S_ISREG(path_after.st_mode)
        or path_after.st_nlink != 1
    ):
        raise ExportError(
            "source_changed", f"Source file changed during export: {relative}"
        )
    if len(content) > max_file_bytes:
        raise ExportError(
            "source_file_too_large",
            f"Source file exceeds {max_file_bytes} bytes: {relative}",
        )
    if (
        _stat_version(handle_before) != _stat_version(handle_after)
        or _stat_identity(handle_after) != _stat_identity(path_after)
        or len(content) != handle_after.st_size
    ):
        raise ExportError(
            "source_changed", f"Source file changed during export: {relative}"
        )
    mode: Literal[0o644, 0o755] = 0o755 if handle_after.st_mode & 0o111 else 0o644
    return (
        SourceFile(
            path=relative,
            size=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            mode=mode,
            link_target=None,
        ),
        content,
    )


def _relative_reserved_path(root: Path, path: Path | None) -> PurePosixPath | None:
    if path is None:
        return None
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    candidate = candidate.resolve(strict=False)
    try:
        relative = candidate.relative_to(root)
    except ValueError:
        return None
    if not relative.parts:
        raise ExportError(
            "invalid_source_destination",
            "The export destination cannot be the source root.",
        )
    return PurePosixPath(relative.as_posix())


def inventory_source(
    root: Path,
    *,
    source_policy: SourcePolicy | None = None,
    destination: Path | None = None,
    archive_path: Path | None = None,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    max_files: int = DEFAULT_MAX_FILES,
    max_path_bytes: int = DEFAULT_MAX_PATH_BYTES,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> SourceInventory:
    """Acquire one bounded immutable source snapshot without following links."""
    checkpoint = _get_checkpoint(cancellation_checkpoint)
    checkpoint()
    try:
        requested_root = root.expanduser().absolute()
        current = Path(requested_root.anchor)
        for part in requested_root.parts[1:]:
            current /= part
            if stat.S_ISLNK(current.lstat().st_mode):
                raise ExportError(
                    "unsupported_source_symlink",
                    "Source roots and their path components cannot be symlinks.",
                )
        resolved_root = requested_root.resolve(strict=True)
        root_stat = resolved_root.stat()
    except ExportError:
        raise
    except OSError as error:
        raise ExportError(
            "invalid_source_root", f"Cannot resolve source root: {error}"
        ) from error
    if not stat.S_ISDIR(root_stat.st_mode):
        raise ExportError("invalid_source_root", "Source root must be a directory.")

    policy = source_policy or SourcePolicy()
    includes = tuple(PurePosixPath(value) for value in policy.include)
    excludes = tuple(PurePosixPath(value) for value in policy.exclude)
    for included in includes:
        if _is_protected_path(included):
            raise ExportError(
                "protected_source_path",
                f"Protected source paths cannot be explicitly included: {included}",
            )

    reserved = tuple(
        item
        for item in (
            _relative_reserved_path(resolved_root, destination),
            _relative_reserved_path(resolved_root, archive_path),
        )
        if item is not None
    )
    staging_parent: PurePosixPath | None = None
    staging_prefix: str | None = None
    reservation_name: str | None = None
    if destination is not None:
        absolute_destination = destination.expanduser()
        if not absolute_destination.is_absolute():
            absolute_destination = Path.cwd() / absolute_destination
        try:
            staging_parent_path = absolute_destination.parent.resolve(
                strict=False
            ).relative_to(resolved_root)
        except ValueError:
            pass
        else:
            staging_parent = PurePosixPath(staging_parent_path.as_posix())
            staging_prefix = f".{absolute_destination.name}.kitaru-"
            reservation_name = f".{absolute_destination.name}.kitaru-reservation"

    files: list[SourceFile] = []
    contents: dict[str, bytes] = {}
    excluded: set[str] = set()
    matched_includes: set[PurePosixPath] = set()
    matched_excludes: set[PurePosixPath] = set()
    total_size = 0
    file_count = 0

    def matches_rule(path: PurePosixPath, rules: tuple[PurePosixPath, ...]) -> bool:
        return any(_path_is_at_or_below(path, rule) for rule in rules)

    def mark_rules(path: PurePosixPath) -> None:
        matched_includes.update(
            rule for rule in includes if _path_is_at_or_below(path, rule)
        )
        matched_excludes.update(
            rule for rule in excludes if _path_is_at_or_below(path, rule)
        )

    def is_reserved(path: PurePosixPath) -> bool:
        if any(_path_is_at_or_below(path, item) for item in reserved):
            return True
        if staging_parent is None or staging_prefix is None or reservation_name is None:
            return False
        parent = path.parent if path.parent.parts else PurePosixPath(".")
        return (
            parent == staging_parent
            and path.name.startswith(staging_prefix)
            and (
                path.name.endswith(".tmp")
                or path.name.endswith(".tmp.zip")
                or path.name.endswith(".tmp.owner")
            )
        ) or (parent == staging_parent and path.name == reservation_name)

    def walk(directory: Path, relative_directory: PurePosixPath) -> None:
        nonlocal file_count, total_size
        checkpoint()
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda item: item.name)
        except OSError as error:
            relative = relative_directory.as_posix()
            raise ExportError(
                "source_read_failed",
                f"Cannot read source directory {relative}: {error}",
            ) from error
        for entry in entries:
            checkpoint()
            path = Path(entry.path)
            relative_path = relative_directory / entry.name
            relative = relative_path.as_posix()
            if len(relative.encode("utf-8")) > max_path_bytes:
                raise ExportError(
                    "source_path_too_long",
                    f"Source path exceeds {max_path_bytes} UTF-8 bytes: {relative}",
                )
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise ExportError(
                    "source_read_failed",
                    f"Cannot inspect source path {relative}: {error}",
                ) from error
            if stat.S_ISLNK(entry_stat.st_mode):
                raise ExportError(
                    "unsupported_source_symlink",
                    f"Source symlinks are not supported: {relative}",
                )
            if stat.S_ISDIR(entry_stat.st_mode):
                if entry_stat.st_dev != root_stat.st_dev:
                    raise ExportError(
                        "unsupported_source_mount",
                        f"Nested source mounts are not supported: {relative}",
                    )
                mark_rules(relative_path)
                protected = _is_protected_path(relative_path)
                generated = entry.name.lower() in _GENERATED_DIRECTORIES
                explicitly_excluded = matches_rule(relative_path, excludes)
                required_for_rule = any(
                    _rule_affects_subtree(rule, relative_path)
                    for rule in (*includes, *excludes)
                )
                if (
                    is_reserved(relative_path)
                    or protected
                    or explicitly_excluded
                    or (generated and not required_for_rule)
                ):
                    excluded.add(relative)
                    continue
                walk(path, relative_path)
                continue
            file_count += 1
            if file_count > max_files:
                raise ExportError(
                    "too_many_source_files",
                    f"Source tree exceeds {max_files} files.",
                )
            _validate_regular_file(entry_stat, relative)
            mark_rules(relative_path)
            if (
                is_reserved(relative_path)
                or _is_protected_path(relative_path)
                or matches_rule(relative_path, excludes)
            ):
                excluded.add(relative)
                continue
            generated = any(
                part.lower() in _GENERATED_DIRECTORIES
                for part in relative_path.parts[:-1]
            )
            if generated and not matches_rule(relative_path, includes):
                excluded.add(relative)
                continue
            if total_size + entry_stat.st_size > max_total_bytes:
                raise ExportError(
                    "source_too_large",
                    f"Source tree exceeds {max_total_bytes} bytes.",
                )
            item, content = _snapshot_file(
                path,
                relative,
                max_file_bytes=max_file_bytes,
                cancellation_checkpoint=cancellation_checkpoint,
            )
            total_size += item.size
            if total_size > max_total_bytes:
                raise ExportError(
                    "source_too_large",
                    f"Source tree exceeds {max_total_bytes} bytes.",
                )
            files.append(item)
            contents[item.path] = content

    walk(resolved_root, PurePosixPath())
    missing_includes = sorted(set(includes) - matched_includes)
    missing_excludes = sorted(set(excludes) - matched_excludes)
    if missing_includes or missing_excludes:
        missing = ", ".join(
            str(item) for item in (*missing_includes, *missing_excludes)
        )
        raise ExportError(
            "source_policy_path_missing",
            f"Source policy paths do not exist: {missing}",
        )

    files.sort(key=lambda item: item.path)
    inventory_hash = hashlib.sha256()
    for item in files:
        checkpoint()
        inventory_hash.update(item.path.encode("utf-8"))
        inventory_hash.update(b"\0")
        inventory_hash.update(item.sha256.encode("ascii"))
        inventory_hash.update(b"\0")
        inventory_hash.update(str(item.size).encode("ascii"))
        inventory_hash.update(b"\0")
        inventory_hash.update(f"{item.mode:o}".encode("ascii"))
        inventory_hash.update(b"\0\n")
    checkpoint()
    return _SnapshotSourceInventory(
        root=resolved_root,
        files=tuple(files),
        excluded=tuple(sorted(excluded)),
        digest=inventory_hash.hexdigest(),
        contents=MappingProxyType(contents),
    )


def source_file_bytes(inventory: SourceInventory, path: str) -> bytes:
    """Return retained bytes for one file in an acquired source snapshot."""
    if isinstance(inventory, _SnapshotSourceInventory):
        content = inventory.contents.get(path)
        if content is not None:
            return content
    raise ExportError(
        "source_snapshot_unavailable",
        f"Source snapshot bytes are unavailable for {path}.",
    )


def copy_source(
    inventory: SourceInventory,
    destination: Path,
    *,
    cancellation_checkpoint: CancellationCheckpoint | None = None,
) -> None:
    """Copy retained source bytes without reopening source paths."""
    checkpoint = _get_checkpoint(cancellation_checkpoint)
    checkpoint()
    destination.mkdir(parents=True, exist_ok=False)
    for item in inventory.files:
        checkpoint()
        target = destination / item.path
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_bytes(source_file_bytes(inventory, item.path))
            target.chmod(item.mode)
        except ExportError:
            raise
        except OSError as error:
            raise ExportError(
                "source_copy_failed",
                f"Cannot copy snapshotted source file {item.path}: {error}",
            ) from error
    checkpoint()
