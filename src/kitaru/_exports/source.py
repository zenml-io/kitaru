"""Safe deterministic agent source inventory."""

import hashlib
import os
from pathlib import Path
from typing import Literal

from kitaru._exports.models import ExportError, SourceFile, SourceInventory

DEFAULT_MAX_FILE_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_TOTAL_BYTES = 1024 * 1024 * 1024

_EXCLUDED_DIRECTORIES = frozenset(
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
_EXCLUDED_FILENAMES = frozenset(
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


def _is_excluded_file(name: str) -> bool:
    lowered = name.lower()
    return (
        lowered == ".env"
        or lowered.startswith(".env.")
        or lowered in _EXCLUDED_FILENAMES
        or lowered.startswith(_PRIVATE_KEY_PREFIXES)
        or lowered.endswith(_PRIVATE_KEY_SUFFIXES)
    )


def inventory_source(
    root: Path,
    *,
    destination: Path | None = None,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
) -> SourceInventory:
    """Inventory a source tree without following unsafe links.

    Args:
        root: Local agent source root.
        destination: Intended output directory, excluded when nested in root.
        max_file_bytes: Maximum allowed size of one included file.
        max_total_bytes: Maximum allowed total included bytes.

    Raises:
        ExportError: The root is invalid, a link escapes it, or a size limit
            is exceeded.

    Returns:
        Stable source inventory.
    """
    try:
        resolved_root = root.expanduser().resolve(strict=True)
    except OSError as error:
        raise ExportError(
            "invalid_source_root", f"Cannot resolve source root: {error}"
        ) from error
    if not resolved_root.is_dir():
        raise ExportError("invalid_source_root", "Source root must be a directory.")

    nested_destination: Path | None = None
    if destination is not None:
        candidate = destination.expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        candidate = candidate.resolve(strict=False)
        if candidate.is_relative_to(resolved_root):
            nested_destination = candidate

    files: list[SourceFile] = []
    excluded: set[str] = set()
    total_size = 0
    for current, directory_names, file_names in os.walk(
        resolved_root, topdown=True, followlinks=False
    ):
        current_path = Path(current)
        kept_directories: list[str] = []
        for name in sorted(directory_names):
            path = current_path / name
            relative = path.relative_to(resolved_root).as_posix()
            if name.lower() in _EXCLUDED_DIRECTORIES or (
                nested_destination is not None
                and (
                    path == nested_destination
                    or path.is_relative_to(nested_destination)
                )
            ):
                excluded.add(relative)
                continue
            if path.is_symlink():
                target = path.resolve(strict=True)
                if not target.is_relative_to(resolved_root):
                    raise ExportError(
                        "source_symlink_escape",
                        f"Source symlink escapes the source root: {relative}",
                    )
                raise ExportError(
                    "unsupported_source_symlink",
                    f"Directory symlinks are not supported: {relative}",
                )
            kept_directories.append(name)
        directory_names[:] = kept_directories

        for name in sorted(file_names):
            path = current_path / name
            relative = path.relative_to(resolved_root).as_posix()
            if _is_excluded_file(name) or (
                nested_destination is not None
                and (
                    path == nested_destination
                    or path.is_relative_to(nested_destination)
                )
            ):
                excluded.add(relative)
                continue

            link_target: str | None = None
            read_path = path
            if path.is_symlink():
                target = path.resolve(strict=True)
                if not target.is_relative_to(resolved_root):
                    raise ExportError(
                        "source_symlink_escape",
                        f"Source symlink escapes the source root: {relative}",
                    )
                if not target.is_file():
                    raise ExportError(
                        "unsupported_source_symlink",
                        f"Source symlink must point to a file: {relative}",
                    )
                read_path = target
                link_target = target.relative_to(resolved_root).as_posix()

            try:
                stat = read_path.stat()
                size = stat.st_size
                mode: Literal[0o644, 0o755] = 0o755 if stat.st_mode & 0o111 else 0o644
                if size > max_file_bytes:
                    raise ExportError(
                        "source_file_too_large",
                        f"Source file exceeds {max_file_bytes} bytes: {relative}",
                    )
                total_size += size
                if total_size > max_total_bytes:
                    raise ExportError(
                        "source_too_large",
                        f"Source tree exceeds {max_total_bytes} bytes.",
                    )
                digest = hashlib.sha256(read_path.read_bytes()).hexdigest()
            except OSError as error:
                raise ExportError(
                    "source_read_failed", f"Cannot read source file {relative}: {error}"
                ) from error
            files.append(
                SourceFile(
                    path=relative,
                    size=size,
                    sha256=digest,
                    mode=mode,
                    link_target=link_target,
                )
            )

    files.sort(key=lambda item: item.path)
    inventory_hash = hashlib.sha256()
    for item in files:
        inventory_hash.update(item.path.encode("utf-8"))
        inventory_hash.update(b"\0")
        inventory_hash.update(item.sha256.encode("ascii"))
        inventory_hash.update(b"\0")
        inventory_hash.update(str(item.size).encode("ascii"))
        inventory_hash.update(b"\0")
        inventory_hash.update(f"{item.mode:o}".encode("ascii"))
        inventory_hash.update(b"\0")
        inventory_hash.update((item.link_target or "").encode("utf-8"))
        inventory_hash.update(b"\n")
    return SourceInventory(
        root=resolved_root,
        files=tuple(files),
        excluded=tuple(sorted(excluded)),
        digest=inventory_hash.hexdigest(),
    )


def copy_source(inventory: SourceInventory, destination: Path) -> None:
    """Copy exactly the inventoried source files into a bundle.

    Args:
        inventory: Previously validated source inventory.
        destination: Empty target source directory.

    Raises:
        ExportError: A source file changed after inventory or cannot be copied.
    """
    destination.mkdir(parents=True, exist_ok=False)
    for item in inventory.files:
        source = inventory.root / item.path
        target = destination / item.path
        try:
            content = source.read_bytes()
        except OSError as error:
            raise ExportError(
                "source_read_failed", f"Cannot read source file {item.path}: {error}"
            ) from error
        if (
            len(content) != item.size
            or hashlib.sha256(content).hexdigest() != item.sha256
        ):
            raise ExportError(
                "source_changed", f"Source file changed during export: {item.path}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        target.chmod(item.mode)
