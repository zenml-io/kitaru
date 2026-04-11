"""Cleanup planning and execution for `kitaru clean`."""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME
from kitaru.config import ActiveEnvironmentVariable
from kitaru.errors import KitaruUsageError

logger = logging.getLogger(__name__)

# Show individual store subdirs in dry-run preview only up to this count;
# beyond this, show a count + total size to avoid overwhelming output.
_LOCAL_STORES_THRESHOLD = 5


class CleanScope(StrEnum):
    """Scope of a cleanup operation."""

    PROJECT = "project"
    GLOBAL = "global"
    ALL = "all"


class PreviewEntryType(StrEnum):
    """Type of node in a cleanup dry-run preview tree."""

    FILE = "file"
    DIRECTORY = "directory"
    BACKUP = "backup"


@dataclass(frozen=True)
class CleanupPreviewEntry:
    """One node in the dry-run preview tree."""

    path: str
    entry_type: PreviewEntryType
    size_bytes: int | None = None
    note: str | None = None
    children: tuple[CleanupPreviewEntry, ...] = ()


@dataclass(frozen=True)
class CleanupPlan:
    """Resolved cleanup intent before confirmation/execution."""

    scope: CleanScope
    repo_root: str | None = None
    project_config_path: str | None = None
    global_config_root: str | None = None
    backup_path: str | None = None
    preview_entries: tuple[CleanupPreviewEntry, ...] = ()
    total_bytes: int = 0
    model_registry_alias_count: int | None = None
    local_server_status: str | None = None
    local_server_would_stop: bool = False
    custom_config_path_warning: str | None = None
    active_environment_overrides: tuple[ActiveEnvironmentVariable, ...] = ()
    can_reinitialize_project: bool = False


@dataclass(frozen=True)
class CleanupResult:
    """Structured outcome of a cleanup operation."""

    scope: CleanScope
    dry_run: bool = False
    aborted: bool = False
    backup_path: str | None = None
    deleted_paths: tuple[str, ...] = ()
    local_server_stopped: bool = False
    local_server_force_killed_pid: int | None = None
    reinitialized_project: bool = False
    warnings: tuple[str, ...] = ()
    active_environment_overrides: tuple[ActiveEnvironmentVariable, ...] = ()
    preview_entries: tuple[CleanupPreviewEntry, ...] = ()
    total_bytes: int = 0
    local_server_status: str | None = None


# ---------------------------------------------------------------------------
# Preview / dry-run tree building
# ---------------------------------------------------------------------------


def _dir_size(path: Path) -> int:
    """Compute total size of a directory tree."""
    total = 0
    try:
        for entry in path.rglob("*"):
            if entry.is_file():
                with contextlib.suppress(OSError):
                    total += entry.stat().st_size
    except OSError:
        pass
    return total


def _file_size(path: Path) -> int:
    """Return file size, 0 on error."""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def format_size(size_bytes: int) -> str:
    """Human-readable file size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _build_project_preview(project_config_path: Path) -> list[CleanupPreviewEntry]:
    """Build preview entries for project-local config."""
    entries: list[CleanupPreviewEntry] = []
    if not project_config_path.exists():
        return entries

    children: list[CleanupPreviewEntry] = []
    total = 0
    try:
        for child in sorted(project_config_path.iterdir()):
            if child.is_file():
                size = _file_size(child)
                total += size
                children.append(
                    CleanupPreviewEntry(
                        path=str(child),
                        entry_type=PreviewEntryType.FILE,
                        size_bytes=size,
                    )
                )
            elif child.is_dir():
                size = _dir_size(child)
                total += size
                children.append(
                    CleanupPreviewEntry(
                        path=str(child),
                        entry_type=PreviewEntryType.DIRECTORY,
                        size_bytes=size,
                    )
                )
    except OSError:
        pass

    entries.append(
        CleanupPreviewEntry(
            path=str(project_config_path),
            entry_type=PreviewEntryType.DIRECTORY,
            size_bytes=total,
            children=tuple(children),
        )
    )
    return entries


def _build_global_preview(
    config_root: Path,
    *,
    alias_count: int | None,
    backup_path: str | None,
) -> list[CleanupPreviewEntry]:
    """Build preview entries for global config root."""
    entries: list[CleanupPreviewEntry] = []
    if not config_root.exists():
        return entries

    children: list[CleanupPreviewEntry] = []
    total = 0

    kitaru_yaml = config_root / "kitaru.yaml"
    if kitaru_yaml.exists():
        size = _file_size(kitaru_yaml)
        total += size
        note = None
        if alias_count is not None and alias_count > 0:
            note = f"model_registry: {alias_count} aliases (use --force to proceed)"
        children.append(
            CleanupPreviewEntry(
                path=str(kitaru_yaml),
                entry_type=PreviewEntryType.FILE,
                size_bytes=size,
                note=note,
            )
        )

    zenml_config = config_root / "config.yaml"
    if zenml_config.exists():
        size = _file_size(zenml_config)
        total += size
        children.append(
            CleanupPreviewEntry(
                path=str(zenml_config),
                entry_type=PreviewEntryType.FILE,
                size_bytes=size,
            )
        )

    local_stores = config_root / "local_stores"
    if local_stores.exists():
        try:
            store_dirs = sorted(d for d in local_stores.iterdir() if d.is_dir())
        except OSError:
            store_dirs = []

        # Compute per-child sizes once, then sum for the parent total
        # to avoid walking the tree twice.
        child_sizes = [(d, _dir_size(d)) for d in store_dirs]
        try:
            top_level_files_size = sum(
                _file_size(f) for f in local_stores.iterdir() if f.is_file()
            )
        except OSError:
            top_level_files_size = 0
        ls_size = sum(s for _, s in child_sizes) + top_level_files_size
        total += ls_size

        ls_children: tuple[CleanupPreviewEntry, ...] = ()
        if len(store_dirs) <= _LOCAL_STORES_THRESHOLD:
            ls_children = tuple(
                CleanupPreviewEntry(
                    path=str(d),
                    entry_type=PreviewEntryType.DIRECTORY,
                    size_bytes=size,
                )
                for d, size in child_sizes
            )

        note = f"{len(store_dirs)} stores" if store_dirs else None
        children.append(
            CleanupPreviewEntry(
                path=str(local_stores),
                entry_type=PreviewEntryType.DIRECTORY,
                size_bytes=ls_size,
                note=note,
                children=ls_children,
            )
        )

    daemon_dir = config_root / "zen_server" / "daemon"
    if daemon_dir.exists():
        size = _dir_size(daemon_dir)
        total += size
        children.append(
            CleanupPreviewEntry(
                path=str(daemon_dir),
                entry_type=PreviewEntryType.DIRECTORY,
                size_bytes=size,
            )
        )

    # Capture any other top-level entries not already covered
    known_names = {"kitaru.yaml", "config.yaml", "local_stores", "zen_server"}
    try:
        for child in sorted(config_root.iterdir()):
            if child.name in known_names:
                continue
            if child.is_file():
                size = _file_size(child)
                total += size
                children.append(
                    CleanupPreviewEntry(
                        path=str(child),
                        entry_type=PreviewEntryType.FILE,
                        size_bytes=size,
                    )
                )
            elif child.is_dir():
                size = _dir_size(child)
                total += size
                children.append(
                    CleanupPreviewEntry(
                        path=str(child),
                        entry_type=PreviewEntryType.DIRECTORY,
                        size_bytes=size,
                    )
                )
    except OSError:
        pass

    entries.append(
        CleanupPreviewEntry(
            path=str(config_root),
            entry_type=PreviewEntryType.DIRECTORY,
            size_bytes=total,
            children=tuple(children),
        )
    )

    if backup_path:
        entries.append(
            CleanupPreviewEntry(
                path=backup_path,
                entry_type=PreviewEntryType.BACKUP,
                note="Database backup will be saved here",
            )
        )

    return entries


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _resolve_repo_root() -> Path | None:
    """Resolve the Kitaru project root by walking up the directory tree.

    Falls back to a manual directory search if the Client cannot read
    the repository — the cleanup tool must work on broken projects.
    """
    from zenml.client import Client

    try:
        root = Client.find_repository()
        if isinstance(root, Path):
            return root
    except Exception:
        logger.debug(
            "Client.find_repository() failed; trying manual search", exc_info=True
        )

    # Manual fallback: walk up from cwd looking for the .kitaru/ marker.
    cwd = Path.cwd()
    for parent in (cwd, *cwd.parents):
        marker = parent / KITARU_REPOSITORY_DIRECTORY_NAME
        if marker.is_dir():
            return parent
    return None


def _resolve_config_root() -> Path:
    """Resolve the effective global config directory."""
    from kitaru._config._log_store import _kitaru_config_dir

    return _kitaru_config_dir()


def _config_path_source() -> str | None:
    """Return the env var name that overrides the config path, if any."""
    from kitaru._config._env import KITARU_CONFIG_PATH_ENV
    from kitaru._env import ZENML_CONFIG_PATH_ENV

    if os.environ.get(KITARU_CONFIG_PATH_ENV):
        return KITARU_CONFIG_PATH_ENV
    if os.environ.get(ZENML_CONFIG_PATH_ENV):
        return ZENML_CONFIG_PATH_ENV
    return None


def _read_alias_count() -> int | None:
    """Read the model registry alias count from global config.

    Returns None if config is unreadable, meaning --force is still required.
    """
    try:
        from kitaru._config._log_store import _read_kitaru_global_config

        config = _read_kitaru_global_config()
        if config.model_registry is None:
            return 0
        return len(config.model_registry.aliases)
    except Exception:
        logger.debug("Could not read model registry alias count", exc_info=True)
        return None


def _find_local_database(config_root: Path) -> Path | None:
    """Locate the first SQLite database file under the config root."""
    candidates = [
        config_root / "local_stores" / "default_zen_store" / "zenml.db",
    ]
    local_stores = config_root / "local_stores"
    if local_stores.exists():
        try:
            for candidate in local_stores.rglob("*.db"):
                if candidate not in candidates:
                    candidates.append(candidate)
        except OSError:
            pass

    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _compute_backup_path(config_root: Path) -> str | None:
    """Compute the SQLite backup destination path.

    Returns None if no local database file is found.
    """
    if _find_local_database(config_root) is None:
        return None

    backup_dir = config_root.parent / f"{config_root.name}-backups"
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S")
    return str(backup_dir / f"backup-{timestamp}.db")


def _describe_local_server_for_cleanup() -> tuple[str | None, bool]:
    """Return local server status description and whether it would be stopped."""
    try:
        from zenml.utils.server_utils import get_local_server
    except ImportError:
        return None, False

    try:
        local_server = get_local_server()
    except Exception:
        return None, False

    if local_server is None:
        return "not running", False

    from kitaru._local_server import _existing_local_server_url

    url = _existing_local_server_url(local_server)
    if url:
        return f"running at {url}", True
    return "registered but not running", True


def build_cleanup_plan(
    scope: CleanScope,
) -> CleanupPlan:
    """Build a cleanup plan describing what would be deleted."""
    repo_root: str | None = None
    project_config_path: str | None = None
    global_config_root: str | None = None
    backup_path: str | None = None
    alias_count: int | None = None
    custom_config_warning: str | None = None
    local_server_status: str | None = None
    local_server_would_stop = False
    can_reinit = False
    preview_entries: list[CleanupPreviewEntry] = []
    total_bytes = 0

    if scope in (CleanScope.PROJECT, CleanScope.ALL):
        resolved_root = _resolve_repo_root()
        if resolved_root is not None:
            repo_root = str(resolved_root)
            project_marker = resolved_root / KITARU_REPOSITORY_DIRECTORY_NAME
            if project_marker.exists():
                project_config_path = str(project_marker)
                can_reinit = True

        if scope == CleanScope.PROJECT and project_config_path is None:
            raise KitaruUsageError(
                "No Kitaru project found. Run `kitaru init` to create one."
            )

    if scope in (CleanScope.GLOBAL, CleanScope.ALL):
        config_root = _resolve_config_root()
        global_config_root = str(config_root)

        source = _config_path_source()
        if source is not None:
            custom_config_warning = (
                f"Cleaning custom config path {config_root} (set by {source})"
            )

        alias_count = _read_alias_count()
        backup_path = _compute_backup_path(config_root)
        local_server_status, local_server_would_stop = (
            _describe_local_server_for_cleanup()
        )

        global_entries = _build_global_preview(
            config_root,
            alias_count=alias_count,
            backup_path=backup_path,
        )
        preview_entries.extend(global_entries)
        for entry in global_entries:
            if (
                entry.entry_type != PreviewEntryType.BACKUP
                and entry.size_bytes is not None
            ):
                total_bytes += entry.size_bytes

    if project_config_path is not None:
        project_entries = _build_project_preview(Path(project_config_path))
        preview_entries.extend(project_entries)
        for entry in project_entries:
            if entry.size_bytes is not None:
                total_bytes += entry.size_bytes

    from kitaru.config import list_active_kitaru_environment_variables

    env_overrides = list_active_kitaru_environment_variables()

    return CleanupPlan(
        scope=scope,
        repo_root=repo_root,
        project_config_path=project_config_path,
        global_config_root=global_config_root,
        backup_path=backup_path,
        preview_entries=tuple(preview_entries),
        total_bytes=total_bytes,
        model_registry_alias_count=alias_count,
        local_server_status=local_server_status,
        local_server_would_stop=local_server_would_stop,
        custom_config_path_warning=custom_config_warning,
        active_environment_overrides=tuple(env_overrides),
        can_reinitialize_project=can_reinit,
    )


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _create_backup(backup_path: str, config_root: Path) -> None:
    """Copy the local SQLite database to the backup location."""
    db_path = _find_local_database(config_root)
    if db_path is None:
        logger.warning("No SQLite database found to back up")
        return

    backup = Path(backup_path)
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup)


_DANGEROUS_PATH_NAMES = frozenset({"/", ""})


def _validate_deletion_target(path: Path) -> None:
    """Raise if *path* is a dangerous deletion target.

    Guards against misconfigured KITARU_CONFIG_PATH pointing at /, $HOME,
    the repo root, or other high-value directories.
    """
    resolved = path.resolve()

    if str(resolved) in _DANGEROUS_PATH_NAMES:
        raise KitaruUsageError(
            f"Refusing to delete '{path}': resolves to filesystem root."
        )

    home = Path.home().resolve()
    if resolved == home:
        raise KitaruUsageError(
            f"Refusing to delete '{path}': resolves to home directory ({home})."
        )

    # Refuse to delete the current working directory or its parents
    cwd = Path.cwd().resolve()
    if resolved == cwd or cwd.is_relative_to(resolved):
        raise KitaruUsageError(
            f"Refusing to delete '{path}': contains the current working "
            f"directory ({cwd})."
        )


def _delete_directory(path: Path) -> bool:
    """Delete a directory tree. Returns True if deleted."""
    if not path.exists():
        return False
    _validate_deletion_target(path)
    shutil.rmtree(path)
    return True


def _reset_global_config() -> None:
    """Reset ZenML global singletons and recreate default config."""
    from zenml.client import Client
    from zenml.config.global_config import GlobalConfiguration

    old_gc = GlobalConfiguration()
    # Preserve identity and opt-in preference so analytics continuity is
    # maintained and the user's tracking consent is not silently reset.
    user_id = getattr(old_gc, "user_id", None)
    analytics_opt_in = getattr(old_gc, "analytics_opt_in", None)

    GlobalConfiguration._reset_instance()
    Client._reset_instance()

    new_gc = GlobalConfiguration()
    if user_id is not None:
        new_gc.user_id = user_id
    if analytics_opt_in is not None:
        new_gc.analytics_opt_in = analytics_opt_in

    try:
        new_gc.set_default_store()
    except ImportError:
        logger.warning(
            "Local store backend unavailable; global config reset to "
            "defaults without a store."
        )


def _reinitialize_project(repo_root: Path) -> bool:
    """Re-initialize a Kitaru project at the given root."""
    from zenml.client import Client

    project_marker = repo_root / KITARU_REPOSITORY_DIRECTORY_NAME
    if project_marker.exists():
        shutil.rmtree(project_marker)

    try:
        Client.initialize(root=repo_root)
        return True
    except Exception:
        logger.warning(
            "Could not re-initialize project at %s", repo_root, exc_info=True
        )
        return False


def execute_cleanup_plan(
    plan: CleanupPlan,
    *,
    yes: bool = False,
    force: bool = False,
    prompt_confirm: Callable[[str], bool] | None = None,
    prompt_reinitialize: Callable[[str], bool] | None = None,
) -> CleanupResult:
    """Execute a cleanup plan.

    Callers must provide ``prompt_confirm`` when ``yes`` is False.
    """
    scope = plan.scope
    warnings: list[str] = []
    deleted_paths: list[str] = []
    server_stopped = False
    force_killed_pid: int | None = None
    reinitialized = False

    if plan.custom_config_path_warning:
        warnings.append(plan.custom_config_path_warning)

    # Enforce --force for model registry protection
    if scope in (CleanScope.GLOBAL, CleanScope.ALL):
        alias_count = plan.model_registry_alias_count
        needs_force = (
            alias_count is not None and alias_count > 0
        ) or alias_count is None
        if needs_force and not force:
            count_label = (
                f"{alias_count} aliases"
                if alias_count is not None
                else "an unknown number of aliases (config unreadable)"
            )
            raise KitaruUsageError(
                f"Model registry has {count_label} that will be lost. "
                "Use --force to proceed."
            )

    if not yes:
        if prompt_confirm is None:
            raise KitaruUsageError(
                "Non-interactive environment requires --yes to proceed."
            )
        if plan.global_config_root and plan.project_config_path:
            config_label = f"{plan.global_config_root} and {plan.project_config_path}"
        else:
            config_label = (
                plan.global_config_root or plan.project_config_path or "unknown"
            )
        if not prompt_confirm(
            f"This will delete Kitaru state at {config_label}. Continue?"
        ):
            return CleanupResult(scope=scope, aborted=True)

    if scope in (CleanScope.GLOBAL, CleanScope.ALL) and plan.global_config_root:
        config_root = Path(plan.global_config_root)

        # Stop the local server BEFORE backing up, so the SQLite database
        # is quiesced and the backup captures all committed data.
        if plan.local_server_would_stop:
            from kitaru._local_server import stop_registered_local_server_for_cleanup

            server_result = stop_registered_local_server_for_cleanup(timeout=10)
            server_stopped = server_result.stopped
            force_killed_pid = server_result.force_killed_pid
            if force_killed_pid is not None:
                warnings.append(
                    "Local server did not shut down gracefully. "
                    f"Force-killed process {force_killed_pid}."
                )
            elif not server_result.stopped:
                warnings.append(
                    "Could not stop the local server. The database backup "
                    "may be incomplete if the server is still writing."
                )

        if plan.backup_path:
            try:
                _create_backup(plan.backup_path, config_root)
            except Exception as exc:
                raise KitaruUsageError(
                    f"Failed to create database backup: {exc}"
                ) from exc

        try:
            if _delete_directory(config_root):
                deleted_paths.append(str(config_root))
            else:
                warnings.append(f"Config directory already absent: {config_root}")
        except (OSError, KitaruUsageError) as exc:
            raise KitaruUsageError(
                f"Failed to delete config directory {config_root}: {exc}"
            ) from exc

        try:
            _reset_global_config()
        except Exception as exc:
            warnings.append(
                f"Could not reinitialize global config after cleanup: {exc}. "
                "On-disk cleanup already completed."
            )

    if plan.project_config_path:
        project_path = Path(plan.project_config_path)
        try:
            if _delete_directory(project_path):
                deleted_paths.append(str(project_path))
        except OSError as exc:
            warnings.append(f"Failed to delete project config {project_path}: {exc}")

    if (
        plan.can_reinitialize_project
        and plan.repo_root
        and not yes
        and prompt_reinitialize is not None
        and prompt_reinitialize("Would you like to re-initialize this project?")
    ):
        reinitialized = _reinitialize_project(Path(plan.repo_root))
        if not reinitialized:
            warnings.append(
                f"Could not re-initialize project at {plan.repo_root}. "
                "Run `kitaru init` manually to set up the project again."
            )

    if plan.active_environment_overrides:
        env_lines = [
            f"  {entry.name}={entry.value}"
            for entry in plan.active_environment_overrides
        ]
        warnings.append(
            "On-disk config has been removed, but active environment "
            "variables may still override runtime behavior:\n" + "\n".join(env_lines)
        )

    return CleanupResult(
        scope=scope,
        dry_run=False,
        aborted=False,
        backup_path=plan.backup_path,
        deleted_paths=tuple(deleted_paths),
        local_server_stopped=server_stopped,
        local_server_force_killed_pid=force_killed_pid,
        reinitialized_project=reinitialized,
        warnings=tuple(warnings),
        active_environment_overrides=plan.active_environment_overrides,
        preview_entries=plan.preview_entries,
        total_bytes=plan.total_bytes,
        local_server_status=plan.local_server_status,
    )


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _serialize_preview_entry(entry: CleanupPreviewEntry) -> dict[str, Any]:
    """Serialize one preview entry for JSON output."""
    payload: dict[str, Any] = {
        "path": entry.path,
        "type": entry.entry_type,
    }
    if entry.size_bytes is not None:
        payload["size_bytes"] = entry.size_bytes
    if entry.note is not None:
        payload["note"] = entry.note
    if entry.children:
        payload["children"] = [
            _serialize_preview_entry(child) for child in entry.children
        ]
    return payload


def serialize_cleanup_result(result: CleanupResult) -> dict[str, Any]:
    """Serialize a cleanup result for the JSON {command, item} envelope."""
    return {
        "scope": result.scope.value,
        "dry_run": result.dry_run,
        "aborted": result.aborted,
        "backup_path": result.backup_path,
        "deleted_paths": result.deleted_paths,
        "local_server_stopped": result.local_server_stopped,
        "local_server_force_killed_pid": result.local_server_force_killed_pid,
        "local_server_status": result.local_server_status,
        "reinitialized_project": result.reinitialized_project,
        "warnings": result.warnings,
        "active_environment_overrides": [
            {"name": entry.name, "value": entry.value}
            for entry in result.active_environment_overrides
        ],
        "preview": [
            _serialize_preview_entry(entry) for entry in result.preview_entries
        ],
        "total_bytes": result.total_bytes,
    }
