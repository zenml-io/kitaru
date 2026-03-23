"""Internal local-server lifecycle helpers shared by CLI and MCP."""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from kitaru.errors import KitaruBackendError, KitaruUsageError

logger = logging.getLogger(__name__)

_DEFAULT_LOCAL_SERVER_HOST = "127.0.0.1"
_DEFAULT_LOCAL_SERVER_PORT = 8383
_LOCAL_INSTALL_GUIDANCE = "\n".join(
    [
        "Local server requires additional dependencies.",
        "Install them with:",
        "  pip install 'kitaru[local]'",
        "  uv pip install 'kitaru[local]'",
    ]
)

_SENTINEL_DIR_NAME = ".kitaru-ui"
_SENTINEL_FILE_NAME = "bundle_manifest.json"
_MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class LocalServerConnectionResult:
    """Structured result for starting or connecting to the local server."""

    url: str
    action: Literal["started", "connected", "restarted"]


@dataclass(frozen=True)
class LocalServerStopResult:
    """Structured result for local-server teardown."""

    stopped: bool
    url: str | None = None


# ---------------------------------------------------------------------------
# Dashboard UI patching helpers
# ---------------------------------------------------------------------------


def _safe_rmtree(path: Path, label: str) -> None:
    """Remove a directory tree, logging a warning on failure."""
    if path.exists():
        try:
            shutil.rmtree(path)
        except OSError:
            logger.warning("Could not remove %s %s", label, path)


def _resolve_zenml_dashboard_dir() -> Path:
    """Locate ZenML's installed dashboard directory."""
    import zenml

    return Path(zenml.__path__[0]) / "zen_server" / "dashboard"


def _resolve_bundled_ui_dir() -> Path | None:
    """Return the path to the bundled Kitaru UI dist directory, or None."""
    ui_dir = Path(__file__).parent / "_ui" / "dist"
    if ui_dir.is_dir() and (ui_dir / "index.html").is_file():
        return ui_dir
    return None


def _load_manifest_json(path: Path) -> dict[str, Any] | None:
    """Load and validate a manifest JSON file.

    Returns the parsed dict if the file exists, is valid JSON, is a dict,
    and has the expected schema version.  Returns None otherwise.
    """
    if not path.is_file():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logger.warning("Manifest %s contains invalid JSON", path)
        return None
    except OSError as exc:
        logger.warning("Could not read manifest %s: %s", path, exc)
        return None
    if not isinstance(data, dict):
        logger.warning("Manifest %s is not a JSON object", path)
        return None
    if data.get("schema_version") != _MANIFEST_SCHEMA_VERSION:
        logger.warning(
            "Manifest %s has unsupported schema version %s (expected %s)",
            path,
            data.get("schema_version"),
            _MANIFEST_SCHEMA_VERSION,
        )
        return None
    return data


def _load_bundled_manifest() -> dict[str, Any] | None:
    """Load the bundled bundle_manifest.json, or None if unavailable."""
    return _load_manifest_json(Path(__file__).parent / "_ui" / _SENTINEL_FILE_NAME)


def _load_installed_sentinel(dashboard_dir: Path) -> dict[str, Any] | None:
    """Load the sentinel manifest from the installed dashboard, or None."""
    return _load_manifest_json(dashboard_dir / _SENTINEL_DIR_NAME / _SENTINEL_FILE_NAME)


def _dashboard_needs_update(dashboard_dir: Path, bundled: dict[str, Any]) -> bool:
    """Check whether the installed dashboard needs to be replaced.

    Returns False only when the installed sentinel matches the bundled
    manifest on both version and checksum, AND index.html is present.
    """
    if not (dashboard_dir / "index.html").is_file():
        return True

    installed = _load_installed_sentinel(dashboard_dir)
    if installed is None:
        return True

    return installed.get("ui_version") != bundled.get("ui_version") or installed.get(
        "bundle_sha256"
    ) != bundled.get("bundle_sha256")


def _apply_dashboard_patch(
    dashboard_dir: Path, bundled_manifest: dict[str, Any]
) -> None:
    """Replace the ZenML dashboard directory with bundled Kitaru UI.

    Uses an atomic-ish rename strategy: copy to a temp sibling, rename
    old to backup, rename temp to target, then remove backup.  If the
    final rename fails, the backup is restored.
    """
    bundled_dir = _resolve_bundled_ui_dir()
    if bundled_dir is None:
        raise KitaruBackendError(
            "Kitaru UI assets are missing from this installation.\n"
            "Reinstall with: pip install 'kitaru[local]'"
        )

    parent = dashboard_dir.parent
    pid = os.getpid()
    ts = int(time.time())
    tmp_dir = parent / f"dashboard.__kitaru_tmp__{pid}_{ts}"
    backup_dir = parent / f"dashboard.__kitaru_bak__{pid}_{ts}"

    try:
        shutil.copytree(bundled_dir, tmp_dir)

        sentinel_dir = tmp_dir / _SENTINEL_DIR_NAME
        sentinel_dir.mkdir(exist_ok=True)
        with open(sentinel_dir / _SENTINEL_FILE_NAME, "w") as f:
            json.dump(bundled_manifest, f, indent=2)

        if not (tmp_dir / "index.html").is_file():
            raise KitaruBackendError(
                "Bundled Kitaru UI is missing index.html — package may be corrupt."
            )

        had_backup = False
        if dashboard_dir.exists():
            dashboard_dir.rename(backup_dir)
            had_backup = True

        try:
            tmp_dir.rename(dashboard_dir)
        except OSError:
            logger.error(
                "Failed to rename %s → %s; restoring backup",
                tmp_dir,
                dashboard_dir,
            )
            if had_backup and backup_dir.exists():
                backup_dir.rename(dashboard_dir)
            raise

        if had_backup:
            _safe_rmtree(backup_dir, "backup directory")

        logger.info(
            "Kitaru UI %s installed.",
            bundled_manifest.get("ui_version", "unknown"),
        )
    finally:
        _safe_rmtree(tmp_dir, "temp directory")


def _ensure_kitaru_dashboard() -> bool:
    """Ensure ZenML's dashboard directory has the Kitaru UI.

    Returns True if the dashboard was updated, False if already current.
    Silently returns False if no bundled UI is available (dev installs).
    """
    if _resolve_bundled_ui_dir() is None:
        logger.debug("No bundled Kitaru UI found; skipping dashboard patch")
        return False

    bundled_manifest = _load_bundled_manifest()
    if bundled_manifest is None:
        logger.warning(
            "Bundled Kitaru UI files exist but manifest is missing or invalid; "
            "skipping dashboard patch"
        )
        return False

    dashboard_dir = _resolve_zenml_dashboard_dir()
    if not _dashboard_needs_update(dashboard_dir, bundled_manifest):
        logger.debug("Kitaru UI already installed and up to date")
        return False

    try:
        _apply_dashboard_patch(dashboard_dir, bundled_manifest)
        return True
    except KitaruBackendError:
        raise
    except Exception as exc:
        raise KitaruBackendError(
            "Could not patch ZenML dashboard with Kitaru UI.\n"
            f"{exc}\n"
            "The local server was not started to avoid showing the wrong dashboard."
        ) from exc


# ---------------------------------------------------------------------------
# Server lifecycle helpers
# ---------------------------------------------------------------------------


def _validate_local_server_inputs(*, port: int | None, timeout: int) -> None:
    """Validate local-server start inputs."""
    if timeout < 1:
        raise KitaruUsageError("`--timeout` must be >= 1.")
    if port is not None and not 1 <= port <= 65535:
        raise KitaruUsageError("`--port` must be between 1 and 65535.")


def _existing_local_server_url(local_server: Any) -> str | None:
    """Return the current or configured local-server URL, if known."""
    status = getattr(local_server, "status", None)
    url = getattr(status, "url", None)
    if url:
        return str(url)

    config = getattr(local_server, "config", None)
    configured_url = getattr(config, "url", None)
    if configured_url:
        return str(configured_url)
    return None


def _existing_local_server_port(local_server: Any) -> int | None:
    """Return the configured local-server port, if known."""
    url = _existing_local_server_url(local_server)
    if url:
        parsed = urlparse(url)
        if parsed.port is not None:
            return parsed.port

    config = getattr(local_server, "config", None)
    port = getattr(config, "port", None)
    return port if isinstance(port, int) else None


def _local_server_log_path() -> str:
    """Resolve the daemon log file path for startup error guidance."""
    try:
        from zenml.utils.io_utils import get_global_config_directory
    except ImportError:
        return str(
            Path.home() / ".config" / "zenml" / "zen_server" / "daemon" / "service.log"
        )

    return str(
        Path(get_global_config_directory()) / "zen_server" / "daemon" / "service.log"
    )


def _local_server_start_error(*, action: str, exc: Exception) -> KitaruBackendError:
    """Build a user-facing startup/restart failure."""
    verb = "restart" if action == "restarted" else "start"
    return KitaruBackendError(
        "\n".join(
            [
                f"Local server failed to {verb}.",
                str(exc),
                "Check server logs:",
                f"  {_local_server_log_path()}",
            ]
        )
    )


def _load_local_server_runtime() -> tuple[type[Any], type[Any], type[Any], Any]:
    """Load local-server runtime helpers lazily."""
    from zenml.enums import ServerProviderType
    from zenml.utils.server_utils import get_local_server
    from zenml.zen_server.deploy import LocalServerDeployer, LocalServerDeploymentConfig

    return (
        LocalServerDeployer,
        LocalServerDeploymentConfig,
        ServerProviderType,
        get_local_server,
    )


def _ensure_local_server_dependencies() -> None:
    """Validate optional local-server dependencies."""
    try:
        from zenml.zen_server.deploy.daemon.daemon_provider import DaemonServerProvider
    except ImportError as exc:  # pragma: no cover - depends on zenml packaging
        raise KitaruUsageError(_LOCAL_INSTALL_GUIDANCE) from exc

    try:
        DaemonServerProvider.check_local_server_dependencies()
    except RuntimeError as exc:
        raise KitaruUsageError(_LOCAL_INSTALL_GUIDANCE) from exc


def _build_local_server_config(
    *, deployment_config_cls: type[Any], provider_type: Any, port: int
) -> Any:
    """Build a daemon-backed local-server deployment config."""
    return deployment_config_cls(
        provider=provider_type.DAEMON,
        ip_address=_DEFAULT_LOCAL_SERVER_HOST,
        port=port,
    )


def _is_server_running(local_server: Any) -> bool:
    """Check whether a local server deployment is running."""
    is_running = getattr(local_server, "is_running", False)
    if callable(is_running):
        return bool(is_running())
    return bool(is_running)


def _deploy_and_connect(
    *,
    deployer: Any,
    deployment_config_cls: type[Any],
    provider_type: Any,
    port: int,
    timeout: int,
    action: Literal["started", "restarted"],
) -> LocalServerConnectionResult:
    """Deploy a new local server, connect, and return the result."""
    config = _build_local_server_config(
        deployment_config_cls=deployment_config_cls,
        provider_type=provider_type,
        port=port,
    )
    try:
        os.environ["ZENML_DEFAULT_ANALYTICS_SOURCE"] = "kitaru-api"
        deployed_server = deployer.deploy_server(config, timeout=timeout)
        deployer.connect_to_server()
    except Exception as exc:
        raise _local_server_start_error(action=action, exc=exc) from exc
    finally:
        os.environ["ZENML_DEFAULT_ANALYTICS_SOURCE"] = "kitaru-python"

    deployed_url = (
        _existing_local_server_url(deployed_server)
        or f"http://{_DEFAULT_LOCAL_SERVER_HOST}:{port}"
    )
    return LocalServerConnectionResult(url=deployed_url, action=action)


def start_or_connect_local_server(
    *,
    port: int | None,
    timeout: int,
) -> LocalServerConnectionResult:
    """Start a daemon local server or connect to an existing one.

    Before starting or connecting, ensures the ZenML dashboard directory
    contains Kitaru UI assets.  If a compatible server is already running
    but serving a stale dashboard, it is restarted.
    """
    _validate_local_server_inputs(port=port, timeout=timeout)
    _ensure_local_server_dependencies()

    (
        local_server_deployer_cls,
        deployment_config_cls,
        server_provider_type,
        get_local_server,
    ) = _load_local_server_runtime()

    dashboard_was_updated = _ensure_kitaru_dashboard()

    deployer = local_server_deployer_cls()
    local_server = get_local_server()

    if local_server is not None:
        existing_url = _existing_local_server_url(local_server)
        existing_port = _existing_local_server_port(local_server)
        is_running = _is_server_running(local_server) and bool(existing_url)

        if is_running and (port is None or port == existing_port):
            if not dashboard_was_updated:
                try:
                    deployer.connect_to_server()
                except Exception as exc:
                    raise KitaruBackendError(
                        f"Failed to connect to local server: {exc}"
                    ) from exc
                assert existing_url is not None
                return LocalServerConnectionResult(
                    url=existing_url,
                    action="connected",
                )

            # Dashboard was just patched — restart so the server picks up
            # the new files (dashboard paths are cached at server startup).
            try:
                deployer.remove_server(timeout=timeout)
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to stop existing local server: {exc}"
                ) from exc

            return _deploy_and_connect(
                deployer=deployer,
                deployment_config_cls=deployment_config_cls,
                provider_type=server_provider_type,
                port=existing_port or port or _DEFAULT_LOCAL_SERVER_PORT,
                timeout=timeout,
                action="restarted",
            )

        # Server exists but isn't running or is on a different port.
        if local_server is not None:
            try:
                deployer.remove_server(timeout=timeout)
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to stop existing local server: {exc}"
                ) from exc

    action: Literal["started", "restarted"] = (
        "restarted" if port is not None and local_server is not None else "started"
    )

    return _deploy_and_connect(
        deployer=deployer,
        deployment_config_cls=deployment_config_cls,
        provider_type=server_provider_type,
        port=port or _DEFAULT_LOCAL_SERVER_PORT,
        timeout=timeout,
        action=action,
    )


@dataclass(frozen=True)
class LocalServerCleanupResult:
    """Structured result for cleanup-specific local-server teardown."""

    stopped: bool
    url: str | None = None
    force_killed_pid: int | None = None


def _force_kill_server_process(local_server: Any) -> int | None:
    """Attempt to force-kill the local server daemon process.

    Returns the killed PID, or None if PID could not be resolved.
    """
    import signal

    pid: int | None = None

    status = getattr(local_server, "status", None)
    if status is not None:
        pid = getattr(status, "pid", None)

    if pid is None:
        config = getattr(local_server, "config", None)
        if config is not None:
            pid = getattr(config, "pid", None)

    if pid is None or not isinstance(pid, int):
        return None

    try:
        os.kill(pid, signal.SIGKILL)
        return pid
    except ProcessLookupError:
        return pid
    except OSError:
        logger.warning("Could not force-kill server process %d", pid)
        return None


def stop_registered_local_server_for_cleanup(
    *,
    timeout: int = 10,
) -> LocalServerCleanupResult:
    """Stop the registered local server for cleanup, with force-kill fallback.

    Unlike ``stop_registered_local_server``, this function:
    - uses a timeout on graceful shutdown
    - force-kills the daemon if graceful stop fails
    - never raises; always returns a result
    """
    try:
        local_server_deployer_cls, _, _, get_local_server = _load_local_server_runtime()
    except ImportError:
        return LocalServerCleanupResult(stopped=False, url=None)

    local_server = get_local_server()
    if local_server is None:
        return LocalServerCleanupResult(stopped=False, url=None)

    url = _existing_local_server_url(local_server)

    try:
        local_server_deployer_cls().remove_server(timeout=timeout)
        return LocalServerCleanupResult(stopped=True, url=url)
    except Exception:
        logger.warning("Graceful local server shutdown failed; attempting force-kill")

    killed_pid = _force_kill_server_process(local_server)
    return LocalServerCleanupResult(
        stopped=True,
        url=url,
        force_killed_pid=killed_pid,
    )


def stop_registered_local_server() -> LocalServerStopResult:
    """Stop the registered local server if one exists."""
    try:
        local_server_deployer_cls, _, _, get_local_server = _load_local_server_runtime()
    except ImportError:
        return LocalServerStopResult(stopped=False, url=None)

    local_server = get_local_server()
    if local_server is None:
        return LocalServerStopResult(stopped=False, url=None)

    url = _existing_local_server_url(local_server)
    try:
        local_server_deployer_cls().remove_server()
    except Exception as exc:
        raise KitaruBackendError(f"Failed to stop local server: {exc}") from exc

    return LocalServerStopResult(stopped=True, url=url)
