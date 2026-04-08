"""Internal local-server lifecycle helpers shared by CLI and MCP."""

from __future__ import annotations

import logging
import os
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
# Bundled UI helpers
# ---------------------------------------------------------------------------


def _resolve_bundled_ui_dir() -> Path | None:
    """Return the path to the bundled Kitaru UI dist directory, or None."""
    ui_dir = Path(__file__).parent / "_ui" / "dist"
    if ui_dir.is_dir() and (ui_dir / "index.html").is_file():
        return ui_dir
    return None


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
        if ui_dir := _resolve_bundled_ui_dir():
            os.environ["ZENML_SERVER_DASHBOARD_FILES_PATH"] = str(ui_dir)
            logger.info("Kitaru UI directory: %s", ui_dir)
        else:
            logger.debug(
                "No bundled Kitaru UI found (expected at %s); "
                "server will use default ZenML dashboard",
                Path(__file__).parent / "_ui" / "dist",
            )
        os.environ["ZENML_DEFAULT_ANALYTICS_SOURCE"] = "kitaru-api"
        deployed_server = deployer.deploy_server(config, timeout=timeout)
        deployer.connect_to_server()
    except Exception as exc:
        raise _local_server_start_error(action=action, exc=exc) from exc
    finally:
        os.environ.pop("ZENML_SERVER_DASHBOARD_FILES_PATH", None)
        os.environ["ZENML_DEFAULT_ANALYTICS_SOURCE"] = "kitaru-python"

    deployed_url = (
        _existing_local_server_url(deployed_server)
        or f"http://{_DEFAULT_LOCAL_SERVER_HOST}:{port}"
    )
    return LocalServerConnectionResult(url=deployed_url, action=action)


def _track_local_server_started(result: LocalServerConnectionResult) -> None:
    """Emit LOCAL_SERVER_STARTED analytics after a successful lifecycle event."""
    from kitaru.analytics import AnalyticsEvent, track

    track(AnalyticsEvent.LOCAL_SERVER_STARTED, {"action": result.action})


def start_or_connect_local_server(
    *,
    port: int | None,
    timeout: int,
) -> LocalServerConnectionResult:
    """Start a daemon local server or connect to an existing one.

    When deploying a new server, sets ZENML_SERVER_DASHBOARD_FILES_PATH to
    the bundled Kitaru UI directory (if available) so the server serves the
    Kitaru dashboard instead of the default ZenML dashboard.
    """
    _validate_local_server_inputs(port=port, timeout=timeout)
    _ensure_local_server_dependencies()

    (
        local_server_deployer_cls,
        deployment_config_cls,
        server_provider_type,
        get_local_server,
    ) = _load_local_server_runtime()

    deployer = local_server_deployer_cls()
    local_server = get_local_server()

    if local_server is not None:
        existing_url = _existing_local_server_url(local_server)
        existing_port = _existing_local_server_port(local_server)
        is_running = _is_server_running(local_server) and bool(existing_url)

        if is_running and (port is None or port == existing_port):
            if _resolve_bundled_ui_dir() is not None:
                logger.debug(
                    "Connecting to existing server; dashboard may differ "
                    "from bundled Kitaru UI. Restart the server to pick "
                    "up UI updates (kitaru logout && kitaru login).",
                )
            try:
                deployer.connect_to_server()
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to connect to local server: {exc}"
                ) from exc
            assert existing_url is not None
            result = LocalServerConnectionResult(
                url=existing_url,
                action="connected",
            )
            _track_local_server_started(result)
            return result

        try:
            deployer.remove_server(timeout=timeout)
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to stop existing local server: {exc}"
            ) from exc

    action: Literal["started", "restarted"] = (
        "restarted" if port is not None and local_server is not None else "started"
    )

    result = _deploy_and_connect(
        deployer=deployer,
        deployment_config_cls=deployment_config_cls,
        provider_type=server_provider_type,
        port=port or _DEFAULT_LOCAL_SERVER_PORT,
        timeout=timeout,
        action=action,
    )

    _track_local_server_started(result)
    return result


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

    from kitaru.analytics import AnalyticsEvent, track

    track(AnalyticsEvent.LOCAL_SERVER_STOPPED, {})

    return LocalServerStopResult(stopped=True, url=url)
