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
# Dashboard UI patching helpers
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

    deployer = local_server_deployer_cls()
    local_server = get_local_server()

    if local_server is not None:
        existing_url = _existing_local_server_url(local_server)
        existing_port = _existing_local_server_port(local_server)
        is_running = _is_server_running(local_server) and bool(existing_url)

        if is_running and (port is None or port == existing_port):
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
