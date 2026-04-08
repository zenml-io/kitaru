"""Shared runtime inspection and JSON serialization helpers."""

from __future__ import annotations

import contextlib
import importlib.metadata
import os
import platform
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.models import SecretResponse
from zenml.utils.server_utils import connected_to_local_server, get_local_server

from kitaru._client._models import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    FailureInfo,
    LogEntry,
    PendingWait,
)
from kitaru._version import resolve_installed_version
from kitaru.config import (
    KITARU_PROJECT_ENV,
    ActiveEnvironmentVariable,
    ActiveStackLogStore,
    ModelAliasEntry,
    ResolvedLogStore,
    StackComponentDetails,
    StackDetails,
    StackInfo,
    _read_runtime_connection_config,
    _StackCreateResult,
    _StackDeleteResult,
    active_stack_log_store,
    list_active_kitaru_environment_variables,
    resolve_log_store,
)

_LOCALHOST_NAMES = {"127.0.0.1", "localhost", "::1"}


@dataclass
class RuntimeSnapshot:
    """Resolved runtime information for status-style output."""

    sdk_version: str
    connection: str
    connection_target: str
    config_directory: str
    server_url: str | None = None
    active_user: str | None = None
    project_override: str | None = None
    active_stack: str | None = None
    repository_root: str | None = None
    server_version: str | None = None
    server_database: str | None = None
    server_deployment_type: str | None = None
    local_server_status: str | None = None
    warning: str | None = None
    log_store_status: str | None = None
    log_store_warning: str | None = None
    environment: list[ActiveEnvironmentVariable] = field(default_factory=list)

    # Config provenance
    kitaru_global_config_path: str | None = None
    zenml_global_config_path: str | None = None
    local_stores_path: str | None = None
    repository_config_path: str | None = None
    uses_repo_local_config: bool = False

    # Connection source breakdown
    connection_sources: dict[str, str] | None = None

    # Active project
    active_project: str | None = None
    active_project_source: str | None = None

    # System info
    python_version: str | None = None
    system_info: dict[str, str] | None = None
    environment_type: str | None = None
    zenml_version: str | None = None

    # Package info (--all / --packages only)
    packages: dict[str, str] | None = None


def _sdk_version() -> str:
    """Resolve the installed SDK version lazily."""
    return resolve_installed_version()


def describe_local_server() -> str:
    """Summarize the state of the local Kitaru-compatible server, if any."""
    try:
        local_server = get_local_server()
    except ImportError:
        return "unavailable (local runtime support not installed)"
    if local_server is None:
        return "not started"

    provider = local_server.config.provider.value
    if local_server.status and local_server.status.url:
        return f"running at {local_server.status.url} ({provider})"

    if local_server.status and local_server.status.status_message:
        return (
            f"registered but unavailable ({provider}: "
            f"{local_server.status.status_message})"
        )

    return f"registered but unavailable ({provider})"


def _localhost_url_identity(url: str | None) -> tuple[str, int] | None:
    """Return a comparable identity for localhost URLs.

    Localhost aliases are treated as equivalent; only the scheme-defaulted port
    matters for matching against the registered local server.
    """
    if not url:
        return None

    parsed = urlparse(url)
    if parsed.hostname not in _LOCALHOST_NAMES:
        return None

    if parsed.port is not None:
        return ("localhost", parsed.port)
    if parsed.scheme == "https":
        return ("localhost", 443)
    return ("localhost", 80)


def is_registered_local_server_url(url: str | None) -> bool:
    """Return whether a URL matches the registered local Kitaru server."""
    candidate = _localhost_url_identity(url)
    if candidate is None:
        return False

    try:
        local_server = get_local_server()
    except ImportError:
        return False
    if local_server is None:
        return False

    status = getattr(local_server, "status", None)
    local_url = getattr(status, "url", None)
    config = getattr(local_server, "config", None)
    if not local_url and config is not None:
        local_url = getattr(config, "url", None)
    if not local_url and config is not None:
        local_port = getattr(config, "port", None)
        if isinstance(local_port, int):
            local_host = getattr(config, "ip_address", None) or "127.0.0.1"
            local_url = f"http://{local_host}:{local_port}"

    return _localhost_url_identity(str(local_url) if local_url else None) == candidate


def connected_to_local_server_safe() -> bool:
    """Safely check whether the current client is bound to a local server."""
    try:
        return connected_to_local_server()
    except ImportError:
        return False


def _build_snapshot_without_local_store(
    _gc: GlobalConfiguration,
    _exc: Exception,
) -> RuntimeSnapshot:
    """Build a degraded snapshot when local runtime support is unavailable."""
    return RuntimeSnapshot(
        sdk_version=_sdk_version(),
        connection="local mode (unavailable)",
        connection_target="unavailable",
        config_directory=_gc.config_directory,
        local_server_status=describe_local_server(),
        warning=combine_warnings(
            (
                "Local Kitaru runtime support is unavailable in this environment. "
                "Connect to a Kitaru server to keep working, or install the local "
                "runtime dependencies if you want the built-in local stack."
            ),
            _legacy_runner_env_warning(),
        ),
        environment=list_active_kitaru_environment_variables(),
    )


def uses_stale_local_server_url(
    server_url: str | None,
    local_server_status: str | None,
) -> bool:
    """Check for a localhost URL that points at a stopped local server."""
    if not server_url or not local_server_status:
        return False

    return is_registered_local_server_url(server_url) and (
        "unavailable" in local_server_status
    )


def _legacy_runner_env_warning() -> str | None:
    """Return a warning when the legacy stack-selection env var is still set."""
    if os.environ.get("KITARU_RUNNER") is None:
        return None
    return "`KITARU_RUNNER` was renamed to `KITARU_STACK`; update your environment."


def log_store_mismatch_details(
    preferred: ResolvedLogStore,
) -> tuple[str | None, str | None]:
    """Return status-row + warning text when preferred and active backends differ."""
    if preferred.source == "default":
        return None, None

    active_store = active_stack_log_store()
    if active_store is None:
        return None, None

    if active_store.backend == preferred.backend:
        return None, None

    status_row = f"{preferred.backend} (preferred) ⚠ stack uses {active_store.backend}"

    active_label = active_store.backend
    if active_store.stack_name:
        active_label = f"{active_store.backend} (stack: {active_store.stack_name})"

    warning = "\n".join(
        [
            f"Active stack uses: {active_label}",
            "The Kitaru log-store preference is not wired into stack selection yet.",
            "Actual runtime logs go to the active stack's ZenML stack log "
            "store, not this preference.",
        ]
    )
    return status_row, warning


def combine_warnings(*warnings: str | None) -> str | None:
    """Combine non-empty warning messages into one multiline block."""
    rendered = [warning for warning in warnings if warning]
    if not rendered:
        return None
    return "\n".join(rendered)


def _collect_zenml_version() -> str | None:
    """Read the installed ZenML version, or None if unavailable."""
    try:
        return importlib.metadata.version("zenml")
    except importlib.metadata.PackageNotFoundError:
        return None


def _collect_python_system_info(
    *,
    include_environment_type: bool = False,
) -> tuple[str, dict[str, str], str | None]:
    """Collect Python version, system info, and optional environment type."""
    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    system_info = {
        "os": platform.platform(),
        "architecture": platform.machine(),
    }
    environment_type: str | None = None
    if include_environment_type:
        environment_type = _detect_environment_type()
    return python_version, system_info, environment_type


def _detect_environment_type() -> str:
    """Detect the current execution environment type."""
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return "kubernetes"
    if os.environ.get("GITHUB_ACTIONS"):
        return "github_actions"
    if os.environ.get("GITLAB_CI"):
        return "gitlab_ci"
    if os.environ.get("CIRCLECI"):
        return "circleci"
    if os.path.exists("/.dockerenv") or os.environ.get("CONTAINER"):
        return "docker"
    return "native"


def _collect_config_provenance(
    gc: GlobalConfiguration,
    *,
    repository_root: str | None,
) -> tuple[str | None, str | None, str | None, str | None, bool]:
    """Collect config file path provenance.

    Returns (kitaru_config, zenml_config, local_stores, repo_config, uses_repo).
    """
    from kitaru._config._log_store import _kitaru_global_config_path

    config_dir = Path(gc.config_directory)

    kitaru_config = _kitaru_global_config_path()
    kitaru_config_str = str(kitaru_config) if kitaru_config.exists() else None

    zenml_config = config_dir / "config.yaml"
    zenml_config_str = str(zenml_config) if zenml_config.exists() else None

    local_stores = config_dir / "local_stores"
    local_stores_str = str(local_stores) if local_stores.exists() else None

    repo_config_str: str | None = None
    uses_repo = False
    if repository_root:
        from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME

        repo_config = (
            Path(repository_root) / KITARU_REPOSITORY_DIRECTORY_NAME / "config.yaml"
        )
        if repo_config.exists():
            repo_config_str = str(repo_config)
            uses_repo = True

    return (
        kitaru_config_str,
        zenml_config_str,
        local_stores_str,
        repo_config_str,
        uses_repo,
    )


def _collect_connection_sources() -> dict[str, str]:
    """Build a per-parameter source breakdown for the current connection."""
    from kitaru._env import (
        KITARU_AUTH_TOKEN_ENV,
        KITARU_SERVER_URL_ENV,
        ZENML_STORE_API_KEY_ENV,
        ZENML_STORE_URL_ENV,
    )

    sources: dict[str, str] = {}
    runtime_conn = _read_runtime_connection_config()

    # server_url source
    if os.environ.get(KITARU_SERVER_URL_ENV):
        sources["server_url"] = f"environment ({KITARU_SERVER_URL_ENV})"
    elif os.environ.get(ZENML_STORE_URL_ENV):
        sources["server_url"] = f"environment ({ZENML_STORE_URL_ENV})"
    elif runtime_conn.server_url:
        sources["server_url"] = "runtime override (kitaru.configure)"
    else:
        sources["server_url"] = "global config"

    # auth_token source
    if os.environ.get(KITARU_AUTH_TOKEN_ENV):
        sources["auth_token"] = f"environment ({KITARU_AUTH_TOKEN_ENV})"
    elif os.environ.get(ZENML_STORE_API_KEY_ENV):
        sources["auth_token"] = f"environment ({ZENML_STORE_API_KEY_ENV})"
    else:
        sources["auth_token"] = "global config"

    # project source
    if os.environ.get(KITARU_PROJECT_ENV):
        sources["project"] = f"environment ({KITARU_PROJECT_ENV})"
    elif runtime_conn.project:
        sources["project"] = "runtime override (kitaru.configure)"
    else:
        try:
            client = Client()
            if client.root:
                from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME

                repo_config = (
                    Path(str(client.root))
                    / KITARU_REPOSITORY_DIRECTORY_NAME
                    / "config.yaml"
                )
                if repo_config.exists():
                    sources["project"] = "repo-local config (.kitaru/)"
                else:
                    sources["project"] = "global config"
            else:
                sources["project"] = "global config"
        except Exception:
            sources["project"] = "global config"

    return sources


def _collect_packages(
    *,
    include_all: bool = False,
    package_names: Sequence[str] | None = None,
) -> dict[str, str] | None:
    """Collect installed package versions."""
    if not include_all and not package_names:
        return None

    if include_all:
        packages: dict[str, str] = {}
        for dist in importlib.metadata.distributions():
            name = dist.metadata["Name"]
            if name:
                normalized = name.lower().replace("-", "-")
                packages[normalized] = dist.version
        return dict(sorted(packages.items()))

    if package_names:
        packages = {}
        for name in package_names:
            try:
                version = importlib.metadata.version(name)
                packages[name.lower()] = version
            except importlib.metadata.PackageNotFoundError:
                packages[name.lower()] = "not installed"
        return dict(sorted(packages.items()))

    return None


def build_runtime_snapshot(
    *,
    include_packages: bool = False,
    package_names: Sequence[str] | None = None,
    include_environment_type: bool = False,
) -> RuntimeSnapshot:
    """Resolve the current Kitaru runtime state from ZenML-backed config."""
    gc = GlobalConfiguration()

    # Collect non-network diagnostics early so they're available even in
    # degraded snapshots that return before reaching the Client.
    zenml_version = _collect_zenml_version()
    python_ver, sys_info, env_type = _collect_python_system_info(
        include_environment_type=include_environment_type,
    )
    packages = _collect_packages(
        include_all=include_packages,
        package_names=package_names,
    )

    try:
        store_cfg = gc.store_configuration
        uses_local_store = gc.uses_local_store
    except ImportError as exc:
        snapshot = _build_snapshot_without_local_store(gc, exc)
        snapshot.zenml_version = zenml_version
        snapshot.python_version = python_ver
        snapshot.system_info = sys_info
        snapshot.environment_type = env_type
        snapshot.packages = packages
        return snapshot

    if uses_local_store:
        connection = "local database"
        server_url = None
    elif connected_to_local_server_safe():
        connection = "local Kitaru server"
        server_url = store_cfg.url
    else:
        connection = "remote Kitaru server"
        server_url = store_cfg.url

    snapshot = RuntimeSnapshot(
        sdk_version=_sdk_version(),
        connection=connection,
        connection_target=store_cfg.url,
        server_url=server_url,
        config_directory=gc.config_directory,
        local_server_status=describe_local_server(),
        environment=list_active_kitaru_environment_variables(),
        zenml_version=zenml_version,
        python_version=python_ver,
        system_info=sys_info,
        environment_type=env_type,
        packages=packages,
    )

    # Connection source breakdown
    with contextlib.suppress(Exception):
        snapshot.connection_sources = _collect_connection_sources()

    if uses_stale_local_server_url(server_url, snapshot.local_server_status):
        snapshot.warning = combine_warnings(
            (
                "The configured Kitaru server points to a stopped local server. "
                "Start it again or run `kitaru logout` to clear the stale "
                "connection."
            ),
            _legacy_runner_env_warning(),
        )
        return snapshot

    project_env = os.environ.get(KITARU_PROJECT_ENV)
    runtime_conn = _read_runtime_connection_config()
    if project_env:
        snapshot.project_override = project_env
    elif runtime_conn.project:
        snapshot.project_override = runtime_conn.project

    try:
        client = Client()
        store_info = client.zen_store.get_store_info()
        snapshot.active_user = client.active_user.name
        snapshot.active_stack = client.active_stack_model.name
        snapshot.repository_root = str(client.root) if client.root else None
        snapshot.server_version = str(store_info.version)
        snapshot.server_database = str(store_info.database_type)
        snapshot.server_deployment_type = str(store_info.deployment_type)

        # Active project
        with contextlib.suppress(Exception):
            snapshot.active_project = client.active_project.name

        # Config provenance (needs repository_root)
        with contextlib.suppress(Exception):
            (
                snapshot.kitaru_global_config_path,
                snapshot.zenml_global_config_path,
                snapshot.local_stores_path,
                snapshot.repository_config_path,
                snapshot.uses_repo_local_config,
            ) = _collect_config_provenance(
                gc,
                repository_root=snapshot.repository_root,
            )

    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        snapshot.warning = f"Unable to query the configured store: {exc}"

    try:
        preferred_log_store = resolve_log_store()
    except ValueError as exc:
        snapshot.log_store_warning = (
            f"Unable to resolve Kitaru log-store preference: {exc}"
        )
        return snapshot

    log_store_status, log_store_warning = log_store_mismatch_details(
        preferred_log_store
    )
    snapshot.log_store_status = log_store_status
    snapshot.log_store_warning = log_store_warning
    snapshot.warning = combine_warnings(snapshot.warning, _legacy_runner_env_warning())
    return snapshot


def _qualified_type_name(value: Any) -> str:
    """Return the fully qualified runtime type name for a value."""
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def to_jsonable(value: Any, *, fallback_repr: bool) -> Any:
    """Convert a value into a JSON-serializable representation."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, Mapping):
        return {
            str(key): to_jsonable(item, fallback_repr=fallback_repr)
            for key, item in value.items()
        }

    if isinstance(value, (set, frozenset)):
        return [
            to_jsonable(item, fallback_repr=fallback_repr)
            for item in sorted(value, key=repr)
        ]

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_jsonable(item, fallback_repr=fallback_repr) for item in value]

    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value), fallback_repr=fallback_repr)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return to_jsonable(model_dump(mode="python"), fallback_repr=fallback_repr)

    if fallback_repr:
        return repr(value)

    raise TypeError(
        "Value cannot be serialized to JSON-compatible data: "
        f"{_qualified_type_name(value)}"
    )


def serialize_failure(failure: FailureInfo | None) -> dict[str, Any] | None:
    """Serialize optional failure details."""
    if failure is None:
        return None

    return {
        "message": failure.message,
        "exception_type": failure.exception_type,
        "traceback": failure.traceback,
        "origin": to_jsonable(failure.origin, fallback_repr=True),
    }


def serialize_pending_wait(wait: PendingWait | None) -> dict[str, Any] | None:
    """Serialize optional pending wait details."""
    if wait is None:
        return None

    return {
        "wait_id": wait.wait_id,
        "name": wait.name,
        "question": wait.question,
        "schema": to_jsonable(wait.schema, fallback_repr=True),
        "metadata": to_jsonable(wait.metadata, fallback_repr=True),
        "entered_waiting_at": to_jsonable(wait.entered_waiting_at, fallback_repr=True),
    }


def serialize_artifact_ref(artifact: ArtifactRef) -> dict[str, Any]:
    """Serialize artifact metadata."""
    return {
        "artifact_id": artifact.artifact_id,
        "name": artifact.name,
        "kind": artifact.kind,
        "save_type": artifact.save_type,
        "producing_call": artifact.producing_call,
        "metadata": to_jsonable(artifact.metadata, fallback_repr=True),
    }


def serialize_artifact_value(value: Any) -> dict[str, Any]:
    """Serialize an artifact payload value for MCP transport."""
    value_type = _qualified_type_name(value)
    try:
        serialized_value = to_jsonable(value, fallback_repr=False)
        return {
            "value": serialized_value,
            "value_format": "json",
            "value_type": value_type,
        }
    except TypeError:
        return {
            "value": repr(value),
            "value_format": "repr",
            "value_type": value_type,
        }


def serialize_checkpoint_attempt(attempt: CheckpointAttempt) -> dict[str, Any]:
    """Serialize checkpoint-attempt details."""
    return {
        "attempt_id": attempt.attempt_id,
        "status": attempt.status.value,
        "started_at": to_jsonable(attempt.started_at, fallback_repr=True),
        "ended_at": to_jsonable(attempt.ended_at, fallback_repr=True),
        "metadata": to_jsonable(attempt.metadata, fallback_repr=True),
        "failure": serialize_failure(attempt.failure),
    }


def serialize_checkpoint_call(checkpoint: CheckpointCall) -> dict[str, Any]:
    """Serialize checkpoint-call details."""
    return {
        "call_id": checkpoint.call_id,
        "name": checkpoint.name,
        "status": checkpoint.status.value,
        "started_at": to_jsonable(checkpoint.started_at, fallback_repr=True),
        "ended_at": to_jsonable(checkpoint.ended_at, fallback_repr=True),
        "metadata": to_jsonable(checkpoint.metadata, fallback_repr=True),
        "original_call_id": checkpoint.original_call_id,
        "parent_call_ids": checkpoint.parent_call_ids,
        "failure": serialize_failure(checkpoint.failure),
        "attempts": [
            serialize_checkpoint_attempt(attempt) for attempt in checkpoint.attempts
        ],
        "artifacts": [
            serialize_artifact_ref(artifact) for artifact in checkpoint.artifacts
        ],
    }


def serialize_execution_summary(execution: Execution) -> dict[str, Any]:
    """Serialize execution list-item details."""
    return {
        "exec_id": execution.exec_id,
        "flow_name": execution.flow_name,
        "status": execution.status.value,
        "started_at": to_jsonable(execution.started_at, fallback_repr=True),
        "ended_at": to_jsonable(execution.ended_at, fallback_repr=True),
        "stack_name": execution.stack_name,
        "status_reason": execution.status_reason,
        "pending_wait": serialize_pending_wait(execution.pending_wait),
        "failure": serialize_failure(execution.failure),
        "metadata": to_jsonable(execution.metadata, fallback_repr=True),
        "checkpoint_count": len(execution.checkpoints),
        "artifact_count": len(execution.artifacts),
    }


def serialize_execution(execution: Execution) -> dict[str, Any]:
    """Serialize full execution details."""
    return {
        **serialize_execution_summary(execution),
        "frozen_execution_spec": to_jsonable(
            execution.frozen_execution_spec,
            fallback_repr=True,
        ),
        "original_exec_id": execution.original_exec_id,
        "checkpoints": [
            serialize_checkpoint_call(checkpoint)
            for checkpoint in execution.checkpoints
        ],
        "artifacts": [
            serialize_artifact_ref(artifact) for artifact in execution.artifacts
        ],
    }


def serialize_stack(
    stack: StackInfo,
    *,
    is_managed: bool | None = None,
) -> dict[str, Any]:
    """Serialize stack information for structured output."""
    payload = {
        "id": stack.id,
        "name": stack.name,
        "is_active": stack.is_active,
    }
    if is_managed is not None:
        payload["is_managed"] = is_managed
    return payload


def serialize_stack_create_result(result: _StackCreateResult) -> dict[str, Any]:
    """Serialize stack-create operation details."""
    payload = serialize_stack(result.stack)
    payload["previous_active_stack"] = result.previous_active_stack
    payload["components_created"] = list(result.components_created)
    payload["stack_type"] = result.stack_type
    if result.service_connectors_created:
        payload["service_connectors_created"] = list(result.service_connectors_created)
    if result.resources:
        payload["resources"] = result.resources
    return payload


def serialize_stack_delete_result(result: _StackDeleteResult) -> dict[str, Any]:
    """Serialize stack-delete operation details."""
    return {
        "deleted_stack": result.deleted_stack,
        "components_deleted": list(result.components_deleted),
        "new_active_stack": result.new_active_stack,
        "recursive": result.recursive,
    }


def _serialize_stack_component_details(
    component: StackComponentDetails,
) -> dict[str, Any]:
    """Serialize one translated stack component for structured stack output."""
    payload: dict[str, Any] = {
        "role": component.role,
        "name": component.name,
    }
    if component.backend is not None:
        payload["backend"] = component.backend
    if component.details:
        payload["details"] = dict(component.details)
    if component.purpose is not None:
        payload["purpose"] = component.purpose
    return payload


def serialize_stack_details(details: StackDetails) -> dict[str, Any]:
    """Serialize stack inspection details for `stack show` style output."""
    payload = serialize_stack(details.stack, is_managed=details.is_managed)
    payload["stack_type"] = details.stack_type
    payload["components"] = [
        _serialize_stack_component_details(component)
        for component in details.components
    ]
    return payload


def serialize_runtime_snapshot(snapshot: RuntimeSnapshot) -> dict[str, Any]:
    """Serialize runtime status details for structured output."""
    return to_jsonable(snapshot, fallback_repr=True)


def serialize_log_entry(entry: LogEntry) -> dict[str, Any]:
    """Serialize one log entry for JSON output."""
    payload: dict[str, Any] = {"message": entry.message}
    for key, value in (
        ("level", entry.level),
        ("timestamp", entry.timestamp),
        ("source", entry.source),
        ("checkpoint_name", entry.checkpoint_name),
        ("module", entry.module),
        ("filename", entry.filename),
        ("lineno", entry.lineno),
    ):
        if value is not None:
            payload[key] = value
    return payload


def serialize_model_alias(entry: ModelAliasEntry) -> dict[str, Any]:
    """Serialize model alias information."""
    return {
        "alias": entry.alias,
        "model": entry.model,
        "secret": entry.secret,
        "is_default": entry.is_default,
    }


def secret_visibility(secret: SecretResponse) -> str:
    """Return a human-readable visibility label for a secret."""
    return "private" if secret.private else "public"


def serialize_secret_summary(secret: SecretResponse) -> dict[str, Any]:
    """Serialize secret summary information."""
    keys = sorted(secret.values.keys())
    return {
        "id": str(secret.id),
        "name": secret.name,
        "visibility": secret_visibility(secret),
        "keys": keys,
        "has_missing_values": secret.has_missing_values,
    }


def serialize_secret_detail(
    secret: SecretResponse,
    *,
    show_values: bool,
) -> dict[str, Any]:
    """Serialize secret detail information."""
    payload = serialize_secret_summary(secret)
    if show_values:
        payload["values"] = {
            key: secret.secret_values.get(key, "unavailable")
            for key in sorted(secret.values.keys())
        }
    else:
        payload["values"] = None
    return payload


def serialize_resolved_log_store(
    snapshot: ResolvedLogStore,
    *,
    active_store: ActiveStackLogStore | None = None,
    warning: str | None = None,
) -> dict[str, Any]:
    """Serialize effective log-store information."""
    return {
        "backend": snapshot.backend,
        "endpoint": snapshot.endpoint,
        "api_key_configured": bool(snapshot.api_key),
        "source": snapshot.source,
        "active_stack_backend": active_store.backend if active_store else None,
        "active_stack_name": active_store.stack_name if active_store else None,
        "warning": warning,
    }
