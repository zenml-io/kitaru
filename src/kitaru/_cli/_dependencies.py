"""Internal dependency seam for CLI command modules.

Command modules should ask this module for runtime dependencies instead of
reaching through ``kitaru.cli``. The seam preserves legacy
``kitaru.cli.<name>`` patch points for compatibility, but new code should patch
or inject dependencies through this module directly.
"""

from __future__ import annotations

import sys
import time
from typing import Any

from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.login.credentials_store import get_credentials_store

from kitaru._interface_secrets import resolve_secret_exact as _resolve_secret_exact
from kitaru._local_server import (
    start_or_connect_local_server,
    stop_registered_local_server,
)
from kitaru.client import KitaruClient
from kitaru.config import (
    _create_stack_operation,
    _delete_stack_operation,
    _list_stack_entries,
    _show_stack_operation,
    create_project,
    current_project,
    delete_project,
    get_project,
    list_model_aliases,
    list_projects,
    login_to_server,
    register_model_alias,
    reset_global_log_store,
    resolve_log_store,
    set_global_log_store,
    use_project,
)
from kitaru.config import current_stack as get_current_stack
from kitaru.config import list_stacks as get_available_stacks
from kitaru.config import use_stack as set_active_stack
from kitaru.inspection import build_runtime_snapshot as _build_runtime_snapshot
from kitaru.inspection import (
    connected_to_local_server_safe as _connected_to_local_server,
)
from kitaru.inspection import log_store_mismatch_details as _log_store_mismatch_details

_MISSING = object()
_ORIGINAL_LEGACY_ATTRS: dict[str, Any] = {
    "time": time,
    "Client": Client,
    "KitaruClient": KitaruClient,
    "GlobalConfiguration": GlobalConfiguration,
    "get_credentials_store": get_credentials_store,
    "start_or_connect_local_server": start_or_connect_local_server,
    "stop_registered_local_server": stop_registered_local_server,
    "login_to_server": login_to_server,
    "_connected_to_local_server": _connected_to_local_server,
    "_build_runtime_snapshot": _build_runtime_snapshot,
    "_log_store_mismatch_details": _log_store_mismatch_details,
    "set_global_log_store": set_global_log_store,
    "resolve_log_store": resolve_log_store,
    "reset_global_log_store": reset_global_log_store,
    "get_current_stack": get_current_stack,
    "get_available_stacks": get_available_stacks,
    "set_active_stack": set_active_stack,
    "_list_stack_entries": _list_stack_entries,
    "_show_stack_operation": _show_stack_operation,
    "_create_stack_operation": _create_stack_operation,
    "_delete_stack_operation": _delete_stack_operation,
    "current_project": current_project,
    "list_projects": list_projects,
    "get_project": get_project,
    "create_project": create_project,
    "use_project": use_project,
    "delete_project": delete_project,
    "register_model_alias": register_model_alias,
    "list_model_aliases": list_model_aliases,
    "_resolve_secret_exact": _resolve_secret_exact,
}


class CLIDependencies:
    """Resolve CLI runtime dependencies with legacy facade patch support."""

    def _legacy_attr(self, name: str, default: Any) -> Any:
        """Return a patched ``kitaru.cli`` attr when one exists.

        Unpatched legacy re-exports should not beat the seam-local default.
        That keeps old ``kitaru.cli.*`` patches working without preventing
        tests from patching this dependency module directly.
        """
        module = sys.modules.get("kitaru.cli")
        if module is None:
            return default
        legacy = getattr(module, name, _MISSING)
        if legacy is _MISSING:
            return default
        original = _ORIGINAL_LEGACY_ATTRS.get(name, _MISSING)
        if original is not _MISSING and legacy is original:
            return default
        return legacy

    def zenml_client(self) -> Any:
        """Return the active low-level ZenML client."""
        return self._legacy_attr("Client", Client)()

    def kitaru_client(self) -> Any:
        """Return the active Kitaru SDK client."""
        return self._legacy_attr("KitaruClient", KitaruClient)()

    def auth_management_client(self) -> Any:
        """Return the SDK client configured for auth-management commands."""
        return self._legacy_attr("KitaruClient", KitaruClient).for_auth_management()

    def global_configuration(self) -> Any:
        """Return the active ZenML global configuration object."""
        return self._legacy_attr("GlobalConfiguration", GlobalConfiguration)()

    def get_credentials_store(self) -> Any:
        """Return the configured credentials store."""
        return self._legacy_attr("get_credentials_store", get_credentials_store)()

    def start_or_connect_local_server(self, *args: Any, **kwargs: Any) -> Any:
        """Start or connect to the registered local Kitaru server."""
        return self._legacy_attr(
            "start_or_connect_local_server",
            start_or_connect_local_server,
        )(*args, **kwargs)

    def stop_registered_local_server(self) -> Any:
        """Stop the registered local Kitaru server if one exists."""
        return self._legacy_attr(
            "stop_registered_local_server",
            stop_registered_local_server,
        )()

    def login_to_server(self, *args: Any, **kwargs: Any) -> Any:
        """Persist/login to a remote Kitaru server."""
        return self._legacy_attr("login_to_server", login_to_server)(*args, **kwargs)

    def connected_to_local_server(self) -> bool:
        """Return whether the current connection points at the local server."""
        return bool(
            self._legacy_attr(
                "_connected_to_local_server",
                _connected_to_local_server,
            )()
        )

    def get_connected_server_url(self) -> str | None:
        """Read the currently configured remote server URL, if available."""
        patched = self._legacy_attr("_get_connected_server_url", None)
        if patched is not None:
            return patched()
        try:
            store_configuration = self.global_configuration().store_configuration
        except Exception:
            return None
        server_url = getattr(store_configuration, "url", None)
        if not server_url:
            return None
        return str(server_url).rstrip("/")

    def build_runtime_snapshot(self, *args: Any, **kwargs: Any) -> Any:
        """Build the runtime diagnostics snapshot."""
        return self._legacy_attr(
            "_build_runtime_snapshot",
            _build_runtime_snapshot,
        )(*args, **kwargs)

    def log_store_mismatch_details(self, *args: Any, **kwargs: Any) -> Any:
        """Return active-stack/log-store mismatch details."""
        return self._legacy_attr(
            "_log_store_mismatch_details",
            _log_store_mismatch_details,
        )(*args, **kwargs)

    def set_global_log_store(self, *args: Any, **kwargs: Any) -> Any:
        """Persist a global log-store override."""
        return self._legacy_attr("set_global_log_store", set_global_log_store)(
            *args,
            **kwargs,
        )

    def resolve_log_store(self) -> Any:
        """Resolve effective log-store configuration."""
        return self._legacy_attr("resolve_log_store", resolve_log_store)()

    def reset_global_log_store(self) -> Any:
        """Clear the global log-store override."""
        return self._legacy_attr("reset_global_log_store", reset_global_log_store)()

    def get_current_stack(self) -> Any:
        """Return the active stack."""
        return self._legacy_attr("get_current_stack", get_current_stack)()

    def get_available_stacks(self) -> Any:
        """Return all available stacks."""
        return self._legacy_attr("get_available_stacks", get_available_stacks)()

    def set_active_stack(self, *args: Any, **kwargs: Any) -> Any:
        """Set the active stack."""
        return self._legacy_attr("set_active_stack", set_active_stack)(
            *args,
            **kwargs,
        )

    def list_stack_entries(self) -> Any:
        """Return stack list entries."""
        return self._legacy_attr("_list_stack_entries", _list_stack_entries)()

    def show_stack_operation(self, *args: Any, **kwargs: Any) -> Any:
        """Return stack details for a stack name or ID."""
        return self._legacy_attr("_show_stack_operation", _show_stack_operation)(
            *args,
            **kwargs,
        )

    def create_stack_operation(self, *args: Any, **kwargs: Any) -> Any:
        """Create a stack from CLI inputs."""
        return self._legacy_attr("_create_stack_operation", _create_stack_operation)(
            *args,
            **kwargs,
        )

    def delete_stack_operation(self, *args: Any, **kwargs: Any) -> Any:
        """Delete a stack from CLI inputs."""
        return self._legacy_attr("_delete_stack_operation", _delete_stack_operation)(
            *args,
            **kwargs,
        )

    def current_project(self) -> Any:
        """Return the active Kitaru project."""
        return self._legacy_attr("current_project", current_project)()

    def list_projects(self, *args: Any, **kwargs: Any) -> Any:
        """Return all Kitaru projects visible to the current user."""
        return self._legacy_attr("list_projects", list_projects)(*args, **kwargs)

    def get_project(self, *args: Any, **kwargs: Any) -> Any:
        """Return one Kitaru project by name or ID."""
        return self._legacy_attr("get_project", get_project)(*args, **kwargs)

    def create_project(self, *args: Any, **kwargs: Any) -> Any:
        """Create a Kitaru project."""
        return self._legacy_attr("create_project", create_project)(*args, **kwargs)

    def use_project(self, *args: Any, **kwargs: Any) -> Any:
        """Set the active Kitaru project."""
        return self._legacy_attr("use_project", use_project)(*args, **kwargs)

    def delete_project(self, *args: Any, **kwargs: Any) -> Any:
        """Delete a Kitaru project."""
        return self._legacy_attr("delete_project", delete_project)(*args, **kwargs)

    def register_model_alias(self, *args: Any, **kwargs: Any) -> Any:
        """Persist a model alias."""
        return self._legacy_attr("register_model_alias", register_model_alias)(
            *args,
            **kwargs,
        )

    def list_model_aliases(self) -> Any:
        """List configured model aliases."""
        return self._legacy_attr("list_model_aliases", list_model_aliases)()

    def resolve_secret_exact(self, *args: Any, **kwargs: Any) -> Any:
        """Resolve one secret by exact name or exact ID."""
        return self._legacy_attr("_resolve_secret_exact", _resolve_secret_exact)(
            *args,
            **kwargs,
        )

    def sleep(self, seconds: float) -> None:
        """Sleep while preserving legacy ``kitaru.cli.time.sleep`` patches."""
        self._legacy_attr("time", time).sleep(seconds)


_DEPENDENCIES = CLIDependencies()


def cli_dependencies() -> CLIDependencies:
    """Return the process-wide CLI dependency provider."""
    return _DEPENDENCIES
