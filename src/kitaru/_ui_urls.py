"""Resolve and build Kitaru UI deep links."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import quote, urlsplit, urlunsplit

from kitaru._config._env import KITARU_UI_URL_ENV
from kitaru._env import _normalized_kitaru_env

if TYPE_CHECKING:
    from kitaru.client import KitaruClient

UiUrlSource = Literal["env", "server_info", "connection_config", "client_store"]
UiRouteKind = Literal["legacy", "pro"]


@dataclass(frozen=True)
class UiUrlContext:
    """Resolved UI origin plus the metadata needed to build Kitaru routes."""

    base_url: str
    route_kind: UiRouteKind
    source: UiUrlSource
    workspace: str | None = None
    explicit_override: bool = False


def _normalize_http_base_url(value: str | None) -> str | None:
    """Return a trimmed HTTP(S) base URL without a trailing slash."""
    if value is None:
        return None
    candidate = value.strip().rstrip("/")
    if candidate.startswith(("http://", "https://")):
        return candidate
    return None


def _http_origin(value: str | None) -> str | None:
    """Return just the scheme and host from an HTTP(S) URL."""
    base_url = _normalize_http_base_url(value)
    if base_url is None:
        return None
    parsed = urlsplit(base_url)
    if not parsed.scheme or not parsed.netloc:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _non_empty_string(value: Any) -> str | None:
    """Return a stripped string when the value has visible content."""
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _server_info_from_client(client: KitaruClient | None) -> Any | None:
    """Read ZenML server metadata without making URL generation fatal."""
    try:
        if client is not None:
            zen_store = client._client().zen_store
        else:
            from zenml.client import Client

            zen_store = Client().zen_store
        get_store_info = getattr(zen_store, "get_store_info", None)
        if get_store_info is None:
            return None
        return get_store_info()
    except Exception:
        return None


def _is_cloud_server(server_info: Any) -> bool:
    """Return whether server metadata describes ZenML Cloud/Pro."""
    deployment_type = getattr(server_info, "deployment_type", None)
    deployment_value = getattr(deployment_type, "value", deployment_type)
    return str(deployment_value).strip().lower() == "cloud"


def _workspace_from_server_info(server_info: Any) -> str | None:
    """Extract the Pro workspace route segment from server metadata."""
    workspace = _non_empty_string(getattr(server_info, "pro_workspace_name", None))
    if workspace is not None:
        return workspace

    metadata = getattr(server_info, "metadata", None)
    if isinstance(metadata, Mapping):
        workspace = _non_empty_string(metadata.get("workspace_name"))
        if workspace is not None:
            return workspace
        workspace = _non_empty_string(metadata.get("workspace_id"))
        if workspace is not None:
            return workspace

    return _non_empty_string(getattr(server_info, "pro_workspace_id", None))


def _pro_context_from_server_info(
    server_info: Any | None,
    *,
    override: str | None,
) -> UiUrlContext | None:
    """Build a Pro URL context when server metadata has enough route data."""
    if server_info is None or not _is_cloud_server(server_info):
        return None

    workspace = _workspace_from_server_info(server_info)
    if workspace is None:
        return None

    base_url = (
        override
        or _normalize_http_base_url(
            _non_empty_string(getattr(server_info, "pro_dashboard_url", None))
        )
        or _http_origin(_non_empty_string(getattr(server_info, "dashboard_url", None)))
    )
    if base_url is None:
        return None

    return UiUrlContext(
        base_url=base_url,
        route_kind="pro",
        source="env" if override is not None else "server_info",
        workspace=workspace,
        explicit_override=override is not None,
    )


def _legacy_context_from_base_url(
    base_url: str,
    *,
    source: UiUrlSource,
    explicit_override: bool = False,
) -> UiUrlContext:
    """Build a legacy context for local/non-Pro Kitaru UI routes."""
    return UiUrlContext(
        base_url=base_url,
        route_kind="legacy",
        source=source,
        explicit_override=explicit_override,
    )


def _connection_config_base_url() -> str | None:
    """Return the configured Kitaru server URL, if one is available."""
    try:
        from kitaru.config import resolve_connection_config

        resolved = resolve_connection_config(validate_for_use=False)
        return _normalize_http_base_url(resolved.server_url)
    except Exception:
        return None


def _client_store_base_url(client: KitaruClient | None) -> str | None:
    """Return the connected ZenML store URL, if a client was supplied."""
    if client is None:
        return None
    try:
        zen_store = client._client().zen_store
        url = getattr(zen_store, "url", None)
    except Exception:
        return None
    return _normalize_http_base_url(url if isinstance(url, str) else None)


def resolve_ui_url_context(client: KitaruClient | None = None) -> UiUrlContext | None:
    """Resolve enough UI context to build Kitaru execution and compare links.

    ``KITARU_UI_URL`` still wins as the origin. If server metadata also says the
    connected backend is Pro, Kitaru keeps that override origin but builds the
    Pro path because the metadata provides the workspace route segment. If the
    backend is known to be Pro but route metadata is incomplete, Kitaru returns
    ``None`` rather than emitting the known-broken legacy server URL.
    """
    override = _normalize_http_base_url(_normalized_kitaru_env(KITARU_UI_URL_ENV))
    server_info = _server_info_from_client(client)
    pro_context = _pro_context_from_server_info(server_info, override=override)
    if pro_context is not None:
        return pro_context

    if server_info is not None and _is_cloud_server(server_info):
        return None

    if override is not None:
        return _legacy_context_from_base_url(
            override,
            source="env",
            explicit_override=True,
        )

    connection_base_url = _connection_config_base_url()
    if connection_base_url is not None:
        return _legacy_context_from_base_url(
            connection_base_url,
            source="connection_config",
        )

    client_store_base_url = _client_store_base_url(client)
    if client_store_base_url is not None:
        return _legacy_context_from_base_url(
            client_store_base_url,
            source="client_store",
        )

    return None


def resolve_ui_base_url(client: KitaruClient | None = None) -> str | None:
    """Return the dashboard base URL for UI compare and execution links.

    This cheap compatibility resolver does not fetch server metadata when an
    explicit ``KITARU_UI_URL`` override is set. Call ``resolve_ui_url_context``
    when route-aware Pro metadata is needed.
    """
    override = _normalize_http_base_url(_normalized_kitaru_env(KITARU_UI_URL_ENV))
    if override is not None:
        return override

    connection_base_url = _connection_config_base_url()
    if connection_base_url is not None:
        return connection_base_url

    return _client_store_base_url(client)


def _path_segment(value: str) -> str:
    """URL-encode one route segment."""
    return quote(value, safe="")


def _normalized_version(version: str) -> str:
    """Return the URL version segment, defaulting empty values to local."""
    return version.strip() or "local"


def build_execution_url_from_context(
    context: UiUrlContext,
    *,
    flow_id: str,
    execution_id: str,
    project_name_or_id: str | None = None,
    version: str = "local",
) -> str | None:
    """Build an execution detail URL for legacy or Pro Kitaru UI routes."""
    base_url = context.base_url.rstrip("/")
    flow_segment = _path_segment(flow_id)
    execution_segment = _path_segment(execution_id)

    if context.route_kind == "legacy":
        return f"{base_url}/flows/{flow_segment}/executions/{execution_segment}"

    if context.workspace is None or project_name_or_id is None:
        return None

    workspace_segment = _path_segment(context.workspace)
    project_segment = _path_segment(project_name_or_id)
    version_segment = _path_segment(_normalized_version(version))
    return (
        f"{base_url}/kitaru-workspaces/{workspace_segment}"
        f"/projects/{project_segment}/flows/{flow_segment}"
        f"/v/{version_segment}/executions/{execution_segment}"
    )


def build_compare_url_from_context(
    context: UiUrlContext,
    *,
    flow_id: str,
    exec_ids: Sequence[str],
    project_name_or_id: str | None = None,
    version: str = "local",
) -> str | None:
    """Build a compare URL for legacy or Pro Kitaru UI routes."""
    normalized = [str(exec_id).strip() for exec_id in exec_ids if str(exec_id).strip()]
    if len(normalized) < 2:
        return None

    base_url = context.base_url.rstrip("/")
    flow_segment = _path_segment(flow_id)
    version_segment = _path_segment(_normalized_version(version))
    execution_segment = quote(",".join(normalized), safe=",")

    if context.route_kind == "legacy":
        return (
            f"{base_url}/flows/{flow_segment}/v/{version_segment}/compare"
            f"?executions={execution_segment}"
        )

    if context.workspace is None or project_name_or_id is None:
        return None

    workspace_segment = _path_segment(context.workspace)
    project_segment = _path_segment(project_name_or_id)
    return (
        f"{base_url}/kitaru-workspaces/{workspace_segment}"
        f"/projects/{project_segment}/flows/{flow_segment}"
        f"/v/{version_segment}/compare?executions={execution_segment}"
    )
