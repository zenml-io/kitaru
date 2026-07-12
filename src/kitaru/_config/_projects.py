"""Kitaru project lifecycle helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict
from zenml.client import Client

from kitaru._env import (
    KITARU_PROJECT_ENV,
    ZENML_ACTIVE_PROJECT_ID_ENV,
    _normalized_kitaru_env,
)
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
    KitaruUsageError,
)


class ProjectInfo(BaseModel):
    """Public Kitaru project information."""

    id: str
    name: str
    display_name: str | None
    description: str | None
    is_active: bool

    model_config = ConfigDict(extra="forbid")


class ProjectCreateResult(BaseModel):
    """Structured result for project creation operations."""

    project: ProjectInfo
    previous_active_project: str | None
    activated: bool

    model_config = ConfigDict(extra="forbid")


class ProjectDeleteResult(BaseModel):
    """Structured result for project deletion operations."""

    deleted_project: ProjectInfo

    model_config = ConfigDict(extra="forbid")


_ProjectManagementOperation = Literal["create", "use", "delete"]


def _normalize_project_selector(value: str, *, field_name: str = "name_or_id") -> str:
    """Return a non-empty project selector."""
    normalized = value.strip()
    if not normalized:
        raise KitaruUsageError(f"Project {field_name} cannot be empty.")
    return normalized


def _is_known_pro_cloud_backend_url(value: Any) -> bool:
    """Return whether a store URL points at a known ZenML Pro/Cloud backend."""
    if not isinstance(value, str):
        return False

    candidate = value.strip()
    if not candidate:
        return False

    parsed = urlsplit(candidate if "://" in candidate else f"//{candidate}")
    hostname = parsed.hostname
    if hostname is None:
        return False

    normalized = hostname.lower()
    return normalized == "cloudinfra.zenml.io" or normalized.endswith(
        ".cloudinfra.zenml.io"
    )


def _connected_store_url_is_known_pro_cloud(client: Any) -> bool:
    """Return whether the connected store URL is a known Pro/Cloud backend."""
    try:
        store_url = client.zen_store.url
    except Exception:
        return False
    return _is_known_pro_cloud_backend_url(store_url)


def _require_pro_cloud_project_management(
    client: Any,
    *,
    operation: _ProjectManagementOperation,
) -> None:
    """Require ZenML Pro/Cloud before project-changing operations."""

    def unknown_server_type_error() -> KitaruBackendError:
        return KitaruBackendError(
            "Kitaru could not verify whether the connected ZenML server is "
            f"Pro/Cloud before project {operation}. Project {operation} was "
            "not attempted. Check your server connection and authentication, "
            "then retry."
        )

    try:
        server_info = client.zen_store.get_store_info()
        is_pro_server = server_info.is_pro_server
    except Exception as exc:
        raise unknown_server_type_error() from exc

    if not callable(is_pro_server):
        raise unknown_server_type_error()

    try:
        is_pro = is_pro_server()
    except Exception as exc:
        raise unknown_server_type_error() from exc

    if not isinstance(is_pro, bool):
        raise unknown_server_type_error()

    if not is_pro and not _connected_store_url_is_known_pro_cloud(client):
        raise KitaruFeatureNotAvailableError(
            f"Kitaru project {operation} requires a ZenML Pro/Cloud server. "
            "You are connected to a non-Pro ZenML server. Use the default "
            "project on local/OSS ZenML, or connect to a ZenML Pro/Cloud "
            "workspace to manage Kitaru projects."
        )


def _safe_optional_project_string(
    project_model: Any, attribute_name: str
) -> str | None:
    """Read an optional project string from a backend model."""
    try:
        value = getattr(project_model, attribute_name)
    except Exception:
        return None
    return _normalize_optional_project_string(value)


def _normalize_optional_project_string(value: Any) -> str | None:
    """Normalize an optional project display string."""
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _project_info_from_model(
    project_model: Any,
    *,
    active_project_id: str | None,
) -> ProjectInfo:
    """Convert a backend project model to Kitaru's public project shape."""
    try:
        project_id_raw = project_model.id
        project_name_raw = project_model.name
    except AttributeError as exc:
        raise KitaruStateError(
            "Unable to read project information from the configured runtime."
        ) from exc

    project_id = str(project_id_raw).strip()
    project_name = str(project_name_raw).strip()
    if (
        not project_id
        or project_id == "None"
        or not project_name
        or project_name == "None"
    ):
        raise KitaruStateError(
            "Unable to read project information from the configured runtime."
        )

    return ProjectInfo(
        id=project_id,
        name=project_name,
        display_name=_safe_optional_project_string(project_model, "display_name"),
        description=_safe_optional_project_string(project_model, "description"),
        is_active=project_id == active_project_id,
    )


def _looks_like_no_active_project_error(exc: Exception) -> bool:
    """Return whether a backend exception means no project is selected."""
    message = str(exc).lower()
    return any(
        hint in message
        for hint in (
            "no active project",
            "no project is currently set as active",
            "active project is not set",
            "no active project is configured",
        )
    )


class _ZenMLActiveProjectWarningFilter(logging.Filter):
    """Hide ZenML dashboard warnings while Kitaru reads project state."""

    def filter(self, record: logging.LogRecord) -> bool:
        return (
            "ZenML OSS dashboard only works with the default" not in record.getMessage()
        )


@contextmanager
def _suppress_zenml_active_project_warning() -> Iterator[None]:
    """Temporarily suppress ZenML-branded active-project warnings."""
    zenml_client_logger = logging.getLogger("zenml.client")
    warning_filter = _ZenMLActiveProjectWarningFilter()
    zenml_client_logger.addFilter(warning_filter)
    try:
        yield
    finally:
        zenml_client_logger.removeFilter(warning_filter)


def _active_project_selector_from_env() -> str | None:
    """Return the env-selected project, preserving Kitaru env precedence."""
    kitaru_project = _normalized_kitaru_env(KITARU_PROJECT_ENV)
    if kitaru_project is not None:
        return kitaru_project
    return _normalized_kitaru_env(ZENML_ACTIVE_PROJECT_ID_ENV)


def _env_selector_matches_project(
    selector: str | None,
    *,
    project_name: str | None = None,
    project_model: Any | None = None,
) -> bool:
    """Return whether an env selector names a project exactly."""
    if selector is None:
        return False
    normalized_selector = selector.strip()
    if not normalized_selector:
        return False

    candidates: set[str] = set()
    if project_name is not None:
        normalized_name = project_name.strip()
        if normalized_name:
            candidates.add(normalized_name)
    if project_model is not None:
        candidates.update(
            candidate
            for candidate in {
                str(getattr(project_model, "id", "")).strip(),
                str(getattr(project_model, "name", "")).strip(),
            }
            if candidate
        )
    return normalized_selector in candidates


def _get_project_by_exact_selector(client: Any, selector: str) -> Any:
    """Resolve a project selector without name-prefix matching."""
    return client.get_project(
        selector,
        allow_name_prefix_match=False,
        hydrate=True,
    )


def _active_project_model(client: Any) -> Any:
    """Return the active backend project or raise a Kitaru-worded error."""
    env_selector = _active_project_selector_from_env()
    if env_selector is not None:
        try:
            return _get_project_by_exact_selector(client, env_selector)
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load active project '{env_selector}': {exc}"
            ) from exc

    try:
        with _suppress_zenml_active_project_warning():
            return client.active_project
    except Exception as exc:
        if _looks_like_no_active_project_error(exc):
            raise KitaruStateError(
                "No Kitaru project is active. Run `kitaru project use <NAME>` "
                "or set KITARU_PROJECT."
            ) from exc
        raise KitaruBackendError(f"Failed to load active project: {exc}") from exc


def _active_project_id(client: Any) -> str | None:
    """Return the active project ID, or None when no project is active."""
    try:
        return str(_active_project_model(client).id)
    except KitaruStateError:
        return None


def _project_models_from_page(page_result: Any) -> list[Any]:
    """Return project models from one backend list-projects page."""
    page_items = getattr(page_result, "items", None)
    if page_items is not None and not callable(page_items):
        if not isinstance(page_items, Iterable) or isinstance(page_items, (str, bytes)):
            raise KitaruStateError(
                "Unexpected project list response from the configured runtime."
            )
        return list(page_items)

    if not isinstance(page_result, Iterable) or isinstance(page_result, (str, bytes)):
        raise KitaruStateError(
            "Unexpected project list response from the configured runtime."
        )
    return list(page_result)


def _iter_projects(
    client: Any,
    *,
    page: int | None = None,
    size: int | None = None,
) -> Iterable[Any]:
    """Return projects from the runtime, fetching all pages by default."""
    if (page is None) != (size is None):
        raise KitaruUsageError("Project pagination requires both page and size.")

    if page is not None and size is not None:
        return _project_models_from_page(
            client.list_projects(page=page, size=size, hydrate=True)
        )

    first_page = client.list_projects(hydrate=True)
    project_models = _project_models_from_page(first_page)

    total_pages_raw = getattr(first_page, "total_pages", 1)
    page_size_raw = getattr(first_page, "max_size", 1)
    try:
        total_pages = int(total_pages_raw)
    except (TypeError, ValueError):
        total_pages = 1

    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 1

    for page_number in range(2, total_pages + 1):
        page_result = client.list_projects(
            page=page_number,
            size=page_size,
            hydrate=True,
        )
        project_models.extend(_project_models_from_page(page_result))

    return project_models


def current_project(*, client_factory: Callable[[], Any] = Client) -> ProjectInfo:
    """Return the currently active Kitaru project."""
    client = client_factory()
    active_project = _active_project_model(client)
    return _project_info_from_model(
        active_project,
        active_project_id=str(active_project.id),
    )


def list_projects(
    *,
    page: int | None = None,
    size: int | None = None,
    client_factory: Callable[[], Any] = Client,
) -> list[ProjectInfo]:
    """List projects visible to the current user and mark the active one."""
    client = client_factory()
    active_project_id = _active_project_id(client)
    return [
        _project_info_from_model(project_model, active_project_id=active_project_id)
        for project_model in _iter_projects(client, page=page, size=size)
    ]


def get_project(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> ProjectInfo:
    """Return a project by name or ID."""
    selector = _normalize_project_selector(name_or_id)
    client = client_factory()
    try:
        project = client.get_project(
            selector,
            allow_name_prefix_match=False,
            hydrate=True,
        )
    except Exception as exc:
        raise KitaruBackendError(f"Failed to load project '{selector}': {exc}") from exc
    return _project_info_from_model(
        project, active_project_id=_active_project_id(client)
    )


def create_project(
    name: str,
    *,
    description: str = "",
    display_name: str | None = None,
    activate: bool = True,
    client_factory: Callable[[], Any] = Client,
) -> ProjectCreateResult:
    """Create a project and optionally make it active."""
    project_name = _normalize_project_selector(name, field_name="name")
    project_description = _normalize_optional_project_string(description) or ""
    project_display_name = _normalize_optional_project_string(display_name)
    client = client_factory()
    _require_pro_cloud_project_management(client, operation="create")
    env_selector = _active_project_selector_from_env()
    active_project_read_failed = False
    try:
        previous_active_project = current_project(client_factory=lambda: client).name
    except KitaruBackendError:
        previous_active_project = None
        active_project_read_failed = True
    except KitaruStateError:
        previous_active_project = None
    try:
        created_project = client.create_project(
            name=project_name,
            description=project_description,
            display_name=project_display_name,
        )
        active_project = created_project
        if activate:
            active_project = client.set_active_project(created_project.id)
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to create project '{project_name}': {exc}"
        ) from exc

    env_selector_matches_created = _env_selector_matches_project(
        env_selector,
        project_name=project_name,
        project_model=created_project,
    )
    if activate:
        active_project_id = (
            str(active_project.id)
            if env_selector is None or env_selector_matches_created
            else None
        )
    elif env_selector_matches_created:
        active_project_id = str(created_project.id)
    elif active_project_read_failed:
        active_project_id = None
    else:
        try:
            active_project_id = _active_project_id(client)
        except KitaruBackendError:
            active_project_id = None

    project_info = _project_info_from_model(
        active_project if activate else created_project,
        active_project_id=active_project_id,
    )
    track(AnalyticsEvent.PROJECT_CREATED, {"activated": activate})
    return ProjectCreateResult(
        project=project_info,
        previous_active_project=previous_active_project,
        activated=activate,
    )


def use_project(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> ProjectInfo:
    """Set the active Kitaru project and return the resulting project info."""
    selector = _normalize_project_selector(name_or_id)
    client = client_factory()
    _require_pro_cloud_project_management(client, operation="use")
    try:
        project = client.get_project(
            selector,
            allow_name_prefix_match=False,
            hydrate=True,
        )
        active_project = client.set_active_project(project.id)
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to activate project '{selector}': {exc}"
        ) from exc
    project_info = _project_info_from_model(
        active_project,
        active_project_id=str(active_project.id),
    )
    track(AnalyticsEvent.PROJECT_ACTIVATED, None)
    return project_info


def delete_project(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> ProjectDeleteResult:
    """Delete a Kitaru project and return structured operation details."""
    selector = _normalize_project_selector(name_or_id)
    client = client_factory()
    _require_pro_cloud_project_management(client, operation="delete")
    try:
        project = client.get_project(
            selector,
            allow_name_prefix_match=False,
            hydrate=True,
        )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load project '{selector}' before deletion: {exc}"
        ) from exc

    active_project_id = _active_project_id(client)
    deleted_project = _project_info_from_model(
        project,
        active_project_id=active_project_id,
    )
    if deleted_project.is_active:
        raise KitaruUsageError(
            f"Cannot delete active Kitaru project '{deleted_project.name}'. "
            "Run `kitaru project use <NAME>` or set KITARU_PROJECT to "
            "another project first."
        )

    try:
        client.zen_store.delete_project(project_name_or_id=project.id)
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to delete project '{selector}': {exc}"
        ) from exc
    track(AnalyticsEvent.PROJECT_DELETED, None)
    return ProjectDeleteResult(deleted_project=deleted_project)
