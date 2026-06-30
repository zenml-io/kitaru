"""Tests for Kitaru project helpers and serialization."""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest

from kitaru._config._projects import (
    ProjectInfo,
    create_project,
    current_project,
    delete_project,
    get_project,
    list_projects,
    use_project,
)
from kitaru._env import KITARU_PROJECT_ENV, ZENML_ACTIVE_PROJECT_ID_ENV
from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruBackendError, KitaruStateError, KitaruUsageError
from kitaru.inspection import serialize_project


class _FakeProjectPage:
    """Page-like response whose real project models live under `.items`."""

    def __init__(
        self,
        *,
        items: list[SimpleNamespace],
        total_pages: int = 1,
        max_size: int = 100,
    ) -> None:
        self.items = items
        self.total_pages = total_pages
        self.max_size = max_size

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        yield "items", self.items
        yield "total_pages", self.total_pages
        yield "max_size", self.max_size


def _project_model(
    project_id: str,
    name: str,
    *,
    display_name: str | None = None,
    description: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=project_id,
        name=name,
        display_name=display_name,
        description=description,
    )


class _EnvSelectedProjectClient:
    """Fake client that fails if helpers touch ZenML's active_project path."""

    def __init__(self) -> None:
        self.projects = {
            "production": _project_model("prod-id", "production"),
            "zenml-project": _project_model("zenml-id", "zenml-project"),
        }
        self.get_project_calls: list[tuple[str, bool, bool]] = []

    @property
    def active_project(self) -> Any:
        raise AssertionError("project helpers should resolve env selectors directly")

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> Any:
        self.get_project_calls.append((selector, allow_name_prefix_match, hydrate))
        return self.projects[selector]

    def list_projects(self, **kwargs: Any) -> _FakeProjectPage:
        assert kwargs == {"hydrate": True}
        return _FakeProjectPage(items=list(self.projects.values()))


class _DeleteProjectClient:
    """Fake client for testing Kitaru's delete guard against stale ZenML state."""

    def __init__(self, *, persisted_active_project_name: str) -> None:
        self.projects = {
            "production": _project_model("prod-id", "production"),
            "staging": _project_model("stage-id", "staging"),
        }
        self.persisted_active_project_name = persisted_active_project_name
        self.get_project_calls: list[tuple[str, bool, bool]] = []
        self.zen_store = Mock()
        self.delete_project = Mock(
            side_effect=AssertionError("Kitaru should bypass Client.delete_project")
        )

    @property
    def active_project(self) -> Any:
        return self.projects[self.persisted_active_project_name]

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> Any:
        self.get_project_calls.append((selector, allow_name_prefix_match, hydrate))
        return self.projects[selector]


def test_serialize_project_includes_nullable_fields() -> None:
    project = ProjectInfo(
        id="project-id",
        name="production",
        display_name=None,
        description=None,
        is_active=True,
    )

    assert serialize_project(project) == {
        "id": "project-id",
        "name": "production",
        "display_name": None,
        "description": None,
        "is_active": True,
    }


def test_serialize_project_marks_inactive_project() -> None:
    project = ProjectInfo(
        id="project-id",
        name="staging",
        display_name="Staging",
        description="Pre-prod project",
        is_active=False,
    )

    assert serialize_project(project)["is_active"] is False


def test_current_project_returns_active_project_info() -> None:
    active = _project_model(
        "active-id",
        "production",
        display_name="Production",
        description="Live workloads",
    )
    fake_client = SimpleNamespace(active_project=active)

    result = current_project(client_factory=lambda: fake_client)

    assert result == ProjectInfo(
        id="active-id",
        name="production",
        display_name="Production",
        description="Live workloads",
        is_active=True,
    )


def test_current_project_rewrites_missing_active_project_error() -> None:
    class _NoActiveProjectClient:
        @property
        def active_project(self) -> Any:
            raise RuntimeError("No active project is configured.")

    with pytest.raises(KitaruStateError, match="No Kitaru project is active"):
        current_project(client_factory=_NoActiveProjectClient)


def test_current_project_resolves_name_based_kitaru_project_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU_PROJECT=production should not be parsed as a UUID by ZenML."""
    monkeypatch.setenv(KITARU_PROJECT_ENV, "production")
    fake_client = _EnvSelectedProjectClient()

    result = current_project(client_factory=lambda: fake_client)

    assert result.name == "production"
    assert result.is_active is True
    assert fake_client.get_project_calls == [("production", False, True)]


def test_list_projects_respects_kitaru_project_over_zenml_project_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Conflicting env vars should still mark KITARU_PROJECT as active."""
    monkeypatch.setenv(ZENML_ACTIVE_PROJECT_ID_ENV, "zenml-project")
    monkeypatch.setenv(KITARU_PROJECT_ENV, "production")
    fake_client = _EnvSelectedProjectClient()

    result = list_projects(client_factory=lambda: fake_client)

    assert [project.name for project in result] == ["production", "zenml-project"]
    assert [project.is_active for project in result] == [True, False]
    assert fake_client.get_project_calls == [("production", False, True)]


def test_list_projects_marks_active_and_reads_later_pages() -> None:
    active = _project_model("prod-id", "production")
    first_page = _FakeProjectPage(
        items=[active],
        total_pages=2,
        max_size=1,
    )
    second_page = _FakeProjectPage(items=[_project_model("stage-id", "staging")])
    fake_client = Mock()
    fake_client.active_project = active
    fake_client.list_projects.side_effect = [first_page, second_page]

    result = list_projects(client_factory=lambda: fake_client)

    assert [project.name for project in result] == ["production", "staging"]
    assert [project.is_active for project in result] == [True, False]
    fake_client.list_projects.assert_any_call(hydrate=True)
    fake_client.list_projects.assert_any_call(page=2, size=1, hydrate=True)


def test_list_projects_with_page_reads_only_requested_backend_page() -> None:
    active = _project_model("prod-id", "production")
    requested_page = _FakeProjectPage(
        items=[_project_model("stage-id", "staging")],
        total_pages=3,
        max_size=1,
    )
    fake_client = Mock()
    fake_client.active_project = active
    fake_client.list_projects.return_value = requested_page

    result = list_projects(page=2, size=1, client_factory=lambda: fake_client)

    assert [project.name for project in result] == ["staging"]
    fake_client.list_projects.assert_called_once_with(page=2, size=1, hydrate=True)


def test_get_project_uses_exact_selector() -> None:
    fake_client = Mock()
    fake_client.active_project = _project_model("prod-id", "production")
    fake_client.get_project.return_value = _project_model("stage-id", "staging")

    result = get_project("staging", client_factory=lambda: fake_client)

    assert result.name == "staging"
    assert result.is_active is False
    fake_client.get_project.assert_called_once_with(
        "staging",
        allow_name_prefix_match=False,
        hydrate=True,
    )


def test_get_project_rejects_empty_selector() -> None:
    with pytest.raises(KitaruUsageError, match="cannot be empty"):
        get_project("  ", client_factory=Mock)


def test_use_project_resolves_exact_selector_and_activates_by_id() -> None:
    fake_client = Mock()
    fake_client.get_project.return_value = _project_model("prod-id", "production")
    fake_client.set_active_project.return_value = _project_model(
        "prod-id", "production"
    )

    with patch("kitaru._config._projects.track", return_value=True) as track_mock:
        result = use_project("production", client_factory=lambda: fake_client)

    assert result.name == "production"
    assert result.is_active is True
    fake_client.get_project.assert_called_once_with(
        "production",
        allow_name_prefix_match=False,
        hydrate=True,
    )
    fake_client.set_active_project.assert_called_once_with("prod-id")
    track_mock.assert_called_once_with(AnalyticsEvent.PROJECT_ACTIVATED, None)


def test_create_project_activates_by_default_and_tracks_safe_metadata() -> None:
    previous = _project_model("old-id", "default")
    created = _project_model("new-id", "production")
    fake_client = Mock()
    fake_client.active_project = previous
    fake_client.create_project.return_value = created
    fake_client.set_active_project.return_value = created

    with patch("kitaru._config._projects.track", return_value=True) as track_mock:
        result = create_project("production", client_factory=lambda: fake_client)

    assert result.project.name == "production"
    assert result.previous_active_project == "default"
    assert result.activated is True
    fake_client.create_project.assert_called_once_with(
        name="production",
        description="",
        display_name=None,
    )
    fake_client.set_active_project.assert_called_once_with("new-id")
    track_mock.assert_called_once_with(
        AnalyticsEvent.PROJECT_CREATED,
        {"activated": True},
    )


def test_create_project_without_activation_leaves_active_project_unchanged() -> None:
    previous = _project_model("old-id", "default")
    created = _project_model("new-id", "staging")
    fake_client = Mock()
    fake_client.active_project = previous
    fake_client.create_project.return_value = created

    with patch("kitaru._config._projects.track", return_value=True) as track_mock:
        result = create_project(
            "staging",
            description="Pre-prod",
            display_name="Staging",
            activate=False,
            client_factory=lambda: fake_client,
        )

    assert result.project.is_active is False
    assert result.previous_active_project == "default"
    assert result.activated is False
    fake_client.set_active_project.assert_not_called()
    track_mock.assert_called_once_with(
        AnalyticsEvent.PROJECT_CREATED,
        {"activated": False},
    )


def test_create_project_handles_no_previous_active_project() -> None:
    created = _project_model("new-id", "first-project")

    class _FakeClient:
        @property
        def active_project(self) -> Any:
            raise RuntimeError("no active project")

        def create_project(self, **kwargs: Any) -> Any:
            assert kwargs["name"] == "first-project"
            return created

        def set_active_project(self, project_id: str) -> Any:
            assert project_id == "new-id"
            return created

    with patch("kitaru._config._projects.track", return_value=True):
        result = create_project("first-project", client_factory=_FakeClient)

    assert result.previous_active_project is None
    assert result.project.is_active is True


def test_delete_project_returns_deleted_project_and_tracks() -> None:
    target = _project_model("stage-id", "staging")
    fake_client = Mock()
    fake_client.active_project = _project_model("prod-id", "production")
    fake_client.get_project.return_value = target

    with patch("kitaru._config._projects.track", return_value=True) as track_mock:
        result = delete_project("staging", client_factory=lambda: fake_client)

    assert result.deleted_project.name == "staging"
    assert result.deleted_project.is_active is False
    fake_client.zen_store.delete_project.assert_called_once_with(
        project_name_or_id="stage-id"
    )
    track_mock.assert_called_once_with(AnalyticsEvent.PROJECT_DELETED, None)


def test_delete_project_blocks_kitaru_project_env_active_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(KITARU_PROJECT_ENV, "production")
    fake_client = _DeleteProjectClient(persisted_active_project_name="staging")

    with pytest.raises(
        KitaruUsageError,
        match="Cannot delete active Kitaru project 'production'",
    ):
        delete_project("production", client_factory=lambda: fake_client)

    fake_client.zen_store.delete_project.assert_not_called()
    fake_client.delete_project.assert_not_called()


def test_delete_project_ignores_stale_persisted_active_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persisted ZenML active state should not block inactive Kitaru deletes."""
    monkeypatch.setenv(KITARU_PROJECT_ENV, "staging")
    fake_client = _DeleteProjectClient(persisted_active_project_name="production")

    result = delete_project("production", client_factory=lambda: fake_client)

    assert result.deleted_project.name == "production"
    assert result.deleted_project.is_active is False
    fake_client.zen_store.delete_project.assert_called_once_with(
        project_name_or_id="prod-id"
    )
    fake_client.delete_project.assert_not_called()


def test_delete_project_deletes_inactive_project_under_kitaru_project_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(KITARU_PROJECT_ENV, "production")
    fake_client = _DeleteProjectClient(persisted_active_project_name="staging")

    result = delete_project("staging", client_factory=lambda: fake_client)

    assert result.deleted_project.name == "staging"
    assert result.deleted_project.is_active is False
    fake_client.zen_store.delete_project.assert_called_once_with(
        project_name_or_id="stage-id"
    )
    fake_client.delete_project.assert_not_called()


def test_project_info_strips_blank_optional_strings() -> None:
    fake_client = Mock()
    fake_client.active_project = _project_model("prod-id", "production")
    fake_client.get_project.return_value = _project_model(
        "stage-id",
        "staging",
        display_name="   ",
        description="  Pre-prod  ",
    )

    result = get_project("staging", client_factory=lambda: fake_client)

    assert result.display_name is None
    assert result.description == "Pre-prod"


def test_current_project_preserves_backend_failure_diagnosis() -> None:
    class _BackendFailureClient:
        @property
        def active_project(self) -> Any:
            raise RuntimeError("connection refused")

    with pytest.raises(KitaruBackendError, match="Failed to load active project"):
        current_project(client_factory=_BackendFailureClient)


def test_project_backend_errors_are_kitaru_worded() -> None:
    fake_client = Mock()
    fake_client.set_active_project.side_effect = RuntimeError("backend exploded")

    with pytest.raises(KitaruBackendError, match="Failed to activate project"):
        use_project("production", client_factory=lambda: fake_client)
