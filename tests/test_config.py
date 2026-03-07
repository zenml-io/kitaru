"""Tests for Kitaru configuration helpers."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest
from zenml.config.global_config import GlobalConfiguration
from zenml.utils import yaml_utils

from kitaru.config import (
    KITARU_LOG_STORE_BACKEND_ENV,
    KITARU_LOG_STORE_ENDPOINT_ENV,
    current_stack,
    list_stacks,
    reset_global_log_store,
    resolve_log_store,
    set_global_log_store,
    use_stack,
)


class _FakeStackPage:
    """Simple iterable page used to test stack pagination behavior."""

    def __init__(
        self,
        *,
        items: list[SimpleNamespace],
        total_pages: int,
        max_size: int,
    ) -> None:
        self.items = items
        self.total_pages = total_pages
        self.max_size = max_size

    def __iter__(self) -> Iterator[SimpleNamespace]:
        return iter(self.items)


def _kitaru_config_path() -> Path:
    """Return the path used for persisted Kitaru global config in tests."""
    return Path(GlobalConfiguration().config_directory) / "kitaru.yaml"


def test_log_store_defaults_to_artifact_store() -> None:
    """Runtime logs should resolve to artifact-store by default."""
    snapshot = resolve_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.endpoint is None
    assert snapshot.api_key is None
    assert snapshot.source == "default"


def test_set_log_store_persists_global_override() -> None:
    """Setting a backend should persist and become the resolved global default."""
    snapshot = set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
        api_key="{{ DATADOG_KEY }}",
    )

    assert snapshot.backend == "datadog"
    assert snapshot.endpoint == "https://logs.datadoghq.com"
    assert snapshot.api_key == "{{ DATADOG_KEY }}"
    assert snapshot.source == "global user config"

    persisted = yaml_utils.read_yaml(str(_kitaru_config_path()))
    assert persisted["log_store"]["backend"] == "datadog"
    assert persisted["log_store"]["endpoint"] == "https://logs.datadoghq.com"


def test_environment_override_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment variables should override persisted global config."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
        api_key="{{ DATADOG_KEY }}",
    )
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "honeycomb")
    monkeypatch.setenv(KITARU_LOG_STORE_ENDPOINT_ENV, "https://api.honeycomb.io")

    snapshot = resolve_log_store()

    assert snapshot.backend == "honeycomb"
    assert snapshot.endpoint == "https://api.honeycomb.io"
    assert snapshot.source == "environment"


def test_environment_can_force_artifact_store_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment should be able to override persisted state back to default."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "artifact-store")

    snapshot = resolve_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.endpoint is None
    assert snapshot.api_key is None
    assert snapshot.source == "environment"


def test_environment_artifact_store_rejects_extra_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """artifact-store env override should not accept endpoint/api-key values."""
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "artifact-store")
    monkeypatch.setenv(KITARU_LOG_STORE_ENDPOINT_ENV, "https://should-not-be-used")

    with pytest.raises(ValueError, match=KITARU_LOG_STORE_ENDPOINT_ENV):
        resolve_log_store()


def test_reset_clears_persisted_log_store_override() -> None:
    """Reset should remove the persisted override and restore defaults."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    snapshot = reset_global_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.endpoint is None
    assert snapshot.source == "default"


def test_partial_env_override_raises_helpful_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend-only environment override should fail with clear guidance."""
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "datadog")

    with pytest.raises(ValueError, match=KITARU_LOG_STORE_ENDPOINT_ENV):
        resolve_log_store()


def test_set_rejects_artifact_store_override() -> None:
    """artifact-store should stay an implicit default, not an override target."""
    with pytest.raises(ValueError, match="already the default"):
        set_global_log_store(
            "artifact-store",
            endpoint="https://unused.example.com",
        )


def test_invalid_persisted_config_raises_error() -> None:
    """Malformed persisted config should raise a clear ValueError."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    with pytest.raises(ValueError, match="global config file is invalid"):
        resolve_log_store()


def test_reset_recovers_from_invalid_persisted_config() -> None:
    """Reset should recover by overwriting malformed persisted config."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    snapshot = reset_global_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.source == "default"


def test_set_overwrites_invalid_persisted_config() -> None:
    """Set should recover by replacing malformed persisted config contents."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    snapshot = set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    assert snapshot.backend == "datadog"
    assert snapshot.source == "global user config"


def test_current_stack_returns_active_stack_info() -> None:
    """current_stack should expose the currently active stack."""
    active_stack = SimpleNamespace(id="stack-local-id", name="local")
    client_mock = SimpleNamespace(active_stack_model=active_stack)

    with patch("kitaru.config.Client", return_value=client_mock):
        stack = current_stack()

    assert stack.id == "stack-local-id"
    assert stack.name == "local"
    assert stack.is_active is True


def test_list_stacks_marks_active_stack() -> None:
    """list_stacks should flag only the active stack in the returned list."""
    local = SimpleNamespace(id="stack-local-id", name="local")
    prod = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = SimpleNamespace(
        active_stack_model=prod,
        list_stacks=lambda: [local, prod],
    )

    with patch("kitaru.config.Client", return_value=client_mock):
        stacks = list_stacks()

    assert [(stack.name, stack.is_active) for stack in stacks] == [
        ("local", False),
        ("prod", True),
    ]


def test_list_stacks_fetches_all_pages() -> None:
    """list_stacks should collect stacks from all pages exposed by the runtime."""
    local = SimpleNamespace(id="stack-local-id", name="local")
    staging = SimpleNamespace(id="stack-staging-id", name="staging")
    prod = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = Mock()
    client_mock.active_stack_model = prod
    client_mock.list_stacks.side_effect = [
        _FakeStackPage(items=[local], total_pages=2, max_size=1),
        _FakeStackPage(items=[staging, prod], total_pages=2, max_size=1),
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        stacks = list_stacks()

    assert [stack.name for stack in stacks] == ["local", "staging", "prod"]
    assert [stack.is_active for stack in stacks] == [False, False, True]
    client_mock.list_stacks.assert_has_calls([call(), call(page=2, size=1)])


def test_use_stack_switches_active_stack() -> None:
    """use_stack should delegate activation and return the new active stack."""
    local_stack = SimpleNamespace(id="stack-local-id", name="local")
    prod_stack = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = SimpleNamespace(active_stack_model=local_stack)

    def _activate_stack(_: str) -> None:
        client_mock.active_stack_model = prod_stack

    activate_stack = Mock(side_effect=_activate_stack)
    client_mock.activate_stack = activate_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        selected = use_stack("prod")

    activate_stack.assert_called_once_with("prod")
    assert selected.name == "prod"
    assert selected.id == "stack-prod-id"
    assert selected.is_active is True


def test_use_stack_rejects_empty_selector() -> None:
    """use_stack should fail fast on empty stack names/IDs."""
    with pytest.raises(ValueError, match="cannot be empty"):
        use_stack("   ")
