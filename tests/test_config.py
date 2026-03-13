"""Tests for Kitaru configuration helpers."""

from __future__ import annotations

import os
import warnings
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, call, patch

import pytest
from zenml.config.docker_settings import DockerSettings
from zenml.constants import ENV_ZENML_ACTIVE_PROJECT_ID
from zenml.enums import StackComponentType
from zenml.exceptions import EntityExistsError
from zenml.utils import io_utils, yaml_utils

from _kitaru_env import apply_env_translations
from kitaru.config import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    KITARU_CACHE_ENV,
    KITARU_CONFIG_PATH_ENV,
    KITARU_DEFAULT_MODEL_ENV,
    KITARU_IMAGE_ENV,
    KITARU_LOG_STORE_BACKEND_ENV,
    KITARU_LOG_STORE_ENDPOINT_ENV,
    KITARU_PROJECT_ENV,
    KITARU_RETRIES_ENV,
    KITARU_SERVER_URL_ENV,
    KITARU_STACK_ENV,
    CloudProvider,
    FrozenExecutionSpec,
    ImageSettings,
    KitaruConfig,
    KubernetesStackSpec,
    ResolvedConnectionConfig,
    ResolvedExecutionConfig,
    StackType,
    _create_kubernetes_stack_operation,
    _create_stack_operation,
    _delete_stack_components_best_effort,
    _delete_stack_operation,
    _list_stack_entries,
    _StackComponent,
    build_frozen_execution_spec,
    configure,
    create_stack,
    current_stack,
    delete_stack,
    image_settings_to_docker_settings,
    list_active_kitaru_environment_variables,
    list_model_aliases,
    list_stacks,
    persist_frozen_execution_spec,
    register_model_alias,
    reset_global_log_store,
    resolve_connection_config,
    resolve_execution_config,
    resolve_log_store,
    resolve_model_selection,
    set_global_log_store,
    use_stack,
)
from kitaru.errors import KitaruUsageError


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


def _stack_component(component_id: str, name: str) -> SimpleNamespace:
    """Return a minimal stack-component model stub for stack tests."""
    return SimpleNamespace(id=component_id, name=name)


def _stack_model(
    *,
    stack_id: str,
    name: str,
    labels: dict[str, str] | None = None,
    orchestrator_id: str | None = None,
    artifact_store_id: str | None = None,
) -> SimpleNamespace:
    """Return a minimal stack model stub for stack tests."""
    components: dict[StackComponentType, list[SimpleNamespace]] = {}
    if orchestrator_id is not None:
        components[StackComponentType.ORCHESTRATOR] = [
            _stack_component(orchestrator_id, name)
        ]
    if artifact_store_id is not None:
        components[StackComponentType.ARTIFACT_STORE] = [
            _stack_component(artifact_store_id, name)
        ]

    return SimpleNamespace(
        id=stack_id,
        name=name,
        labels=labels or {},
        components=components,
    )


def _kitaru_config_path() -> Path:
    """Return the path used for persisted Kitaru global config in tests."""
    from kitaru.config import _kitaru_global_config_path

    return _kitaru_global_config_path()


def test_apply_env_translations_sets_zenml_mirrors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Public KITARU env vars should populate the equivalent ZenML vars."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://server.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")
    monkeypatch.setenv("KITARU_PROJECT", "demo-project")
    monkeypatch.setenv("KITARU_DEBUG", "false")
    monkeypatch.setenv("KITARU_ANALYTICS_OPT_IN", "true")

    apply_env_translations()

    assert os.environ["ZENML_STORE_URL"] == "https://server.example.com"
    assert os.environ["ZENML_STORE_API_KEY"] == "token-123"
    assert os.environ["ZENML_ACTIVE_PROJECT_ID"] == "demo-project"
    assert os.environ["ZENML_DEBUG"] == "false"
    assert os.environ["ZENML_ANALYTICS_OPT_IN"] == "true"


def test_apply_env_translations_warns_and_overwrites_conflicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU env vars should win over conflicting ZenML env vars."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://kitaru.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")
    monkeypatch.setenv("ZENML_STORE_URL", "https://zenml.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "other-token")

    with pytest.warns(UserWarning, match="KITARU_SERVER_URL"):
        apply_env_translations()

    assert os.environ["ZENML_STORE_URL"] == "https://kitaru.example.com"
    assert os.environ["ZENML_STORE_API_KEY"] == "token-123"


def test_apply_env_translations_does_not_warn_when_values_already_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Matching KITARU/ZENML values should stay quiet."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://same.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")
    monkeypatch.setenv("ZENML_STORE_URL", "https://same.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "token-123")

    with warnings.catch_warnings(record=True) as recorded:
        apply_env_translations()

    assert len(recorded) == 0


def test_apply_env_translations_is_idempotent_after_first_overwrite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-running translation should not keep warning once values match."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://kitaru.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")
    monkeypatch.setenv("ZENML_STORE_URL", "https://zenml.example.com")

    with pytest.warns(UserWarning):
        apply_env_translations()

    with warnings.catch_warnings(record=True) as recorded:
        apply_env_translations()

    assert len(recorded) == 0


def test_apply_env_translations_rejects_partial_server_only_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A server URL without any auth token should fail fast."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://server.example.com")

    with pytest.raises(RuntimeError, match="KITARU_AUTH_TOKEN"):
        apply_env_translations()


def test_apply_env_translations_accepts_cross_namespace_auth_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct ZenML auth should satisfy fast-fail validation."""
    monkeypatch.setenv("KITARU_SERVER_URL", "https://server.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "fallback-token")

    apply_env_translations()

    assert os.environ["ZENML_STORE_URL"] == "https://server.example.com"


def test_apply_env_translations_rejects_partial_token_only_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token without any server URL should also fail fast."""
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")

    with pytest.raises(RuntimeError, match="KITARU_SERVER_URL"):
        apply_env_translations()


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


def test_set_log_store_preserves_model_registry() -> None:
    """Log-store updates should not clobber persisted model aliases."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")

    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    aliases = list_model_aliases()
    assert len(aliases) == 1
    assert aliases[0].alias == "fast"
    assert aliases[0].model == "openai/gpt-4o-mini"
    assert aliases[0].secret == "openai-creds"


def test_register_model_alias_preserves_log_store_settings() -> None:
    """Model alias writes should preserve existing log-store overrides."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    alias = register_model_alias("fast", model="openai/gpt-4o-mini")

    assert alias.alias == "fast"
    assert alias.is_default is True

    snapshot = resolve_log_store()
    assert snapshot.backend == "datadog"
    assert snapshot.endpoint == "https://logs.datadoghq.com"


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


def test_reset_log_store_preserves_model_registry() -> None:
    """Resetting log-store config should keep model aliases intact."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    reset_global_log_store()

    aliases = list_model_aliases()
    assert len(aliases) == 1
    assert aliases[0].alias == "fast"


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


def test_register_model_alias_sets_first_alias_as_default() -> None:
    """The first registered alias should become the default model alias."""
    fast = register_model_alias("FAST", model="openai/gpt-4o-mini")
    smart = register_model_alias("smart", model="anthropic/claude-sonnet-4-20250514")

    aliases = list_model_aliases()

    assert fast.alias == "fast"
    assert fast.is_default is True
    assert smart.is_default is False
    assert [entry.alias for entry in aliases] == ["fast", "smart"]


def test_register_model_alias_updates_existing_alias() -> None:
    """Re-registering an alias should update model/secret values."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")

    updated = register_model_alias(
        "fast",
        model="openai/gpt-4.1-mini",
        secret="openai-prod",
    )

    assert updated.alias == "fast"
    assert updated.model == "openai/gpt-4.1-mini"
    assert updated.secret == "openai-prod"
    assert updated.is_default is True


def test_resolve_model_selection_prefers_aliases_and_defaults() -> None:
    """Model resolution should honor aliases and default fallback behavior."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")

    alias_selection = resolve_model_selection("fast")
    concrete_selection = resolve_model_selection("openai/gpt-4.1-mini")
    default_selection = resolve_model_selection(None)

    assert alias_selection.alias == "fast"
    assert alias_selection.resolved_model == "openai/gpt-4o-mini"
    assert alias_selection.secret == "openai-creds"

    assert concrete_selection.alias is None
    assert concrete_selection.resolved_model == "openai/gpt-4.1-mini"
    assert concrete_selection.secret is None

    assert default_selection.alias == "fast"
    assert default_selection.resolved_model == "openai/gpt-4o-mini"


def test_resolve_model_selection_uses_env_default_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU_DEFAULT_MODEL should resolve through aliases first."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")
    monkeypatch.setenv(KITARU_DEFAULT_MODEL_ENV, "fast")

    selection = resolve_model_selection(None)

    assert selection.alias == "fast"
    assert selection.resolved_model == "openai/gpt-4o-mini"
    assert selection.secret == "openai-creds"


def test_resolve_model_selection_uses_env_default_raw_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown env default values should pass through as raw model strings."""
    monkeypatch.setenv(KITARU_DEFAULT_MODEL_ENV, "openai/gpt-4.1-mini")

    selection = resolve_model_selection(None)

    assert selection.alias is None
    assert selection.resolved_model == "openai/gpt-4.1-mini"


def test_explicit_model_beats_env_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit model argument should beat KITARU_DEFAULT_MODEL."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    monkeypatch.setenv(KITARU_DEFAULT_MODEL_ENV, "fast")

    selection = resolve_model_selection("anthropic/claude-sonnet-4-20250514")

    assert selection.alias is None
    assert selection.resolved_model == "anthropic/claude-sonnet-4-20250514"


def test_empty_env_default_model_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty KITARU_DEFAULT_MODEL should fail with env-specific guidance."""
    monkeypatch.setenv(KITARU_DEFAULT_MODEL_ENV, "   ")

    with pytest.raises(ValueError, match=KITARU_DEFAULT_MODEL_ENV):
        resolve_model_selection(None)


def test_resolve_model_selection_requires_default_or_explicit_model() -> None:
    """`kitaru.llm(model=None)` should fail without a configured default alias."""
    with pytest.raises(ValueError, match="No model alias is configured"):
        resolve_model_selection(None)


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


def test_list_stack_entries_include_managed_flag() -> None:
    """Structured stack entries should expose derived managed status."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
    )
    client_mock = SimpleNamespace(
        active_stack_model=default,
        list_stacks=lambda: [default, dev],
    )

    with patch("kitaru.config.Client", return_value=client_mock):
        entries = _list_stack_entries()

    assert [(entry.stack.name, entry.is_managed) for entry in entries] == [
        ("default", False),
        ("dev", True),
    ]


def test_create_stack_creates_local_components_and_activates() -> None:
    """Create should build local components, create the stack, and activate it."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
        orchestrator_id="orc-dev-id",
        artifact_store_id="art-dev-id",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.create_stack_component.side_effect = [
        SimpleNamespace(id="orc-dev-id"),
        SimpleNamespace(id="art-dev-id"),
    ]
    client_mock.create_stack.return_value = created_stack

    def _activate_stack(_: str) -> None:
        client_mock.active_stack_model = created_stack

    client_mock.activate_stack.side_effect = _activate_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_stack_operation("dev")

    client_mock.create_stack_component.assert_has_calls(
        [
            call(
                name="dev",
                flavor="local",
                component_type=StackComponentType.ORCHESTRATOR,
                configuration={},
            ),
            call(
                name="dev",
                flavor="local",
                component_type=StackComponentType.ARTIFACT_STORE,
                configuration={},
            ),
        ]
    )
    assert result.stack.name == "dev"
    assert result.stack.is_active is True
    assert result.previous_active_stack == "default"
    assert result.components_created == (
        "dev (orchestrator)",
        "dev (artifact_store)",
    )
    assert result.stack_type == "local"
    assert result.service_connectors_created == ()
    assert result.resources is None


def test_create_stack_public_wrapper_returns_stack_info() -> None:
    """Public create_stack should return only the stack info from the operation."""
    with patch("kitaru.config._create_stack_operation") as mock_create:
        mock_create.return_value = SimpleNamespace(
            stack=SimpleNamespace(id="stack-dev-id", name="dev", is_active=True)
        )

        stack = create_stack("dev")

    mock_create.assert_called_once_with("dev", activate=True, labels=None)
    assert stack.name == "dev"
    assert stack.is_active is True


def test_create_stack_dispatcher_defaults_to_local_flow() -> None:
    """Dispatcher should route default create requests to the local flow."""
    expected_result = SimpleNamespace(name="local-result")

    with patch(
        "kitaru.config._create_local_stack_operation",
        return_value=expected_result,
    ) as mock_create_local:
        result = _create_stack_operation("dev")

    mock_create_local.assert_called_once_with("dev", activate=True, labels=None)
    assert result is expected_result


def test_create_stack_dispatcher_requires_kubernetes_spec() -> None:
    """Kubernetes dispatcher requests should fail fast without a spec."""
    with (
        patch("kitaru.config.Client") as mock_client,
        pytest.raises(
            ValueError,
            match=r"Kubernetes spec required for --type kubernetes\.",
        ),
    ):
        _create_stack_operation("dev", stack_type=StackType.KUBERNETES)

    mock_client.assert_not_called()


def test_create_stack_dispatcher_routes_kubernetes_requests() -> None:
    """Dispatcher should pass Kubernetes requests through to the future helper."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789.dkr.ecr.eu-west-1.amazonaws.com/my-repo",
        cluster="demo-cluster",
        region="eu-west-1",
        namespace="ml",
        credentials="aws-dev",
        verify=False,
    )
    expected_result = SimpleNamespace(name="kubernetes-result")

    with patch(
        "kitaru.config._create_kubernetes_stack_operation",
        return_value=expected_result,
    ) as mock_create_kubernetes:
        result = _create_stack_operation(
            "dev",
            stack_type=StackType.KUBERNETES,
            kubernetes=spec,
            activate=False,
            labels={"owner": "ml"},
        )

    mock_create_kubernetes.assert_called_once_with(
        "dev",
        spec=spec,
        activate=False,
        labels={"owner": "ml"},
    )
    assert result is expected_result


def test_create_stack_dispatcher_rejects_unsupported_stack_type() -> None:
    """Dispatcher should reject unknown stack types instead of guessing."""
    with (
        patch("kitaru.config.Client") as mock_client,
        pytest.raises(ValueError, match="Unsupported stack type: weird"),
    ):
        _create_stack_operation(
            "dev",
            stack_type=cast(Any, "weird"),
        )

    mock_client.assert_not_called()


def test_create_kubernetes_stack_operation_not_implemented() -> None:
    """Phase 1 should keep Kubernetes creation as an explicit stub."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.GCP,
        artifact_store="gs://bucket/path",
        container_registry="europe-west4-docker.pkg.dev/demo/repo",
        cluster="demo-cluster",
        region="europe-west4",
    )

    with (
        patch("kitaru.config.Client") as mock_client,
        pytest.raises(
            NotImplementedError,
            match=r"Kubernetes stack creation is not implemented yet\.",
        ),
    ):
        _create_kubernetes_stack_operation("dev", spec=spec)

    mock_client.assert_not_called()


def test_create_stack_without_activation_keeps_previous_active_stack() -> None:
    """Create with activate=False should not switch the active stack."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
        orchestrator_id="orc-dev-id",
        artifact_store_id="art-dev-id",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.create_stack_component.side_effect = [
        SimpleNamespace(id="orc-dev-id"),
        SimpleNamespace(id="art-dev-id"),
    ]
    client_mock.create_stack.return_value = created_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_stack_operation("dev", activate=False)

    client_mock.activate_stack.assert_not_called()
    assert client_mock.active_stack_model.name == "default"
    assert result.previous_active_stack is None
    assert result.stack.name == "dev"
    assert result.stack.is_active is False
    assert result.stack_type == "local"
    assert result.service_connectors_created == ()
    assert result.resources is None


def test_create_stack_rejects_existing_stack_name() -> None:
    """Create should fail fast with a helpful message when the stack exists."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    existing = _stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = SimpleNamespace(
        active_stack_model=default,
        list_stacks=lambda: [default, existing],
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(ValueError, match='A stack named "dev" already exists'),
    ):
        create_stack("dev")


def test_create_stack_component_collision_reports_fresh_component_policy() -> None:
    """Create should explain that Kitaru never reuses existing components."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.create_stack_component.side_effect = EntityExistsError("exists")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(ValueError, match="never reuses existing ones"),
    ):
        create_stack("dev")


def test_create_stack_cleans_up_components_if_stack_creation_fails() -> None:
    """Create should delete orphaned components in reverse order on stack failure."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.create_stack_component.side_effect = [
        SimpleNamespace(id="orc-dev-id"),
        SimpleNamespace(id="art-dev-id"),
    ]
    client_mock.create_stack.side_effect = RuntimeError("stack create failed")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(RuntimeError, match="stack create failed"),
    ):
        create_stack("dev")

    client_mock.delete_stack_component.assert_has_calls(
        [
            call("art-dev-id", StackComponentType.ARTIFACT_STORE),
            call("orc-dev-id", StackComponentType.ORCHESTRATOR),
        ]
    )


def test_delete_stack_components_best_effort_handles_container_registry() -> None:
    """Cleanup should delete container registries using the correct component type."""
    client_mock = Mock()
    component = _StackComponent(
        component_id="registry-dev-id",
        name="dev",
        kind="container_registry",
    )

    warning = _delete_stack_components_best_effort(client_mock, [component])

    assert warning is None
    client_mock.delete_stack_component.assert_called_once_with(
        "registry-dev-id",
        StackComponentType.CONTAINER_REGISTRY,
    )


def test_create_stack_applies_managed_label_and_preserves_extra_labels() -> None:
    """Create should always force the managed label while preserving caller labels."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true", "owner": "ml"},
        orchestrator_id="orc-dev-id",
        artifact_store_id="art-dev-id",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.create_stack_component.side_effect = [
        SimpleNamespace(id="orc-dev-id"),
        SimpleNamespace(id="art-dev-id"),
    ]
    client_mock.create_stack.return_value = created_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        create_stack(
            "dev",
            labels={"owner": "ml", "kitaru.managed": "false"},
        )

    assert client_mock.create_stack.call_args.kwargs["labels"] == {
        "owner": "ml",
        "kitaru.managed": "true",
    }


def test_delete_stack_deletes_non_active_stack() -> None:
    """Delete should remove a non-active stack without switching anything."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.return_value = dev

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev")
        public_result = delete_stack("dev-id")

    client_mock.get_stack.assert_has_calls(
        [
            call("dev", allow_name_prefix_match=False),
            call("dev-id", allow_name_prefix_match=False),
        ]
    )
    client_mock.delete_stack.assert_has_calls(
        [
            call("stack-dev-id", recursive=False),
            call("stack-dev-id", recursive=False),
        ]
    )
    assert result.deleted_stack == "dev"
    assert result.components_deleted == ()
    assert result.new_active_stack is None
    assert public_result is None


def test_delete_stack_recursive_managed_stack_reports_unshared_components() -> None:
    """Recursive delete should report only the managed components it can remove."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
        orchestrator_id="orc-dev-id",
        artifact_store_id="art-dev-id",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.return_value = dev
    client_mock.list_stacks.side_effect = [[dev], [dev]]

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev", recursive=True)

    client_mock.delete_stack.assert_called_once_with("stack-dev-id", recursive=True)
    assert result.components_deleted == (
        "dev (orchestrator)",
        "dev (artifact_store)",
    )
    assert result.recursive is True


def test_delete_stack_recursive_unmanaged_stack_reports_no_components() -> None:
    """Recursive delete should not claim component ownership for unmanaged stacks."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    legacy = _stack_model(
        stack_id="stack-legacy-id",
        name="legacy",
        orchestrator_id="orc-legacy-id",
        artifact_store_id="art-legacy-id",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.return_value = legacy

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("legacy", recursive=True)

    client_mock.list_stacks.assert_not_called()
    assert result.components_deleted == ()


def test_delete_stack_rejects_active_stack_without_force() -> None:
    """Delete should guard against removing the active stack by default."""
    active = _stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = active
    client_mock.get_stack.return_value = active

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(ValueError, match="Cannot delete the active stack"),
    ):
        delete_stack("dev")

    client_mock.delete_stack.assert_not_called()


def test_delete_stack_force_switches_to_default_before_deleting() -> None:
    """Forced delete should fall back to the default stack before removal."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    active = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
    )
    client_mock = Mock()
    client_mock.active_stack_model = active
    client_mock.get_stack.return_value = active

    def _activate_stack(_: str) -> None:
        client_mock.active_stack_model = default

    client_mock.activate_stack.side_effect = _activate_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev", force=True)

    client_mock.activate_stack.assert_called_once_with("default")
    client_mock.delete_stack.assert_called_once_with("stack-dev-id", recursive=False)
    assert result.new_active_stack == "default"


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


def test_configure_sets_runtime_execution_defaults() -> None:
    """configure should update process-local execution defaults."""
    snapshot = configure(
        stack="gpu-prod",
        cache=False,
        retries=2,
        image={
            "base_image": "python:3.12-slim",
            "environment": {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
        },
    )

    assert snapshot.stack == "gpu-prod"
    assert snapshot.cache is False
    assert snapshot.retries == 2
    assert snapshot.image is not None
    assert snapshot.image.base_image == "python:3.12-slim"
    assert snapshot.image.environment == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}


def test_configure_can_clear_runtime_override_fields() -> None:
    """configure should allow clearing previously set runtime overrides."""
    configure(stack="gpu-prod", cache=False, retries=2)

    snapshot = configure(stack=None, cache=None, retries=None, image=None)

    assert snapshot.stack is None
    assert snapshot.cache is None
    assert snapshot.retries is None
    assert snapshot.image is None


def test_configure_sets_runtime_project_override() -> None:
    """configure(project=...) should set an override in the connection layer."""
    configure(project="staging-project")

    resolved = resolve_connection_config()
    assert resolved.project == "staging-project"


def test_configure_clears_runtime_project_override() -> None:
    """configure(project=None) should clear a previously set project override."""
    configure(project="staging-project")
    configure(project=None)

    resolved = resolve_connection_config()
    assert resolved.project is None


def test_configure_project_independent_of_execution() -> None:
    """Project and execution overrides should not interfere with each other."""
    configure(stack="gpu-prod", cache=False, project="staging-project")

    exec_resolved = resolve_execution_config()
    conn_resolved = resolve_connection_config()

    assert exec_resolved.stack == "gpu-prod"
    assert exec_resolved.cache is False
    assert conn_resolved.project == "staging-project"


def test_global_connection_config_does_not_infer_project() -> None:
    """Global connection config should not include inferred project."""
    from kitaru.config import _read_global_connection_config

    config = _read_global_connection_config()
    assert config.project is None


def test_kitaru_config_path_uses_kitaru_dir() -> None:
    """Kitaru's config file should live under the app-specific config dir."""
    path = _kitaru_config_path()
    assert path.parent.name == "kitaru-config"
    assert path.name == "config.yaml"


def test_kitaru_config_path_env_overrides_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """KITARU_CONFIG_PATH should override the config directory lookup."""
    from kitaru.config import _kitaru_config_dir

    custom_dir = tmp_path / "custom-kitaru-home"
    monkeypatch.setenv(KITARU_CONFIG_PATH_ENV, str(custom_dir))

    resolved_dir = _kitaru_config_dir()

    assert resolved_dir == custom_dir
    assert not custom_dir.exists()


def test_kitaru_config_path_dir_is_created_on_first_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The custom config directory should still be created lazily on write."""
    custom_dir = tmp_path / "custom-kitaru-home"
    monkeypatch.setenv(KITARU_CONFIG_PATH_ENV, str(custom_dir))

    register_model_alias("fast", model="openai/gpt-4o-mini")

    assert custom_dir.exists()
    assert (custom_dir / "config.yaml").exists()


def test_active_environment_variables_mask_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Status surfaces should receive masked secret values from config helpers."""
    monkeypatch.setenv(KITARU_SERVER_URL_ENV, "https://server.example.com")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123456")
    monkeypatch.setenv(KITARU_DEFAULT_MODEL_ENV, "openai/gpt-4o")

    active = list_active_kitaru_environment_variables()

    assert [(entry.name, entry.value) for entry in active] == [
        (KITARU_SERVER_URL_ENV, "https://server.example.com"),
        ("KITARU_AUTH_TOKEN", "token-12***"),
        (KITARU_DEFAULT_MODEL_ENV, "openai/gpt-4o"),
    ]


def test_legacy_config_is_ignored(tmp_path: Path) -> None:
    """Legacy ZenML-side config should no longer be migrated or read."""
    legacy_path = tmp_path / ".zenml" / "kitaru.yaml"
    io_utils.create_dir_recursive_if_not_exists(str(legacy_path.parent))
    yaml_utils.write_yaml(
        str(legacy_path),
        {
            "version": 1,
            "log_store": {
                "backend": "datadog",
                "endpoint": "https://logs.datadoghq.com",
            },
        },
    )

    snapshot = resolve_log_store()
    assert snapshot.backend == "artifact-store"

    new_path = _kitaru_config_path()
    assert not new_path.exists()


def test_resolve_execution_config_applies_phase10_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execution config resolution should follow the Phase 10 precedence chain."""
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        """
[tool.kitaru]
stack = "project-stack"
cache = false
retries = 1

[tool.kitaru.image]
base_image = "python:3.12"

[tool.kitaru.image.environment]
FROM_PROJECT = "1"
SHARED = "project"
""".strip()
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(KITARU_STACK_ENV, "env-stack")
    monkeypatch.setenv(KITARU_CACHE_ENV, "true")
    monkeypatch.setenv(KITARU_RETRIES_ENV, "3")
    monkeypatch.setenv(
        KITARU_IMAGE_ENV,
        (
            '{"base_image": "python:3.13", '
            '"environment": {"FROM_ENV": "1", "SHARED": "env"}}'
        ),
    )
    configure(
        stack="runtime-stack",
        cache=False,
        retries=4,
        image={"environment": {"FROM_RUNTIME": "1", "SHARED": "runtime"}},
    )

    with patch(
        "kitaru.config.current_stack",
        return_value=SimpleNamespace(name="global-stack"),
    ):
        resolved = resolve_execution_config(
            decorator_overrides=KitaruConfig(
                cache=True,
                retries=5,
                image=ImageSettings(
                    environment={"FROM_DECORATOR": "1", "SHARED": "decorator"}
                ),
            ),
            invocation_overrides=KitaruConfig(
                stack="invocation-stack",
                retries=6,
                image=ImageSettings(
                    environment={"FROM_INVOCATION": "1", "SHARED": "invocation"}
                ),
            ),
            start_dir=tmp_path,
        )

    assert resolved.stack == "invocation-stack"
    assert resolved.cache is True
    assert resolved.retries == 6
    assert resolved.image is not None
    assert resolved.image.base_image == "python:3.13"
    assert resolved.image.environment == {
        "FROM_PROJECT": "1",
        "SHARED": "invocation",
        "FROM_ENV": "1",
        "FROM_RUNTIME": "1",
        "FROM_DECORATOR": "1",
        "FROM_INVOCATION": "1",
    }


def test_resolve_execution_config_rejects_invalid_cache_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid cache env values should raise clear parse errors."""
    monkeypatch.setenv(KITARU_CACHE_ENV, "not-a-bool")

    with pytest.raises(ValueError, match=KITARU_CACHE_ENV):
        resolve_execution_config()


def test_resolve_execution_config_supports_string_image_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU_IMAGE should accept plain image strings for compatibility."""
    monkeypatch.setenv(KITARU_IMAGE_ENV, "python:3.12-slim")

    with patch(
        "kitaru.config.current_stack",
        return_value=SimpleNamespace(name="global-stack"),
    ):
        resolved = resolve_execution_config()

    assert resolved.image is not None
    assert resolved.image.base_image == "python:3.12-slim"


def test_image_settings_can_be_converted_to_docker_settings() -> None:
    """Resolved image settings should map cleanly to ZenML Docker settings."""
    image_settings = ImageSettings(
        base_image="python:3.12",
        requirements=["httpx"],
        dockerfile="Dockerfile",
        environment={"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
    )

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert isinstance(docker_settings, DockerSettings)
    assert docker_settings.parent_image == "python:3.12"
    # kitaru is NOT auto-injected because base_image/dockerfile are set
    assert docker_settings.requirements == ["httpx"]
    assert docker_settings.dockerfile == "Dockerfile"
    assert docker_settings.environment == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}


def test_kitaru_auto_injected_when_no_image_settings() -> None:
    """Kitaru should be auto-injected even when no image settings are given."""
    docker_settings = image_settings_to_docker_settings(None)

    assert isinstance(docker_settings, DockerSettings)
    assert docker_settings.requirements == ["kitaru"]


def test_kitaru_auto_injected_when_image_settings_empty() -> None:
    """Kitaru should be auto-injected when image settings are empty."""
    docker_settings = image_settings_to_docker_settings(ImageSettings())

    assert isinstance(docker_settings, DockerSettings)
    assert docker_settings.requirements == ["kitaru"]


def test_kitaru_not_injected_when_custom_base_image() -> None:
    """Kitaru should not be auto-injected when a custom base_image is set."""
    image_settings = ImageSettings(
        base_image="my-registry/my-image:latest",
        requirements=["httpx"],
    )

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.requirements == ["httpx"]


def test_kitaru_not_injected_when_custom_dockerfile() -> None:
    """Kitaru should not be auto-injected when a custom dockerfile is set."""
    image_settings = ImageSettings(
        dockerfile="Dockerfile.custom",
        requirements=["httpx"],
    )

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.requirements == ["httpx"]


def test_kitaru_auto_injected_with_only_requirements() -> None:
    """Kitaru should be auto-injected when only requirements are set."""
    image_settings = ImageSettings(requirements=["httpx"])

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.requirements == ["httpx", "kitaru"]


def test_kitaru_not_duplicated_when_already_in_requirements() -> None:
    """Kitaru should not be added again if already present in requirements."""
    image_settings = ImageSettings(requirements=["kitaru", "httpx"])

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.requirements == ["kitaru", "httpx"]


def test_kitaru_not_duplicated_when_pinned_version_in_requirements() -> None:
    """Kitaru should not be added if a pinned version is already present."""
    image_settings = ImageSettings(requirements=["kitaru>=0.2.0", "httpx"])

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.requirements == ["kitaru>=0.2.0", "httpx"]


def test_kitaru_not_duplicated_when_git_url_in_requirements() -> None:
    """Kitaru should not be added if a git direct reference is already present."""
    git_ref = "kitaru @ git+https://github.com/zenml-io/kitaru.git@develop"
    image_settings = ImageSettings(requirements=[git_ref, "httpx"])

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.requirements == [git_ref, "httpx"]


def test_replicate_local_python_environment_passes_through() -> None:
    """replicate_local_python_environment should flow through to DockerSettings."""
    image_settings = ImageSettings(replicate_local_python_environment=True)

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.replicate_local_python_environment is True
    assert docker_settings.requirements == ["kitaru"]


def test_apt_packages_passes_through() -> None:
    """apt_packages should flow through to DockerSettings."""
    image_settings = ImageSettings(apt_packages=["git", "curl"])

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert docker_settings.apt_packages == ["git", "curl"]
    assert docker_settings.requirements == ["kitaru"]


def test_connection_resolution_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connection config should resolve as explicit > env > global."""
    monkeypatch.setenv(KITARU_SERVER_URL_ENV, "https://env.example.com")
    monkeypatch.setenv(KITARU_PROJECT_ENV, "env-project")

    with patch(
        "kitaru.config._read_global_connection_config",
        return_value=KitaruConfig(
            server_url="https://global.example.com",
            auth_token="global-token",
            project="global-project",
        ),
    ):
        resolved = resolve_connection_config(
            explicit=KitaruConfig(project="explicit-project"),
        )

    assert isinstance(resolved, ResolvedConnectionConfig)
    assert resolved.server_url == "https://env.example.com"
    assert resolved.project == "explicit-project"
    assert resolved.auth_token == "global-token"


def test_connection_resolution_reads_direct_zenml_env_below_kitaru(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct ZenML env should work, but KITARU env should still win."""
    monkeypatch.setenv("ZENML_STORE_URL", "https://zenml.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "zenml-token")
    monkeypatch.setenv(ENV_ZENML_ACTIVE_PROJECT_ID, "zenml-project")
    monkeypatch.setenv(KITARU_SERVER_URL_ENV, "https://kitaru.example.com")
    monkeypatch.setenv(KITARU_PROJECT_ENV, "kitaru-project")

    with patch(
        "kitaru.config._read_global_connection_config",
        return_value=KitaruConfig(
            server_url="https://global.example.com",
            auth_token="global-token",
            project="global-project",
        ),
    ):
        resolved = resolve_connection_config()

    assert resolved.server_url == "https://kitaru.example.com"
    assert resolved.auth_token == "zenml-token"
    assert resolved.project == "kitaru-project"


def test_connection_validation_requires_project_for_env_remote_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Env-driven remote connections should require an explicit project."""
    monkeypatch.setenv(KITARU_SERVER_URL_ENV, "https://server.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "token-123")

    with pytest.raises(KitaruUsageError, match="KITARU_PROJECT"):
        resolve_connection_config(validate_for_use=True)


def test_connection_validation_accepts_zenml_project_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ZENML_ACTIVE_PROJECT_ID should satisfy the lazy project requirement."""
    monkeypatch.setenv("ZENML_STORE_URL", "https://server.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "token-123")
    monkeypatch.setenv(ENV_ZENML_ACTIVE_PROJECT_ID, "demo-project")

    resolved = resolve_connection_config(validate_for_use=True)

    assert resolved.project == "demo-project"


def test_connection_validation_does_not_require_project_for_global_connection() -> None:
    """Persisted global connections should keep working without env project state."""
    with patch(
        "kitaru.config._read_global_connection_config",
        return_value=KitaruConfig(
            server_url="https://global.example.com",
            auth_token="global-token",
        ),
    ):
        resolved = resolve_connection_config(validate_for_use=True)

    assert resolved.server_url == "https://global.example.com"
    assert resolved.project is None


def test_build_and_persist_frozen_execution_spec() -> None:
    """Frozen execution specs should be serializable and persisted as metadata."""
    frozen_execution_spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="prod",
            cache=False,
            retries=2,
            image=ImageSettings(
                base_image="python:3.12",
                environment={"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
            ),
        ),
        flow_defaults=KitaruConfig(cache=False),
        connection=ResolvedConnectionConfig(
            server_url="https://server.example.com",
            project="demo",
        ),
    )

    assert isinstance(frozen_execution_spec, FrozenExecutionSpec)
    assert (
        frozen_execution_spec.resolved_execution.image is not None
        and frozen_execution_spec.resolved_execution.image.environment
        == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}
    )

    with patch("kitaru.config.Client") as client_cls:
        persist_frozen_execution_spec(
            run_id="00000000-0000-0000-0000-000000000123",
            frozen_execution_spec=frozen_execution_spec,
        )

    create_metadata = client_cls.return_value.create_run_metadata
    create_metadata.assert_called_once()
    metadata_payload = create_metadata.call_args.kwargs["metadata"]
    assert FROZEN_EXECUTION_SPEC_METADATA_KEY in metadata_payload
    assert (
        metadata_payload[FROZEN_EXECUTION_SPEC_METADATA_KEY]["resolved_execution"][
            "stack"
        ]
        == "prod"
    )
