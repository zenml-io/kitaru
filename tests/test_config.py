"""Tests for Kitaru configuration helpers."""

from __future__ import annotations

import json
import logging
import os
import warnings
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, call, patch

import click
import pytest
from zenml.config.docker_settings import DockerSettings
from zenml.constants import ENV_ZENML_ACTIVE_PROJECT_ID
from zenml.enums import StackComponentType
from zenml.exceptions import EntityExistsError
from zenml.utils import io_utils, yaml_utils

import kitaru.config as config_module
from kitaru._config._env import detect_explicit_execution_overrides_impl
from kitaru._env import apply_env_translations
from kitaru.config import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    KITARU_CACHE_ENV,
    KITARU_CONFIG_PATH_ENV,
    KITARU_DEFAULT_MODEL_ENV,
    KITARU_IMAGE_ENV,
    KITARU_LOG_STORE_BACKEND_ENV,
    KITARU_LOG_STORE_ENDPOINT_ENV,
    KITARU_MODEL_REGISTRY_ENV,
    KITARU_PROJECT_ENV,
    KITARU_RETRIES_ENV,
    KITARU_SERVER_URL_ENV,
    KITARU_STACK_ENV,
    AzureMLStackSpec,
    CloudProvider,
    ExplicitOverrides,
    FrozenExecutionSpec,
    ImageSettings,
    KitaruConfig,
    KubernetesStackSpec,
    ModelAliasConfig,
    ModelRegistryConfig,
    ResolvedConnectionConfig,
    ResolvedExecutionConfig,
    SagemakerStackSpec,
    StackComponentConfigOverrides,
    StackType,
    VertexStackSpec,
    _create_azureml_stack_operation,
    _create_kubernetes_stack_operation,
    _create_sagemaker_stack_operation,
    _create_stack_operation,
    _create_vertex_stack_operation,
    _delete_stack_components_best_effort,
    _delete_stack_operation,
    _list_stack_entries,
    _show_stack_operation,
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
from kitaru.errors import KitaruBackendError, KitaruStateError, KitaruUsageError


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


def _stack_component(
    component_id: str,
    name: str,
    *,
    flavor: str | None = None,
    configuration: dict[str, Any] | None = None,
    connector: SimpleNamespace | None = None,
    service_connector_resource_id: str | None = None,
    connector_resource_id: str | None = None,
    resource_id: str | None = None,
) -> SimpleNamespace:
    """Return a minimal hydrated stack-component model stub for stack tests."""
    return SimpleNamespace(
        id=component_id,
        name=name,
        flavor=flavor,
        configuration=configuration or {},
        connector=connector,
        service_connector_resource_id=service_connector_resource_id,
        connector_resource_id=connector_resource_id,
        resource_id=resource_id,
    )


def _stack_model(
    *,
    stack_id: str,
    name: str,
    labels: dict[str, str] | None = None,
    orchestrator_id: str | None = None,
    artifact_store_id: str | None = None,
    components: dict[Any, list[SimpleNamespace]] | None = None,
) -> SimpleNamespace:
    """Return a minimal stack model stub for stack tests."""
    stack_components = dict(components or {})
    if orchestrator_id is not None:
        stack_components[StackComponentType.ORCHESTRATOR] = [
            _stack_component(orchestrator_id, name)
        ]
    if artifact_store_id is not None:
        stack_components[StackComponentType.ARTIFACT_STORE] = [
            _stack_component(artifact_store_id, name)
        ]

    return SimpleNamespace(
        id=stack_id,
        name=name,
        labels=labels or {},
        components=stack_components,
    )


def _kubernetes_stack_component(
    component_id: str,
    name: str,
    *,
    connector_name: str | None = None,
    connector_id: str | None = None,
    flavor: str | None = None,
    configuration: dict[str, Any] | None = None,
    connector_configuration: dict[str, Any] | None = None,
    service_connector_resource_id: str | None = None,
) -> SimpleNamespace:
    """Return a minimal hydrated Kubernetes stack-component stub."""
    connector = (
        SimpleNamespace(
            name=connector_name,
            id=connector_id,
            configuration=connector_configuration or {},
        )
        if connector_name is not None or connector_id is not None
        else None
    )
    return _stack_component(
        component_id,
        name,
        flavor=flavor,
        configuration=configuration,
        connector=connector,
        service_connector_resource_id=service_connector_resource_id,
    )


def _kubernetes_stack_model(
    *,
    stack_id: str,
    name: str,
    connector_name: str | None = "dev-connector",
    connector_id: str | None = None,
    orchestrator_name: str = "dev-orchestrator",
    artifact_store_name: str = "dev-artifacts",
    container_registry_name: str = "dev-registry",
) -> SimpleNamespace:
    """Return a minimal hydrated Kubernetes stack model stub."""
    return SimpleNamespace(
        id=stack_id,
        name=name,
        labels={"kitaru.managed": "true"},
        components={
            StackComponentType.ORCHESTRATOR: [
                _kubernetes_stack_component(
                    "orc-id",
                    orchestrator_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="kubernetes",
                    configuration={"kubernetes_namespace": "default"},
                    connector_configuration={"region": "us-east-1"},
                    service_connector_resource_id="demo-cluster",
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                _kubernetes_stack_component(
                    "art-id",
                    artifact_store_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="s3",
                    configuration={"path": "s3://bucket/kitaru"},
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                _kubernetes_stack_component(
                    "reg-id",
                    container_registry_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="aws",
                    configuration={
                        "uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com"
                    },
                )
            ],
        },
    )


def _vertex_stack_model(
    *,
    stack_id: str,
    name: str,
    connector_name: str | None = "vertex-connector",
    connector_id: str | None = None,
    orchestrator_name: str = "vertex-orchestrator",
    artifact_store_name: str = "vertex-artifacts",
    container_registry_name: str = "vertex-registry",
) -> SimpleNamespace:
    """Return a minimal hydrated Vertex stack model stub."""
    return SimpleNamespace(
        id=stack_id,
        name=name,
        labels={"kitaru.managed": "true"},
        components={
            StackComponentType.ORCHESTRATOR: [
                _kubernetes_stack_component(
                    "orc-id",
                    orchestrator_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="vertex",
                    configuration={"location": "us-central1"},
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                _kubernetes_stack_component(
                    "art-id",
                    artifact_store_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="gcp",
                    configuration={"path": "gs://bucket/kitaru"},
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                _kubernetes_stack_component(
                    "reg-id",
                    container_registry_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="gcp",
                    configuration={"uri": "us-central1-docker.pkg.dev/demo/repo"},
                )
            ],
        },
    )


def _sagemaker_stack_model(
    *,
    stack_id: str,
    name: str,
    connector_name: str | None = "sagemaker-connector",
    connector_id: str | None = None,
    orchestrator_name: str = "sagemaker-orchestrator",
    artifact_store_name: str = "sagemaker-artifacts",
    container_registry_name: str = "sagemaker-registry",
) -> SimpleNamespace:
    """Return a minimal hydrated SageMaker stack model stub."""
    return SimpleNamespace(
        id=stack_id,
        name=name,
        labels={"kitaru.managed": "true"},
        components={
            StackComponentType.ORCHESTRATOR: [
                _kubernetes_stack_component(
                    "orc-id",
                    orchestrator_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="sagemaker",
                    configuration={
                        "execution_role": (
                            "arn:aws:iam::123456789012:role/SageMakerRole"
                        )
                    },
                    connector_configuration={"region": "us-east-1"},
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                _kubernetes_stack_component(
                    "art-id",
                    artifact_store_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="s3",
                    configuration={"path": "s3://bucket/kitaru"},
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                _kubernetes_stack_component(
                    "reg-id",
                    container_registry_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="aws",
                    configuration={
                        "uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com"
                    },
                )
            ],
        },
    )


def _azureml_stack_model(
    *,
    stack_id: str,
    name: str,
    connector_name: str | None = "azureml-connector",
    connector_id: str | None = None,
    orchestrator_name: str = "azureml-orchestrator",
    artifact_store_name: str = "azureml-artifacts",
    container_registry_name: str = "azureml-registry",
) -> SimpleNamespace:
    """Return a minimal hydrated AzureML stack model stub."""
    return SimpleNamespace(
        id=stack_id,
        name=name,
        labels={"kitaru.managed": "true"},
        components={
            StackComponentType.ORCHESTRATOR: [
                _kubernetes_stack_component(
                    "orc-id",
                    orchestrator_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="azureml",
                    configuration={
                        "subscription_id": "00000000-0000-0000-0000-000000000123",
                        "resource_group": "rg-demo",
                        "workspace": "ws-demo",
                        "location": "westeurope",
                    },
                    connector_configuration={
                        "subscription_id": "00000000-0000-0000-0000-000000000123"
                    },
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                _kubernetes_stack_component(
                    "art-id",
                    artifact_store_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="azure",
                    configuration={"path": "az://container/kitaru"},
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                _kubernetes_stack_component(
                    "reg-id",
                    container_registry_name,
                    connector_name=connector_name,
                    connector_id=connector_id,
                    flavor="azure",
                    configuration={"uri": "demo.azurecr.io/team/image"},
                )
            ],
        },
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
    monkeypatch.setenv("KITARU_DEFAULT_ANALYTICS_SOURCE", "kitaru-cli")

    apply_env_translations()

    assert os.environ["ZENML_STORE_URL"] == "https://server.example.com"
    assert os.environ["ZENML_STORE_API_KEY"] == "token-123"
    assert os.environ["ZENML_ACTIVE_PROJECT_ID"] == "demo-project"
    assert os.environ["ZENML_DEBUG"] == "false"
    assert os.environ["ZENML_ANALYTICS_OPT_IN"] == "true"
    assert os.environ["ZENML_DEFAULT_ANALYTICS_SOURCE"] == "kitaru-cli"


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


def test_apply_env_translations_ignores_empty_kitaru_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blank KITARU env vars should not clobber an otherwise valid ZenML setup."""
    monkeypatch.setenv("KITARU_SERVER_URL", "   ")
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "")
    monkeypatch.setenv("ZENML_STORE_URL", "https://zenml.example.com")
    monkeypatch.setenv("ZENML_STORE_API_KEY", "zenml-token")

    apply_env_translations()

    assert os.environ["ZENML_STORE_URL"] == "https://zenml.example.com"
    assert os.environ["ZENML_STORE_API_KEY"] == "zenml-token"


def test_apply_env_translations_rejects_partial_token_only_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token without any server URL should also fail fast."""
    monkeypatch.setenv("KITARU_AUTH_TOKEN", "token-123")

    with pytest.raises(RuntimeError, match="KITARU_SERVER_URL"):
        apply_env_translations()


def test_apply_env_translations_defaults_rich_traceback_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rich traceback should be disabled by default for Kitaru."""
    monkeypatch.delenv("ZENML_ENABLE_RICH_TRACEBACK", raising=False)

    apply_env_translations()

    assert os.environ.get("ZENML_ENABLE_RICH_TRACEBACK") == "0"


def test_apply_env_translations_preserves_explicit_rich_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit user opt-in should not be overridden."""
    monkeypatch.setenv("ZENML_ENABLE_RICH_TRACEBACK", "1")

    apply_env_translations()

    assert os.environ.get("ZENML_ENABLE_RICH_TRACEBACK") == "1"


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

    with pytest.raises(KitaruUsageError, match=KITARU_LOG_STORE_ENDPOINT_ENV):
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

    with pytest.raises(KitaruUsageError, match=KITARU_LOG_STORE_ENDPOINT_ENV):
        resolve_log_store()


def test_set_rejects_artifact_store_override() -> None:
    """artifact-store should stay an implicit default, not an override target."""
    with pytest.raises(KitaruUsageError, match="already the default"):
        set_global_log_store(
            "artifact-store",
            endpoint="https://unused.example.com",
        )


def test_invalid_persisted_config_raises_error() -> None:
    """Malformed persisted config should raise a clear KitaruUsageError."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    with pytest.raises(KitaruUsageError, match="global config file is invalid"):
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


def test_read_env_model_registry_reads_valid_json() -> None:
    """Transported model registry JSON should round-trip through the schema."""
    transported_registry = ModelRegistryConfig(
        aliases={
            "fast": ModelAliasConfig(
                model="openai/gpt-4o-mini",
                secret="openai-creds",
            )
        },
        default="fast",
    )

    registry = config_module._config_models._read_env_model_registry(
        environ={
            KITARU_MODEL_REGISTRY_ENV: transported_registry.model_dump_json(
                exclude_none=True
            )
        }
    )

    assert registry == transported_registry


def test_read_env_model_registry_ignores_missing_or_blank_values() -> None:
    """Unset and blank transport env vars should behave like no registry."""
    assert config_module._config_models._read_env_model_registry(environ={}) is None
    assert (
        config_module._config_models._read_env_model_registry(
            environ={KITARU_MODEL_REGISTRY_ENV: "   "}
        )
        is None
    )


def test_read_env_model_registry_rejects_invalid_payloads() -> None:
    """Malformed transported registries should fail with env-specific guidance."""
    with pytest.raises(KitaruUsageError, match=KITARU_MODEL_REGISTRY_ENV):
        config_module._config_models._read_env_model_registry(
            environ={KITARU_MODEL_REGISTRY_ENV: "not-json"}
        )

    with pytest.raises(KitaruUsageError, match="model registry schema"):
        config_module._config_models._read_env_model_registry(
            environ={
                KITARU_MODEL_REGISTRY_ENV: json.dumps(
                    {"aliases": {}, "default": "missing"}
                )
            }
        )


def test_effective_model_registry_merges_local_and_transported_aliases() -> None:
    """Transported aliases should override same-name local aliases."""
    local_registry = ModelRegistryConfig(
        aliases={
            "fast": ModelAliasConfig(
                model="openai/gpt-4o-mini",
                secret="local-secret",
            ),
            "smart": ModelAliasConfig(model="anthropic/claude-sonnet-4-20250514"),
        },
        default="fast",
    )
    transported_registry = ModelRegistryConfig(
        aliases={
            "fast": ModelAliasConfig(
                model="openai/gpt-4.1-mini",
                secret="remote-secret",
            ),
            "code": ModelAliasConfig(model="openai/o4-mini"),
        },
        default="code",
    )

    registry = config_module._config_models._effective_model_registry(
        read_global_config=lambda: SimpleNamespace(model_registry=local_registry),
        environ={
            KITARU_MODEL_REGISTRY_ENV: transported_registry.model_dump_json(
                exclude_none=True
            )
        },
    )

    assert sorted(registry.aliases) == ["code", "fast", "smart"]
    assert registry.aliases["fast"].model == "openai/gpt-4.1-mini"
    assert registry.aliases["fast"].secret == "remote-secret"
    assert registry.aliases["smart"].model == "anthropic/claude-sonnet-4-20250514"
    assert registry.default == "code"


def test_list_model_aliases_reads_current_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Model listing should include aliases transported into the current env."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    monkeypatch.setenv(
        KITARU_MODEL_REGISTRY_ENV,
        ModelRegistryConfig(
            aliases={
                "smart": ModelAliasConfig(model="anthropic/claude-sonnet-4-20250514")
            },
            default="smart",
        ).model_dump_json(exclude_none=True),
    )

    aliases = list_model_aliases()

    assert [entry.alias for entry in aliases] == ["fast", "smart"]
    assert aliases[0].is_default is False
    assert aliases[1].is_default is True


def test_effective_model_registry_ignores_invalid_local_config_with_transport() -> None:
    """A valid transported registry should not depend on readable local config."""
    transported_registry = ModelRegistryConfig(
        aliases={"fast": ModelAliasConfig(model="openai/gpt-4o-mini")},
        default="fast",
    )

    def _broken_global_config() -> Any:
        raise KitaruUsageError("local config is broken")

    registry = config_module._config_models._effective_model_registry(
        read_global_config=_broken_global_config,
        environ={
            KITARU_MODEL_REGISTRY_ENV: transported_registry.model_dump_json(
                exclude_none=True
            )
        },
    )

    assert registry == transported_registry


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


def test_resolve_model_selection_uses_transported_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transported aliases should override the local registry at runtime."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="local-secret")
    monkeypatch.setenv(
        KITARU_MODEL_REGISTRY_ENV,
        ModelRegistryConfig(
            aliases={
                "fast": ModelAliasConfig(
                    model="openai/gpt-4.1-mini",
                    secret="remote-secret",
                )
            },
            default="fast",
        ).model_dump_json(exclude_none=True),
    )

    selection = resolve_model_selection(None)

    assert selection.alias == "fast"
    assert selection.resolved_model == "openai/gpt-4.1-mini"
    assert selection.secret == "remote-secret"


def test_resolve_model_selection_keeps_explicit_raw_models_with_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit raw models should still bypass alias lookup when unknown."""
    monkeypatch.setenv(
        KITARU_MODEL_REGISTRY_ENV,
        ModelRegistryConfig(
            aliases={"fast": ModelAliasConfig(model="openai/gpt-4o-mini")},
            default="fast",
        ).model_dump_json(exclude_none=True),
    )

    selection = resolve_model_selection("openai/gpt-4.1-mini")

    assert selection.alias is None
    assert selection.resolved_model == "openai/gpt-4.1-mini"
    assert selection.secret is None


def test_resolve_model_selection_reports_empty_transported_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty transported registry should produce transport-specific guidance."""
    monkeypatch.setenv(
        KITARU_MODEL_REGISTRY_ENV,
        ModelRegistryConfig().model_dump_json(exclude_none=True),
    )

    with pytest.raises(KitaruUsageError, match="transported model registry"):
        resolve_model_selection(None)


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

    with pytest.raises(KitaruUsageError, match=KITARU_DEFAULT_MODEL_ENV):
        resolve_model_selection(None)


def test_resolve_model_selection_passes_through_providerless_string() -> None:
    """Config resolution stays permissive for bare model strings without a prefix.

    The runtime backend in kitaru.llm() enforces provider support — config
    should not. This locks in the boundary between the config and runtime layers.
    """
    selection = resolve_model_selection("gpt-4o-mini")

    assert selection.alias is None
    assert selection.resolved_model == "gpt-4o-mini"
    assert selection.secret is None


def test_resolve_model_selection_requires_default_or_explicit_model() -> None:
    """`kitaru.llm(model=None)` should fail without a configured default alias."""
    with pytest.raises(KitaruUsageError, match="No model alias is configured"):
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


def test_show_stack_operation_returns_local_stack_details() -> None:
    """stack show should translate a local stack into Kitaru roles."""
    stack_summary = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
    )
    hydrated_stack = _stack_model(
        stack_id="stack-dev-id",
        name="dev",
        labels={"kitaru.managed": "true"},
        components={
            StackComponentType.ORCHESTRATOR: [
                _stack_component("orc-dev-id", "dev-runner", flavor="local")
            ],
            StackComponentType.ARTIFACT_STORE: [
                _stack_component(
                    "art-dev-id",
                    "dev-storage",
                    flavor="local",
                    configuration={"path": "/tmp/kitaru"},
                )
            ],
        },
    )
    client_mock = Mock()
    client_mock.active_stack_model = stack_summary
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("dev")

    assert details.stack.name == "dev"
    assert details.stack.id == "stack-dev-id"
    assert details.stack.is_active is True
    assert details.is_managed is True
    assert details.stack_type == "local"
    assert [component.role for component in details.components] == [
        "runner",
        "storage",
    ]
    assert details.components[0].name == "dev-runner"
    assert details.components[0].backend == "local"
    assert details.components[0].details == ()
    assert details.components[1].details == (("location", "/tmp/kitaru"),)
    client_mock.get_stack.assert_called_once_with("stack-dev-id", hydrate=True)


def test_show_stack_operation_prefers_exact_id_over_name_collision() -> None:
    """stack show should prefer an exact ID match over a same-text name match."""
    id_match_summary = _stack_model(stack_id="shared-selector", name="dev")
    name_match_summary = _stack_model(stack_id="stack-other-id", name="shared-selector")
    hydrated_stack = _stack_model(
        stack_id="shared-selector",
        name="dev",
        components={
            StackComponentType.ORCHESTRATOR: [
                _stack_component("orc-dev-id", "dev-runner", flavor="local")
            ]
        },
    )
    client_mock = Mock()
    client_mock.active_stack_model = name_match_summary
    client_mock.list_stacks.return_value = [name_match_summary, id_match_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("shared-selector")

    assert details.stack.id == "shared-selector"
    assert details.stack.name == "dev"
    client_mock.get_stack.assert_called_once_with("shared-selector", hydrate=True)


def test_show_stack_operation_returns_kubernetes_stack_details() -> None:
    """stack show should translate Kubernetes stack fields into Kitaru terms."""
    stack_summary = _stack_model(stack_id="stack-k8s-id", name="my-k8s")
    hydrated_stack = _kubernetes_stack_model(stack_id="stack-k8s-id", name="my-k8s")
    client_mock = Mock()
    client_mock.active_stack_model = hydrated_stack
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("my-k8s")

    assert details.stack_type == "kubernetes"
    assert details.is_managed is True
    assert [component.role for component in details.components] == [
        "runner",
        "storage",
        "image_registry",
    ]
    assert details.components[0].details == (
        ("cluster", "demo-cluster"),
        ("region", "us-east-1"),
        ("namespace", "default"),
    )
    assert details.components[1].details == (("location", "s3://bucket/kitaru"),)
    assert details.components[2].details == (
        ("location", "123456789012.dkr.ecr.us-east-1.amazonaws.com"),
    )


def test_show_stack_operation_returns_vertex_stack_details() -> None:
    """stack show should classify Vertex stacks and expose the location field."""
    stack_summary = _stack_model(stack_id="stack-vertex-id", name="my-vertex")
    hydrated_stack = _vertex_stack_model(stack_id="stack-vertex-id", name="my-vertex")
    client_mock = Mock()
    client_mock.active_stack_model = hydrated_stack
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("my-vertex")

    assert details.stack_type == "vertex"
    assert details.is_managed is True
    assert [component.role for component in details.components] == [
        "runner",
        "storage",
        "image_registry",
    ]
    assert details.components[0].backend == "vertex"
    assert details.components[0].details == (("location", "us-central1"),)
    assert details.components[1].details == (("location", "gs://bucket/kitaru"),)
    assert details.components[2].details == (
        ("location", "us-central1-docker.pkg.dev/demo/repo"),
    )


def test_show_stack_operation_returns_sagemaker_stack_details() -> None:
    """stack show should classify SageMaker stacks and expose role metadata."""
    stack_summary = _stack_model(stack_id="stack-sm-id", name="my-sagemaker")
    hydrated_stack = _sagemaker_stack_model(
        stack_id="stack-sm-id",
        name="my-sagemaker",
    )
    client_mock = Mock()
    client_mock.active_stack_model = hydrated_stack
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("my-sagemaker")

    assert details.stack_type == "sagemaker"
    assert details.is_managed is True
    assert [component.role for component in details.components] == [
        "runner",
        "storage",
        "image_registry",
    ]
    assert details.components[0].backend == "sagemaker"
    assert details.components[0].details == (
        ("region", "us-east-1"),
        ("execution_role", "arn:aws:iam::123456789012:role/SageMakerRole"),
    )
    assert details.components[1].details == (("location", "s3://bucket/kitaru"),)
    assert details.components[2].details == (
        ("location", "123456789012.dkr.ecr.us-east-1.amazonaws.com"),
    )


def test_show_stack_operation_returns_azureml_stack_details() -> None:
    """stack show should classify AzureML stacks and expose Azure metadata."""
    stack_summary = _stack_model(stack_id="stack-azure-id", name="my-azureml")
    hydrated_stack = _azureml_stack_model(
        stack_id="stack-azure-id",
        name="my-azureml",
    )
    client_mock = Mock()
    client_mock.active_stack_model = hydrated_stack
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("my-azureml")

    assert details.stack_type == "azureml"
    assert details.is_managed is True
    assert [component.role for component in details.components] == [
        "runner",
        "storage",
        "image_registry",
    ]
    assert details.components[0].backend == "azureml"
    assert details.components[0].details == (
        ("subscription_id", "00000000-0000-0000-0000-000000000123"),
        ("resource_group", "rg-demo"),
        ("workspace", "ws-demo"),
        ("location", "westeurope"),
    )
    assert details.components[1].details == (("location", "az://container/kitaru"),)
    assert details.components[2].details == (
        ("location", "demo.azurecr.io/team/image"),
    )


def test_show_stack_operation_classifies_custom_stacks_and_additional_components() -> (
    None
):
    """Non-local/Kubernetes stacks should fall back to custom details output."""
    stack_summary = _stack_model(stack_id="stack-custom-id", name="custom")
    hydrated_stack = _stack_model(
        stack_id="stack-custom-id",
        name="custom",
        components={
            StackComponentType.ORCHESTRATOR: [
                _stack_component("orc-custom-id", "custom-runner", flavor="airflow")
            ],
            StackComponentType.EXPERIMENT_TRACKER: [
                _stack_component("exp-custom-id", "mlflow", flavor="mlflow")
            ],
        },
    )
    client_mock = Mock()
    client_mock.active_stack_model = stack_summary
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("custom")

    assert details.stack_type == "custom"
    assert [component.role for component in details.components] == [
        "runner",
        "additional_component",
    ]
    assert details.components[1].purpose == "experiment_tracker"


def test_show_stack_operation_preserves_unknown_component_types() -> None:
    """stack show should keep unknown component types as additional components."""
    stack_summary = _stack_model(stack_id="stack-future-id", name="future")
    hydrated_stack = _stack_model(
        stack_id="stack-future-id",
        name="future",
        components={
            "future_component": [
                _stack_component("future-id", "future-addon", flavor="mystery")
            ]
        },
    )
    client_mock = Mock()
    client_mock.active_stack_model = stack_summary
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        details = _show_stack_operation("future")

    assert details.stack_type == "custom"
    assert len(details.components) == 1
    assert details.components[0].role == "additional_component"
    assert details.components[0].purpose == "future_component"


def test_show_stack_operation_raises_when_stack_is_missing() -> None:
    """stack show should report missing stacks with the selector in the error."""
    client_mock = Mock()
    client_mock.active_stack_model = _stack_model(
        stack_id="stack-default-id", name="default"
    )
    client_mock.list_stacks.return_value = []

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(KitaruStateError, match=r"Stack 'ghost' not found\."),
    ):
        _show_stack_operation("ghost")


def test_show_stack_operation_wraps_hydration_failures() -> None:
    """stack show should wrap backend hydration errors with stack context."""
    stack_summary = _stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = stack_summary
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.side_effect = RuntimeError("backend unavailable")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruBackendError,
            match="Unable to inspect stack 'dev': backend unavailable",
        ),
    ):
        _show_stack_operation("dev")


def test_show_stack_operation_rejects_malformed_component_metadata() -> None:
    """stack show should fail if the runtime returns malformed component data."""
    stack_summary = _stack_model(stack_id="stack-dev-id", name="dev")
    hydrated_stack = SimpleNamespace(
        id="stack-dev-id",
        name="dev",
        labels={},
        components=None,
    )
    client_mock = Mock()
    client_mock.active_stack_model = stack_summary
    client_mock.list_stacks.return_value = [stack_summary]
    client_mock.get_stack.return_value = hydrated_stack

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruStateError,
            match=r"Stack 'dev' returned malformed component metadata\.",
        ),
    ):
        _show_stack_operation("dev")


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
            KitaruUsageError,
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
            remote_spec=spec,
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


def test_create_stack_dispatcher_forwards_component_overrides() -> None:
    """Dispatcher should forward advanced component overrides to stack helpers."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789.dkr.ecr.eu-west-1.amazonaws.com/my-repo",
        cluster="demo-cluster",
        region="eu-west-1",
    )
    overrides = StackComponentConfigOverrides(
        orchestrator={"synchronous": False},
        container_registry={"default_repository": "team-ml"},
    )
    expected_result = SimpleNamespace(name="kubernetes-result")

    with patch(
        "kitaru.config._create_kubernetes_stack_operation",
        return_value=expected_result,
    ) as mock_create_kubernetes:
        result = _create_stack_operation(
            "dev",
            stack_type=StackType.KUBERNETES,
            remote_spec=spec,
            component_overrides=overrides,
        )

    mock_create_kubernetes.assert_called_once_with(
        "dev",
        spec=spec,
        activate=True,
        labels=None,
        component_overrides=overrides,
    )
    assert result is expected_result


def test_create_stack_dispatcher_routes_vertex_requests() -> None:
    """Dispatcher should pass Vertex requests through to the Vertex helper."""
    spec = VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="us-docker.pkg.dev/demo-project/demo-repo",
        region="us-central1",
        credentials="gcp-service-account:/tmp/demo.json",
        verify=False,
    )
    expected_result = SimpleNamespace(name="vertex-result")

    with patch(
        "kitaru.config._create_vertex_stack_operation",
        return_value=expected_result,
    ) as mock_create_vertex:
        result = _create_stack_operation(
            "vertex-dev",
            stack_type=StackType.VERTEX,
            remote_spec=spec,
            activate=False,
            labels={"owner": "ml"},
        )

    mock_create_vertex.assert_called_once_with(
        "vertex-dev",
        spec=spec,
        activate=False,
        labels={"owner": "ml"},
    )
    assert result is expected_result


def test_create_stack_dispatcher_routes_sagemaker_requests() -> None:
    """Dispatcher should pass SageMaker requests through to the SageMaker helper."""
    spec = SagemakerStackSpec(
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
        region="us-east-1",
        execution_role="arn:aws:iam::123456789012:role/SageMakerRole",
        credentials="aws-profile:team-ml",
        verify=False,
    )
    expected_result = SimpleNamespace(name="sagemaker-result")

    with patch(
        "kitaru.config._create_sagemaker_stack_operation",
        return_value=expected_result,
    ) as mock_create_sagemaker:
        result = _create_stack_operation(
            "sagemaker-dev",
            stack_type=StackType.SAGEMAKER,
            remote_spec=spec,
            activate=False,
            labels={"owner": "ml"},
        )

    mock_create_sagemaker.assert_called_once_with(
        "sagemaker-dev",
        spec=spec,
        activate=False,
        labels={"owner": "ml"},
    )
    assert result is expected_result


def test_create_stack_dispatcher_routes_azureml_requests() -> None:
    """Dispatcher should pass AzureML requests through to the Azure helper."""
    spec = AzureMLStackSpec(
        artifact_store="az://container/path",
        container_registry="demo.azurecr.io/team/image",
        subscription_id="00000000-0000-0000-0000-000000000123",
        resource_group="rg-demo",
        workspace="ws-demo",
        region="westeurope",
        credentials="implicit",
        verify=False,
    )
    expected_result = SimpleNamespace(name="azure-result")

    with patch(
        "kitaru.config._create_azureml_stack_operation",
        return_value=expected_result,
    ) as mock_create_azureml:
        result = _create_stack_operation(
            "azure-dev",
            stack_type=StackType.AZUREML,
            remote_spec=spec,
            activate=False,
            labels={"owner": "ml"},
        )

    mock_create_azureml.assert_called_once_with(
        "azure-dev",
        spec=spec,
        activate=False,
        labels={"owner": "ml"},
    )
    assert result is expected_result


def test_create_stack_dispatcher_rejects_unsupported_stack_type() -> None:
    """Dispatcher should reject unknown stack types instead of guessing."""
    with (
        patch("kitaru.config.Client") as mock_client,
        pytest.raises(KitaruUsageError, match="Unsupported stack type: weird"),
    ):
        _create_stack_operation(
            "dev",
            stack_type=cast(Any, "weird"),
        )

    mock_client.assert_not_called()


def test_create_kubernetes_stack_operation_creates_aws_stack_and_activates() -> None:
    """Kubernetes create should build a one-shot AWS stack request and activate it."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        cluster="demo-cluster",
        region="eu-west-1",
        namespace="ml",
        credentials="aws-access-keys:AKIA123:secret456",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _kubernetes_stack_model(
        stack_id="stack-dev-id",
        name="dev",
        connector_name=None,
        orchestrator_name="dev-orchestrator-4x9z",
        artifact_store_name="dev-artifacts-4x9z",
        container_registry_name="dev-registry-4x9z",
    )
    hydrated_stack = _kubernetes_stack_model(
        stack_id="stack-dev-id",
        name="dev",
        connector_name="dev-aws-4x9z",
        orchestrator_name="dev-orchestrator-4x9z",
        artifact_store_name="dev-artifacts-4x9z",
        container_registry_name="dev-registry-4x9z",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_kubernetes_stack_operation(
            "dev",
            spec=spec,
            labels={"owner": "ml"},
        )

    client_mock.create_service_connector.assert_called_once_with(
        name="dev",
        connector_type="aws",
        resource_type="aws-generic",
        auth_method="secret-key",
        configuration={
            "region": "eu-west-1",
            "aws_access_key_id": "AKIA123",
            "aws_secret_access_key": "secret456",
        },
        verify=True,
        list_resources=False,
        register=False,
    )
    stack_request = client_mock._validate_stack_configuration.call_args.args[0]
    assert stack_request.name == "dev"
    assert stack_request.labels == {"owner": "ml", "kitaru.managed": "true"}
    assert len(stack_request.service_connectors) == 1
    connector_info = stack_request.service_connectors[0]
    assert connector_info.type == "aws"
    assert connector_info.auth_method == "secret-key"
    assert connector_info.configuration == {
        "region": "eu-west-1",
        "aws_access_key_id": "AKIA123",
        "aws_secret_access_key": "secret456",
    }

    orchestrator = stack_request.components[StackComponentType.ORCHESTRATOR][0]
    assert orchestrator.flavor == "kubernetes"
    assert orchestrator.configuration == {
        "kubernetes_namespace": "ml",
    }
    assert orchestrator.service_connector_index == 0
    assert orchestrator.service_connector_resource_id == "demo-cluster"

    artifact_store = stack_request.components[StackComponentType.ARTIFACT_STORE][0]
    assert artifact_store.flavor == "s3"
    assert artifact_store.configuration == {"path": "s3://bucket/path"}
    assert artifact_store.service_connector_index == 0
    assert artifact_store.service_connector_resource_id == "s3://bucket"

    container_registry = stack_request.components[
        StackComponentType.CONTAINER_REGISTRY
    ][0]
    assert container_registry.flavor == "aws"
    assert container_registry.configuration == {
        "uri": "123456789012.dkr.ecr.eu-west-1.amazonaws.com"
    }
    assert container_registry.service_connector_index == 0
    assert (
        container_registry.service_connector_resource_id
        == "123456789012.dkr.ecr.eu-west-1.amazonaws.com"
    )

    client_mock.zen_store.create_stack.assert_called_once_with(stack=stack_request)
    client_mock.get_stack.assert_called_once_with("stack-dev-id", hydrate=True)
    client_mock.activate_stack.assert_called_once_with("stack-dev-id")
    assert result.stack.name == "dev"
    assert result.stack.is_active is True
    assert result.previous_active_stack == "default"
    assert result.components_created == (
        "dev-orchestrator-4x9z (orchestrator)",
        "dev-artifacts-4x9z (artifact_store)",
        "dev-registry-4x9z (container_registry)",
    )
    assert result.stack_type == "kubernetes"
    assert result.service_connectors_created == ("dev-aws-4x9z",)
    assert result.resources == {
        "provider": "aws",
        "cluster": "demo-cluster",
        "region": "eu-west-1",
        "namespace": "ml",
        "artifact_store": "s3://bucket/path",
        "container_registry": "123456789012.dkr.ecr.eu-west-1.amazonaws.com",
    }


def test_create_kubernetes_stack_operation_creates_gcp_stack_without_verification(
    tmp_path: Path,
) -> None:
    """GCP Kubernetes create should read service-account JSON and honor verify=False."""
    service_account_path = tmp_path / "gcp-service-account.json"
    service_account_json = json.dumps(
        {
            "type": "service_account",
            "project_id": "demo-project",
            "private_key_id": "key-id",
            "private_key": (
                "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n"
            ),
            "client_email": "demo@demo-project.iam.gserviceaccount.com",
            "client_id": "1234567890",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/demo",
        }
    )
    service_account_path.write_text(service_account_json, encoding="utf-8")
    spec = KubernetesStackSpec(
        provider=CloudProvider.GCP,
        artifact_store="gs://bucket/path",
        container_registry="europe-west4-docker.pkg.dev/demo-project/demo-repo",
        cluster="demo-gke",
        region="europe-west4",
        credentials=f"gcp-service-account:{service_account_path}",
        verify=False,
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _kubernetes_stack_model(
        stack_id="stack-dev-id",
        name="dev",
        connector_name="dev-gcp",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_kubernetes_stack_operation(
            "dev",
            spec=spec,
            activate=False,
        )

    client_mock.create_service_connector.assert_called_once_with(
        name="dev",
        connector_type="gcp",
        resource_type="gcp-generic",
        auth_method="service-account",
        configuration={
            "project_id": "demo-project",
            "service_account_json": service_account_json,
        },
        verify=False,
        list_resources=False,
        register=False,
    )
    stack_request = client_mock._validate_stack_configuration.call_args.args[0]
    orchestrator = stack_request.components[StackComponentType.ORCHESTRATOR][0]
    assert orchestrator.configuration == {
        "kubernetes_namespace": "default",
    }
    artifact_store = stack_request.components[StackComponentType.ARTIFACT_STORE][0]
    assert artifact_store.flavor == "gcp"
    assert artifact_store.service_connector_resource_id == "gs://bucket"
    container_registry = stack_request.components[
        StackComponentType.CONTAINER_REGISTRY
    ][0]
    assert container_registry.flavor == "gcp"
    assert (
        container_registry.service_connector_resource_id
        == "europe-west4-docker.pkg.dev/demo-project/demo-repo"
    )
    client_mock.activate_stack.assert_not_called()
    assert result.stack.is_active is False
    assert result.previous_active_stack is None
    assert result.service_connectors_created == ("dev-gcp",)
    assert result.resources == {
        "provider": "gcp",
        "cluster": "demo-gke",
        "region": "europe-west4",
        "namespace": "default",
        "artifact_store": "gs://bucket/path",
        "container_registry": "europe-west4-docker.pkg.dev/demo-project/demo-repo",
    }


def test_create_vertex_stack_operation_creates_gcp_stack_and_activates(
    tmp_path: Path,
) -> None:
    """Vertex create should build a one-shot GCP stack request and activate it."""
    service_account_path = tmp_path / "vertex-service-account.json"
    service_account_json = json.dumps(
        {
            "type": "service_account",
            "project_id": "demo-project",
            "private_key_id": "key-id",
            "private_key": (
                "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n"
            ),
            "client_email": "demo@demo-project.iam.gserviceaccount.com",
        }
    )
    service_account_path.write_text(service_account_json, encoding="utf-8")
    spec = VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="us-central1-docker.pkg.dev/demo-project/demo-repo",
        region="us-central1",
        credentials=f"gcp-service-account:{service_account_path}",
        verify=False,
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _vertex_stack_model(
        stack_id="stack-vertex-id",
        name="vertex-dev",
        connector_name=None,
        orchestrator_name="vertex-dev-orchestrator",
        artifact_store_name="vertex-dev-artifacts",
        container_registry_name="vertex-dev-registry",
    )
    hydrated_stack = _vertex_stack_model(
        stack_id="stack-vertex-id",
        name="vertex-dev",
        connector_name="vertex-dev-gcp",
        orchestrator_name="vertex-dev-orchestrator",
        artifact_store_name="vertex-dev-artifacts",
        container_registry_name="vertex-dev-registry",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_vertex_stack_operation("vertex-dev", spec=spec)

    client_mock.create_service_connector.assert_called_once_with(
        name="vertex-dev",
        connector_type="gcp",
        resource_type="gcp-generic",
        auth_method="service-account",
        configuration={
            "project_id": "demo-project",
            "service_account_json": service_account_json,
        },
        verify=False,
        list_resources=False,
        register=False,
    )
    stack_request = client_mock._validate_stack_configuration.call_args.args[0]
    assert stack_request.name == "vertex-dev"
    assert stack_request.labels == {
        "kitaru.managed": "true",
    }
    connector_info = stack_request.service_connectors[0]
    assert connector_info.type == "gcp"
    assert connector_info.auth_method == "service-account"
    assert connector_info.configuration == {
        "project_id": "demo-project",
        "service_account_json": service_account_json,
    }

    orchestrator = stack_request.components[StackComponentType.ORCHESTRATOR][0]
    assert orchestrator.flavor == "vertex"
    assert orchestrator.configuration == {"location": "us-central1"}
    assert orchestrator.service_connector_index == 0
    assert getattr(orchestrator, "service_connector_resource_id", None) is None

    artifact_store = stack_request.components[StackComponentType.ARTIFACT_STORE][0]
    assert artifact_store.flavor == "gcp"
    assert artifact_store.configuration == {"path": "gs://bucket/path"}
    assert artifact_store.service_connector_index == 0
    assert artifact_store.service_connector_resource_id == "gs://bucket"

    container_registry = stack_request.components[
        StackComponentType.CONTAINER_REGISTRY
    ][0]
    assert container_registry.flavor == "gcp"
    assert container_registry.configuration == {
        "uri": "us-central1-docker.pkg.dev/demo-project/demo-repo"
    }
    assert container_registry.service_connector_index == 0
    assert (
        container_registry.service_connector_resource_id
        == "us-central1-docker.pkg.dev/demo-project/demo-repo"
    )

    client_mock.zen_store.create_stack.assert_called_once_with(stack=stack_request)
    client_mock.get_stack.assert_called_once_with("stack-vertex-id", hydrate=True)
    client_mock.activate_stack.assert_called_once_with("stack-vertex-id")
    assert result.stack.name == "vertex-dev"
    assert result.stack.is_active is True
    assert result.previous_active_stack == "default"
    assert result.components_created == (
        "vertex-dev-orchestrator (orchestrator)",
        "vertex-dev-artifacts (artifact_store)",
        "vertex-dev-registry (container_registry)",
    )
    assert result.stack_type == "vertex"
    assert result.service_connectors_created == ("vertex-dev-gcp",)
    assert result.resources == {
        "provider": "gcp",
        "region": "us-central1",
        "artifact_store": "gs://bucket/path",
        "container_registry": "us-central1-docker.pkg.dev/demo-project/demo-repo",
    }


def test_create_vertex_stack_operation_merges_component_overrides() -> None:
    """Vertex stack creation should merge advanced component overrides."""
    spec = VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="us-central1-docker.pkg.dev/demo-project/demo-repo",
        region="us-central1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _vertex_stack_model(
        stack_id="stack-vertex-id",
        name="vertex-dev",
        connector_name="vertex-dev-gcp",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    overrides = StackComponentConfigOverrides(
        orchestrator={
            "synchronous": False,
            "pipeline_root": "gs://bucket/root",
        },
        container_registry={"default_repository": "team-ml"},
    )

    with patch("kitaru.config.Client", return_value=client_mock):
        _create_vertex_stack_operation(
            "vertex-dev",
            spec=spec,
            activate=False,
            component_overrides=overrides,
        )

    stack_request = client_mock._validate_stack_configuration.call_args.args[0]
    orchestrator = stack_request.components[StackComponentType.ORCHESTRATOR][0]
    assert orchestrator.configuration == {
        "location": "us-central1",
        "synchronous": False,
        "pipeline_root": "gs://bucket/root",
    }
    container_registry = stack_request.components[
        StackComponentType.CONTAINER_REGISTRY
    ][0]
    assert container_registry.configuration == {
        "uri": "us-central1-docker.pkg.dev/demo-project/demo-repo",
        "default_repository": "team-ml",
    }


def test_create_vertex_stack_operation_rewrites_invalid_component_option() -> None:
    """Invalid advanced component options should surface as KitaruUsageError."""
    spec = VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="us-central1-docker.pkg.dev/demo-project/demo-repo",
        region="us-central1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    invalid_key = "syn" + "cronous"
    overrides = StackComponentConfigOverrides(orchestrator={invalid_key: False})

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruUsageError,
            match=r"Invalid ZenML option for orchestrator",
        ) as exc_info,
    ):
        _create_vertex_stack_operation(
            "vertex-dev",
            spec=spec,
            component_overrides=overrides,
        )

    assert "Did you mean `synchronous`?" in str(exc_info.value)
    assert "orchestrators/vertex" in str(exc_info.value)
    client_mock.create_service_connector.assert_not_called()


def test_create_sagemaker_stack_operation_creates_aws_stack_and_activates() -> None:
    """SageMaker create should build a one-shot AWS stack request and activate it."""
    spec = SagemakerStackSpec(
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
        region="us-east-1",
        execution_role="arn:aws:iam::123456789012:role/SageMakerRole",
        credentials="aws-profile:ml-team",
        verify=False,
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _sagemaker_stack_model(
        stack_id="stack-sm-id",
        name="sagemaker-dev",
        connector_name=None,
        orchestrator_name="sagemaker-dev-orchestrator",
        artifact_store_name="sagemaker-dev-artifacts",
        container_registry_name="sagemaker-dev-registry",
    )
    hydrated_stack = _sagemaker_stack_model(
        stack_id="stack-sm-id",
        name="sagemaker-dev",
        connector_name="sagemaker-dev-aws",
        orchestrator_name="sagemaker-dev-orchestrator",
        artifact_store_name="sagemaker-dev-artifacts",
        container_registry_name="sagemaker-dev-registry",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.get_stack.return_value = hydrated_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_sagemaker_stack_operation("sagemaker-dev", spec=spec)

    client_mock.create_service_connector.assert_called_once_with(
        name="sagemaker-dev",
        connector_type="aws",
        resource_type="aws-generic",
        auth_method="implicit",
        configuration={
            "region": "us-east-1",
            "profile_name": "ml-team",
        },
        verify=False,
        list_resources=False,
        register=False,
    )
    stack_request = client_mock._validate_stack_configuration.call_args.args[0]
    assert stack_request.name == "sagemaker-dev"
    assert stack_request.labels == {"kitaru.managed": "true"}
    connector_info = stack_request.service_connectors[0]
    assert connector_info.type == "aws"
    assert connector_info.auth_method == "implicit"
    assert connector_info.configuration == {
        "region": "us-east-1",
        "profile_name": "ml-team",
    }

    orchestrator = stack_request.components[StackComponentType.ORCHESTRATOR][0]
    assert orchestrator.flavor == "sagemaker"
    assert orchestrator.configuration == {
        "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole"
    }
    assert orchestrator.service_connector_index == 0
    assert getattr(orchestrator, "service_connector_resource_id", None) is None

    artifact_store = stack_request.components[StackComponentType.ARTIFACT_STORE][0]
    assert artifact_store.flavor == "s3"
    assert artifact_store.configuration == {"path": "s3://bucket/path"}
    assert artifact_store.service_connector_index == 0
    assert artifact_store.service_connector_resource_id == "s3://bucket"

    container_registry = stack_request.components[
        StackComponentType.CONTAINER_REGISTRY
    ][0]
    assert container_registry.flavor == "aws"
    assert container_registry.configuration == {
        "uri": "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    }
    assert container_registry.service_connector_index == 0
    assert (
        container_registry.service_connector_resource_id
        == "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    )

    client_mock.zen_store.create_stack.assert_called_once_with(stack=stack_request)
    client_mock.get_stack.assert_called_once_with("stack-sm-id", hydrate=True)
    client_mock.activate_stack.assert_called_once_with("stack-sm-id")
    assert result.stack.name == "sagemaker-dev"
    assert result.stack.is_active is True
    assert result.previous_active_stack == "default"
    assert result.components_created == (
        "sagemaker-dev-orchestrator (orchestrator)",
        "sagemaker-dev-artifacts (artifact_store)",
        "sagemaker-dev-registry (container_registry)",
    )
    assert result.stack_type == "sagemaker"
    assert result.service_connectors_created == ("sagemaker-dev-aws",)
    assert result.resources == {
        "provider": "aws",
        "region": "us-east-1",
        "artifact_store": "s3://bucket/path",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
    }


def test_create_azureml_stack_operation_creates_stack_and_skips_activation() -> None:
    """AzureML create should build a one-shot Azure stack request cleanly."""
    spec = AzureMLStackSpec(
        artifact_store="abfss://container@demo.dfs.core.windows.net/kitaru/path",
        container_registry="demo.azurecr.io/team/image",
        subscription_id="00000000-0000-0000-0000-000000000123",
        resource_group="rg-demo",
        workspace="ws-demo",
        region="westeurope",
        credentials=(
            "azure-service-principal:"
            "11111111-1111-1111-1111-111111111111:"
            "22222222-2222-2222-2222-222222222222:"
            "super-secret"
        ),
        verify=False,
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _azureml_stack_model(
        stack_id="stack-azure-id",
        name="azure-dev",
        connector_name="azure-dev-connector",
        orchestrator_name="azure-dev-orchestrator",
        artifact_store_name="azure-dev-artifacts",
        container_registry_name="azure-dev-registry",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_azureml_stack_operation(
            "azure-dev",
            spec=spec,
            activate=False,
        )

    client_mock.create_service_connector.assert_called_once_with(
        name="azure-dev",
        connector_type="azure",
        resource_type="azure-generic",
        auth_method="service-principal",
        configuration={
            "subscription_id": "00000000-0000-0000-0000-000000000123",
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "client_id": "22222222-2222-2222-2222-222222222222",
            "client_secret": "super-secret",
        },
        verify=False,
        list_resources=False,
        register=False,
    )
    stack_request = client_mock._validate_stack_configuration.call_args.args[0]
    assert stack_request.name == "azure-dev"
    assert stack_request.labels == {"kitaru.managed": "true"}
    connector_info = stack_request.service_connectors[0]
    assert connector_info.type == "azure"
    assert connector_info.auth_method == "service-principal"
    assert connector_info.configuration == {
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "tenant_id": "11111111-1111-1111-1111-111111111111",
        "client_id": "22222222-2222-2222-2222-222222222222",
        "client_secret": "super-secret",
    }

    orchestrator = stack_request.components[StackComponentType.ORCHESTRATOR][0]
    assert orchestrator.flavor == "azureml"
    assert orchestrator.configuration == {
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "resource_group": "rg-demo",
        "workspace": "ws-demo",
        "location": "westeurope",
    }
    assert orchestrator.service_connector_index == 0

    artifact_store = stack_request.components[StackComponentType.ARTIFACT_STORE][0]
    assert artifact_store.flavor == "azure"
    assert artifact_store.configuration == {
        "path": "abfs://container@demo.dfs.core.windows.net/kitaru/path"
    }
    assert artifact_store.service_connector_index == 0
    assert (
        artifact_store.service_connector_resource_id
        == "abfs://container@demo.dfs.core.windows.net"
    )

    container_registry = stack_request.components[
        StackComponentType.CONTAINER_REGISTRY
    ][0]
    assert container_registry.flavor == "azure"
    assert container_registry.configuration == {"uri": "demo.azurecr.io/team/image"}
    assert container_registry.service_connector_index == 0
    assert container_registry.service_connector_resource_id == "demo.azurecr.io"

    client_mock.zen_store.create_stack.assert_called_once_with(stack=stack_request)
    client_mock.activate_stack.assert_not_called()
    assert result.stack.name == "azure-dev"
    assert result.stack.is_active is False
    assert result.previous_active_stack is None
    assert result.components_created == (
        "azure-dev-orchestrator (orchestrator)",
        "azure-dev-artifacts (artifact_store)",
        "azure-dev-registry (container_registry)",
    )
    assert result.stack_type == "azureml"
    assert result.service_connectors_created == ("azure-dev-connector",)
    assert result.resources == {
        "provider": "azure",
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "resource_group": "rg-demo",
        "workspace": "ws-demo",
        "artifact_store": "abfss://container@demo.dfs.core.windows.net/kitaru/path",
        "container_registry": "demo.azurecr.io/team/image",
        "region": "westeurope",
    }


@pytest.mark.parametrize(
    ("credentials", "match"),
    [
        (
            "azure-service-principal:tenant:client",
            r"azure-service-principal credentials must be in the format",
        ),
        ("azure-access-token:", r"Azure access token cannot be empty\."),
        ("azure-something:nope", r"Unsupported Azure credentials method"),
    ],
)
def test_create_azureml_stack_operation_rejects_invalid_credentials(
    credentials: str, match: str
) -> None:
    """AzureML credential parsing should fail before backend connector calls."""
    spec = AzureMLStackSpec(
        artifact_store="az://container/path",
        container_registry="demo.azurecr.io/team/image",
        subscription_id="00000000-0000-0000-0000-000000000123",
        resource_group="rg-demo",
        workspace="ws-demo",
        credentials=credentials,
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(KitaruUsageError, match=match),
    ):
        _create_azureml_stack_operation("azure-dev", spec=spec)

    client_mock.create_service_connector.assert_not_called()
    client_mock._validate_stack_configuration.assert_not_called()


def test_azure_resource_id_helpers_normalize_storage_and_registry_uris() -> None:
    """Azure resource IDs should normalize to stable connector resource roots."""
    assert (
        config_module._artifact_store_resource_id(
            "az://container/path/to/artifacts",
            CloudProvider.AZURE,
        )
        == "az://container"
    )
    assert (
        config_module._artifact_store_resource_id(
            "abfs://container@demo.dfs.core.windows.net/path/to/artifacts",
            CloudProvider.AZURE,
        )
        == "abfs://container@demo.dfs.core.windows.net"
    )
    assert (
        config_module._artifact_store_resource_id(
            "abfss://container@demo.dfs.core.windows.net/path/to/artifacts",
            CloudProvider.AZURE,
        )
        == "abfss://container@demo.dfs.core.windows.net"
    )
    assert (
        config_module._container_registry_resource_id(
            "demo.azurecr.io/team/image",
            CloudProvider.AZURE,
        )
        == "demo.azurecr.io"
    )


def test_create_vertex_stack_operation_rejects_non_gs_artifact_store() -> None:
    """Vertex create should fail before backend calls when the bucket URI is wrong."""
    spec = VertexStackSpec(
        artifact_store="s3://bucket/path",
        container_registry="us-central1-docker.pkg.dev/demo-project/demo-repo",
        region="us-central1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruUsageError,
            match=(
                r"Unsupported artifact store URI 's3://bucket/path' "
                r"for provider 'gcp'\."
            ),
        ),
    ):
        _create_vertex_stack_operation("vertex-dev", spec=spec)

    client_mock.create_service_connector.assert_not_called()
    client_mock._validate_stack_configuration.assert_not_called()


def test_create_vertex_stack_operation_rejects_unparsable_gcp_registry() -> None:
    """Vertex create should fail early if the GCP project ID cannot be inferred."""
    spec = VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="registry.example.com/demo",
        region="us-central1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruUsageError,
            match=r"Cannot infer GCP project ID from container registry URI",
        ),
    ):
        _create_vertex_stack_operation("vertex-dev", spec=spec)

    client_mock.create_service_connector.assert_not_called()
    client_mock._validate_stack_configuration.assert_not_called()


def test_create_vertex_stack_operation_reports_activation_failure() -> None:
    """Activation failures should keep the created Vertex stack and guide recovery."""
    spec = VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="us-central1-docker.pkg.dev/demo-project/demo-repo",
        region="us-central1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _vertex_stack_model(
        stack_id="stack-vertex-id",
        name="vertex-dev",
        connector_name="vertex-dev-gcp",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.activate_stack.side_effect = RuntimeError("cannot switch")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruBackendError,
            match=r"Created Vertex stack 'vertex-dev' but failed to activate it",
        ),
    ):
        _create_vertex_stack_operation("vertex-dev", spec=spec)

    client_mock.zen_store.create_stack.assert_called_once()


def test_create_sagemaker_stack_operation_reports_activation_failure() -> None:
    """Activation failures should keep the stack and guide manual recovery."""
    spec = SagemakerStackSpec(
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
        region="us-east-1",
        execution_role="arn:aws:iam::123456789012:role/SageMakerRole",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _sagemaker_stack_model(
        stack_id="stack-sm-id",
        name="sagemaker-dev",
        connector_name="sagemaker-dev-aws",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.activate_stack.side_effect = RuntimeError("cannot switch")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruBackendError,
            match=(
                r"Created SageMaker stack 'sagemaker-dev' "
                r"but failed to activate it"
            ),
        ),
    ):
        _create_sagemaker_stack_operation("sagemaker-dev", spec=spec)

    client_mock.zen_store.create_stack.assert_called_once()


def test_create_kubernetes_stack_operation_tolerates_refetch_failure() -> None:
    """Metadata hydration should be best-effort after the stack is already created."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        cluster="demo-cluster",
        region="eu-west-1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _kubernetes_stack_model(
        stack_id="stack-dev-id",
        name="dev",
        connector_name=None,
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.get_stack.side_effect = RuntimeError("hydrate failed")

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _create_kubernetes_stack_operation(
            "dev",
            spec=spec,
            activate=False,
        )

    client_mock.get_stack.assert_called_once_with("stack-dev-id", hydrate=True)
    assert result.components_created == (
        "dev-orchestrator (orchestrator)",
        "dev-artifacts (artifact_store)",
        "dev-registry (container_registry)",
    )
    assert result.service_connectors_created == ()


def test_create_kubernetes_stack_operation_rejects_invalid_aws_credentials() -> None:
    """Malformed AWS credentials should fail before any connector or stack calls."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        cluster="demo-cluster",
        region="eu-west-1",
        credentials="aws-access-keys:missing-secret-only",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruUsageError,
            match=r"aws-access-keys credentials must be in the format",
        ),
    ):
        _create_kubernetes_stack_operation("dev", spec=spec)

    client_mock.create_service_connector.assert_not_called()
    client_mock._validate_stack_configuration.assert_not_called()


def test_create_kubernetes_stack_operation_rejects_empty_gcp_service_account_path() -> (
    None
):
    """An empty GCP service-account credential path should fail clearly."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.GCP,
        artifact_store="gs://bucket/path",
        container_registry="europe-west4-docker.pkg.dev/demo-project/demo-repo",
        cluster="demo-gke",
        region="europe-west4",
        credentials="gcp-service-account:",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruUsageError,
            match=r"GCP service account file path cannot be empty\.",
        ),
    ):
        _create_kubernetes_stack_operation("dev", spec=spec)

    client_mock.create_service_connector.assert_not_called()


def test_create_kubernetes_stack_operation_rejects_unparsable_gcp_registry() -> None:
    """GCP creation should fail early if the project ID cannot be inferred."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.GCP,
        artifact_store="gs://bucket/path",
        container_registry="registry.example.com/demo",
        cluster="demo-gke",
        region="europe-west4",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruUsageError,
            match=r"Cannot infer GCP project ID from container registry URI",
        ),
    ):
        _create_kubernetes_stack_operation("dev", spec=spec)

    client_mock.create_service_connector.assert_not_called()
    client_mock._validate_stack_configuration.assert_not_called()


def test_create_kubernetes_stack_operation_wraps_store_create_failure() -> None:
    """Store create failures should surface rollback guidance and skip activation."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        cluster="demo-cluster",
        region="eu-west-1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.side_effect = RuntimeError("boom")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruBackendError,
            match=(
                r"ZenML rolled back any partially created components and "
                r"service connectors"
            ),
        ),
    ):
        _create_kubernetes_stack_operation("dev", spec=spec)

    client_mock.activate_stack.assert_not_called()
    client_mock.delete_stack_component.assert_not_called()
    client_mock.delete_service_connector.assert_not_called()


def test_create_kubernetes_stack_operation_reports_activation_failure() -> None:
    """Activation failures should keep the created stack and guide manual recovery."""
    spec = KubernetesStackSpec(
        provider=CloudProvider.AWS,
        artifact_store="s3://bucket/path",
        container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        cluster="demo-cluster",
        region="eu-west-1",
    )
    default = _stack_model(stack_id="stack-default-id", name="default")
    created_stack = _kubernetes_stack_model(
        stack_id="stack-dev-id",
        name="dev",
        connector_name="dev-aws",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.list_stacks.return_value = _FakeStackPage(
        items=[default],
        total_pages=1,
        max_size=50,
    )
    client_mock.zen_store = Mock()
    client_mock.zen_store.create_stack.return_value = created_stack
    client_mock.activate_stack.side_effect = RuntimeError("cannot switch")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruBackendError,
            match=r"Created Kubernetes stack 'dev' but failed to activate it",
        ),
    ):
        _create_kubernetes_stack_operation("dev", spec=spec)

    client_mock.zen_store.create_stack.assert_called_once()
    client_mock.delete_stack_component.assert_not_called()
    client_mock.delete_service_connector.assert_not_called()


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


def test_create_local_stack_operation_applies_component_overrides() -> None:
    """Local stack creation should pass merged orchestrator/store overrides through."""
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
    overrides = StackComponentConfigOverrides(
        artifact_store={"path": "/tmp/kitaru-artifacts"},
    )

    with patch("kitaru.config.Client", return_value=client_mock):
        _create_stack_operation("dev", activate=False, component_overrides=overrides)

    orchestrator_call = client_mock.create_stack_component.call_args_list[0]
    assert orchestrator_call.kwargs["configuration"] == {}
    artifact_store_call = client_mock.create_stack_component.call_args_list[1]
    assert artifact_store_call.kwargs["configuration"] == {
        "path": "/tmp/kitaru-artifacts"
    }


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
        pytest.raises(KitaruStateError, match='A stack named "dev" already exists'),
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
        pytest.raises(KitaruStateError, match="never reuses existing ones"),
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
        pytest.raises(KitaruBackendError, match="stack create failed"),
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
    client_mock.list_service_connectors.assert_not_called()
    client_mock.delete_service_connector.assert_not_called()
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
    client_mock.list_service_connectors.assert_not_called()
    client_mock.delete_service_connector.assert_not_called()
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
    client_mock.list_service_connectors.assert_not_called()
    client_mock.delete_service_connector.assert_not_called()
    assert result.components_deleted == ()


def test_delete_stack_rejects_active_stack_without_force() -> None:
    """Delete should guard against removing the active stack by default."""
    active = _stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = active
    client_mock.get_stack.return_value = active

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(KitaruStateError, match="Cannot delete the active stack"),
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


def test_delete_stack_recursive_kubernetes_deletes_unshared_connector() -> None:
    """Recursive Kubernetes delete should include the registry.

    It should also clean up orphaned connectors.
    """
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.side_effect = [dev, default]
    client_mock.list_stacks.side_effect = [
        [dev],
        [dev],
        [dev],
        _FakeStackPage(items=[default], total_pages=1, max_size=50),
    ]
    client_mock.list_service_connectors.return_value = [
        SimpleNamespace(name="dev-connector")
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev", recursive=True)

    client_mock.delete_stack.assert_called_once_with("stack-dev-id", recursive=True)
    client_mock.list_service_connectors.assert_called_once_with(
        name="dev-connector",
        page=1,
        size=2,
        hydrate=True,
    )
    client_mock.delete_service_connector.assert_called_once_with("dev-connector")
    assert result.components_deleted == (
        "dev-orchestrator (orchestrator)",
        "dev-artifacts (artifact_store)",
        "dev-registry (container_registry)",
    )


def test_delete_stack_recursive_kubernetes_keeps_shared_connector() -> None:
    """Recursive Kubernetes delete should leave shared connectors alone."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    prod = _kubernetes_stack_model(
        stack_id="stack-prod-id",
        name="prod",
        connector_name="dev-connector",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.side_effect = [dev, default, prod]
    client_mock.list_stacks.side_effect = [
        [dev],
        [dev],
        [dev],
        _FakeStackPage(items=[default, prod], total_pages=1, max_size=50),
    ]
    client_mock.list_service_connectors.return_value = [
        SimpleNamespace(name="dev-connector")
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        _delete_stack_operation("dev", recursive=True)

    client_mock.delete_service_connector.assert_not_called()


def test_delete_stack_k8s_keeps_shared_connector_with_id_stack() -> None:
    """Shared connectors should be preserved with an ID-only remaining stack."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    prod = _kubernetes_stack_model(
        stack_id="stack-prod-id",
        name="prod",
        connector_name=None,
        connector_id="connector-dev-id",
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.side_effect = [dev, default, prod]
    client_mock.list_stacks.side_effect = [
        [dev],
        [dev],
        [dev],
        _FakeStackPage(items=[default, prod], total_pages=1, max_size=50),
    ]

    def _list_service_connectors(**kwargs: Any) -> list[SimpleNamespace]:
        if kwargs.get("name") == "dev-connector":
            return [SimpleNamespace(id="connector-dev-id", name="dev-connector")]
        if kwargs.get("id") == "connector-dev-id":
            return [SimpleNamespace(id="connector-dev-id", name="dev-connector")]
        return []

    client_mock.list_service_connectors.side_effect = _list_service_connectors

    with patch("kitaru.config.Client", return_value=client_mock):
        _delete_stack_operation("dev", recursive=True)

    client_mock.delete_service_connector.assert_not_called()


def test_delete_stack_k8s_skips_cleanup_on_incomplete_metadata() -> None:
    """Cleanup should stop when another stack has an unidentifiable connector."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    prod = SimpleNamespace(
        id="stack-prod-id",
        name="prod",
        labels={"kitaru.managed": "true"},
        components={
            StackComponentType.ORCHESTRATOR: [
                _stack_component(
                    "prod-orc-id",
                    "prod-orchestrator",
                    flavor="kubernetes",
                    connector=SimpleNamespace(configuration={"region": "us-east-1"}),
                )
            ]
        },
    )
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.side_effect = [dev, default, prod]
    client_mock.list_stacks.side_effect = [
        [dev],
        [dev],
        [dev],
        _FakeStackPage(items=[default, prod], total_pages=1, max_size=50),
    ]
    client_mock.list_service_connectors.return_value = [
        SimpleNamespace(id="connector-dev-id", name="dev-connector")
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        _delete_stack_operation("dev", recursive=True)

    client_mock.delete_service_connector.assert_not_called()


def test_delete_stack_recursive_kubernetes_skips_cleanup_on_scan_failure() -> None:
    """Connector cleanup should bail out when the remaining-stack scan is uncertain."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.return_value = dev
    client_mock.list_stacks.side_effect = [
        [dev],
        [dev],
        [dev],
        RuntimeError("scan failed"),
    ]
    client_mock.list_service_connectors.return_value = [
        SimpleNamespace(name="dev-connector")
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev", recursive=True)

    client_mock.delete_stack.assert_called_once_with("stack-dev-id", recursive=True)
    client_mock.delete_service_connector.assert_not_called()
    assert result.deleted_stack == "dev"


def test_delete_stack_recursive_kubernetes_skips_cleanup_on_lookup_failure() -> None:
    """Connector lookup errors after stack deletion should not bubble up."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.return_value = dev
    client_mock.list_stacks.side_effect = [[dev], [dev], [dev]]
    client_mock.list_service_connectors.side_effect = RuntimeError(
        "connector lookup failed"
    )

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev", recursive=True)

    client_mock.delete_stack.assert_called_once_with("stack-dev-id", recursive=True)
    client_mock.delete_service_connector.assert_not_called()
    assert result.deleted_stack == "dev"


def test_delete_stack_recursive_kubernetes_skips_cleanup_on_delete_failure() -> None:
    """A failed stack delete should not attempt any connector cleanup afterwards."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.return_value = dev
    client_mock.list_stacks.side_effect = [[dev], [dev], [dev]]
    client_mock.list_service_connectors.return_value = [
        SimpleNamespace(name="dev-connector")
    ]
    client_mock.delete_stack.side_effect = RuntimeError("delete failed")

    with (
        patch("kitaru.config.Client", return_value=client_mock),
        pytest.raises(
            KitaruBackendError, match="Failed to delete stack 'dev': delete failed"
        ),
    ):
        _delete_stack_operation("dev", recursive=True)

    client_mock.delete_service_connector.assert_not_called()


def test_delete_stack_recursive_kubernetes_does_not_report_shared_registry() -> None:
    """Shared container registries should be omitted from the delete summary."""
    default = _stack_model(stack_id="stack-default-id", name="default")
    dev = _kubernetes_stack_model(stack_id="stack-dev-id", name="dev")
    shared_stack = _stack_model(stack_id="stack-shared-id", name="shared")
    client_mock = Mock()
    client_mock.active_stack_model = default
    client_mock.get_stack.side_effect = [dev, default]
    client_mock.list_stacks.side_effect = [
        [dev],
        [dev],
        [dev, shared_stack],
        _FakeStackPage(items=[default], total_pages=1, max_size=50),
    ]
    client_mock.list_service_connectors.return_value = [
        SimpleNamespace(id="connector-dev-id", name="dev-connector")
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        result = _delete_stack_operation("dev", recursive=True)

    assert result.components_deleted == (
        "dev-orchestrator (orchestrator)",
        "dev-artifacts (artifact_store)",
    )


def test_use_stack_switches_active_stack() -> None:
    """use_stack should delegate activation and return the new active stack."""
    local_stack = SimpleNamespace(id="stack-local-id", name="local")
    prod_stack = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = SimpleNamespace(
        active_stack_model=local_stack,
        list_stacks=lambda: [local_stack, prod_stack],
    )

    def _activate_stack(_: str) -> None:
        client_mock.active_stack_model = prod_stack

    activate_stack = Mock(side_effect=_activate_stack)
    client_mock.activate_stack = activate_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        selected = use_stack("prod")

    activate_stack.assert_called_once_with("stack-prod-id")
    assert selected.name == "prod"
    assert selected.id == "stack-prod-id"
    assert selected.is_active is True


def test_use_stack_rejects_empty_selector() -> None:
    """use_stack should fail fast on empty stack names/IDs."""
    with pytest.raises(KitaruUsageError, match="cannot be empty"):
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
    """Kitaru's config file should live under the unified config dir."""
    path = _kitaru_config_path()
    assert path.parent.name == "kitaru-config"
    assert path.name == "kitaru.yaml"


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
    assert (custom_dir / "kitaru.yaml").exists()


def test_apply_env_translations_sets_zenml_config_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """KITARU_CONFIG_PATH should populate ZENML_CONFIG_PATH."""
    custom_dir = tmp_path / "custom-kitaru-home"
    monkeypatch.delenv("ZENML_CONFIG_PATH", raising=False)
    monkeypatch.setenv("KITARU_CONFIG_PATH", str(custom_dir))

    apply_env_translations()

    assert os.environ["ZENML_CONFIG_PATH"] == str(custom_dir)


def test_apply_env_translations_warns_on_config_path_conflict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """KITARU_CONFIG_PATH should win over a conflicting ZENML_CONFIG_PATH."""
    kitaru_dir = tmp_path / "kitaru-home"
    zenml_dir = tmp_path / "zenml-home"
    monkeypatch.setenv("KITARU_CONFIG_PATH", str(kitaru_dir))
    monkeypatch.setenv("ZENML_CONFIG_PATH", str(zenml_dir))

    with pytest.warns(UserWarning, match="KITARU_CONFIG_PATH"):
        apply_env_translations()

    assert os.environ["ZENML_CONFIG_PATH"] == str(kitaru_dir)


def test_apply_env_translations_defaults_zenml_config_path_to_kitaru_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ZENML_CONFIG_PATH defaults to kitaru's app dir."""
    monkeypatch.delenv("KITARU_CONFIG_PATH", raising=False)
    monkeypatch.delenv("ZENML_CONFIG_PATH", raising=False)

    apply_env_translations()

    import click

    expected = click.get_app_dir("kitaru")
    assert os.environ["ZENML_CONFIG_PATH"] == expected


def test_apply_env_translations_preserves_existing_zenml_config_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Existing ZENML_CONFIG_PATH (server subprocess) is not overwritten."""
    server_dir = tmp_path / "server-config"
    monkeypatch.delenv("KITARU_CONFIG_PATH", raising=False)
    monkeypatch.setenv("ZENML_CONFIG_PATH", str(server_dir))

    apply_env_translations()

    assert os.environ["ZENML_CONFIG_PATH"] == str(server_dir)


def test_kitaru_config_dir_follows_zenml_config_path_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Kitaru config dir follows ZENML_CONFIG_PATH fallback."""
    from kitaru.config import _kitaru_config_dir

    server_dir = tmp_path / "server-config"
    monkeypatch.delenv("KITARU_CONFIG_PATH", raising=False)
    monkeypatch.setenv("ZENML_CONFIG_PATH", str(server_dir))

    assert _kitaru_config_dir() == server_dir


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

    with pytest.raises(KitaruUsageError, match=KITARU_CACHE_ENV):
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


def test_suppress_zenml_cli_messages_silences_cli_helpers_and_restores_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI suppression should silence ZenML helpers and restore prior state."""
    original_disable = logging.root.manager.disable
    helper_calls: list[str] = []

    def declare_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        helper_calls.append("declare")

    def success_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        helper_calls.append("success")

    monkeypatch.setattr(config_module.zenml_cli_utils, "declare", declare_spy)
    monkeypatch.setattr(config_module.zenml_cli_utils, "success", success_spy)

    try:
        with config_module._suppress_zenml_cli_messages():
            config_module.zenml_cli_utils.declare("hello")
            config_module.zenml_cli_utils.success("world")

            assert helper_calls == []
            assert logging.root.manager.disable == logging.CRITICAL

        assert config_module.zenml_cli_utils.declare is declare_spy
        assert config_module.zenml_cli_utils.success is success_spy
        assert logging.root.manager.disable == original_disable
    finally:
        logging.disable(original_disable)


class _SuppressionSentinelError(RuntimeError):
    """Sentinel error used to verify suppression cleanup on failure."""


def test_suppress_zenml_cli_messages_restores_state_after_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI suppression should restore helpers and logging after exceptions."""
    original_disable = logging.root.manager.disable
    logging.disable(logging.ERROR)

    def declare_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    def success_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    monkeypatch.setattr(config_module.zenml_cli_utils, "declare", declare_spy)
    monkeypatch.setattr(config_module.zenml_cli_utils, "success", success_spy)

    try:
        with (
            pytest.raises(_SuppressionSentinelError, match="boom"),
            config_module._suppress_zenml_cli_messages(),
        ):
            assert logging.root.manager.disable == logging.CRITICAL
            raise _SuppressionSentinelError("boom")

        assert config_module.zenml_cli_utils.declare is declare_spy
        assert config_module.zenml_cli_utils.success is success_spy
        assert logging.root.manager.disable == logging.ERROR
    finally:
        logging.disable(original_disable)


def test_login_to_server_target_direct_server_path_runs_under_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct server login should run with ZenML CLI chatter suppressed."""
    original_disable = logging.root.manager.disable
    helper_calls: list[str] = []

    def declare_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        helper_calls.append("declare")

    def success_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        helper_calls.append("success")

    def fake_connect_to_server(**kwargs: Any) -> None:
        assert logging.root.manager.disable == logging.CRITICAL
        config_module.zenml_cli_utils.declare("connecting")
        config_module.zenml_cli_utils.success("connected")
        assert kwargs == {
            "url": "https://example.com",
            "api_key": "secret-key",
            "verify_ssl": False,
            "refresh": True,
            "project": "demo-project",
        }

    monkeypatch.setattr(config_module.zenml_cli_utils, "declare", declare_spy)
    monkeypatch.setattr(config_module.zenml_cli_utils, "success", success_spy)

    try:
        with (
            patch("kitaru.config._zenml_is_pro_server", return_value=(False, None)),
            patch(
                "kitaru.config._zenml_connect_to_server",
                side_effect=fake_connect_to_server,
            ) as mock_connect_to_server,
        ):
            config_module._login_to_server_target(
                "https://example.com/",
                api_key="secret-key",
                refresh=True,
                project="demo-project",
                verify_ssl=False,
            )

        mock_connect_to_server.assert_called_once_with(
            url="https://example.com",
            api_key="secret-key",
            verify_ssl=False,
            refresh=True,
            project="demo-project",
        )
        assert helper_calls == []
        assert config_module.zenml_cli_utils.declare is declare_spy
        assert config_module.zenml_cli_utils.success is success_spy
        assert logging.root.manager.disable == original_disable
    finally:
        logging.disable(original_disable)


def test_login_to_server_target_forwards_timeout_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit timeout should be forwarded when the helper supports it."""
    original_disable = logging.root.manager.disable

    def declare_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    def success_spy(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    captured_kwargs: dict[str, Any] = {}

    def fake_connect_to_server(
        *,
        url: str,
        api_key: str | None,
        verify_ssl: bool,
        refresh: bool,
        project: str | None,
        timeout: int | None = None,
    ) -> None:
        captured_kwargs.update(
            {
                "url": url,
                "api_key": api_key,
                "verify_ssl": verify_ssl,
                "refresh": refresh,
                "project": project,
                "timeout": timeout,
            }
        )

    monkeypatch.setattr(config_module.zenml_cli_utils, "declare", declare_spy)
    monkeypatch.setattr(config_module.zenml_cli_utils, "success", success_spy)

    try:
        config_module._config_connection._login_to_server_target_impl(
            "https://example.com/",
            api_key=None,
            refresh=False,
            project=None,
            verify_ssl=True,
            cloud_api_url=None,
            timeout=45,
            suppress_zenml_cli_messages=config_module._suppress_zenml_cli_messages,
            zenml_connect_to_server=fake_connect_to_server,
            zenml_connect_to_pro_server=lambda **kwargs: None,
            zenml_is_pro_server=lambda target: (False, None),
        )
    finally:
        logging.disable(original_disable)

    assert captured_kwargs == {
        "url": "https://example.com",
        "api_key": None,
        "verify_ssl": True,
        "refresh": False,
        "project": None,
        "timeout": 45,
    }


def test_login_to_server_target_wraps_click_failures() -> None:
    """Login helpers should translate ZenML CLI failures into backend errors."""
    with (
        patch("kitaru.config._zenml_is_pro_server", return_value=(False, None)),
        patch(
            "kitaru.config._zenml_connect_to_server",
            side_effect=click.ClickException("login failed"),
        ),
        pytest.raises(KitaruBackendError, match="login failed"),
    ):
        config_module._login_to_server_target("https://example.com")


def test_persist_frozen_execution_spec_rejects_invalid_run_id() -> None:
    """Frozen execution spec persistence should require a UUID run ID."""
    frozen_execution_spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="prod",
            cache=False,
            retries=2,
            image=None,
        ),
        flow_defaults=KitaruConfig(cache=False),
        connection=ResolvedConnectionConfig(
            server_url="https://server.example.com",
            project="demo",
        ),
    )

    with (
        patch("kitaru.config.Client") as client_cls,
        pytest.raises(KitaruUsageError, match="expected a UUID pipeline run ID"),
    ):
        persist_frozen_execution_spec(
            run_id="not-a-uuid",
            frozen_execution_spec=frozen_execution_spec,
        )

    client_cls.return_value.create_run_metadata.assert_not_called()


def test_build_and_persist_frozen_execution_spec() -> None:
    """Frozen execution specs should be serializable and persisted as metadata."""
    model_registry = ModelRegistryConfig(
        aliases={
            "fast": ModelAliasConfig(
                model="openai/gpt-4o-mini",
                secret="openai-creds",
            )
        },
        default="fast",
    )
    frozen_execution_spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="prod",
            cache=False,
            retries=2,
            image=ImageSettings(
                base_image="python:3.12",
                environment={
                    "OPENAI_API_KEY": "sk-real-secret",
                    "BATCH_SIZE": "32",
                },
            ),
        ),
        flow_defaults=KitaruConfig(cache=False),
        connection=ResolvedConnectionConfig(
            server_url="https://server.example.com",
            auth_token="super-secret-token",
            project="demo",
        ),
        model_registry=model_registry,
    )

    assert isinstance(frozen_execution_spec, FrozenExecutionSpec)
    assert frozen_execution_spec.model_registry == model_registry

    # auth_token must be stripped from the frozen spec (Fix 2)
    assert frozen_execution_spec.connection.auth_token is None
    assert frozen_execution_spec.connection.server_url == "https://server.example.com"
    assert frozen_execution_spec.connection.project == "demo"

    # Secret-looking env vars are redacted; non-secret ones preserved (Fix 3)
    assert frozen_execution_spec.resolved_execution.image is not None
    assert frozen_execution_spec.resolved_execution.image.environment == {
        "OPENAI_API_KEY": "***",
        "BATCH_SIZE": "32",
    }

    with patch("kitaru.config.Client") as client_cls:
        persist_frozen_execution_spec(
            run_id="00000000-0000-0000-0000-000000000123",
            frozen_execution_spec=frozen_execution_spec,
        )

    create_metadata = client_cls.return_value.create_run_metadata
    create_metadata.assert_called_once()
    metadata_payload = create_metadata.call_args.kwargs["metadata"]
    assert FROZEN_EXECUTION_SPEC_METADATA_KEY in metadata_payload

    spec_payload = metadata_payload[FROZEN_EXECUTION_SPEC_METADATA_KEY]
    assert spec_payload["resolved_execution"]["stack"] == "prod"

    # auth_token must not appear in serialized payload (Fix 1 defense in depth)
    assert "auth_token" not in spec_payload.get("connection", {})
    assert "auth_token" not in spec_payload.get("flow_defaults", {})

    assert spec_payload["model_registry"] == {
        "aliases": {
            "fast": {
                "model": "openai/gpt-4o-mini",
                "secret": "openai-creds",
            }
        },
        "default": "fast",
    }


@pytest.mark.parametrize(
    "env_key, should_redact",
    [
        ("OPENAI_API_KEY", True),
        ("AWS_SECRET_ACCESS_KEY", True),
        ("DB_PASSWORD", True),
        ("AUTH_TOKEN", True),
        ("MY_CREDENTIAL", True),
        ("GCP_SECRET", True),
        ("BATCH_SIZE", False),
        ("MODEL_NAME", False),
        ("NUM_WORKERS", False),
        ("PYTHONPATH", False),
    ],
)
def test_frozen_spec_redacts_secret_env_vars(
    env_key: str,
    should_redact: bool,
) -> None:
    """Secret-looking environment variable values should be redacted."""
    spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="local",
            cache=True,
            retries=0,
            image=ImageSettings(environment={env_key: "real-value"}),
        ),
        flow_defaults=KitaruConfig(),
        connection=ResolvedConnectionConfig(),
    )
    assert spec.resolved_execution.image is not None
    env = spec.resolved_execution.image.environment
    assert env is not None
    if should_redact:
        assert env[env_key] == "***"
    else:
        assert env[env_key] == "real-value"


def test_frozen_spec_strips_flow_defaults_auth_token() -> None:
    """auth_token on flow_defaults should be stripped from the frozen spec."""
    spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="local", cache=True, retries=0
        ),
        flow_defaults=KitaruConfig(auth_token="should-not-persist"),
        connection=ResolvedConnectionConfig(),
    )
    assert spec.flow_defaults.auth_token is None


def test_frozen_spec_preserves_none_image() -> None:
    """build_frozen_execution_spec should handle None image gracefully."""
    spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="local", cache=True, retries=0, image=None
        ),
        flow_defaults=KitaruConfig(),
        connection=ResolvedConnectionConfig(),
    )
    assert spec.resolved_execution.image is None


# ═══════════════════════════════════════════════════════════════════════════
# ExplicitOverrides detection
# ═══════════════════════════════════════════════════════════════════════════


def _empty_config(*_args: Any, **_kwargs: Any) -> KitaruConfig:
    return KitaruConfig()


class TestExplicitOverrides:
    def _detect(self, **kwargs: Any) -> ExplicitOverrides:
        """Shorthand: fills in empty-config readers when not overridden."""
        kwargs.setdefault("read_project_config", _empty_config)
        kwargs.setdefault("read_execution_env_config", _empty_config)
        kwargs.setdefault("read_runtime_execution_config", _empty_config)
        return detect_explicit_execution_overrides_impl(**kwargs)

    def test_no_explicit_overrides(self) -> None:
        assert self._detect() == ExplicitOverrides()

    def test_decorator_stack_detected(self) -> None:
        result = self._detect(decorator_overrides=KitaruConfig(stack="prod"))
        assert result.stack is True
        assert result.image is False

    def test_invocation_image_detected(self) -> None:
        result = self._detect(
            invocation_overrides=KitaruConfig(
                image=ImageSettings(base_image="python:3.12")
            ),
        )
        assert result.image is True
        assert result.stack is False

    def test_env_cache_detected(self) -> None:
        result = self._detect(
            read_execution_env_config=lambda: KitaruConfig(cache=False),
        )
        assert result.cache is True

    def test_runtime_stack_detected(self) -> None:
        result = self._detect(
            read_runtime_execution_config=lambda: KitaruConfig(stack="staging"),
        )
        assert result.stack is True

    def test_project_config_image_detected(self) -> None:
        result = self._detect(
            read_project_config=lambda _sd=None: KitaruConfig(
                image=ImageSettings(base_image="my-image:latest")
            ),
        )
        assert result.image is True

    def test_multiple_overrides_all_detected(self) -> None:
        result = self._detect(
            decorator_overrides=KitaruConfig(stack="prod"),
            invocation_overrides=KitaruConfig(cache=True),
        )
        assert result.stack is True
        assert result.cache is True
        assert result.image is False
