"""Configuration and connection management.

This module contains:

- global config helpers for runtime log-store settings
- stack-selection helpers
- runtime configuration via ``kitaru.configure(...)``
- config precedence resolution for execution and connection settings
"""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

import click
from zenml.cli.login import connect_to_pro_server as _zenml_connect_to_pro_server
from zenml.cli.login import connect_to_server as _zenml_connect_to_server
from zenml.cli.login import is_pro_server as _zenml_is_pro_server
from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration

from kitaru import _env as _kitaru_env
from kitaru._config import _connection as _config_connection
from kitaru._config import _core as _config_core
from kitaru._config import _env as _config_env
from kitaru._config import _execution_spec as _config_execution_spec
from kitaru._config import _images as _config_images
from kitaru._config import _log_store as _config_log_store
from kitaru._config import _models as _config_models
from kitaru._config import _stacks as _config_stacks
from kitaru._env import ZENML_CONFIG_PATH_ENV as _ZENML_CONFIG_PATH_ENV
from kitaru._env import ZENML_STORE_API_KEY_ENV as _ZENML_STORE_API_KEY_ENV
from kitaru._env import ZENML_STORE_URL_ENV as _ZENML_STORE_URL_ENV

zenml_cli_utils = importlib.import_module("zenml.cli.utils")

_DEFAULT_LOG_STORE_BACKEND = _config_log_store._DEFAULT_LOG_STORE_BACKEND
_KITARU_GLOBAL_CONFIG_FILENAME = _config_log_store._KITARU_GLOBAL_CONFIG_FILENAME
_LOG_STORE_SOURCE_DEFAULT = _config_log_store._LOG_STORE_SOURCE_DEFAULT
_LOG_STORE_SOURCE_ENVIRONMENT = _config_log_store._LOG_STORE_SOURCE_ENVIRONMENT
_LOG_STORE_SOURCE_GLOBAL_USER_CONFIG = (
    _config_log_store._LOG_STORE_SOURCE_GLOBAL_USER_CONFIG
)
_LOG_STORE_BACKEND_PATTERN = _config_log_store._LOG_STORE_BACKEND_PATTERN
_MODEL_ALIAS_PATTERN = _config_models._MODEL_ALIAS_PATTERN
_STACK_MANAGED_LABEL_KEY = _config_stacks._STACK_MANAGED_LABEL_KEY
_STACK_MANAGED_LABEL_VALUE = _config_stacks._STACK_MANAGED_LABEL_VALUE

KITARU_ANALYTICS_OPT_IN_ENV = _kitaru_env.KITARU_ANALYTICS_OPT_IN_ENV
KITARU_AUTH_TOKEN_ENV = _kitaru_env.KITARU_AUTH_TOKEN_ENV
KITARU_DEBUG_ENV = _kitaru_env.KITARU_DEBUG_ENV
KITARU_PROJECT_ENV = _kitaru_env.KITARU_PROJECT_ENV
KITARU_SERVER_URL_ENV = _kitaru_env.KITARU_SERVER_URL_ENV

KITARU_LOG_STORE_BACKEND_ENV = _config_env.KITARU_LOG_STORE_BACKEND_ENV
KITARU_LOG_STORE_ENDPOINT_ENV = _config_env.KITARU_LOG_STORE_ENDPOINT_ENV
KITARU_LOG_STORE_API_KEY_ENV = _config_env.KITARU_LOG_STORE_API_KEY_ENV

KITARU_STACK_ENV = _config_env.KITARU_STACK_ENV
KITARU_CACHE_ENV = _config_env.KITARU_CACHE_ENV
KITARU_RETRIES_ENV = _config_env.KITARU_RETRIES_ENV
KITARU_IMAGE_ENV = _config_env.KITARU_IMAGE_ENV
KITARU_DEFAULT_MODEL_ENV = _config_env.KITARU_DEFAULT_MODEL_ENV
KITARU_CONFIG_PATH_ENV = _config_env.KITARU_CONFIG_PATH_ENV
KITARU_MODEL_REGISTRY_ENV = _kitaru_env.KITARU_MODEL_REGISTRY_ENV
ZENML_CONFIG_PATH_ENV = _ZENML_CONFIG_PATH_ENV
ZENML_STORE_API_KEY_ENV = _ZENML_STORE_API_KEY_ENV
ZENML_STORE_URL_ENV = _ZENML_STORE_URL_ENV

FROZEN_EXECUTION_SPEC_METADATA_KEY = (
    _config_execution_spec.FROZEN_EXECUTION_SPEC_METADATA_KEY
)
_TRUTHY_VALUES = _config_env._TRUTHY_VALUES
_FALSY_VALUES = _config_env._FALSY_VALUES
_UNSET = object()

ImageSettings = _config_images.ImageSettings
ImageInput = _config_images.ImageInput
KitaruConfig = _config_core.KitaruConfig
ResolvedExecutionConfig = _config_core.ResolvedExecutionConfig
ResolvedConnectionConfig = _config_core.ResolvedConnectionConfig
ActiveEnvironmentVariable = _config_core.ActiveEnvironmentVariable
FrozenExecutionSpec = _config_execution_spec.FrozenExecutionSpec

LogStoreOverride = _config_log_store.LogStoreOverride
ResolvedLogStore = _config_log_store.ResolvedLogStore
ActiveStackLogStore = _config_log_store.ActiveStackLogStore
_KitaruGlobalConfig = _config_log_store._KitaruGlobalConfig

ModelAliasConfig = _config_models.ModelAliasConfig
ModelRegistryConfig = _config_models.ModelRegistryConfig
ModelAliasEntry = _config_models.ModelAliasEntry
ResolvedModelSelection = _config_models.ResolvedModelSelection

StackInfo = _config_stacks.StackInfo
StackType = _config_stacks.StackType
CloudProvider = _config_stacks.CloudProvider
KubernetesStackSpec = _config_stacks.KubernetesStackSpec
VertexStackSpec = _config_stacks.VertexStackSpec
SagemakerStackSpec = _config_stacks.SagemakerStackSpec
AzureMLStackSpec = _config_stacks.AzureMLStackSpec
RemoteStackSpec = _config_stacks.RemoteStackSpec
StackComponentConfigOverrides = _config_stacks.StackComponentConfigOverrides
_ResolvedConnectorSpec = _config_stacks._ResolvedConnectorSpec
_StackComponent = _config_stacks._StackComponent
_StackListEntry = _config_stacks._StackListEntry
_StackCreateResult = _config_stacks._StackCreateResult
_StackDeleteResult = _config_stacks._StackDeleteResult
StackComponentDetails = _config_stacks.StackComponentDetails
StackDetails = _config_stacks.StackDetails
_RECURSIVE_DELETE_COMPONENT_TYPES = _config_stacks._RECURSIVE_DELETE_COMPONENT_TYPES

_coerce_image_input = _config_images._coerce_image_input
_merge_image_settings = _config_images._merge_image_settings
_parse_bool_env = _config_env._parse_bool_env
_find_pyproject = _config_env._find_pyproject
_read_project_config = _config_env._read_project_config
_read_execution_env_config = _config_env._read_execution_env_config
_read_connection_env_config = _config_env._read_connection_env_config
_read_zenml_connection_env_config = _config_env._read_zenml_connection_env_config
_extract_store_token = _config_core._extract_store_token
_read_runtime_execution_config = _config_core._read_runtime_execution_config
_read_runtime_connection_config = _config_core._read_runtime_connection_config
_merge_execution_layer = _config_env._merge_execution_layer
_merge_connection_layer = _config_env._merge_connection_layer
_environment_has_remote_server_override = (
    _config_env._environment_has_remote_server_override
)
_validate_connection_config_for_use = _config_env._validate_connection_config_for_use
_requirements_include_kitaru = _config_images._requirements_include_kitaru
image_settings_to_docker_settings = _config_images.image_settings_to_docker_settings
build_frozen_execution_spec = _config_execution_spec.build_frozen_execution_spec
_parse_run_uuid = _config_execution_spec._parse_run_uuid
_reset_runtime_configuration = _config_core._reset_runtime_configuration
_normalize_server_url = _config_connection._normalize_server_url
_normalize_login_target = _config_connection._normalize_login_target
_is_server_url = _config_connection._is_server_url
_looks_like_server_address_without_scheme = (
    _config_connection._looks_like_server_address_without_scheme
)
_noop_zenml_cli_message = _config_connection._noop_zenml_cli_message

_normalize_model_alias = _config_models._normalize_model_alias
_read_env_model_registry = _config_models._read_env_model_registry
_normalize_log_store_backend_name = _config_log_store._normalize_log_store_backend_name
_extract_log_store_endpoint = _config_log_store._extract_log_store_endpoint
_mask_environment_value = _config_log_store._mask_environment_value

_infer_gcp_project_id_from_container_registry = (
    _config_stacks._infer_gcp_project_id_from_container_registry
)
_artifact_store_resource_id = _config_stacks._artifact_store_resource_id
_container_registry_resource_id = _config_stacks._container_registry_resource_id
_resolve_kubernetes_connector_spec = _config_stacks._resolve_kubernetes_connector_spec
_build_kubernetes_stack_request = _config_stacks._build_kubernetes_stack_request
_get_required_stack_component = _config_stacks._get_required_stack_component
_extract_remote_stack_components = _config_stacks._extract_remote_stack_components
_normalize_stack_selector = _config_stacks._normalize_stack_selector
_stack_name_collision_message = _config_stacks._stack_name_collision_message
_component_collision_message = _config_stacks._component_collision_message
_stack_is_managed = _config_stacks._stack_is_managed
_format_stack_component_label = _config_stacks._format_stack_component_label
_delete_stack_components_best_effort = (
    _config_stacks._delete_stack_components_best_effort
)
_normalize_stack_detail_value = _config_stacks._normalize_stack_detail_value
_stack_component_models_for_type = _config_stacks._stack_component_models_for_type
_iter_stack_component_models = _config_stacks._iter_stack_component_models
_recursive_delete_component_labels = _config_stacks._recursive_delete_component_labels
_linked_service_connector_selectors_for_stack = (
    _config_stacks._linked_service_connector_selectors_for_stack
)
_resolve_service_connector_selectors = (
    _config_stacks._resolve_service_connector_selectors
)
_delete_unshared_service_connectors_best_effort = (
    _config_stacks._delete_unshared_service_connectors_best_effort
)
_resolve_stack_for_show = _config_stacks._resolve_stack_for_show
_stack_component_details_from_model = _config_stacks._stack_component_details_from_model
_infer_stack_details_type = _config_stacks._infer_stack_details_type
_stack_info_from_model = _config_stacks._stack_info_from_model
_iter_available_stacks = _config_stacks._iter_available_stacks


def _read_global_execution_config() -> KitaruConfig:
    """Read execution defaults from global user config/runtime state."""
    return _config_env._read_global_execution_config_impl(
        current_stack_getter=current_stack,
    )


def _read_global_connection_config() -> KitaruConfig:
    """Read connection defaults from global user config/runtime state.

    Only reads ``server_url`` and ``auth_token`` from ZenML's persisted
    store configuration. Project is intentionally omitted here — it is
    only populated by explicit overrides (env var or runtime configure).
    """
    return _config_core._read_global_connection_config_impl(
        global_configuration_factory=GlobalConfiguration,
    )


def resolve_execution_config(
    *,
    decorator_overrides: KitaruConfig | None = None,
    invocation_overrides: KitaruConfig | None = None,
    start_dir: Path | None = None,
) -> ResolvedExecutionConfig:
    """Resolve execution configuration according to Phase 10 precedence."""
    return _config_env.resolve_execution_config_impl(
        decorator_overrides=decorator_overrides,
        invocation_overrides=invocation_overrides,
        start_dir=start_dir,
        read_global_execution_config=_read_global_execution_config,
        read_project_config=_read_project_config,
        read_execution_env_config=_read_execution_env_config,
        read_runtime_execution_config=_read_runtime_execution_config,
    )


def resolve_connection_config(
    *,
    explicit: KitaruConfig | None = None,
    validate_for_use: bool = False,
) -> ResolvedConnectionConfig:
    """Resolve connection configuration with connection-specific precedence.

    Precedence (lowest to highest):
    1. Global ZenML-backed defaults (server_url, auth_token only)
    2. Environment variable overrides (KITARU_SERVER_URL, etc.)
    3. Runtime overrides from ``kitaru.configure(project=...)``
    4. Explicit argument passed by the caller
    """
    return _config_env.resolve_connection_config_impl(
        explicit=explicit,
        validate_for_use=validate_for_use,
        read_global_connection_config=_read_global_connection_config,
        read_zenml_connection_env_config=_read_zenml_connection_env_config,
        read_connection_env_config=_read_connection_env_config,
        read_runtime_connection_config=_read_runtime_connection_config,
        validate_connection_config_for_use=_validate_connection_config_for_use,
    )


def persist_frozen_execution_spec(
    *,
    run_id: UUID | str,
    frozen_execution_spec: FrozenExecutionSpec,
) -> None:
    """Persist a frozen execution spec as pipeline-run metadata."""
    _config_core.persist_frozen_execution_spec_impl(
        run_id=run_id,
        frozen_execution_spec=frozen_execution_spec,
        client_factory=Client,
    )


def _kitaru_config_dir() -> Path:
    """Return the Kitaru-owned global config directory."""
    return _config_core._kitaru_config_dir_impl(
        config_path_env_name=KITARU_CONFIG_PATH_ENV,
        app_dir_getter=click.get_app_dir,
        fallback_config_path_env_name=ZENML_CONFIG_PATH_ENV,
    )


def _kitaru_global_config_path() -> Path:
    """Return the path to Kitaru's global config file."""
    return _config_core._kitaru_global_config_path_impl(
        config_dir_getter=_kitaru_config_dir,
        filename=_KITARU_GLOBAL_CONFIG_FILENAME,
    )


def _parse_kitaru_config_file(config_path: Path) -> _KitaruGlobalConfig | None:
    """Parse a Kitaru global config file, returning ``None`` if absent."""
    return _config_core._parse_kitaru_config_file(
        config_path,
        global_config_model=_KitaruGlobalConfig,
    )


def _read_kitaru_global_config() -> _KitaruGlobalConfig:
    """Read Kitaru global config from disk.

    Returns:
        Parsed Kitaru global config.

    Raises:
        KitaruUsageError: If the config file exists but is malformed.
    """
    return _config_core._read_kitaru_global_config_impl(
        config_path_getter=_kitaru_global_config_path,
        global_config_model=_KitaruGlobalConfig,
    )


def _write_kitaru_global_config(config: _KitaruGlobalConfig) -> None:
    """Write Kitaru global config to disk."""
    _config_core._write_kitaru_global_config_impl(
        config,
        config_path_getter=_kitaru_global_config_path,
    )


def _read_kitaru_global_config_for_update() -> _KitaruGlobalConfig:
    """Read global config for mutation, recovering from malformed files."""
    return _config_core._read_kitaru_global_config_for_update_impl(
        reader=_read_kitaru_global_config,
        global_config_model=_KitaruGlobalConfig,
    )


def _update_kitaru_global_config(
    mutator: Callable[[_KitaruGlobalConfig], None],
) -> _KitaruGlobalConfig:
    """Apply an in-place mutation and persist the resulting global config."""
    return _config_core._update_kitaru_global_config_impl(
        mutator,
        read_for_update=_read_kitaru_global_config_for_update,
        write=_write_kitaru_global_config,
    )


def _read_log_store_env_override() -> ResolvedLogStore | None:
    """Parse an optional log-store override from environment variables."""
    return _config_log_store._read_log_store_env_override()


def _resolved_log_store_from_override(
    override: LogStoreOverride,
    *,
    source: Literal["environment", "global user config"],
) -> ResolvedLogStore:
    """Convert a persisted/env override into a resolved log-store view."""
    return _config_log_store._resolved_log_store_from_override(
        override,
        source=source,
    )


def resolve_log_store() -> ResolvedLogStore:
    """Resolve the effective runtime log-store backend."""
    return _config_log_store.resolve_log_store(
        read_log_store_env_override=_read_log_store_env_override,
        read_global_config=_read_kitaru_global_config,
    )


def active_stack_log_store() -> ActiveStackLogStore | None:
    """Return the runtime log-store backend from the active stack."""
    return _config_log_store.active_stack_log_store(
        client_factory=Client,
        normalize_log_store_backend_name=_normalize_log_store_backend_name,
        extract_log_store_endpoint=_extract_log_store_endpoint,
    )


def set_global_log_store(
    backend: str,
    *,
    endpoint: str,
    api_key: str | None = None,
) -> ResolvedLogStore:
    """Persist a global log-store override backend."""
    result = _config_log_store.set_global_log_store(
        backend,
        endpoint=endpoint,
        api_key=api_key,
        update_global_config=_update_kitaru_global_config,
        resolve_log_store_fn=resolve_log_store,
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.LOG_STORE_CONFIGURED,
        {
            "requested_backend": backend,
            "effective_backend": result.backend,
            "effective_source": result.source,
            "api_key_provided": api_key is not None,
        },
    )
    return result


def reset_global_log_store() -> ResolvedLogStore:
    """Clear the persisted global log-store override."""
    return _config_log_store.reset_global_log_store(
        update_global_config=_update_kitaru_global_config,
        resolve_log_store_fn=resolve_log_store,
    )


def _read_model_registry_config() -> ModelRegistryConfig:
    """Read the local model registry from global config."""
    return _config_models._read_model_registry_config(
        read_global_config=_read_kitaru_global_config,
    )


def register_model_alias(
    alias: str,
    *,
    model: str,
    secret: str | None = None,
) -> ModelAliasEntry:
    """Register or update a local model alias for `kitaru.llm()`."""
    result = _config_models.register_model_alias(
        alias,
        model=model,
        secret=secret,
        update_global_config=_update_kitaru_global_config,
        normalize_model_alias=_normalize_model_alias,
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.MODEL_ALIAS_REGISTERED,
        {
            "has_secret": result.secret is not None,
            "is_default": result.is_default,
        },
    )
    return result


def list_model_aliases() -> list[ModelAliasEntry]:
    """List model aliases visible in the current process environment."""
    return _config_models.list_model_aliases(
        read_global_config=_read_kitaru_global_config,
        environ=os.environ,
    )


def resolve_model_selection(model: str | None) -> ResolvedModelSelection:
    """Resolve an explicit/default model input to a concrete model string."""
    return _config_models.resolve_model_selection(
        model,
        read_global_config=_read_kitaru_global_config,
        environ=os.environ,
        default_model_env_name=KITARU_DEFAULT_MODEL_ENV,
        normalize_model_alias=_normalize_model_alias,
    )


def list_active_kitaru_environment_variables() -> list[ActiveEnvironmentVariable]:
    """Return the active public Kitaru environment variables in stable order."""
    return _config_log_store.list_active_kitaru_environment_variables(
        environ=os.environ,
        mask_environment_value=_mask_environment_value,
    )


def current_stack() -> StackInfo:
    """Return the currently active stack.

    The active stack is managed by the underlying runtime and persisted in the
    runtime's global user configuration.
    """
    return _config_stacks.current_stack(client_factory=Client)


def _list_stack_entries() -> list[_StackListEntry]:
    """List stacks with active + managed metadata for structured output."""
    return _config_stacks._list_stack_entries(client_factory=Client)


def classify_stack_deployment_type(
    name_or_id: str | None = None,
) -> _config_stacks._StackShowType:
    """Classify a stack into Kitaru's low-cardinality deployment taxonomy."""
    return _config_stacks.classify_stack_deployment_type(
        name_or_id,
        client_factory=Client,
    )


def _show_stack_operation(name_or_id: str) -> StackDetails:
    """Inspect one stack and translate its component metadata for CLI display."""
    return _config_stacks._show_stack_operation(
        name_or_id,
        client_factory=Client,
    )


def _create_kubernetes_stack_operation(
    name: str,
    *,
    spec: KubernetesStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
) -> _StackCreateResult:
    """Create a Kubernetes-backed stack via ZenML's one-shot stack API."""
    return _config_stacks._create_kubernetes_stack_operation(
        name,
        spec=spec,
        activate=activate,
        labels=labels,
        component_overrides=component_overrides,
        client_factory=Client,
    )


def _create_vertex_stack_operation(
    name: str,
    *,
    spec: VertexStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
) -> _StackCreateResult:
    """Create a Vertex AI stack via ZenML's one-shot stack API."""
    return _config_stacks._create_vertex_stack_operation(
        name,
        spec=spec,
        activate=activate,
        labels=labels,
        component_overrides=component_overrides,
        client_factory=Client,
    )


def _create_sagemaker_stack_operation(
    name: str,
    *,
    spec: SagemakerStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
) -> _StackCreateResult:
    """Create a SageMaker stack via ZenML's one-shot stack API."""
    return _config_stacks._create_sagemaker_stack_operation(
        name,
        spec=spec,
        activate=activate,
        labels=labels,
        component_overrides=component_overrides,
        client_factory=Client,
    )


def _create_azureml_stack_operation(
    name: str,
    *,
    spec: AzureMLStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
) -> _StackCreateResult:
    """Create an AzureML stack via ZenML's one-shot stack API."""
    return _config_stacks._create_azureml_stack_operation(
        name,
        spec=spec,
        activate=activate,
        labels=labels,
        component_overrides=component_overrides,
        client_factory=Client,
    )


def _create_stack_operation(
    name: str,
    *,
    stack_type: StackType = StackType.LOCAL,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    remote_spec: RemoteStackSpec | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
) -> _StackCreateResult:
    """Create a stack by dispatching to the requested stack type flow."""
    result = _config_stacks._create_stack_operation(
        name,
        stack_type=stack_type,
        activate=activate,
        labels=labels,
        remote_spec=remote_spec,
        component_overrides=component_overrides,
        operation_overrides=cast(
            dict[StackType, Callable[..., _StackCreateResult]],
            {
                StackType.LOCAL: _create_local_stack_operation,
                StackType.KUBERNETES: _create_kubernetes_stack_operation,
                StackType.VERTEX: _create_vertex_stack_operation,
                StackType.SAGEMAKER: _create_sagemaker_stack_operation,
                StackType.AZUREML: _create_azureml_stack_operation,
            },
        ),
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.STACK_CREATED,
        {
            "stack_type": stack_type.value,
            "activate_requested": activate,
        },
    )
    return result


def _create_local_stack_operation(
    name: str,
    *,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
) -> _StackCreateResult:
    """Create a new local stack and return structured operation details."""
    return _config_stacks._create_local_stack_operation(
        name,
        activate=activate,
        labels=labels,
        component_overrides=component_overrides,
        client_factory=Client,
        current_stack_getter=current_stack,
    )


def _delete_stack_operation(
    name_or_id: str,
    *,
    recursive: bool = False,
    force: bool = False,
) -> _StackDeleteResult:
    """Delete a stack and return structured operation details."""
    return _config_stacks._delete_stack_operation(
        name_or_id,
        recursive=recursive,
        force=force,
        client_factory=Client,
        current_stack_getter=current_stack,
    )


def list_stacks() -> list[StackInfo]:
    """List stacks visible to the current user and mark the active one."""
    return [entry.stack for entry in _list_stack_entries()]


def create_stack(
    name: str,
    *,
    activate: bool = True,
    labels: dict[str, str] | None = None,
) -> StackInfo:
    """Create a new local stack and optionally activate it."""
    return _create_stack_operation(
        name,
        activate=activate,
        labels=labels,
    ).stack


def delete_stack(
    name_or_id: str,
    *,
    recursive: bool = False,
    force: bool = False,
) -> None:
    """Delete a stack and optionally its components."""
    _delete_stack_operation(
        name_or_id,
        recursive=recursive,
        force=force,
    )


def use_stack(name_or_id: str) -> StackInfo:
    """Set the active stack and return the resulting active stack info.

    Args:
        name_or_id: Stack name or stack ID.

    Returns:
        Information about the newly active stack.

    Raises:
        KitaruUsageError: If the selector is empty.
        KitaruStateError: If the requested stack cannot be activated due to
            current runtime state.
    """
    result = _config_stacks.use_stack(
        name_or_id,
        client_factory=Client,
        current_stack_getter=current_stack,
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(AnalyticsEvent.STACK_ACTIVATED, {})
    return result


@contextmanager
def _suppress_zenml_cli_messages() -> Iterator[None]:
    """Silence ZenML success/progress chatter while Kitaru reuses its helpers.

    This keeps the user-facing CLI output in Kitaru terms while still using
    ZenML's connection/authentication machinery underneath.
    """
    with _config_connection._suppress_zenml_cli_messages_impl(
        zenml_cli_utils_module=zenml_cli_utils,
        logging_module=logging,
    ):
        yield


def _login_to_server_target(
    server: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    verify_ssl: bool | str = True,
    cloud_api_url: str | None = None,
    timeout: int | None = None,
) -> None:
    """Connect to a Kitaru server URL or managed workspace target.

    Args:
        server: Kitaru server URL, workspace name, or workspace ID.
        api_key: API key used to authenticate with the server.
        refresh: Force a fresh authentication flow.
        project: Project name or ID to activate after connecting.
        verify_ssl: TLS verification mode or CA bundle path.
        cloud_api_url: Optional managed-cloud API URL used for staging or
            custom control planes.
        timeout: Optional connection timeout forwarded when supported by the
            underlying runtime.

    Raises:
        KitaruBackendError: If the underlying ZenML login flow fails.
        KitaruUsageError: If the login target is malformed.
    """
    _config_connection._login_to_server_target_impl(
        server,
        api_key=api_key,
        refresh=refresh,
        project=project,
        verify_ssl=verify_ssl,
        cloud_api_url=cloud_api_url,
        timeout=timeout,
        suppress_zenml_cli_messages=_suppress_zenml_cli_messages,
        zenml_connect_to_server=_zenml_connect_to_server,
        zenml_connect_to_pro_server=_zenml_connect_to_pro_server,
        zenml_is_pro_server=_zenml_is_pro_server,
    )


def configure(
    *,
    stack: str | None | object = _UNSET,
    image: ImageInput | None | object = _UNSET,
    cache: bool | None | object = _UNSET,
    retries: int | None | object = _UNSET,
    project: str | None | object = _UNSET,
) -> KitaruConfig:
    """Set process-local runtime defaults.

    Execution-level fields (``stack``, ``image``, ``cache``, ``retries``)
    update the execution precedence chain. The ``project`` field updates
    the connection precedence chain and is intended as an internal /
    testing escape hatch — it is not a normal user-facing setting.

    Args:
        stack: Default stack name/ID override.
        image: Default image settings override.
        cache: Default cache behavior override.
        retries: Default retry-count override.
        project: Project override (internal/testing). Set to ``None``
            to clear.

    Returns:
        The current runtime override layer after applying updates.
    """
    return _config_core.configure_impl(
        stack=stack,
        image=image,
        cache=cache,
        retries=retries,
        project=project,
        unset_sentinel=_UNSET,
    )


def connect(
    server_url: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    no_verify_ssl: bool = False,
    ssl_ca_cert: str | None = None,
    cloud_api_url: str | None = None,
    timeout: int | None = None,
) -> None:
    """Connect to a Kitaru server.

    Under the hood, this connects to a ZenML server and stores the resolved
    connection/auth state in ZenML's global user configuration.

    Args:
        server_url: URL of the Kitaru server.
        api_key: API key used to authenticate with the server.
        refresh: Force a fresh authentication flow.
        project: Project name or ID to activate after connecting.
        no_verify_ssl: Disable TLS certificate verification.
        ssl_ca_cert: Path to a CA bundle used to verify the server.
        cloud_api_url: Optional managed-cloud API URL used when the server URL
            points at a managed Kitaru deployment or staging environment.
        timeout: Optional connection timeout forwarded when supported by the
            underlying runtime.

    Raises:
        KitaruUsageError: If the server URL is invalid.
        KitaruBackendError: If the underlying ZenML connection flow fails.
    """
    normalized_url = _normalize_server_url(server_url)
    verify_ssl: bool | str = (
        ssl_ca_cert if ssl_ca_cert is not None else not no_verify_ssl
    )
    _login_to_server_target(
        normalized_url,
        api_key=api_key,
        refresh=refresh,
        project=project,
        verify_ssl=verify_ssl,
        cloud_api_url=cloud_api_url,
        timeout=timeout,
    )


def login_to_server(
    server: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    no_verify_ssl: bool = False,
    ssl_ca_cert: str | None = None,
    cloud_api_url: str | None = None,
    timeout: int | None = None,
) -> None:
    """Connect to a Kitaru server URL or managed workspace target.

    Args:
        server: Kitaru server URL, workspace name, or workspace ID.
        api_key: API key used to authenticate with the server.
        refresh: Force a fresh authentication flow.
        project: Project name or ID to activate after connecting.
        no_verify_ssl: Disable TLS certificate verification.
        ssl_ca_cert: Path to a CA bundle used to verify the server.
        cloud_api_url: Optional managed-cloud API URL used when connecting to
            staging or another non-default control plane.
        timeout: Optional connection timeout forwarded when supported by the
            underlying runtime.
    """
    verify_ssl: bool | str = (
        ssl_ca_cert if ssl_ca_cert is not None else not no_verify_ssl
    )
    _login_to_server_target(
        server,
        api_key=api_key,
        refresh=refresh,
        project=project,
        verify_ssl=verify_ssl,
        cloud_api_url=cloud_api_url,
        timeout=timeout,
    )
