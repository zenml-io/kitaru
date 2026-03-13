"""Configuration and connection management.

This module contains:

- global config helpers for runtime log-store settings
- stack-selection helpers
- runtime configuration via ``kitaru.configure(...)``
- config precedence resolution for execution and connection settings
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
import tomllib
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch
from urllib.parse import urlparse
from uuid import UUID

import click
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from zenml.cli.login import connect_to_pro_server as _zenml_connect_to_pro_server
from zenml.cli.login import connect_to_server as _zenml_connect_to_server
from zenml.cli.login import is_pro_server as _zenml_is_pro_server
from zenml.client import Client
from zenml.config.docker_settings import DockerSettings
from zenml.config.global_config import GlobalConfiguration
from zenml.constants import ENV_ZENML_ACTIVE_PROJECT_ID
from zenml.enums import MetadataResourceTypes, StackComponentType
from zenml.exceptions import AuthorizationException, EntityExistsError
from zenml.models.v2.misc.run_metadata import RunMetadataResource
from zenml.utils import io_utils, yaml_utils

from kitaru.errors import KitaruUsageError

zenml_cli_utils = importlib.import_module("zenml.cli.utils")

_DEFAULT_LOG_STORE_BACKEND = "artifact-store"
_KITARU_GLOBAL_CONFIG_FILENAME = "config.yaml"
_LOG_STORE_SOURCE_DEFAULT = "default"
_LOG_STORE_SOURCE_ENVIRONMENT = "environment"
_LOG_STORE_SOURCE_GLOBAL_USER_CONFIG = "global user config"
_LOG_STORE_BACKEND_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_MODEL_ALIAS_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_STACK_MANAGED_LABEL_KEY = "kitaru.managed"
_STACK_MANAGED_LABEL_VALUE = "true"

KITARU_LOG_STORE_BACKEND_ENV = "KITARU_LOG_STORE_BACKEND"
KITARU_LOG_STORE_ENDPOINT_ENV = "KITARU_LOG_STORE_ENDPOINT"
KITARU_LOG_STORE_API_KEY_ENV = "KITARU_LOG_STORE_API_KEY"

KITARU_STACK_ENV = "KITARU_STACK"
KITARU_CACHE_ENV = "KITARU_CACHE"
KITARU_RETRIES_ENV = "KITARU_RETRIES"
KITARU_IMAGE_ENV = "KITARU_IMAGE"
KITARU_SERVER_URL_ENV = "KITARU_SERVER_URL"
KITARU_AUTH_TOKEN_ENV = "KITARU_AUTH_TOKEN"
KITARU_PROJECT_ENV = "KITARU_PROJECT"
KITARU_DEFAULT_MODEL_ENV = "KITARU_DEFAULT_MODEL"
KITARU_CONFIG_PATH_ENV = "KITARU_CONFIG_PATH"
KITARU_DEBUG_ENV = "KITARU_DEBUG"
KITARU_ANALYTICS_OPT_IN_ENV = "KITARU_ANALYTICS_OPT_IN"

ZENML_STORE_URL_ENV = "ZENML_STORE_URL"
ZENML_STORE_API_KEY_ENV = "ZENML_STORE_API_KEY"

FROZEN_EXECUTION_SPEC_METADATA_KEY = "kitaru_execution_spec"

_TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSY_VALUES = {"0", "false", "f", "no", "n", "off"}
_UNSET = object()


def _normalize_model_alias(alias: str) -> str:
    """Normalize and validate a local model alias name."""
    normalized_alias = alias.strip().lower()
    if not normalized_alias:
        raise ValueError("Model alias cannot be empty.")

    if not _MODEL_ALIAS_PATTERN.fullmatch(normalized_alias):
        raise ValueError(
            "Invalid model alias. Use lowercase letters, numbers, underscores, "
            "or hyphens, and start with a letter or number."
        )

    return normalized_alias


class LogStoreOverride(BaseModel):
    """Global log-store override values for non-default backends."""

    backend: str
    endpoint: str
    api_key: str | None = None

    @field_validator("backend")
    @classmethod
    def _validate_backend(cls, value: str) -> str:
        normalized_value = value.strip().lower()
        if not normalized_value:
            raise ValueError("Log-store backend cannot be empty.")

        if not _LOG_STORE_BACKEND_PATTERN.fullmatch(normalized_value):
            raise ValueError(
                "Invalid log-store backend. Use lowercase letters, numbers, "
                "dots, underscores, or hyphens."
            )

        return normalized_value

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, value: str) -> str:
        normalized_value = value.strip().rstrip("/")
        if not normalized_value:
            raise ValueError("Log-store endpoint cannot be empty.")

        parsed = urlparse(normalized_value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(
                "Invalid log-store endpoint. Please use an http:// or https:// URL."
            )

        return normalized_value

    @field_validator("api_key")
    @classmethod
    def _validate_api_key(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Log-store API key cannot be empty.")

        return normalized_value


class ResolvedLogStore(BaseModel):
    """Effective log-store configuration after applying precedence rules."""

    backend: str
    endpoint: str | None = None
    api_key: str | None = None
    source: Literal[
        "default",
        "environment",
        "global user config",
    ]


class ActiveStackLogStore(BaseModel):
    """Active stack log-store backend resolved from the current ZenML stack."""

    backend: str
    endpoint: str | None = None
    stack_name: str | None = None


class ModelAliasConfig(BaseModel):
    """Local model alias settings used by `kitaru.llm()`."""

    model: str
    secret: str | None = None

    @field_validator("model")
    @classmethod
    def _validate_model(cls, value: str) -> str:
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Model identifier cannot be empty.")
        return normalized_value

    @field_validator("secret")
    @classmethod
    def _validate_secret(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Secret reference cannot be empty.")

        return normalized_value


class ModelRegistryConfig(BaseModel):
    """Persisted model alias registry for `kitaru.llm()`."""

    aliases: dict[str, ModelAliasConfig] = Field(default_factory=dict)
    default: str | None = None

    @field_validator("aliases", mode="before")
    @classmethod
    def _validate_aliases(
        cls,
        value: dict[str, ModelAliasConfig] | None,
    ) -> dict[str, ModelAliasConfig]:
        if value is None:
            return {}

        normalized_aliases: dict[str, ModelAliasConfig] = {}
        for alias, alias_config in value.items():
            normalized_alias = _normalize_model_alias(alias)
            normalized_aliases[normalized_alias] = alias_config

        return normalized_aliases

    @field_validator("default")
    @classmethod
    def _validate_default(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_model_alias(value)

    @model_validator(mode="after")
    def _validate_default_exists(self) -> ModelRegistryConfig:
        if self.default is not None and self.default not in self.aliases:
            raise ValueError(
                "Model registry default alias must reference a configured alias."
            )
        return self


class ModelAliasEntry(BaseModel):
    """Public local model alias shape used by CLI/SDK helpers."""

    alias: str
    model: str
    secret: str | None = None
    is_default: bool = False


class ResolvedModelSelection(BaseModel):
    """Model resolution result used by `kitaru.llm()`."""

    requested_model: str | None
    alias: str | None
    resolved_model: str
    secret: str | None = None


class _KitaruGlobalConfig(BaseModel):
    """Persisted Kitaru global configuration."""

    version: int = 1
    log_store: LogStoreOverride | None = None
    model_registry: ModelRegistryConfig | None = None


class StackInfo(BaseModel):
    """Public stack information exposed by Kitaru SDK helpers."""

    id: str
    name: str
    is_active: bool


@dataclass(frozen=True)
class _StackComponent:
    """Internal reference to a stack-owned stack component."""

    component_id: str
    name: str
    kind: Literal["orchestrator", "artifact_store"]


@dataclass(frozen=True)
class _StackListEntry:
    """Internal structured stack list item with managed-state metadata."""

    stack: StackInfo
    is_managed: bool


@dataclass(frozen=True)
class _StackCreateResult:
    """Structured result for stack creation operations."""

    stack: StackInfo
    previous_active_stack: str | None
    components_created: tuple[str, ...]


@dataclass(frozen=True)
class _StackDeleteResult:
    """Structured result for stack deletion operations."""

    deleted_stack: str
    components_deleted: tuple[str, ...]
    new_active_stack: str | None
    recursive: bool


class ImageSettings(BaseModel):
    """Image and runtime environment settings for a flow execution."""

    base_image: str | None = None
    requirements: list[str] | None = None
    dockerfile: str | None = None
    environment: dict[str, str] | None = None
    apt_packages: list[str] | None = None
    replicate_local_python_environment: bool | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("base_image", "dockerfile")
    @classmethod
    def _validate_optional_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Image string values cannot be empty.")
        return normalized_value

    @field_validator("requirements")
    @classmethod
    def _validate_requirements(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized_requirements: list[str] = []
        for requirement in value:
            normalized_requirement = requirement.strip()
            if not normalized_requirement:
                raise ValueError("Image requirements cannot contain empty strings.")
            normalized_requirements.append(normalized_requirement)
        return normalized_requirements

    @field_validator("environment")
    @classmethod
    def _validate_environment(
        cls,
        value: dict[str, str] | None,
    ) -> dict[str, str] | None:
        if value is None:
            return None
        normalized_environment: dict[str, str] = {}
        for key, environment_value in value.items():
            normalized_key = key.strip()
            if not normalized_key:
                raise ValueError("Image environment keys cannot be empty.")
            normalized_environment[normalized_key] = str(environment_value)
        return normalized_environment

    def is_empty(self) -> bool:
        """Return whether this object carries any configured values."""
        return (
            self.base_image is None
            and self.requirements is None
            and self.dockerfile is None
            and self.environment is None
            and self.apt_packages is None
            and self.replicate_local_python_environment is None
        )


ImageInput = str | DockerSettings | Mapping[str, Any] | ImageSettings


class KitaruConfig(BaseModel):
    """Unified Kitaru configuration model."""

    stack: str | None = None
    image: ImageSettings | None = None
    cache: bool | None = None
    retries: int | None = None
    server_url: str | None = None
    auth_token: str | None = None
    project: str | None = None

    model_config = ConfigDict(extra="ignore")

    @field_validator("stack", "auth_token", "project")
    @classmethod
    def _normalize_optional_non_empty(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Configuration string values cannot be empty.")
        return normalized_value

    @field_validator("server_url")
    @classmethod
    def _normalize_optional_server_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_server_url(value)

    @field_validator("retries")
    @classmethod
    def _validate_retries(cls, value: int | None) -> int | None:
        if value is None:
            return None
        if value < 0:
            raise ValueError("Flow retries must be >= 0.")
        return value

    @field_validator("image", mode="before")
    @classmethod
    def _coerce_image_input(cls, value: Any) -> Any:
        return _coerce_image_input(value)


class ResolvedExecutionConfig(BaseModel):
    """Fully resolved execution settings for a flow run."""

    stack: str | None = None
    image: ImageSettings | None = None
    cache: bool
    retries: int

    @field_validator("stack")
    @classmethod
    def _validate_stack(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Stack cannot be empty.")
        return normalized_value

    @field_validator("retries")
    @classmethod
    def _validate_retries(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Flow retries must be >= 0.")
        return value


class ResolvedConnectionConfig(BaseModel):
    """Connection settings resolved for the current process context."""

    server_url: str | None = None
    auth_token: str | None = None
    project: str | None = None


@dataclass(frozen=True)
class ActiveEnvironmentVariable:
    """Public status view for one active Kitaru environment variable."""

    name: str
    value: str


class FrozenExecutionSpec(BaseModel):
    """Versioned execution-spec snapshot persisted with each run."""

    version: int = 1
    resolved_execution: ResolvedExecutionConfig
    flow_defaults: KitaruConfig
    connection: ResolvedConnectionConfig

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_stack_fields(
        cls,
        value: Any,
    ) -> Any:
        if not isinstance(value, Mapping):
            return value

        normalized = dict(value)
        for field_name in ("resolved_execution", "flow_defaults"):
            field_value = normalized.get(field_name)
            if not isinstance(field_value, Mapping):
                continue
            if "stack" in field_value or "runner" not in field_value:
                continue
            normalized[field_name] = {
                **field_value,
                "stack": field_value["runner"],
            }

        return normalized


_RUNTIME_EXECUTION_OVERRIDES: dict[str, Any] = {}
_RUNTIME_CONNECTION_OVERRIDES: dict[str, Any] = {}


def _coerce_image_input(value: Any) -> ImageSettings | None:
    """Coerce supported image inputs into :class:`ImageSettings`.

    Args:
        value: Image input as ``ImageSettings``, ``DockerSettings``, string,
            mapping, or ``None``.

    Returns:
        Parsed image settings, or ``None`` when no image is configured.

    Raises:
        TypeError: If the input type is unsupported.
        ValueError: If the image payload is malformed.
    """
    if value is None:
        return None
    if isinstance(value, ImageSettings):
        return value
    if isinstance(value, DockerSettings):
        replicate = value.replicate_local_python_environment
        return ImageSettings(
            base_image=value.parent_image,
            requirements=value.requirements,
            dockerfile=value.dockerfile,
            environment=value.environment,
            apt_packages=value.apt_packages or None,
            replicate_local_python_environment=(
                replicate if isinstance(replicate, bool) else None
            ),
        )
    if isinstance(value, str):
        normalized_image = value.strip()
        if not normalized_image:
            raise ValueError("Image string values cannot be empty.")
        return ImageSettings(base_image=normalized_image)
    if isinstance(value, Mapping):
        normalized_payload = dict(value)
        if (
            "parent_image" in normalized_payload
            and "base_image" not in normalized_payload
        ):
            normalized_payload["base_image"] = normalized_payload.pop("parent_image")
        return ImageSettings.model_validate(normalized_payload)

    raise TypeError(
        "Unsupported image configuration type. Expected str, dict, "
        "ImageSettings, DockerSettings, or None."
    )


def _merge_image_settings(
    *,
    base: ImageSettings | None,
    override: ImageSettings,
) -> ImageSettings:
    """Merge image settings with higher-precedence override values."""
    if base is None:
        return override

    merged_environment: dict[str, str] | None
    if override.environment is None:
        merged_environment = base.environment
    else:
        merged_environment = {
            **(base.environment or {}),
            **override.environment,
        }

    return ImageSettings(
        base_image=(
            override.base_image if override.base_image is not None else base.base_image
        ),
        requirements=(
            override.requirements
            if override.requirements is not None
            else base.requirements
        ),
        dockerfile=(
            override.dockerfile if override.dockerfile is not None else base.dockerfile
        ),
        environment=merged_environment,
        apt_packages=(
            override.apt_packages
            if override.apt_packages is not None
            else base.apt_packages
        ),
        replicate_local_python_environment=(
            override.replicate_local_python_environment
            if override.replicate_local_python_environment is not None
            else base.replicate_local_python_environment
        ),
    )


def _parse_bool_env(name: str, value: str) -> bool:
    """Parse a boolean Kitaru environment variable value."""
    normalized_value = value.strip().lower()
    if normalized_value in _TRUTHY_VALUES:
        return True
    if normalized_value in _FALSY_VALUES:
        return False
    raise ValueError(
        f"Invalid value for {name}: {value!r}. Use one of true/false/1/0/yes/no/on/off."
    )


def _find_pyproject(start_dir: Path | None = None) -> Path | None:
    """Locate the nearest ``pyproject.toml`` by walking parent directories."""
    candidate_dir = start_dir or Path.cwd()
    if candidate_dir.is_file():
        candidate_dir = candidate_dir.parent

    for directory in [candidate_dir, *candidate_dir.parents]:
        pyproject_path = directory / "pyproject.toml"
        if pyproject_path.exists():
            return pyproject_path
    return None


def _read_project_config(start_dir: Path | None = None) -> KitaruConfig:
    """Read ``[tool.kitaru]`` config from ``pyproject.toml`` if available."""
    pyproject_path = _find_pyproject(start_dir)
    if pyproject_path is None:
        return KitaruConfig()

    with pyproject_path.open("rb") as pyproject_file:
        pyproject_data = tomllib.load(pyproject_file)

    tool_config = pyproject_data.get("tool", {})
    if not isinstance(tool_config, dict):
        raise ValueError(f"Invalid {pyproject_path}: expected [tool] to be a table.")

    kitaru_config = tool_config.get("kitaru")
    if kitaru_config is None:
        return KitaruConfig()
    if not isinstance(kitaru_config, dict):
        raise ValueError(
            f"Invalid {pyproject_path}: expected [tool.kitaru] to be a table."
        )
    if "runner" in kitaru_config:
        raise ValueError(
            f"Invalid {pyproject_path}: `[tool.kitaru].runner` was renamed to "
            "`[tool.kitaru].stack`."
        )

    return KitaruConfig.model_validate(kitaru_config)


def _read_execution_env_config() -> KitaruConfig:
    """Read execution-related Kitaru config values from environment."""
    values: dict[str, Any] = {}

    raw_legacy_runner = os.environ.get("KITARU_RUNNER")
    if raw_legacy_runner is not None:
        raise ValueError(
            "`KITARU_RUNNER` was renamed to `KITARU_STACK`; use the new name."
        )

    raw_stack = os.environ.get(KITARU_STACK_ENV)
    if raw_stack is not None:
        values["stack"] = raw_stack

    raw_cache = os.environ.get(KITARU_CACHE_ENV)
    if raw_cache is not None:
        values["cache"] = _parse_bool_env(KITARU_CACHE_ENV, raw_cache)

    raw_retries = os.environ.get(KITARU_RETRIES_ENV)
    if raw_retries is not None:
        try:
            values["retries"] = int(raw_retries.strip())
        except ValueError as exc:
            raise ValueError(
                f"Invalid value for {KITARU_RETRIES_ENV}: {raw_retries!r}. "
                "Expected an integer."
            ) from exc

    raw_image = os.environ.get(KITARU_IMAGE_ENV)
    if raw_image is not None:
        stripped_image = raw_image.strip()
        if not stripped_image:
            raise ValueError(f"{KITARU_IMAGE_ENV} cannot be empty.")
        try:
            parsed_image = json.loads(stripped_image)
        except json.JSONDecodeError:
            parsed_image = stripped_image
        values["image"] = parsed_image

    return KitaruConfig.model_validate(values)


def _read_connection_env_config() -> KitaruConfig:
    """Read connection-related Kitaru config values from environment."""
    values: dict[str, Any] = {}

    raw_server_url = os.environ.get(KITARU_SERVER_URL_ENV)
    if raw_server_url is not None:
        values["server_url"] = raw_server_url

    raw_auth_token = os.environ.get(KITARU_AUTH_TOKEN_ENV)
    if raw_auth_token is not None:
        values["auth_token"] = raw_auth_token

    raw_project = os.environ.get(KITARU_PROJECT_ENV)
    if raw_project is not None:
        values["project"] = raw_project

    return KitaruConfig.model_validate(values)


def _read_zenml_connection_env_config() -> KitaruConfig:
    """Read direct ZenML connection env vars for compatibility."""
    values: dict[str, Any] = {}

    raw_server_url = os.environ.get(ZENML_STORE_URL_ENV)
    if raw_server_url is not None:
        values["server_url"] = raw_server_url

    raw_auth_token = os.environ.get(ZENML_STORE_API_KEY_ENV)
    if raw_auth_token is not None:
        values["auth_token"] = raw_auth_token

    raw_project = os.environ.get(ENV_ZENML_ACTIVE_PROJECT_ID)
    if raw_project is not None:
        values["project"] = raw_project

    return KitaruConfig.model_validate(values)


def _read_global_execution_config() -> KitaruConfig:
    """Read execution defaults from global user config/runtime state."""
    try:
        active_stack_name = current_stack().name
    except Exception:
        active_stack_name = None

    return KitaruConfig(stack=active_stack_name)


def _extract_store_token(store: Any) -> str | None:
    """Best-effort extraction of auth token-like fields from ZenML store config."""
    token_attribute_names = ("api_key", "auth_token", "token")
    for attribute_name in token_attribute_names:
        attribute_value = getattr(store, attribute_name, None)
        if isinstance(attribute_value, str) and attribute_value.strip():
            return attribute_value.strip()

    if hasattr(store, "model_dump"):
        dumped_store = store.model_dump(mode="python")
        for attribute_name in token_attribute_names:
            dumped_value = dumped_store.get(attribute_name)
            if isinstance(dumped_value, str) and dumped_value.strip():
                return dumped_value.strip()

    return None


def _read_global_connection_config() -> KitaruConfig:
    """Read connection defaults from global user config/runtime state.

    Only reads ``server_url`` and ``auth_token`` from ZenML's persisted
    store configuration. Project is intentionally omitted here — it is
    only populated by explicit overrides (env var or runtime configure).
    """
    server_url: str | None = None
    auth_token: str | None = None

    global_config = GlobalConfiguration()
    store = global_config.store
    if store is not None:
        raw_store_url = getattr(store, "url", None)
        if isinstance(raw_store_url, str):
            stripped_store_url = raw_store_url.strip()
            if stripped_store_url.startswith(("http://", "https://")):
                server_url = _normalize_server_url(stripped_store_url)
        auth_token = _extract_store_token(store)

    return KitaruConfig(
        server_url=server_url,
        auth_token=auth_token,
    )


def _read_runtime_execution_config() -> KitaruConfig:
    """Read in-memory execution overrides set by ``kitaru.configure()``."""
    return KitaruConfig.model_validate(dict(_RUNTIME_EXECUTION_OVERRIDES))


def _read_runtime_connection_config() -> KitaruConfig:
    """Read in-memory connection overrides set by ``kitaru.configure()``."""
    return KitaruConfig.model_validate(dict(_RUNTIME_CONNECTION_OVERRIDES))


def _merge_execution_layer(
    resolved: ResolvedExecutionConfig,
    layer: KitaruConfig,
) -> ResolvedExecutionConfig:
    """Apply one execution config layer onto an already-resolved result."""
    merged_image = resolved.image
    if layer.image is not None:
        merged_image = _merge_image_settings(base=merged_image, override=layer.image)
        if merged_image.is_empty():
            merged_image = None

    return ResolvedExecutionConfig(
        stack=layer.stack if layer.stack is not None else resolved.stack,
        image=merged_image,
        cache=layer.cache if layer.cache is not None else resolved.cache,
        retries=layer.retries if layer.retries is not None else resolved.retries,
    )


def _merge_connection_layer(
    resolved: ResolvedConnectionConfig,
    layer: KitaruConfig,
) -> ResolvedConnectionConfig:
    """Apply one connection config layer onto an already-resolved result."""
    return ResolvedConnectionConfig(
        server_url=(
            layer.server_url if layer.server_url is not None else resolved.server_url
        ),
        auth_token=(
            layer.auth_token if layer.auth_token is not None else resolved.auth_token
        ),
        project=layer.project if layer.project is not None else resolved.project,
    )


def _environment_has_remote_server_override() -> bool:
    """Return whether env vars are driving a remote connection."""
    for env_name in (KITARU_SERVER_URL_ENV, ZENML_STORE_URL_ENV):
        raw_value = os.environ.get(env_name)
        if raw_value is not None and raw_value.strip():
            return True
    return False


def _validate_connection_config_for_use(
    resolved: ResolvedConnectionConfig,
) -> None:
    """Validate connection config at first use."""
    if _environment_has_remote_server_override() and not resolved.project:
        raise KitaruUsageError(
            "A remote Kitaru server is configured via environment variables, but "
            "no project is active. Set KITARU_PROJECT (preferred) or "
            "ZENML_ACTIVE_PROJECT_ID before using the SDK."
        )


def resolve_execution_config(
    *,
    decorator_overrides: KitaruConfig | None = None,
    invocation_overrides: KitaruConfig | None = None,
    start_dir: Path | None = None,
) -> ResolvedExecutionConfig:
    """Resolve execution configuration according to Phase 10 precedence."""
    resolved = ResolvedExecutionConfig(
        stack=None,
        image=None,
        cache=True,
        retries=0,
    )
    for layer in (
        _read_global_execution_config(),
        _read_project_config(start_dir),
        _read_execution_env_config(),
        _read_runtime_execution_config(),
        decorator_overrides or KitaruConfig(),
        invocation_overrides or KitaruConfig(),
    ):
        resolved = _merge_execution_layer(resolved, layer)

    return resolved


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
    resolved = ResolvedConnectionConfig()
    for layer in (
        _read_global_connection_config(),
        _read_zenml_connection_env_config(),
        _read_connection_env_config(),
        _read_runtime_connection_config(),
        explicit or KitaruConfig(),
    ):
        resolved = _merge_connection_layer(resolved, layer)

    if validate_for_use:
        _validate_connection_config_for_use(resolved)

    return resolved


_KITARU_PKG_SPECIFIER_RE = re.compile(r"[\[><=!~;@\s]")


def _requirements_include_kitaru(requirements: list[str]) -> bool:
    """Check whether a requirements list already contains the kitaru package."""
    return any(
        _KITARU_PKG_SPECIFIER_RE.split(req, maxsplit=1)[0].lower() == "kitaru"
        for req in requirements
    )


def image_settings_to_docker_settings(
    image_settings: ImageSettings | None,
) -> DockerSettings:
    """Convert resolved image settings into ZenML Docker settings.

    Kitaru is auto-injected into the requirements list so that remote
    containers have the SDK available at runtime — unless a custom
    ``base_image`` or ``dockerfile`` is set, in which case the user
    controls the image content.
    """
    if image_settings is None or image_settings.is_empty():
        return DockerSettings(requirements=["kitaru"])

    # When the user provides a custom base image or Dockerfile, they own
    # the image content — don't inject kitaru automatically.
    user_controls_image = (
        image_settings.base_image is not None or image_settings.dockerfile is not None
    )

    docker_settings_kwargs: dict[str, Any] = {}
    if image_settings.base_image is not None:
        docker_settings_kwargs["parent_image"] = image_settings.base_image

    requirements = list(image_settings.requirements or [])
    if not user_controls_image and not _requirements_include_kitaru(requirements):
        requirements.append("kitaru")
    if requirements:
        docker_settings_kwargs["requirements"] = requirements

    if image_settings.dockerfile is not None:
        docker_settings_kwargs["dockerfile"] = image_settings.dockerfile
    if image_settings.environment is not None:
        docker_settings_kwargs["environment"] = image_settings.environment
    if image_settings.apt_packages is not None:
        docker_settings_kwargs["apt_packages"] = image_settings.apt_packages
    if image_settings.replicate_local_python_environment is not None:
        docker_settings_kwargs["replicate_local_python_environment"] = (
            image_settings.replicate_local_python_environment
        )

    return DockerSettings(**docker_settings_kwargs)


def build_frozen_execution_spec(
    *,
    resolved_execution: ResolvedExecutionConfig,
    flow_defaults: KitaruConfig,
    connection: ResolvedConnectionConfig,
) -> FrozenExecutionSpec:
    """Create a frozen execution-spec payload persisted with each run."""
    return FrozenExecutionSpec(
        resolved_execution=resolved_execution,
        flow_defaults=flow_defaults,
        connection=connection,
    )


def _parse_run_uuid(run_id: UUID | str) -> UUID:
    """Parse a run identifier as UUID."""
    if isinstance(run_id, UUID):
        return run_id
    try:
        return UUID(str(run_id))
    except ValueError as exc:
        raise RuntimeError(
            "Frozen execution spec persistence expected a UUID pipeline run ID, "
            f"got {run_id!r}."
        ) from exc


def persist_frozen_execution_spec(
    *,
    run_id: UUID | str,
    frozen_execution_spec: FrozenExecutionSpec,
) -> None:
    """Persist a frozen execution spec as pipeline-run metadata."""
    run_uuid = _parse_run_uuid(run_id)
    Client().create_run_metadata(
        metadata={
            FROZEN_EXECUTION_SPEC_METADATA_KEY: frozen_execution_spec.model_dump(
                mode="json",
                exclude_none=True,
            )
        },
        resources=[
            RunMetadataResource(
                id=run_uuid,
                type=MetadataResourceTypes.PIPELINE_RUN,
            )
        ],
    )


def _reset_runtime_configuration() -> None:
    """Reset in-memory runtime config overrides.

    This helper is intended for tests.
    """
    _RUNTIME_EXECUTION_OVERRIDES.clear()
    _RUNTIME_CONNECTION_OVERRIDES.clear()


def _normalize_server_url(server_url: str) -> str:
    """Validate and normalize a Kitaru server URL.

    Args:
        server_url: Candidate Kitaru server URL.

    Returns:
        The normalized server URL without a trailing slash.

    Raises:
        ValueError: If the URL is empty or is not an HTTP(S) URL.
    """
    normalized_url = server_url.strip().rstrip("/")
    if not normalized_url:
        raise ValueError("Kitaru server URL cannot be empty.")

    parsed = urlparse(normalized_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "Invalid Kitaru server URL. Please use an http:// or https:// URL."
        )

    return normalized_url


def _normalize_login_target(server: str) -> str:
    """Normalize a CLI login target while preserving workspace names/IDs.

    Args:
        server: Kitaru server URL, workspace name, or workspace ID.

    Returns:
        The normalized target value.

    Raises:
        ValueError: If the target is empty or looks like an invalid URL.
    """
    normalized_target = server.strip().rstrip("/")
    if not normalized_target:
        raise ValueError("Kitaru server target cannot be empty.")

    if normalized_target.startswith(("http:", "https:")):
        return _normalize_server_url(normalized_target)

    if _looks_like_server_address_without_scheme(normalized_target):
        raise ValueError(
            "Invalid Kitaru server URL. Please use an http:// or https:// URL, "
            "or pass a managed workspace name or ID."
        )

    return normalized_target


def _is_server_url(server: str) -> bool:
    """Return whether a normalized login target is an HTTP(S) server URL."""
    parsed = urlparse(server)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _looks_like_server_address_without_scheme(target: str) -> bool:
    """Return whether a target resembles a host/URL but lacks http(s)://."""
    localhost_names = {"localhost", "127.0.0.1", "::1"}
    return (
        target in localhost_names
        or any(target.startswith(f"{name}:") for name in localhost_names)
        or "." in target
        or ":" in target
        or "/" in target
    )


@contextmanager
def _suppress_zenml_cli_messages() -> Iterator[None]:
    """Silence ZenML success/progress chatter while Kitaru reuses its helpers.

    This keeps the user-facing CLI output in Kitaru terms while still using
    ZenML's connection/authentication machinery underneath.
    """
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with (
            patch.object(zenml_cli_utils, "declare", return_value=None),
            patch.object(zenml_cli_utils, "success", return_value=None),
        ):
            yield
    finally:
        logging.disable(previous_disable_level)


def _kitaru_config_dir() -> Path:
    """Return the Kitaru-owned global config directory."""
    custom_dir = os.environ.get(KITARU_CONFIG_PATH_ENV)
    if custom_dir:
        return Path(custom_dir)
    return Path(click.get_app_dir("kitaru"))


def _kitaru_global_config_path() -> Path:
    """Return the path to Kitaru's global config file."""
    return _kitaru_config_dir() / _KITARU_GLOBAL_CONFIG_FILENAME


def _parse_kitaru_config_file(config_path: Path) -> _KitaruGlobalConfig | None:
    """Parse a Kitaru global config file, returning ``None`` if absent."""
    if not config_path.exists():
        return None

    config_values = yaml_utils.read_yaml(str(config_path))
    if config_values is None:
        return None

    if not isinstance(config_values, dict):
        raise ValueError(
            "The Kitaru global config file is invalid. Expected a YAML mapping at "
            f"{config_path}."
        )

    try:
        return _KitaruGlobalConfig.model_validate(config_values)
    except ValidationError as exc:
        raise ValueError(
            "The Kitaru global config file is invalid. Fix or delete "
            f"{config_path} and try again."
        ) from exc


def _read_kitaru_global_config() -> _KitaruGlobalConfig:
    """Read Kitaru global config from disk.

    Returns:
        Parsed Kitaru global config.

    Raises:
        ValueError: If the config file exists but is malformed.
    """
    config_path = _kitaru_global_config_path()
    parsed = _parse_kitaru_config_file(config_path)
    if parsed is not None:
        return parsed

    return _KitaruGlobalConfig()


def _write_kitaru_global_config(config: _KitaruGlobalConfig) -> None:
    """Write Kitaru global config to disk."""
    config_path = _kitaru_global_config_path()
    io_utils.create_dir_recursive_if_not_exists(str(config_path.parent))
    yaml_utils.write_yaml(
        str(config_path), config.model_dump(mode="json", exclude_none=True)
    )


def _read_kitaru_global_config_for_update() -> _KitaruGlobalConfig:
    """Read global config for mutation, recovering from malformed files."""
    try:
        return _read_kitaru_global_config()
    except ValueError:
        return _KitaruGlobalConfig()


def _update_kitaru_global_config(
    mutator: Callable[[_KitaruGlobalConfig], None],
) -> _KitaruGlobalConfig:
    """Apply an in-place mutation and persist the resulting global config."""
    global_config = _read_kitaru_global_config_for_update()
    mutator(global_config)
    _write_kitaru_global_config(global_config)
    return global_config


def _read_log_store_env_override() -> ResolvedLogStore | None:
    """Parse an optional log-store override from environment variables.

    Returns:
        A resolved override if configured via environment variables, otherwise
        ``None``.

    Raises:
        ValueError: If the environment variables are set incompletely or with
            invalid values.
    """
    raw_backend = os.environ.get(KITARU_LOG_STORE_BACKEND_ENV)
    raw_endpoint = os.environ.get(KITARU_LOG_STORE_ENDPOINT_ENV)
    raw_api_key = os.environ.get(KITARU_LOG_STORE_API_KEY_ENV)

    if raw_backend is None and raw_endpoint is None and raw_api_key is None:
        return None

    if raw_backend is None:
        raise ValueError(
            f"{KITARU_LOG_STORE_BACKEND_ENV} must be set when defining a log-store "
            "environment override."
        )

    normalized_backend = raw_backend.strip().lower()
    if normalized_backend == _DEFAULT_LOG_STORE_BACKEND:
        if raw_endpoint not in (None, ""):
            raise ValueError(
                f"{KITARU_LOG_STORE_ENDPOINT_ENV} must be unset when "
                f"{KITARU_LOG_STORE_BACKEND_ENV}=artifact-store."
            )
        if raw_api_key not in (None, ""):
            raise ValueError(
                f"{KITARU_LOG_STORE_API_KEY_ENV} must be unset when "
                f"{KITARU_LOG_STORE_BACKEND_ENV}=artifact-store."
            )

        return ResolvedLogStore(
            backend=_DEFAULT_LOG_STORE_BACKEND,
            endpoint=None,
            api_key=None,
            source=_LOG_STORE_SOURCE_ENVIRONMENT,
        )

    if raw_endpoint is None:
        raise ValueError(
            f"{KITARU_LOG_STORE_ENDPOINT_ENV} must be set when "
            f"{KITARU_LOG_STORE_BACKEND_ENV} is configured."
        )

    override = LogStoreOverride(
        backend=normalized_backend,
        endpoint=raw_endpoint,
        api_key=raw_api_key,
    )
    return _resolved_log_store_from_override(
        override,
        source=_LOG_STORE_SOURCE_ENVIRONMENT,
    )


def _resolved_log_store_from_override(
    override: LogStoreOverride,
    *,
    source: Literal[
        "environment",
        "global user config",
    ],
) -> ResolvedLogStore:
    """Convert a persisted/env override into a resolved log-store view."""
    return ResolvedLogStore(
        backend=override.backend,
        endpoint=override.endpoint,
        api_key=override.api_key,
        source=source,
    )


def resolve_log_store() -> ResolvedLogStore:
    """Resolve the effective runtime log-store backend.

    Resolution order (highest to lowest):
    1. Environment variables
    2. Kitaru global user config
    3. Built-in default (artifact store)

    Returns:
        The effective log-store configuration.

    Raises:
        ValueError: If persisted or environment config is malformed.
    """
    env_override = _read_log_store_env_override()
    if env_override is not None:
        return env_override

    global_config = _read_kitaru_global_config()
    if global_config.log_store is not None:
        return _resolved_log_store_from_override(
            global_config.log_store,
            source=_LOG_STORE_SOURCE_GLOBAL_USER_CONFIG,
        )

    return ResolvedLogStore(
        backend=_DEFAULT_LOG_STORE_BACKEND,
        endpoint=None,
        api_key=None,
        source=_LOG_STORE_SOURCE_DEFAULT,
    )


def _normalize_log_store_backend_name(raw_backend: str | None) -> str:
    """Normalize backend identifiers for user-facing comparisons."""
    if raw_backend is None:
        return "unknown"

    normalized = raw_backend.strip().lower().replace("_", "-")
    if not normalized:
        return "unknown"

    if normalized in {"artifact", "artifact-store", "artifactstore"}:
        return "artifact-store"
    if normalized in {"datadog"}:
        return "datadog"
    if normalized in {"otel", "otlp", "open-telemetry", "open telemetry"}:
        return "otel"
    return normalized


def _extract_log_store_endpoint(log_store: Any) -> str | None:
    """Best-effort extraction of an endpoint from a log-store component."""
    config = getattr(log_store, "config", None)
    if config is None:
        return None

    endpoint = getattr(config, "endpoint", None)
    if not isinstance(endpoint, str):
        return None

    normalized = endpoint.strip().rstrip("/")
    if not normalized:
        return None
    return normalized


def active_stack_log_store() -> ActiveStackLogStore | None:
    """Return the runtime log-store backend from the active stack."""
    try:
        client = Client()
        active_stack = client.active_stack
        active_stack_model = client.active_stack_model
        log_store = active_stack.log_store
    except Exception:
        return None

    flavor = getattr(log_store, "flavor", None)
    raw_backend = flavor if isinstance(flavor, str) else log_store.__class__.__name__

    stack_name = getattr(active_stack_model, "name", None)
    if not isinstance(stack_name, str):
        stack_name = None

    return ActiveStackLogStore(
        backend=_normalize_log_store_backend_name(raw_backend),
        endpoint=_extract_log_store_endpoint(log_store),
        stack_name=stack_name,
    )


def set_global_log_store(
    backend: str,
    *,
    endpoint: str,
    api_key: str | None = None,
) -> ResolvedLogStore:
    """Persist a global log-store override backend.

    Args:
        backend: External runtime log backend name (for example ``datadog``).
        endpoint: HTTP(S) endpoint for the log backend.
        api_key: Optional API key or secret placeholder.

    Returns:
        The effective resolved log-store configuration after persisting.

    Raises:
        ValueError: If validation fails.
    """
    if backend.strip().lower() == _DEFAULT_LOG_STORE_BACKEND:
        raise ValueError(
            "The artifact-store backend is already the default. Use "
            "`kitaru log-store reset` to return to defaults."
        )

    def _mutate(global_config: _KitaruGlobalConfig) -> None:
        global_config.log_store = LogStoreOverride(
            backend=backend,
            endpoint=endpoint,
            api_key=api_key,
        )

    _update_kitaru_global_config(_mutate)

    return resolve_log_store()


def reset_global_log_store() -> ResolvedLogStore:
    """Clear the persisted global log-store override.

    Returns:
        The effective resolved log-store configuration after clearing.

    Raises:
        ValueError: If persisted or environment config is malformed.
    """

    def _mutate(global_config: _KitaruGlobalConfig) -> None:
        global_config.log_store = None

    _update_kitaru_global_config(_mutate)

    return resolve_log_store()


def _read_model_registry_config() -> ModelRegistryConfig:
    """Read the local model registry from global config."""
    global_config = _read_kitaru_global_config()
    if global_config.model_registry is None:
        return ModelRegistryConfig()
    return global_config.model_registry


def register_model_alias(
    alias: str,
    *,
    model: str,
    secret: str | None = None,
) -> ModelAliasEntry:
    """Register or update a local model alias for `kitaru.llm()`."""
    normalized_alias = _normalize_model_alias(alias)

    def _mutate(global_config: _KitaruGlobalConfig) -> None:
        registry = (
            global_config.model_registry.model_copy(deep=True)
            if global_config.model_registry is not None
            else ModelRegistryConfig()
        )
        registry.aliases[normalized_alias] = ModelAliasConfig(
            model=model, secret=secret
        )
        if registry.default is None:
            registry.default = normalized_alias
        global_config.model_registry = registry

    updated_global_config = _update_kitaru_global_config(_mutate)
    registry = updated_global_config.model_registry or ModelRegistryConfig()
    alias_config = registry.aliases[normalized_alias]
    return ModelAliasEntry(
        alias=normalized_alias,
        model=alias_config.model,
        secret=alias_config.secret,
        is_default=registry.default == normalized_alias,
    )


def list_model_aliases() -> list[ModelAliasEntry]:
    """List local model aliases in stable order for CLI rendering."""
    registry = _read_model_registry_config()
    aliases: list[ModelAliasEntry] = []
    for alias in sorted(registry.aliases):
        alias_config = registry.aliases[alias]
        aliases.append(
            ModelAliasEntry(
                alias=alias,
                model=alias_config.model,
                secret=alias_config.secret,
                is_default=registry.default == alias,
            )
        )
    return aliases


def resolve_model_selection(model: str | None) -> ResolvedModelSelection:
    """Resolve an explicit/default model input to a concrete LiteLLM model."""
    registry = _read_model_registry_config()

    def _resolve_requested_model(requested_model: str) -> ResolvedModelSelection:
        if not requested_model:
            raise ValueError("Model identifier cannot be empty.")

        alias_candidate: str | None
        try:
            alias_candidate = _normalize_model_alias(requested_model)
        except ValueError:
            alias_candidate = None

        if alias_candidate is not None and alias_candidate in registry.aliases:
            alias_config = registry.aliases[alias_candidate]
            return ResolvedModelSelection(
                requested_model=requested_model,
                alias=alias_candidate,
                resolved_model=alias_config.model,
                secret=alias_config.secret,
            )

        return ResolvedModelSelection(
            requested_model=requested_model,
            alias=None,
            resolved_model=requested_model,
            secret=None,
        )

    if model is not None:
        return _resolve_requested_model(model.strip())

    env_default_model = os.environ.get(KITARU_DEFAULT_MODEL_ENV)
    if env_default_model is not None:
        stripped_env_default_model = env_default_model.strip()
        if not stripped_env_default_model:
            raise ValueError(f"`{KITARU_DEFAULT_MODEL_ENV}` is set but empty.")
        return _resolve_requested_model(stripped_env_default_model)

    if not registry.aliases:
        raise ValueError(
            "No model alias is configured. Run `kitaru model register <alias> --model "
            f"<provider/model>` first, set {KITARU_DEFAULT_MODEL_ENV}, or pass a "
            "concrete model to kitaru.llm(...)."
        )

    default_alias = registry.default
    if default_alias is None:
        if len(registry.aliases) == 1:
            default_alias = next(iter(registry.aliases))
        else:
            raise ValueError(
                "Multiple model aliases are configured but no default alias is set. "
                "Re-register one alias to restore a default."
            )

    if default_alias not in registry.aliases:
        raise ValueError(
            "The configured default model alias is missing. Re-register an alias with "
            "`kitaru model register ...` to repair local config."
        )

    alias_config = registry.aliases[default_alias]
    return ResolvedModelSelection(
        requested_model=None,
        alias=default_alias,
        resolved_model=alias_config.model,
        secret=alias_config.secret,
    )


def _mask_environment_value(name: str, value: str) -> str:
    """Mask secret-like environment values for status surfaces."""
    if name not in {KITARU_AUTH_TOKEN_ENV, KITARU_LOG_STORE_API_KEY_ENV}:
        return value

    if len(value) >= 8:
        return f"{value[:8]}***"
    if len(value) >= 6:
        return f"{value[:6]}***"
    return "***"


def list_active_kitaru_environment_variables() -> list[ActiveEnvironmentVariable]:
    """Return the active public Kitaru environment variables in stable order."""
    ordered_env_vars = (
        KITARU_SERVER_URL_ENV,
        KITARU_AUTH_TOKEN_ENV,
        KITARU_PROJECT_ENV,
        "KITARU_RUNNER",
        KITARU_STACK_ENV,
        KITARU_CACHE_ENV,
        KITARU_RETRIES_ENV,
        KITARU_IMAGE_ENV,
        KITARU_LOG_STORE_BACKEND_ENV,
        KITARU_LOG_STORE_ENDPOINT_ENV,
        KITARU_LOG_STORE_API_KEY_ENV,
        KITARU_DEFAULT_MODEL_ENV,
        KITARU_CONFIG_PATH_ENV,
        KITARU_DEBUG_ENV,
        KITARU_ANALYTICS_OPT_IN_ENV,
    )

    active: list[ActiveEnvironmentVariable] = []
    for env_name in ordered_env_vars:
        raw_value = os.environ.get(env_name)
        if raw_value is None:
            continue
        active.append(
            ActiveEnvironmentVariable(
                name=env_name,
                value=_mask_environment_value(env_name, raw_value),
            )
        )
    return active


def _normalize_stack_selector(name_or_id: str) -> str:
    """Validate and normalize a stack selector provided by a user."""
    normalized_selector = name_or_id.strip()
    if not normalized_selector:
        raise ValueError("Stack name or ID cannot be empty.")

    return normalized_selector


def _stack_name_collision_message(name: str) -> str:
    """Return the user-facing message for stack-name collisions."""
    return (
        f'A stack named "{name}" already exists. To activate it, run '
        f"'kitaru stack use {name}'."
    )


def _component_collision_message(
    name: str,
    component_type: StackComponentType,
) -> str:
    """Return the user-facing message for stack component collisions."""
    return (
        f"Cannot create stack '{name}' because a {component_type.value} named "
        f"'{name}' already exists. Kitaru always creates fresh stack "
        "components and never reuses existing ones."
    )


def _stack_is_managed(stack_model: Any) -> bool:
    """Return whether a stack carries Kitaru's managed-stack label."""
    raw_labels = getattr(stack_model, "labels", None)
    if not isinstance(raw_labels, Mapping):
        return False

    raw_value = raw_labels.get(_STACK_MANAGED_LABEL_KEY)
    if raw_value is None:
        return False

    return str(raw_value).strip().lower() == _STACK_MANAGED_LABEL_VALUE


def _format_stack_component_label(
    name: str,
    kind: Literal["orchestrator", "artifact_store"],
) -> str:
    """Format one stack component for user-facing structured output."""
    return f"{name} ({kind})"


def _delete_stack_components_best_effort(
    client: Client,
    components: list[_StackComponent],
) -> str | None:
    """Best-effort cleanup for stack components created during a failed create."""
    cleanup_errors: list[str] = []

    for component in reversed(components):
        component_type = (
            StackComponentType.ORCHESTRATOR
            if component.kind == "orchestrator"
            else StackComponentType.ARTIFACT_STORE
        )
        try:
            client.delete_stack_component(component.component_id, component_type)
        except Exception as exc:  # pragma: no cover - cleanup failure path
            cleanup_errors.append(
                f"{_format_stack_component_label(component.name, component.kind)}: "
                f"{exc}"
            )

    if not cleanup_errors:
        return None

    return "Cleanup also failed for: " + "; ".join(cleanup_errors)


def _list_stack_entries() -> list[_StackListEntry]:
    """List stacks with active + managed metadata for structured output."""
    client = Client()
    active_stack_id = str(client.active_stack_model.id)

    return [
        _StackListEntry(
            stack=_stack_info_from_model(
                stack_model,
                active_stack_id=active_stack_id,
            ),
            is_managed=_stack_is_managed(stack_model),
        )
        for stack_model in _iter_available_stacks(client)
    ]


def _create_stack_operation(
    name: str,
    *,
    activate: bool = True,
    labels: dict[str, str] | None = None,
) -> _StackCreateResult:
    """Create a new local stack and return structured operation details."""
    selector = _normalize_stack_selector(name)
    client = Client()

    if any(
        stack_model.name == selector for stack_model in _iter_available_stacks(client)
    ):
        raise ValueError(_stack_name_collision_message(selector))

    previous_active_stack = str(client.active_stack_model.name) if activate else None
    merged_labels = dict(labels or {})
    merged_labels[_STACK_MANAGED_LABEL_KEY] = _STACK_MANAGED_LABEL_VALUE

    created_components: list[_StackComponent] = []
    components_created = (
        _format_stack_component_label(selector, "orchestrator"),
        _format_stack_component_label(selector, "artifact_store"),
    )

    try:
        orchestrator = client.create_stack_component(
            name=selector,
            flavor="local",
            component_type=StackComponentType.ORCHESTRATOR,
            configuration={},
        )
        created_components.append(
            _StackComponent(
                component_id=str(orchestrator.id),
                name=selector,
                kind="orchestrator",
            )
        )
    except EntityExistsError as exc:
        raise ValueError(
            _component_collision_message(selector, StackComponentType.ORCHESTRATOR)
        ) from exc

    try:
        artifact_store = client.create_stack_component(
            name=selector,
            flavor="local",
            component_type=StackComponentType.ARTIFACT_STORE,
            configuration={},
        )
        created_components.append(
            _StackComponent(
                component_id=str(artifact_store.id),
                name=selector,
                kind="artifact_store",
            )
        )
    except EntityExistsError as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client, created_components
        )
        message = _component_collision_message(
            selector, StackComponentType.ARTIFACT_STORE
        )
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise ValueError(message) from exc
    except Exception as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client, created_components
        )
        if cleanup_warning:
            raise RuntimeError(f"{exc} {cleanup_warning}") from exc
        raise

    try:
        stack_model = client.create_stack(
            name=selector,
            components={
                StackComponentType.ORCHESTRATOR: selector,
                StackComponentType.ARTIFACT_STORE: selector,
            },
            labels=merged_labels,
        )
    except EntityExistsError as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client, created_components
        )
        message = _stack_name_collision_message(selector)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise ValueError(message) from exc
    except Exception as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client, created_components
        )
        message = str(exc)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise RuntimeError(message) from exc

    if activate:
        client.activate_stack(selector)
        stack = current_stack()
    else:
        stack = _stack_info_from_model(
            stack_model,
            active_stack_id=str(client.active_stack_model.id),
        )

    return _StackCreateResult(
        stack=stack,
        previous_active_stack=previous_active_stack,
        components_created=components_created,
    )


def _delete_stack_operation(
    name_or_id: str,
    *,
    recursive: bool = False,
    force: bool = False,
) -> _StackDeleteResult:
    """Delete a stack and return structured operation details."""
    selector = _normalize_stack_selector(name_or_id)
    client = Client()
    target_stack = client.get_stack(
        selector,
        allow_name_prefix_match=False,
    )
    active_stack = client.active_stack_model
    is_active = str(target_stack.id) == str(active_stack.id)

    if is_active and not force:
        raise ValueError(
            "Cannot delete the active stack. Use '--force' to delete and fall "
            "back to the default stack, or switch first with 'kitaru stack use "
            "<other>'."
        )

    components_deleted: tuple[str, ...] = ()
    if recursive and _stack_is_managed(target_stack):
        deletable_components: list[str] = []
        for component_type, component_kind in (
            (StackComponentType.ORCHESTRATOR, "orchestrator"),
            (StackComponentType.ARTIFACT_STORE, "artifact_store"),
        ):
            component_models = target_stack.components.get(component_type, [])
            if not component_models:
                continue

            component_model = component_models[0]
            stacks = client.list_stacks(component_id=component_model.id, size=2, page=1)
            if len(stacks) == 1 and str(stacks[0].id) == str(target_stack.id):
                deletable_components.append(
                    _format_stack_component_label(target_stack.name, component_kind)
                )
        components_deleted = tuple(deletable_components)

    new_active_stack: str | None = None
    if is_active and force:
        client.activate_stack("default")
        new_active_stack = current_stack().name

    client.delete_stack(target_stack.id, recursive=recursive)

    return _StackDeleteResult(
        deleted_stack=str(target_stack.name),
        components_deleted=components_deleted,
        new_active_stack=new_active_stack,
        recursive=recursive,
    )


def _stack_info_from_model(
    stack_model: Any,
    *,
    active_stack_id: str | None,
) -> StackInfo:
    """Convert a runtime stack model to Kitaru's public stack shape."""
    try:
        stack_id = str(stack_model.id)
        stack_name = str(stack_model.name)
    except AttributeError as exc:
        raise RuntimeError(
            "Unable to read stack information from the configured runtime."
        ) from exc

    return StackInfo(
        id=stack_id,
        name=stack_name,
        is_active=stack_id == active_stack_id,
    )


def _iter_available_stacks(client: Client) -> Iterable[Any]:
    """Return all available stacks from the runtime, including later pages."""
    first_page = client.list_stacks()
    if not isinstance(first_page, Iterable) or isinstance(first_page, (str, bytes)):
        raise RuntimeError(
            "Unexpected stack list response from the configured runtime."
        )

    stack_models = list(first_page)

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
        page_result = client.list_stacks(page=page_number, size=page_size)
        if not isinstance(page_result, Iterable) or isinstance(
            page_result, (str, bytes)
        ):
            raise RuntimeError(
                "Unexpected stack list response from the configured runtime."
            )
        stack_models.extend(page_result)

    return stack_models


def current_stack() -> StackInfo:
    """Return the currently active stack.

    The active stack is managed by the underlying runtime and persisted in the
    runtime's global user configuration.
    """
    active_stack_model = Client().active_stack_model
    active_stack_id = str(active_stack_model.id)
    return _stack_info_from_model(
        active_stack_model,
        active_stack_id=active_stack_id,
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
        ValueError: If the selector is empty.
    """
    selector = _normalize_stack_selector(name_or_id)
    client = Client()
    client.activate_stack(selector)
    return current_stack()


def _login_to_server_target(
    server: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    verify_ssl: bool | str = True,
    cloud_api_url: str | None = None,
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

    Raises:
        RuntimeError: If the underlying ZenML login flow fails.
        ValueError: If the login target is malformed.
    """
    normalized_target = _normalize_login_target(server)

    try:
        with _suppress_zenml_cli_messages():
            if _is_server_url(normalized_target):
                if cloud_api_url:
                    _zenml_connect_to_pro_server(
                        pro_server=normalized_target,
                        api_key=api_key,
                        refresh=refresh,
                        pro_api_url=cloud_api_url,
                        verify_ssl=verify_ssl,
                        project=project,
                    )
                    return

                server_is_pro, detected_cloud_api_url = _zenml_is_pro_server(
                    normalized_target
                )
                if server_is_pro:
                    _zenml_connect_to_pro_server(
                        pro_server=normalized_target,
                        api_key=api_key,
                        refresh=refresh,
                        pro_api_url=detected_cloud_api_url,
                        verify_ssl=verify_ssl,
                        project=project,
                    )
                    return

                _zenml_connect_to_server(
                    url=normalized_target,
                    api_key=api_key,
                    verify_ssl=verify_ssl,
                    refresh=refresh,
                    project=project,
                )
                return

            _zenml_connect_to_pro_server(
                pro_server=normalized_target,
                api_key=api_key,
                refresh=refresh,
                pro_api_url=cloud_api_url,
                verify_ssl=verify_ssl,
                project=project,
            )
    except click.ClickException as exc:
        raise RuntimeError(exc.format_message()) from exc
    except AuthorizationException as exc:
        raise RuntimeError(str(exc)) from exc
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


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
    update the execution precedence chain.  The ``project`` field updates
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
    candidate_execution = dict(_RUNTIME_EXECUTION_OVERRIDES)

    if stack is not _UNSET:
        if stack is None:
            candidate_execution.pop("stack", None)
        else:
            candidate_execution["stack"] = stack

    if image is not _UNSET:
        if image is None:
            candidate_execution.pop("image", None)
        else:
            candidate_execution["image"] = _coerce_image_input(image)

    if cache is not _UNSET:
        if cache is None:
            candidate_execution.pop("cache", None)
        else:
            candidate_execution["cache"] = cache

    if retries is not _UNSET:
        if retries is None:
            candidate_execution.pop("retries", None)
        else:
            candidate_execution["retries"] = retries

    validated_execution = KitaruConfig.model_validate(candidate_execution)
    _RUNTIME_EXECUTION_OVERRIDES.clear()
    _RUNTIME_EXECUTION_OVERRIDES.update(
        validated_execution.model_dump(mode="python", exclude_none=True)
    )

    if project is not _UNSET:
        candidate_connection = dict(_RUNTIME_CONNECTION_OVERRIDES)
        if project is None:
            candidate_connection.pop("project", None)
        else:
            candidate_connection["project"] = project
        validated_connection = KitaruConfig.model_validate(candidate_connection)
        _RUNTIME_CONNECTION_OVERRIDES.clear()
        _RUNTIME_CONNECTION_OVERRIDES.update(
            validated_connection.model_dump(mode="python", exclude_none=True)
        )

    return validated_execution


def connect(
    server_url: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    no_verify_ssl: bool = False,
    ssl_ca_cert: str | None = None,
    cloud_api_url: str | None = None,
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

    Raises:
        ValueError: If the server URL is invalid.
        RuntimeError: If the underlying ZenML connection flow fails.
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
    )
