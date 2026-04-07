"""Core configuration models, runtime state, and persistence helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    field_validator,
    model_validator,
)
from zenml.config.docker_settings import DockerSettings
from zenml.enums import MetadataResourceTypes
from zenml.models.v2.misc.run_metadata import RunMetadataResource
from zenml.utils import io_utils, yaml_utils

from kitaru._config._connection import _normalize_server_url
from kitaru.errors import KitaruUsageError

_KITARU_GLOBAL_CONFIG_FILENAME = "kitaru.yaml"
FROZEN_EXECUTION_SPEC_METADATA_KEY = "kitaru_execution_spec"

if TYPE_CHECKING:
    from kitaru._config._models import ModelRegistryConfig


class ImageSettings(BaseModel):
    """Image and runtime environment settings for a flow execution."""

    base_image: str | None = None
    requirements: list[str] | None = None
    dockerfile: str | None = None
    build_context_root: str | None = None
    environment: dict[str, str] | None = None
    apt_packages: list[str] | None = None
    replicate_local_python_environment: bool | None = None
    image_tag: str | None = None
    target_repository: str | None = None
    user: str | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator(
        "base_image",
        "dockerfile",
        "build_context_root",
        "image_tag",
        "target_repository",
        "user",
    )
    @classmethod
    def _validate_optional_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized_value = value.strip()
        if not normalized_value:
            raise KitaruUsageError("Image string values cannot be empty.")
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
                raise KitaruUsageError(
                    "Image requirements cannot contain empty strings."
                )
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
                raise KitaruUsageError("Image environment keys cannot be empty.")
            normalized_environment[normalized_key] = str(environment_value)
        return normalized_environment

    def is_empty(self) -> bool:
        """Return whether this object carries any configured values."""
        return (
            self.base_image is None
            and self.requirements is None
            and self.dockerfile is None
            and self.build_context_root is None
            and self.environment is None
            and self.apt_packages is None
            and self.replicate_local_python_environment is None
            and self.image_tag is None
            and self.target_repository is None
            and self.user is None
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
            raise KitaruUsageError("Configuration string values cannot be empty.")
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
            raise KitaruUsageError("Flow retries must be >= 0.")
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
            raise KitaruUsageError("Stack cannot be empty.")
        return normalized_value

    @field_validator("retries")
    @classmethod
    def _validate_retries(cls, value: int) -> int:
        if value < 0:
            raise KitaruUsageError("Flow retries must be >= 0.")
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
    model_registry: ModelRegistryConfig | None = None

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


_GlobalConfigT = TypeVar("_GlobalConfigT", bound=BaseModel)


_RUNTIME_EXECUTION_OVERRIDES: dict[str, Any] = {}
_RUNTIME_CONNECTION_OVERRIDES: dict[str, Any] = {}


def _coerce_image_input(value: Any) -> ImageSettings | None:
    """Coerce supported image inputs into :class:`ImageSettings`."""
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
            build_context_root=value.build_context_root,
            environment=value.environment,
            apt_packages=value.apt_packages or None,
            replicate_local_python_environment=(
                replicate if isinstance(replicate, bool) else None
            ),
            image_tag=value.image_tag,
            target_repository=value.target_repository,
            user=value.user,
        )
    if isinstance(value, str):
        normalized_image = value.strip()
        if not normalized_image:
            raise KitaruUsageError("Image string values cannot be empty.")
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
        build_context_root=(
            override.build_context_root
            if override.build_context_root is not None
            else base.build_context_root
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
        image_tag=(
            override.image_tag if override.image_tag is not None else base.image_tag
        ),
        target_repository=(
            override.target_repository
            if override.target_repository is not None
            else base.target_repository
        ),
        user=(override.user if override.user is not None else base.user),
    )


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


def _read_runtime_execution_config() -> KitaruConfig:
    """Read in-memory execution overrides set by ``kitaru.configure()``."""
    return KitaruConfig.model_validate(dict(_RUNTIME_EXECUTION_OVERRIDES))


def _read_runtime_connection_config() -> KitaruConfig:
    """Read in-memory connection overrides set by ``kitaru.configure()``."""
    return KitaruConfig.model_validate(dict(_RUNTIME_CONNECTION_OVERRIDES))


def _requirements_include_kitaru(requirements: list[str]) -> bool:
    """Check whether a requirements list already contains the kitaru package."""
    return any(
        _KITARU_PKG_SPECIFIER_RE.split(req, maxsplit=1)[0].lower() == "kitaru"
        for req in requirements
    )


def image_settings_to_docker_settings(
    image_settings: ImageSettings | None,
) -> DockerSettings:
    """Convert resolved image settings into ZenML Docker settings."""
    if image_settings is None or image_settings.is_empty():
        return DockerSettings(requirements=["kitaru"])

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
    if image_settings.build_context_root is not None:
        docker_settings_kwargs["build_context_root"] = image_settings.build_context_root
    if image_settings.environment is not None:
        docker_settings_kwargs["environment"] = image_settings.environment
    if image_settings.apt_packages is not None:
        docker_settings_kwargs["apt_packages"] = image_settings.apt_packages
    if image_settings.replicate_local_python_environment is not None:
        docker_settings_kwargs["replicate_local_python_environment"] = (
            image_settings.replicate_local_python_environment
        )
    if image_settings.image_tag is not None:
        docker_settings_kwargs["image_tag"] = image_settings.image_tag
    if image_settings.target_repository is not None:
        docker_settings_kwargs["target_repository"] = image_settings.target_repository
    if image_settings.user is not None:
        docker_settings_kwargs["user"] = image_settings.user

    return DockerSettings(**docker_settings_kwargs)


_SECRET_ENV_KEY_PATTERN = re.compile(
    r"(KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|AUTH)",
    re.IGNORECASE,
)
_REDACTED = "***"


def _redact_image_environment(
    image: ImageSettings | None,
) -> ImageSettings | None:
    """Return a copy of *image* with secret-looking environment values redacted."""
    if image is None or not image.environment:
        return image
    redacted_env = {
        k: (_REDACTED if _SECRET_ENV_KEY_PATTERN.search(k) else v)
        for k, v in image.environment.items()
    }
    return image.model_copy(update={"environment": redacted_env})


def build_frozen_execution_spec(
    *,
    resolved_execution: ResolvedExecutionConfig,
    flow_defaults: KitaruConfig,
    connection: ResolvedConnectionConfig,
    model_registry: ModelRegistryConfig | None = None,
) -> FrozenExecutionSpec:
    """Create a frozen execution-spec payload persisted with each run.

    Sensitive fields (auth tokens, secret-looking environment variables) are
    stripped or redacted so that the persisted metadata never contains
    plaintext secrets.
    """
    safe_connection = ResolvedConnectionConfig(
        server_url=connection.server_url,
        project=connection.project,
    )
    safe_flow_defaults = flow_defaults.model_copy(update={"auth_token": None})
    safe_resolved_execution = resolved_execution.model_copy(
        update={"image": _redact_image_environment(resolved_execution.image)},
    )
    safe_flow_defaults_image = _redact_image_environment(safe_flow_defaults.image)
    if safe_flow_defaults_image is not safe_flow_defaults.image:
        safe_flow_defaults = safe_flow_defaults.model_copy(
            update={"image": safe_flow_defaults_image},
        )
    return FrozenExecutionSpec(
        resolved_execution=safe_resolved_execution,
        flow_defaults=safe_flow_defaults,
        connection=safe_connection,
        model_registry=model_registry,
    )


def _parse_run_uuid(run_id: UUID | str) -> UUID:
    """Parse a run identifier as UUID."""
    if isinstance(run_id, UUID):
        return run_id
    try:
        return UUID(str(run_id))
    except ValueError as exc:
        raise KitaruUsageError(
            "Frozen execution spec persistence expected a UUID pipeline run ID, "
            f"got {run_id!r}."
        ) from exc


def persist_frozen_execution_spec_impl(
    *,
    run_id: UUID | str,
    frozen_execution_spec: FrozenExecutionSpec,
    client_factory: Callable[[], Any],
) -> None:
    """Persist a frozen execution spec as pipeline-run metadata."""
    run_uuid = _parse_run_uuid(run_id)
    client_factory().create_run_metadata(
        metadata={
            FROZEN_EXECUTION_SPEC_METADATA_KEY: frozen_execution_spec.model_dump(
                mode="json",
                exclude_none=True,
                exclude={
                    "connection": {"auth_token"},
                    "flow_defaults": {"auth_token"},
                },
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
    """Reset in-memory runtime config overrides."""
    _RUNTIME_EXECUTION_OVERRIDES.clear()
    _RUNTIME_CONNECTION_OVERRIDES.clear()


def _kitaru_config_dir_impl(
    *,
    config_path_env_name: str,
    app_dir_getter: Callable[[str], str],
    fallback_config_path_env_name: str | None = None,
) -> Path:
    """Return the Kitaru-owned global config directory.

    Precedence:
    1. ``KITARU_CONFIG_PATH`` (explicit user override)
    2. ``ZENML_CONFIG_PATH`` (set by the init hook or a server subprocess)
    3. ``click.get_app_dir("kitaru")`` (platform default)
    """
    custom_dir = os.environ.get(config_path_env_name)
    if custom_dir:
        return Path(custom_dir)

    if fallback_config_path_env_name:
        fallback_dir = os.environ.get(fallback_config_path_env_name)
        if fallback_dir:
            return Path(fallback_dir)

    return Path(app_dir_getter("kitaru"))


def _kitaru_global_config_path_impl(
    *,
    config_dir_getter: Callable[[], Path],
    filename: str = _KITARU_GLOBAL_CONFIG_FILENAME,
) -> Path:
    """Return the path to Kitaru's global config file."""
    return config_dir_getter() / filename


def _parse_kitaru_config_file(
    config_path: Path,
    *,
    global_config_model: type[_GlobalConfigT],
) -> _GlobalConfigT | None:
    """Parse a Kitaru global config file, returning ``None`` if absent."""
    if not config_path.exists():
        return None

    config_values = yaml_utils.read_yaml(str(config_path))
    if config_values is None:
        return None

    if not isinstance(config_values, dict):
        raise KitaruUsageError(
            "The Kitaru global config file is invalid. Expected a YAML mapping at "
            f"{config_path}."
        )

    try:
        return global_config_model.model_validate(config_values)
    except ValidationError as exc:
        raise KitaruUsageError(
            "The Kitaru global config file is invalid. Fix or delete "
            f"{config_path} and try again."
        ) from exc


def _read_kitaru_global_config_impl(
    *,
    config_path_getter: Callable[[], Path],
    global_config_model: type[_GlobalConfigT],
) -> _GlobalConfigT:
    """Read Kitaru global config from disk."""
    config_path = config_path_getter()
    parsed = _parse_kitaru_config_file(
        config_path,
        global_config_model=global_config_model,
    )
    if parsed is not None:
        return parsed

    return global_config_model()


def _write_kitaru_global_config_impl(
    config: BaseModel,
    *,
    config_path_getter: Callable[[], Path],
) -> None:
    """Write Kitaru global config to disk."""
    config_path = config_path_getter()
    io_utils.create_dir_recursive_if_not_exists(str(config_path.parent))
    yaml_utils.write_yaml(
        str(config_path), config.model_dump(mode="json", exclude_none=True)
    )


def _read_kitaru_global_config_for_update_impl(
    *,
    reader: Callable[[], _GlobalConfigT],
    global_config_model: type[_GlobalConfigT],
) -> _GlobalConfigT:
    """Read global config for mutation, recovering from malformed files."""
    try:
        return reader()
    except ValueError:
        return global_config_model()


def _update_kitaru_global_config_impl(
    mutator: Callable[[_GlobalConfigT], None],
    *,
    read_for_update: Callable[[], _GlobalConfigT],
    write: Callable[[_GlobalConfigT], None],
) -> _GlobalConfigT:
    """Apply an in-place mutation and persist the resulting global config."""
    global_config = read_for_update()
    mutator(global_config)
    write(global_config)
    return global_config


def _read_global_connection_config_impl(
    *,
    global_configuration_factory: Callable[[], Any],
) -> KitaruConfig:
    """Read connection defaults from global user config/runtime state."""
    server_url: str | None = None
    auth_token: str | None = None

    global_config = global_configuration_factory()
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


def configure_impl(
    *,
    stack: str | None | object,
    image: ImageInput | None | object,
    cache: bool | None | object,
    retries: int | None | object,
    project: str | None | object,
    unset_sentinel: object,
) -> KitaruConfig:
    """Set process-local runtime defaults."""
    candidate_execution = dict(_RUNTIME_EXECUTION_OVERRIDES)

    if stack is not unset_sentinel:
        if stack is None:
            candidate_execution.pop("stack", None)
        else:
            candidate_execution["stack"] = stack

    if image is not unset_sentinel:
        if image is None:
            candidate_execution.pop("image", None)
        else:
            candidate_execution["image"] = _coerce_image_input(image)

    if cache is not unset_sentinel:
        if cache is None:
            candidate_execution.pop("cache", None)
        else:
            candidate_execution["cache"] = cache

    if retries is not unset_sentinel:
        if retries is None:
            candidate_execution.pop("retries", None)
        else:
            candidate_execution["retries"] = retries

    validated_execution = KitaruConfig.model_validate(candidate_execution)
    _RUNTIME_EXECUTION_OVERRIDES.clear()
    _RUNTIME_EXECUTION_OVERRIDES.update(
        validated_execution.model_dump(mode="python", exclude_none=True)
    )

    if project is not unset_sentinel:
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


_KITARU_PKG_SPECIFIER_RE = re.compile(r"[\[><=!~;@\s]")
