"""Environment parsing and configuration precedence helpers."""

from __future__ import annotations

import json
import os
import tomllib
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, TypeVar

from zenml.constants import ENV_ZENML_ACTIVE_PROJECT_ID

from kitaru._config._core import (
    KitaruConfig,
    ResolvedConnectionConfig,
    ResolvedExecutionConfig,
    _merge_image_settings,
)
from kitaru._env import (
    KITARU_AUTH_TOKEN_ENV,
    KITARU_PROJECT_ENV,
    KITARU_SERVER_URL_ENV,
    ZENML_STORE_API_KEY_ENV,
    ZENML_STORE_URL_ENV,
    _normalized_kitaru_env,
)
from kitaru.errors import KitaruUsageError

KITARU_LOG_STORE_BACKEND_ENV = "KITARU_LOG_STORE_BACKEND"
KITARU_LOG_STORE_ENDPOINT_ENV = "KITARU_LOG_STORE_ENDPOINT"
KITARU_LOG_STORE_API_KEY_ENV = "KITARU_LOG_STORE_API_KEY"

KITARU_STACK_ENV = "KITARU_STACK"
KITARU_CACHE_ENV = "KITARU_CACHE"
KITARU_RETRIES_ENV = "KITARU_RETRIES"
KITARU_IMAGE_ENV = "KITARU_IMAGE"
KITARU_DEFAULT_MODEL_ENV = "KITARU_DEFAULT_MODEL"
KITARU_CONFIG_PATH_ENV = "KITARU_CONFIG_PATH"

_TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSY_VALUES = {"0", "false", "f", "no", "n", "off"}

_TResolvedConfig = TypeVar(
    "_TResolvedConfig",
    ResolvedExecutionConfig,
    ResolvedConnectionConfig,
)


def _parse_bool_env(name: str, value: str) -> bool:
    """Parse a boolean Kitaru environment variable value."""
    normalized_value = value.strip().lower()
    if normalized_value in _TRUTHY_VALUES:
        return True
    if normalized_value in _FALSY_VALUES:
        return False
    raise KitaruUsageError(
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
        raise KitaruUsageError(
            f"Invalid {pyproject_path}: expected [tool] to be a table."
        )

    kitaru_config = tool_config.get("kitaru")
    if kitaru_config is None:
        return KitaruConfig()
    if not isinstance(kitaru_config, dict):
        raise KitaruUsageError(
            f"Invalid {pyproject_path}: expected [tool.kitaru] to be a table."
        )
    if "runner" in kitaru_config:
        raise KitaruUsageError(
            f"Invalid {pyproject_path}: `[tool.kitaru].runner` was renamed to "
            "`[tool.kitaru].stack`."
        )

    return KitaruConfig.model_validate(kitaru_config)


def _read_execution_env_config() -> KitaruConfig:
    """Read execution-related Kitaru config values from environment."""
    values: dict[str, Any] = {}

    raw_legacy_runner = os.environ.get("KITARU_RUNNER")
    if raw_legacy_runner is not None:
        raise KitaruUsageError(
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
            raise KitaruUsageError(
                f"Invalid value for {KITARU_RETRIES_ENV}: {raw_retries!r}. "
                "Expected an integer."
            ) from exc

    raw_image = os.environ.get(KITARU_IMAGE_ENV)
    if raw_image is not None:
        stripped_image = raw_image.strip()
        if not stripped_image:
            raise KitaruUsageError(f"{KITARU_IMAGE_ENV} cannot be empty.")
        try:
            parsed_image = json.loads(stripped_image)
        except json.JSONDecodeError:
            parsed_image = stripped_image
        values["image"] = parsed_image

    return KitaruConfig.model_validate(values)


def _read_connection_env_config() -> KitaruConfig:
    """Read connection-related Kitaru config values from environment."""
    values: dict[str, Any] = {}

    raw_server_url = _normalized_kitaru_env(KITARU_SERVER_URL_ENV)
    if raw_server_url is not None:
        values["server_url"] = raw_server_url

    raw_auth_token = _normalized_kitaru_env(KITARU_AUTH_TOKEN_ENV)
    if raw_auth_token is not None:
        values["auth_token"] = raw_auth_token

    raw_project = _normalized_kitaru_env(KITARU_PROJECT_ENV)
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


def _read_global_execution_config_impl(
    *,
    current_stack_getter: Callable[[], Any],
) -> KitaruConfig:
    """Read execution defaults from global user config/runtime state."""
    try:
        active_stack_name = current_stack_getter().name
    except Exception:
        active_stack_name = None

    return KitaruConfig(stack=active_stack_name)


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
    if _normalized_kitaru_env(KITARU_SERVER_URL_ENV) is not None:
        return True
    raw_zenml = os.environ.get(ZENML_STORE_URL_ENV)
    return raw_zenml is not None and bool(raw_zenml.strip())


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


def _apply_layers(
    resolved: _TResolvedConfig,
    layers: Iterable[KitaruConfig],
    merge_layer: Callable[[_TResolvedConfig, KitaruConfig], _TResolvedConfig],
) -> _TResolvedConfig:
    """Apply a sequence of config layers to an initial resolved value."""
    current = resolved
    for layer in layers:
        current = merge_layer(current, layer)
    return current


def resolve_execution_config_impl(
    *,
    decorator_overrides: KitaruConfig | None = None,
    invocation_overrides: KitaruConfig | None = None,
    start_dir: Path | None = None,
    read_global_execution_config: Callable[[], KitaruConfig],
    read_project_config: Callable[[Path | None], KitaruConfig],
    read_execution_env_config: Callable[[], KitaruConfig],
    read_runtime_execution_config: Callable[[], KitaruConfig],
) -> ResolvedExecutionConfig:
    """Resolve execution configuration according to Phase 10 precedence."""
    return _apply_layers(
        ResolvedExecutionConfig(
            stack=None,
            image=None,
            cache=True,
            retries=0,
        ),
        (
            read_global_execution_config(),
            read_project_config(start_dir),
            read_execution_env_config(),
            read_runtime_execution_config(),
            decorator_overrides or KitaruConfig(),
            invocation_overrides or KitaruConfig(),
        ),
        _merge_execution_layer,
    )


def resolve_connection_config_impl(
    *,
    explicit: KitaruConfig | None = None,
    validate_for_use: bool = False,
    read_global_connection_config: Callable[[], KitaruConfig],
    read_zenml_connection_env_config: Callable[[], KitaruConfig],
    read_connection_env_config: Callable[[], KitaruConfig],
    read_runtime_connection_config: Callable[[], KitaruConfig],
    validate_connection_config_for_use: Callable[[ResolvedConnectionConfig], None],
) -> ResolvedConnectionConfig:
    """Resolve connection configuration with connection-specific precedence."""
    resolved = _apply_layers(
        ResolvedConnectionConfig(),
        (
            read_global_connection_config(),
            read_zenml_connection_env_config(),
            read_connection_env_config(),
            read_runtime_connection_config(),
            explicit or KitaruConfig(),
        ),
        _merge_connection_layer,
    )

    if validate_for_use:
        validate_connection_config_for_use(resolved)

    return resolved
