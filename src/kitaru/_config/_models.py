"""Model registry helpers for local config and transported runtime state."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from kitaru._config._env import KITARU_DEFAULT_MODEL_ENV
from kitaru._env import KITARU_MODEL_REGISTRY_ENV
from kitaru.errors import KitaruUsageError

_MODEL_ALIAS_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def _normalize_model_alias(alias: str) -> str:
    """Normalize and validate a local model alias name."""
    normalized_alias = alias.strip().lower()
    if not normalized_alias:
        raise KitaruUsageError("Model alias cannot be empty.")

    if not _MODEL_ALIAS_PATTERN.fullmatch(normalized_alias):
        raise KitaruUsageError(
            "Invalid model alias. Use lowercase letters, numbers, underscores, "
            "or hyphens, and start with a letter or number."
        )

    return normalized_alias


class ModelAliasConfig(BaseModel):
    """Local model alias settings used by `kitaru.llm()`."""

    model: str
    secret: str | None = None

    @field_validator("model")
    @classmethod
    def _validate_model(cls, value: str) -> str:
        normalized_value = value.strip()
        if not normalized_value:
            raise KitaruUsageError("Model identifier cannot be empty.")
        return normalized_value

    @field_validator("secret")
    @classmethod
    def _validate_secret(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized_value = value.strip()
        if not normalized_value:
            raise KitaruUsageError("Secret reference cannot be empty.")

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
            raise KitaruUsageError(
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


def _read_model_registry_config(
    *,
    read_global_config: Callable[[], Any],
) -> ModelRegistryConfig:
    """Read the local model registry from global config."""
    global_config = read_global_config()
    if global_config.model_registry is None:
        return ModelRegistryConfig()
    return global_config.model_registry


def _read_env_model_registry(
    *,
    environ: Mapping[str, str] | None = None,
    env_var_name: str = KITARU_MODEL_REGISTRY_ENV,
    source_label: str = "environment",
) -> ModelRegistryConfig | None:
    """Read a transported model registry snapshot from the environment."""
    env = os.environ if environ is None else environ
    raw_registry = env.get(env_var_name)
    if raw_registry is None or not raw_registry.strip():
        return None

    try:
        return ModelRegistryConfig.model_validate_json(raw_registry)
    except (ValidationError, ValueError, TypeError) as exc:
        raise KitaruUsageError(
            f"`{env_var_name}` from {source_label} must be valid JSON matching "
            "the Kitaru model registry schema."
        ) from exc


def _merge_model_registries(
    *,
    local_registry: ModelRegistryConfig,
    env_registry: ModelRegistryConfig | None,
) -> ModelRegistryConfig:
    """Merge local and transported registries with env precedence."""
    if env_registry is None:
        return local_registry.model_copy()

    merged_aliases = dict(local_registry.aliases)
    merged_aliases.update(env_registry.aliases)

    return ModelRegistryConfig(
        aliases=merged_aliases,
        default=(
            env_registry.default
            if env_registry.default is not None
            else local_registry.default
        ),
    )


def _load_and_merge_registries(
    *,
    read_global_config: Callable[[], Any],
    environ: Mapping[str, str] | None = None,
) -> tuple[ModelRegistryConfig, ModelRegistryConfig | None]:
    """Load local + transported registries and merge them.

    Returns the merged registry and the raw transported registry (``None``
    when no env-var transport was present).  Callers that need to
    distinguish "transported" from "local-only" error messages use the
    second element; callers that just want the effective view ignore it.
    """
    env_registry = _read_env_model_registry(environ=environ)
    try:
        local_registry = _read_model_registry_config(
            read_global_config=read_global_config
        )
    except KitaruUsageError:
        if env_registry is None:
            raise
        local_registry = ModelRegistryConfig()
    merged = _merge_model_registries(
        local_registry=local_registry,
        env_registry=env_registry,
    )
    return merged, env_registry


def _effective_model_registry(
    *,
    read_global_config: Callable[[], Any],
    environ: Mapping[str, str] | None = None,
) -> ModelRegistryConfig:
    """Read the effective registry visible in the current environment."""
    merged, _ = _load_and_merge_registries(
        read_global_config=read_global_config,
        environ=environ,
    )
    return merged


def register_model_alias(
    alias: str,
    *,
    model: str,
    secret: str | None = None,
    update_global_config: Callable[[Callable[[Any], None]], Any],
    normalize_model_alias: Callable[[str], str] = _normalize_model_alias,
) -> ModelAliasEntry:
    """Register or update a local model alias for `kitaru.llm()`."""
    normalized_alias = normalize_model_alias(alias)

    def _mutate(global_config: Any) -> None:
        registry = (
            global_config.model_registry.model_copy(deep=True)
            if global_config.model_registry is not None
            else ModelRegistryConfig()
        )
        registry.aliases[normalized_alias] = ModelAliasConfig(
            model=model,
            secret=secret,
        )
        if registry.default is None:
            registry.default = normalized_alias
        global_config.model_registry = registry

    updated_global_config = update_global_config(_mutate)
    registry = updated_global_config.model_registry or ModelRegistryConfig()
    alias_config = registry.aliases[normalized_alias]
    return ModelAliasEntry(
        alias=normalized_alias,
        model=alias_config.model,
        secret=alias_config.secret,
        is_default=registry.default == normalized_alias,
    )


def list_model_aliases(
    *,
    read_global_config: Callable[[], Any],
    environ: Mapping[str, str] | None = None,
) -> list[ModelAliasEntry]:
    """List model aliases in stable order for CLI rendering."""
    registry = _effective_model_registry(
        read_global_config=read_global_config,
        environ=environ,
    )
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


def resolve_model_selection(
    model: str | None,
    *,
    read_global_config: Callable[[], Any],
    environ: Mapping[str, str] | None = None,
    default_model_env_name: str = KITARU_DEFAULT_MODEL_ENV,
    normalize_model_alias: Callable[[str], str] = _normalize_model_alias,
) -> ResolvedModelSelection:
    """Resolve an explicit/default model input to a concrete model string."""
    env = os.environ if environ is None else environ
    registry, transported_registry = _load_and_merge_registries(
        read_global_config=read_global_config,
        environ=env,
    )

    def _resolve_requested_model(requested_model: str) -> ResolvedModelSelection:
        if not requested_model:
            raise KitaruUsageError("Model identifier cannot be empty.")

        alias_candidate: str | None
        try:
            alias_candidate = normalize_model_alias(requested_model)
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

    env_default_model = env.get(default_model_env_name)
    if env_default_model is not None:
        stripped_env_default_model = env_default_model.strip()
        if not stripped_env_default_model:
            raise KitaruUsageError(f"`{default_model_env_name}` is set but empty.")
        return _resolve_requested_model(stripped_env_default_model)

    if not registry.aliases:
        if transported_registry is not None:
            raise KitaruUsageError(
                "No model alias is configured in the transported model registry. "
                "Check your local `kitaru model register` configuration and "
                f"resubmit the flow, set {default_model_env_name}, or pass a "
                "concrete model to kitaru.llm(...)."
            )
        raise KitaruUsageError(
            "No model alias is configured. Run `kitaru model register <alias> --model "
            f"<provider/model>` first, set {default_model_env_name}, or pass a "
            "concrete model to kitaru.llm(...)."
        )

    default_alias = registry.default
    if default_alias is None:
        if len(registry.aliases) == 1:
            default_alias = next(iter(registry.aliases))
        else:
            raise KitaruUsageError(
                "Multiple model aliases are configured but no default alias is set. "
                "Re-register one alias to restore a default."
            )

    if default_alias not in registry.aliases:
        if transported_registry is not None:
            raise KitaruUsageError(
                "The transported model registry default alias is missing. Check "
                "your local `kitaru model register` configuration and resubmit "
                "the flow."
            )
        raise KitaruUsageError(
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
