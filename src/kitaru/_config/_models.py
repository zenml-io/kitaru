"""Local model alias registry helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from kitaru._config._env import KITARU_DEFAULT_MODEL_ENV

_MODEL_ALIAS_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


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


def _read_model_registry_config(
    *,
    read_global_config: Callable[[], Any],
) -> ModelRegistryConfig:
    """Read the local model registry from global config."""
    global_config = read_global_config()
    if global_config.model_registry is None:
        return ModelRegistryConfig()
    return global_config.model_registry


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
) -> list[ModelAliasEntry]:
    """List local model aliases in stable order for CLI rendering."""
    registry = _read_model_registry_config(read_global_config=read_global_config)
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
    """Resolve an explicit/default model input to a concrete LiteLLM model."""
    registry = _read_model_registry_config(read_global_config=read_global_config)
    env = os.environ if environ is None else environ

    def _resolve_requested_model(requested_model: str) -> ResolvedModelSelection:
        if not requested_model:
            raise ValueError("Model identifier cannot be empty.")

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
            raise ValueError(f"`{default_model_env_name}` is set but empty.")
        return _resolve_requested_model(stripped_env_default_model)

    if not registry.aliases:
        raise ValueError(
            "No model alias is configured. Run `kitaru model register <alias> --model "
            f"<provider/model>` first, set {default_model_env_name}, or pass a "
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
