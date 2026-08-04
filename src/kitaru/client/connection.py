#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Neutral persisted connection configuration and target resolution."""

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from kitaru.analytics.source import AnalyticsSource
from kitaru.api_models.v1.base import JsonValue
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import (
    DIRECTORY_MODE,
    FILE_MODE,
    CredentialStore,
    get_config_directory,
)
from kitaru.client.credentials import ServerCredentials

ENV_CONFIG_PATH = "KITARU_CONFIG_PATH"
CONFIG_FILE_NAME = "config.json"
CONTEXT_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")
CONFIG_KEYS = ("cli.machine_mode",)

ConnectionErrorKind = Literal["invalid_arguments", "invalid_configuration"]
ConnectionErrorDetails = dict[str, JsonValue] | list[dict[str, JsonValue]]


@dataclass(slots=True)
class ConnectionConfigurationError(Exception):
    """Bounded failure while validating or loading connection configuration."""

    kind: ConnectionErrorKind
    message: str
    details: ConnectionErrorDetails | None = None
    hint: str | None = None

    def __post_init__(self) -> None:
        """Initialize the base exception message."""
        Exception.__init__(self, self.message)


@dataclass(slots=True)
class ConfigurationMutationError(Exception):
    """CLI-facing conflict produced by a persisted configuration mutation."""

    kind: Literal["not_found", "conflict"]
    message: str

    def __post_init__(self) -> None:
        """Initialize the base exception message."""
        Exception.__init__(self, self.message)


class ContextConfig(BaseModel):
    """One named server target."""

    model_config = ConfigDict(extra="forbid")

    server_url: str


class CLISettings(BaseModel):
    """Persisted presentation preferences."""

    model_config = ConfigDict(extra="forbid")

    machine_mode: bool = False


class LocalConfig(BaseModel):
    """Versioned global CLI configuration document."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    active_context: str | None = None
    contexts: dict[str, ContextConfig] = Field(default_factory=dict)
    cli: CLISettings = Field(default_factory=CLISettings)


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    """Server URL selected for one invocation."""

    server_url: str
    source: Literal["explicit", "context", "environment", "active_context"]
    context_name: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedCredential:
    """Credential selected for one server without exposing it to output."""

    source: Literal["environment", "stored", "none"]
    api_key: str | None = None
    stored: ServerCredentials | None = None


class ConfigStore:
    """Atomically persisted CLI configuration."""

    def __init__(self, path: Path | None = None) -> None:
        """Initialize the store.

        Args:
            path: Configuration path. Defaults to ``KITARU_CONFIG_PATH`` or
                the Kitaru XDG configuration directory.
        """
        override = os.environ.get(ENV_CONFIG_PATH)
        self.path = path or get_config_path()
        self._secure_parent = path is None and override is None

    @property
    def manages_parent_directory(self) -> bool:
        """Report whether the CLI owns the config file's parent directory."""
        return self._secure_parent

    def load(self) -> LocalConfig:
        """Load and validate the current configuration.

        Raises:
            ConnectionConfigurationError: The file is unreadable or invalid.

        Returns:
            Parsed configuration, or defaults when the file does not exist.
        """
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return LocalConfig()
        except OSError as error:
            raise ConnectionConfigurationError(
                "invalid_configuration",
                f"Cannot read CLI config at {self.path}: {error}",
            ) from error
        try:
            payload = json.loads(raw)
            config = LocalConfig.model_validate(payload)
        except (ValueError, ValidationError) as error:
            details = (
                _get_validation_details(error)
                if isinstance(error, ValidationError)
                else None
            )
            raise ConnectionConfigurationError(
                "invalid_configuration",
                f"CLI config at {self.path} is invalid.",
                details=details,
                hint="Fix or remove the file, then retry.",
            ) from error
        self._validate_document(config)
        return config

    def save(self, config: LocalConfig) -> None:
        """Write a complete validated configuration atomically.

        Args:
            config: Configuration to persist.

        Raises:
            ConnectionConfigurationError: Validation or persistence failed.
        """
        self._validate_document(config)
        try:
            parent_existed = self.path.parent.exists()
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
            if os.name == "posix" and (self._secure_parent or not parent_existed):
                os.chmod(self.path.parent, DIRECTORY_MODE)
            handle, temporary = tempfile.mkstemp(
                dir=self.path.parent, prefix=f".{self.path.name}."
            )
            try:
                with os.fdopen(handle, "w", encoding="utf-8") as file:
                    json.dump(
                        config.model_dump(mode="json"),
                        file,
                        indent=2,
                        sort_keys=True,
                    )
                    file.write("\n")
                if os.name == "posix":
                    os.chmod(temporary, FILE_MODE)
                os.replace(temporary, self.path)
            except OSError:
                Path(temporary).unlink(missing_ok=True)
                raise
        except OSError as error:
            raise ConnectionConfigurationError(
                "invalid_configuration",
                f"Cannot write CLI config at {self.path}: {error}",
            ) from error

    def set_machine_mode(self, value: bool) -> LocalConfig:
        """Persist the allowlisted machine-rendering preference.

        Args:
            value: New preference.

        Returns:
            Updated configuration.
        """
        config = self.load()
        config.cli.machine_mode = value
        self.save(config)
        return config

    def add_context(
        self, name: str, server_url: str, *, activate: bool = False
    ) -> LocalConfig:
        """Add or update a named server context.

        Args:
            name: Context name.
            server_url: Server base URL.
            activate: Whether to select this context.

        Returns:
            Updated configuration.
        """
        validate_context_name(name)
        server_url = validate_server_url(server_url)
        config = self.load()
        config.contexts[name] = ContextConfig(server_url=server_url)
        if activate:
            config.active_context = name
        self.save(config)
        return config

    def use_context(self, name: str) -> LocalConfig:
        """Select an existing context.

        Args:
            name: Context name.

        Raises:
            ConnectionConfigurationError: The context does not exist.

        Returns:
            Updated configuration.
        """
        validate_context_name(name)
        config = self.load()
        if name not in config.contexts:
            raise ConnectionConfigurationError(
                "invalid_configuration", f"Context {name!r} does not exist."
            )
        config.active_context = name
        self.save(config)
        return config

    def remove_context(self, name: str, *, force: bool = False) -> LocalConfig:
        """Remove a context without touching credentials.

        Args:
            name: Context name.
            force: Clear the active pointer when removing the selected context.

        Raises:
            ConnectionConfigurationError: The context is absent or active without
                ``force``.

        Returns:
            Updated configuration.
        """
        validate_context_name(name)
        config = self.load()
        if name not in config.contexts:
            raise ConfigurationMutationError(
                "not_found", f"Context {name!r} does not exist."
            )
        if config.active_context == name and not force:
            raise ConfigurationMutationError(
                "conflict",
                f"Context {name!r} is active and cannot be removed without --force.",
            )
        del config.contexts[name]
        if config.active_context == name:
            config.active_context = None
        self.save(config)
        return config

    @staticmethod
    def _validate_document(config: LocalConfig) -> None:
        """Validate cross-field and normalized-value invariants."""
        if config.active_context is not None:
            validate_context_name(config.active_context, configuration=True)
        for name, context in config.contexts.items():
            validate_context_name(name, configuration=True)
            normalized = validate_server_url(context.server_url, configuration=True)
            if normalized != context.server_url:
                raise ConnectionConfigurationError(
                    "invalid_configuration",
                    f"Context {name!r} contains a non-normalized server URL.",
                    hint=f"Use {normalized!r} instead.",
                )


def _get_validation_details(error: ValidationError) -> list[dict[str, JsonValue]]:
    """Return bounded validation diagnostics without echoing input values."""
    return [
        {
            "type": issue["type"],
            "loc": [str(part) for part in issue["loc"]],
            "message": issue["msg"],
        }
        for issue in error.errors(include_url=False, include_input=False)[:10]
    ]


def get_config_path() -> Path:
    """Return the CLI config path without reading the file.

    Returns:
        Explicit override or the XDG-based default path.
    """
    override = os.environ.get(ENV_CONFIG_PATH)
    return Path(override) if override else get_config_directory() / CONFIG_FILE_NAME


def validate_context_name(name: str, *, configuration: bool = False) -> str:
    """Validate a context name.

    Args:
        name: Candidate context name.
        configuration: Whether the value came from persisted configuration.

    Raises:
        ConnectionConfigurationError: The value is invalid.

    Returns:
        The unchanged name.
    """
    if not CONTEXT_NAME_PATTERN.fullmatch(name):
        kind = "invalid_configuration" if configuration else "invalid_arguments"
        raise ConnectionConfigurationError(
            kind,
            "Context names must start with a letter and contain at most 64 "
            "letters, digits, dots, underscores, or hyphens.",
        )
    return name


def validate_server_url(url: str, *, configuration: bool = False) -> str:
    """Validate and normalize an HTTP(S) server base URL.

    Args:
        url: Candidate URL.
        configuration: Whether the value came from persisted configuration.

    Raises:
        ConnectionConfigurationError: The value is unsafe or malformed.

    Returns:
        URL with trailing slashes removed from its path.
    """
    kind = "invalid_configuration" if configuration else "invalid_arguments"
    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError as error:
        raise ConnectionConfigurationError(
            kind, f"Invalid server URL: {error}"
        ) from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ConnectionConfigurationError(
            kind,
            "Server URL must be an absolute HTTP(S) URL with a host and no "
            "credentials, query, or fragment.",
        )
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def resolve_target(
    store: ConfigStore,
    *,
    explicit_server: str | None = None,
    context_name: str | None = None,
) -> ResolvedTarget:
    """Resolve one server using the documented precedence chain.

    Args:
        store: Local configuration store.
        explicit_server: Command-specific or global server URL.
        context_name: Explicit named context.

    Raises:
        ConnectionConfigurationError: No valid target can be resolved.

    Returns:
        Selected normalized target and provenance.
    """
    if explicit_server is not None:
        return ResolvedTarget(validate_server_url(explicit_server), "explicit")
    if context_name is not None:
        validate_context_name(context_name)
        config = store.load()
        context = config.contexts.get(context_name)
        if context is None:
            raise ConnectionConfigurationError(
                "invalid_configuration",
                f"Selected context {context_name!r} does not exist.",
            )
        return ResolvedTarget(context.server_url, "context", context_name)
    environment = os.environ.get("KITARU_API_URL")
    if environment:
        return ResolvedTarget(
            validate_server_url(environment, configuration=True), "environment"
        )
    config = store.load()
    if config.active_context:
        context = config.contexts.get(config.active_context)
        if context is None:
            raise ConnectionConfigurationError(
                "invalid_configuration",
                f"Active context {config.active_context!r} does not exist.",
                hint="Use `kitaru context use NAME` or remove the stale pointer.",
            )
        return ResolvedTarget(
            context.server_url, "active_context", config.active_context
        )
    raise ConnectionConfigurationError(
        "invalid_configuration",
        "No Kitaru server is configured.",
        hint="Pass --server, set KITARU_API_URL, or select a context.",
    )


def resolve_credential(
    server_url: str, credential_store: CredentialStore
) -> ResolvedCredential:
    """Resolve one credential without exposing its value.

    Args:
        server_url: Normalized selected server URL.
        credential_store: Credential store to query.

    Returns:
        Credential and provenance.
    """
    api_key = os.environ.get("KITARU_API_KEY")
    if api_key:
        return ResolvedCredential("environment", api_key=api_key)
    stored = credential_store.get(server_url)
    if stored is not None:
        return ResolvedCredential("stored", stored=stored)
    return ResolvedCredential("none")


def build_api_client(
    server_url: str,
    credential: ResolvedCredential,
    credential_store: CredentialStore,
    timeout: float,
    *,
    retries: int = 3,
    pool_size: int = 20,
    source: AnalyticsSource = AnalyticsSource.PYTHON,
) -> KitaruAPIClient:
    """Build one SDK client from resolved credential provenance."""
    if credential.source == "environment":
        return KitaruAPIClient(
            base_url=server_url,
            api_key=credential.api_key,
            timeout=timeout,
            retries=retries,
            pool_size=pool_size,
            source=source,
        )
    if credential.source == "stored":
        return KitaruAPIClient(
            base_url=server_url,
            credential_store=credential_store,
            timeout=timeout,
            retries=retries,
            pool_size=pool_size,
            source=source,
        )
    return KitaruAPIClient(
        base_url=server_url,
        timeout=timeout,
        retries=retries,
        pool_size=pool_size,
        source=source,
    )
