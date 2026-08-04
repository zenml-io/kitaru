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
"""CLI compatibility adapters for neutral connection configuration."""

from kitaru.cli.output import CLIError
from kitaru.client.connection import (
    CONFIG_FILE_NAME,
    CONFIG_KEYS,
    CONTEXT_NAME_PATTERN,
    ENV_CONFIG_PATH,
    CLISettings,
    ConfigurationMutationError,
    ConnectionConfigurationError,
    ContextConfig,
    LocalConfig,
    ResolvedCredential,
    ResolvedTarget,
    build_api_client,
    get_config_path,
    resolve_credential,
)
from kitaru.client.connection import ConfigStore as _ConfigStore
from kitaru.client.connection import resolve_target as _resolve_target
from kitaru.client.connection import validate_context_name as _validate_context_name
from kitaru.client.connection import validate_server_url as _validate_server_url


def _get_cli_error(
    error: ConnectionConfigurationError | ConfigurationMutationError,
) -> CLIError:
    """Translate one neutral configuration failure into the CLI contract."""
    if isinstance(error, ConnectionConfigurationError):
        return CLIError(
            error.kind,
            error.message,
            details=error.details,
            hint=error.hint,
        )
    return CLIError(error.kind, error.message)


class ConfigStore(_ConfigStore):
    """CLI-compatible configuration store backed by the neutral implementation."""

    def load(self) -> LocalConfig:
        """Load configuration and expose failures as ``CLIError``."""
        try:
            return super().load()
        except (ConnectionConfigurationError, ConfigurationMutationError) as error:
            raise _get_cli_error(error) from error

    def save(self, config: LocalConfig) -> None:
        """Save configuration and expose failures as ``CLIError``."""
        try:
            super().save(config)
        except (ConnectionConfigurationError, ConfigurationMutationError) as error:
            raise _get_cli_error(error) from error

    def set_machine_mode(self, value: bool) -> LocalConfig:
        """Set machine mode and expose failures as ``CLIError``."""
        try:
            return super().set_machine_mode(value)
        except (ConnectionConfigurationError, ConfigurationMutationError) as error:
            raise _get_cli_error(error) from error

    def add_context(
        self, name: str, server_url: str, *, activate: bool = False
    ) -> LocalConfig:
        """Add a context and expose failures as ``CLIError``."""
        try:
            return super().add_context(name, server_url, activate=activate)
        except (ConnectionConfigurationError, ConfigurationMutationError) as error:
            raise _get_cli_error(error) from error

    def use_context(self, name: str) -> LocalConfig:
        """Select a context and expose failures as ``CLIError``."""
        try:
            return super().use_context(name)
        except (ConnectionConfigurationError, ConfigurationMutationError) as error:
            raise _get_cli_error(error) from error

    def remove_context(self, name: str, *, force: bool = False) -> LocalConfig:
        """Remove a context and expose failures as ``CLIError``."""
        try:
            return super().remove_context(name, force=force)
        except (ConnectionConfigurationError, ConfigurationMutationError) as error:
            raise _get_cli_error(error) from error


def validate_context_name(name: str, *, configuration: bool = False) -> str:
    """Validate a context name and expose failures as ``CLIError``."""
    try:
        return _validate_context_name(name, configuration=configuration)
    except ConnectionConfigurationError as error:
        raise _get_cli_error(error) from error


def validate_server_url(url: str, *, configuration: bool = False) -> str:
    """Validate a server URL and expose failures as ``CLIError``."""
    try:
        return _validate_server_url(url, configuration=configuration)
    except ConnectionConfigurationError as error:
        raise _get_cli_error(error) from error


def resolve_target(
    store: ConfigStore,
    *,
    explicit_server: str | None = None,
    context_name: str | None = None,
) -> ResolvedTarget:
    """Resolve a CLI target and expose failures as ``CLIError``."""
    try:
        return _resolve_target(
            store,
            explicit_server=explicit_server,
            context_name=context_name,
        )
    except ConnectionConfigurationError as error:
        raise _get_cli_error(error) from error


__all__ = [
    "CONFIG_FILE_NAME",
    "CONFIG_KEYS",
    "CONTEXT_NAME_PATTERN",
    "ENV_CONFIG_PATH",
    "CLISettings",
    "ConfigStore",
    "ContextConfig",
    "LocalConfig",
    "ResolvedCredential",
    "ResolvedTarget",
    "build_api_client",
    "get_config_path",
    "resolve_credential",
    "resolve_target",
    "validate_context_name",
    "validate_server_url",
]
