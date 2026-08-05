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
"""CLI-local presentation configuration and connection resolution."""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from kitaru.cli.output import CLIError
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
CONFIG_KEYS = ("cli.machine_mode",)


class CLISettings(BaseModel):
    """Persisted CLI presentation preferences."""

    model_config = ConfigDict(extra="forbid")

    machine_mode: bool = False


class LocalConfig(BaseModel):
    """Versioned CLI-local configuration document.

    Unknown top-level fields are ignored so older context-bearing files remain
    readable. The next CLI config write drops those obsolete fields.
    """

    model_config = ConfigDict(extra="ignore")

    schema_version: Literal[1] = 1
    cli: CLISettings = Field(default_factory=CLISettings)


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    """Server URL selected for one invocation."""

    server_url: str
    source: Literal["explicit", "environment"]


@dataclass(frozen=True, slots=True)
class ResolvedCredential:
    """Credential selected for one server without exposing its value."""

    source: Literal["environment", "stored", "none"]
    api_key: str | None = None
    stored: ServerCredentials | None = None


class ConfigStore:
    """Atomically persist the small CLI-only configuration document."""

    def __init__(self, path: Path | None = None) -> None:
        """Initialize the store."""
        override = os.environ.get(ENV_CONFIG_PATH)
        self.path = path or get_config_path()
        self._secure_parent = path is None and override is None

    @property
    def manages_parent_directory(self) -> bool:
        """Report whether the CLI owns the config file's parent directory."""
        return self._secure_parent

    def load(self) -> LocalConfig:
        """Load the current CLI preferences."""
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return LocalConfig()
        except OSError as error:
            raise CLIError(
                "invalid_configuration",
                f"Cannot read CLI config at {self.path}: {error}",
            ) from error
        try:
            return LocalConfig.model_validate_json(raw)
        except (ValueError, ValidationError) as error:
            details = None
            if isinstance(error, ValidationError):
                details = [
                    {
                        "type": issue["type"],
                        "loc": [str(part) for part in issue["loc"]],
                        "message": issue["msg"],
                    }
                    for issue in error.errors(include_url=False, include_input=False)[
                        :10
                    ]
                ]
            raise CLIError(
                "invalid_configuration",
                f"CLI config at {self.path} is invalid.",
                details=details,
                hint="Fix or remove the file, then retry.",
            ) from error

    def save(self, config: LocalConfig) -> None:
        """Write the complete CLI preference document atomically."""
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
            raise CLIError(
                "invalid_configuration",
                f"Cannot write CLI config at {self.path}: {error}",
            ) from error

    def set_machine_mode(self, value: bool) -> LocalConfig:
        """Persist the machine-rendering preference."""
        config = self.load()
        config.cli.machine_mode = value
        self.save(config)
        return config


def get_config_path() -> Path:
    """Return the CLI config path without reading it."""
    override = os.environ.get(ENV_CONFIG_PATH)
    return Path(override) if override else get_config_directory() / CONFIG_FILE_NAME


def validate_server_url(url: str, *, configuration: bool = False) -> str:
    """Validate and normalize an HTTP(S) server base URL."""
    kind = "invalid_configuration" if configuration else "invalid_arguments"
    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError as error:
        raise CLIError(kind, f"Invalid server URL: {error}") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise CLIError(
            kind,
            "Server URL must be an absolute HTTP(S) URL with a host and no "
            "credentials, query, or fragment.",
        )
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def resolve_target(*, explicit_server: str | None = None) -> ResolvedTarget:
    """Resolve a server from an explicit option or ``KITARU_API_URL``."""
    if explicit_server is not None:
        return ResolvedTarget(validate_server_url(explicit_server), "explicit")
    environment = os.environ.get("KITARU_API_URL")
    if environment:
        return ResolvedTarget(
            validate_server_url(environment, configuration=True), "environment"
        )
    raise CLIError(
        "invalid_configuration",
        "No Kitaru server was resolved.",
        hint="Pass --server or set KITARU_API_URL.",
    )


def resolve_credential(
    server_url: str, credential_store: CredentialStore
) -> ResolvedCredential:
    """Resolve an environment or stored credential for one server."""
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
) -> KitaruAPIClient:
    """Build one SDK client from resolved credential provenance."""
    if credential.source == "environment":
        return KitaruAPIClient(
            base_url=server_url,
            api_key=credential.api_key,
            timeout=timeout,
            retries=retries,
            pool_size=pool_size,
        )
    if credential.source == "stored":
        return KitaruAPIClient(
            base_url=server_url,
            credential_store=credential_store,
            timeout=timeout,
            retries=retries,
            pool_size=pool_size,
        )
    return KitaruAPIClient(
        base_url=server_url,
        timeout=timeout,
        retries=retries,
        pool_size=pool_size,
    )


__all__ = [
    "CONFIG_FILE_NAME",
    "CONFIG_KEYS",
    "ENV_CONFIG_PATH",
    "CLISettings",
    "ConfigStore",
    "LocalConfig",
    "ResolvedCredential",
    "ResolvedTarget",
    "build_api_client",
    "get_config_path",
    "resolve_credential",
    "resolve_target",
    "validate_server_url",
]
