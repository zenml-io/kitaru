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
"""CLI configuration access and connection resolution."""

import os
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import ValidationError

from kitaru.cli.output import CLIError
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.config import (
    ClientConfig,
    get_config_path,
    get_server_url,
    save_config,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ServerCredentials

CONFIG_KEYS = ("cli.machine_mode",)


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    """Server URL selected for one invocation."""

    server_url: str
    source: Literal["explicit", "environment", "stored"]


@dataclass(frozen=True, slots=True)
class ResolvedCredential:
    """Credential selected for one server without exposing its value."""

    source: Literal["environment", "stored", "none"]
    api_key: str | None = None
    stored: ServerCredentials | None = None


def read_config() -> ClientConfig:
    """Load the configuration, refusing a malformed document."""
    path = get_config_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ClientConfig()
    except OSError as error:
        raise CLIError(
            "invalid_configuration",
            f"Cannot read CLI config at {path}: {error}",
        ) from error
    try:
        return ClientConfig.model_validate_json(raw)
    except (ValueError, ValidationError) as error:
        details = None
        if isinstance(error, ValidationError):
            details = [
                {
                    "type": issue["type"],
                    "loc": [str(part) for part in issue["loc"]],
                    "message": issue["msg"],
                }
                for issue in error.errors(include_url=False, include_input=False)[:10]
            ]
        raise CLIError(
            "invalid_configuration",
            f"CLI config at {path} is invalid.",
            details=details,
            hint="Fix or remove the file, then retry.",
        ) from error


def write_config(config: ClientConfig) -> None:
    """Write the configuration, reporting a failed write as a CLI error."""
    path = get_config_path()
    try:
        save_config(config)
    except OSError as error:
        raise CLIError(
            "invalid_configuration",
            f"Cannot write CLI config at {path}: {error}",
        ) from error


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
    """Resolve a server from an explicit option, environment, or client config."""
    if explicit_server is not None:
        return ResolvedTarget(validate_server_url(explicit_server), "explicit")
    environment = os.environ.get("KITARU_API_URL")
    if environment:
        return ResolvedTarget(
            validate_server_url(environment, configuration=True), "environment"
        )
    stored = get_server_url()
    if stored:
        return ResolvedTarget(validate_server_url(stored, configuration=True), "stored")
    raise CLIError(
        "invalid_configuration",
        "No Kitaru server was resolved.",
        hint="Pass --server, set KITARU_API_URL, or run `kitaru login`.",
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
    "CONFIG_KEYS",
    "ResolvedCredential",
    "ResolvedTarget",
    "build_api_client",
    "read_config",
    "resolve_credential",
    "resolve_target",
    "validate_server_url",
    "write_config",
]
