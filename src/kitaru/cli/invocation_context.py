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
"""Per-invocation settings shared by CLI commands."""

from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TextIO

from kitaru.cli import registration
from kitaru.cli.config import ResolvedTarget, resolve_target, validate_server_url
from kitaru.cli.output import CLIError
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore


@dataclass(slots=True)
class Invocation:
    """Resolved non-secret settings and lazy stores for one invocation."""

    server: str | None
    request_timeout: float
    non_interactive: bool
    no_browser: bool
    stdin: TextIO
    _credential_store: CredentialStore | None = None
    _resolved_target: ResolvedTarget | None = None

    @property
    def credential_store(self) -> CredentialStore:
        """Return the lazily constructed credential store."""
        if self._credential_store is None:
            self._credential_store = CredentialStore()
        return self._credential_store

    @property
    def resolved_server_url(self) -> str | None:
        """Return the server resolved during this invocation, if any."""
        if self._resolved_target is None:
            return None
        return self._resolved_target.server_url

    def resolve_target(self, explicit_server: str | None = None) -> ResolvedTarget:
        """Resolve a server from command and global inputs."""
        if explicit_server and self.server:
            command_url = validate_server_url(explicit_server)
            global_url = validate_server_url(self.server)
            if command_url != global_url:
                raise CLIError(
                    "invalid_arguments",
                    "The command SERVER and global --server identify "
                    "different servers.",
                )
        target = resolve_target(explicit_server=explicit_server or self.server)
        self._resolved_target = target
        return target


INVOCATION: ContextVar[Invocation | None] = ContextVar(
    "kitaru_cli_invocation", default=None
)


def get_invocation() -> Invocation:
    """Return the current command invocation."""
    value = INVOCATION.get()
    if value is None:
        raise RuntimeError("No CLI invocation is active")
    return value


def open_asset_client() -> AbstractAsyncContextManager[KitaruAPIClient]:
    """Open an SDK client for the invocation's resolved target."""
    invocation = get_invocation()
    target = invocation.resolve_target()
    return registration.open_client(
        target.server_url, invocation.credential_store, invocation.request_timeout
    )
