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
"""Selection and authentication of the destination of a workspace push."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from kitaru.analytics.source import AnalyticsSource
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli.config import (
    ResolvedCredential,
    build_api_client,
    validate_server_url,
)
from kitaru.cli.diagnostics import get_server_info
from kitaru.cli.output import CLIError
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore


@asynccontextmanager
async def open_destination_client(
    destination: str,
    source: KitaruAPIClient,
    *,
    source_url: str,
    credential_store: CredentialStore,
    timeout: float,
) -> AsyncGenerator[KitaruAPIClient]:
    """Open an SDK client for the destination once it proves to be another server.

    Both servers describe themselves through the unauthenticated info
    endpoint first, so the check also catches two URLs of one server.

    Args:
        destination: Destination server URL.
        source: Client for the source server.
        source_url: Source server URL.
        credential_store: Store holding the destination's credential.
        timeout: HTTP timeout in seconds.

    Yields:
        Client for the destination, authenticated when it requires it.
    """
    probe = build_destination_client(
        destination, ResolvedCredential("none"), credential_store, timeout
    )
    try:
        destination_info = await get_server_info(probe, destination)
    finally:
        await probe.close()
    source_info = await get_server_info(source, source_url)
    check_distinct_servers(source_info, destination_info, destination)
    credential = select_destination_credential(
        destination_info, destination, credential_store
    )
    client = build_destination_client(
        destination, credential, credential_store, timeout
    )
    try:
        yield client
    finally:
        await client.close()


def build_destination_client(
    destination: str,
    credential: ResolvedCredential,
    credential_store: CredentialStore,
    timeout: float,
) -> KitaruAPIClient:
    """Build a client for the destination that authenticates only as selected.

    Args:
        destination: Destination server URL.
        credential: Credential selected for the destination.
        credential_store: Store holding the destination's credential.
        timeout: HTTP timeout in seconds.

    Returns:
        Client for the destination.
    """
    if credential.source != "none":
        return build_api_client(destination, credential, credential_store, timeout)
    # An SDK client given neither a key nor a store falls back to
    # KITARU_API_TOKEN and KITARU_API_KEY, which belong to the source, so an
    # empty in-memory store keeps the source's key off the destination.
    return KitaruAPIClient(
        base_url=destination,
        credential_store=CredentialStore(persist=False),
        timeout=timeout,
        analytics_source=AnalyticsSource.CLI,
    )


def check_distinct_servers(
    source: ServerInfoResponse, destination: ServerInfoResponse, destination_url: str
) -> None:
    """Refuse a destination that reports the server id of the source.

    Args:
        source: Info of the source server.
        destination: Info of the destination server.
        destination_url: Destination server URL.

    Raises:
        CLIError: Both servers report the same id.
    """
    if source.id is not None and source.id == destination.id:
        raise CLIError(
            "invalid_arguments",
            f"The destination {destination_url} is the source server: both "
            f"report the server id {source.id}.",
            hint="Select the source with --server and name another server in --to.",
        )


def select_destination_credential(
    info: ServerInfoResponse, destination: str, credential_store: CredentialStore
) -> ResolvedCredential:
    """Select how the push authenticates against the destination.

    ``KITARU_API_KEY`` is ignored here because it authenticates the source
    server of the same invocation.

    Args:
        info: Info of the destination server.
        destination: Destination server URL.
        credential_store: Store holding the destination's credential.

    Raises:
        CLIError: The destination requires authentication and no credential
            is stored for it.

    Returns:
        No credential for a destination without authentication, otherwise
        the stored one.
    """
    if info.auth_scheme is AuthScheme.NONE:
        return ResolvedCredential("none")
    stored = credential_store.get(destination)
    if stored is None:
        raise CLIError(
            "authentication_failed",
            f"No stored credential for the destination {destination}.",
            hint=build_login_hint(destination),
        )
    return ResolvedCredential("stored", stored=stored)


def validate_destination(destination: str, source_url: str) -> str:
    """Normalize the destination URL and require it to differ from the source."""
    destination_url = validate_server_url(destination)
    if destination_url == source_url:
        raise CLIError(
            "invalid_arguments",
            f"The destination {destination_url} is the source server.",
            hint="Select the source with --server and name another server in --to.",
        )
    return destination_url


def build_login_hint(destination: str) -> str:
    """Build the hint for a destination that needs a fresh credential."""
    return (
        f"Run `kitaru login {destination}`, then rerun the push with `--server` "
        "naming the source."
    )
