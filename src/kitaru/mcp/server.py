#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Lazy MCP SDK v2 server construction and stdio runtime."""

import argparse
import asyncio
import logging
import sys
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager

from mcp.server import MCPServer
from pydantic import ValidationError

from kitaru.analytics.source import AnalyticsSource
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.connection import (
    ConfigStore,
    build_api_client,
    resolve_credential,
    resolve_target,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.mcp.lifecycle import MCPConnection, MCPServerState
from kitaru.mcp.registry import register_tools
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from kitaru.redaction import redact

ClientFactory = Callable[[], KitaruAPIClient]
logger = logging.getLogger("kitaru.mcp")


def create_server(
    settings: MCPSettings,
    connection: MCPConnection | None = None,
    *,
    client_factory: ClientFactory | None = None,
) -> MCPServer[MCPServerState]:
    """Create a capability-filtered server without opening a client or network."""

    @asynccontextmanager
    async def lifespan(
        _server: MCPServer[MCPServerState],
    ) -> AsyncIterator[MCPServerState]:
        if connection is None:
            raise RuntimeError(
                "A resolved connection is required to run the MCP server."
            )
        client = (
            client_factory()
            if client_factory is not None
            else _build_client(settings, connection)
        )
        state = MCPServerState(settings=settings, connection=connection, client=client)
        try:
            yield state
        finally:
            await state.close()

    server = MCPServer[MCPServerState](
        "kitaru",
        description="Bounded typed access to Kitaru v2.",
        debug=settings.debug,
        log_level="DEBUG" if settings.debug else "WARNING",
        lifespan=lifespan,
    )
    register_tools(server, settings.mode)
    return server


async def run_stdio(settings: MCPSettings) -> None:
    """Resolve one fixed connection and run protocol-only stdio."""
    connection = resolve_connection(settings)
    logger.warning(
        "Kitaru MCP target=%s target_source=%s credential_source=%s",
        redact(connection.target.server_url),
        connection.target.source,
        connection.credential.source,
    )
    server = create_server(settings, connection)
    await server.run_stdio_async()


def resolve_connection(settings: MCPSettings) -> MCPConnection:
    """Resolve the fixed target and credential before protocol traffic."""
    config_store = ConfigStore()
    credential_store = CredentialStore()
    target = resolve_target(
        config_store,
        explicit_server=settings.server_url,
        context_name=settings.context_name,
    )
    credential = resolve_credential(target.server_url, credential_store)
    return MCPConnection(target, credential, credential_store)


def _build_client(settings: MCPSettings, connection: MCPConnection) -> KitaruAPIClient:
    return build_api_client(
        connection.target.server_url,
        connection.credential,
        connection.credential_store,
        settings.timeout,
        retries=settings.retries,
        pool_size=settings.pool_size,
        source=AnalyticsSource.MCP,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Parse runtime settings and run the local stdio server."""
    _configure_logging(False)
    try:
        arguments = _parse_arguments(argv)
        settings = MCPSettings.from_environment(None, **arguments)
        _configure_logging(settings.debug)
        asyncio.run(run_stdio(settings))
    except SystemExit as error:
        return int(error.code) if isinstance(error.code, int) else 2
    except (ValidationError, ValueError) as error:
        print(redact(str(error)), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 0
    except Exception as error:
        print(redact(str(error)), file=sys.stderr)
        return 2
    return 0


def _parse_arguments(argv: Sequence[str] | None) -> dict[str, object]:
    parser = argparse.ArgumentParser(prog="kitaru-mcp", add_help=False)
    parser.add_argument("--mode", choices=[mode.value for mode in CapabilityMode])
    parser.add_argument("--server", dest="server_url")
    parser.add_argument("--context", dest="context_name")
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--handler-timeout", type=float, dest="handler_timeout")
    parser.add_argument("--retries", type=int)
    parser.add_argument("--pool-size", type=int, dest="pool_size")
    parser.add_argument("--max-concurrency", type=int, dest="max_concurrency")
    parser.add_argument("--debug", action="store_true", default=None)
    namespace = parser.parse_args(list(argv or ()))
    return {key: value for key, value in vars(namespace).items() if value is not None}


def _configure_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.WARNING,
        stream=sys.stderr,
        force=True,
        format="%(levelname)s %(name)s: %(message)s",
    )
