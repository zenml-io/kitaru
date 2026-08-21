#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""MCP-local fixed-target and credential resolution."""

import os
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from kitaru.api_models.v1.base import JsonValue
from kitaru.client.credential_store import CredentialStore


@dataclass(slots=True)
class ConnectionConfigurationError(Exception):
    """Bounded failure while resolving the MCP process connection."""

    kind: Literal["invalid_arguments", "invalid_configuration"]
    message: str
    details: dict[str, JsonValue] | None = None
    hint: str | None = None

    def __post_init__(self) -> None:
        """Initialize the `Exception` base with the error message."""
        Exception.__init__(self, self.message)


@dataclass(frozen=True, slots=True)
class MCPConnection:
    """Fixed process connection without exposed credential values."""

    server_url: str
    credential_source: Literal["environment", "stored", "none"]
    api_key: str | None
    credential_store: CredentialStore | None


def resolve_connection(server_url: str | None) -> MCPConnection:
    """Resolve one explicit URL and its ambient credential."""
    if server_url is None:
        raise ConnectionConfigurationError(
            "invalid_configuration",
            "No Kitaru server URL is configured for the MCP process.",
            hint="Pass --server or set KITARU_MCP_SERVER or KITARU_API_URL.",
        )
    normalized = _normalize_server_url(server_url)
    api_key = os.environ.get("KITARU_API_KEY") or None
    if api_key is not None:
        return MCPConnection(normalized, "environment", api_key, None)
    credential_store = CredentialStore()
    source = "stored" if credential_store.get(normalized) is not None else "none"
    return MCPConnection(normalized, source, None, credential_store)


def _normalize_server_url(value: str) -> str:
    """Validate and normalize an HTTP(S) server URL."""
    candidate = value.strip()
    parsed = urlsplit(candidate)
    try:
        _ = parsed.port
    except ValueError as error:
        raise ConnectionConfigurationError(
            "invalid_arguments", "The Kitaru server URL has an invalid port."
        ) from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ConnectionConfigurationError(
            "invalid_arguments",
            "The Kitaru server must be an HTTP(S) URL without credentials, "
            "a query, or a fragment.",
        )
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
