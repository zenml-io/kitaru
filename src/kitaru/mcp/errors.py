#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Canonical MCP results and stable redacted exception mapping."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import httpx
from mcp.types import CallToolResult, TextContent
from pydantic import ValidationError

from kitaru.api_models.v1.base import JsonValue
from kitaru.client.exceptions import APIError
from kitaru.mcp.connection import ConnectionConfigurationError
from kitaru.mcp.models.common import ToolError, ToolResult
from kitaru.mcp.redaction import redact, redact_data
from kitaru.mcp.references import ReferenceResolutionError


@dataclass(slots=True)
class MCPToolError(Exception):
    """Expected handler failure with stable MCP semantics."""

    code: str
    message: str
    retryable: bool = False
    details: dict[str, JsonValue] | None = None
    recovery: str | None = None

    def __post_init__(self) -> None:
        """Initialize the `Exception` base with the error message."""
        Exception.__init__(self, self.message)


@dataclass(slots=True)
class MCPOutputValidationError(Exception):
    """Pydantic failure produced after validated tool arguments entered a handler."""

    validation_error: ValidationError

    def __post_init__(self) -> None:
        """Initialize the `Exception` base with a fixed validation message."""
        Exception.__init__(self, "MCP handler output validation failed")


def success_result(
    result_type: type[ToolResult],
    data: JsonValue,
    *,
    warnings: list[str] | None = None,
    links: Mapping[str, str] | None = None,
) -> ToolResult:
    """Build a versioned success envelope."""
    if warnings is None and links is None:
        return result_type(ok=True, data=data)
    values: dict[str, object] = {"ok": True, "data": data}
    if warnings:
        values["warnings"] = warnings
    if links:
        values["links"] = dict(links)
    return result_type.model_validate(values)


def error_result(result_type: type[ToolResult], error: BaseException) -> ToolResult:
    """Map an expected or unexpected exception to a bounded envelope."""
    mapped = map_exception(error)
    return result_type(
        ok=False,
        error=ToolError(
            code=mapped.code,
            message=redact(mapped.message),
            retryable=mapped.retryable,
            details=cast(dict[str, JsonValue] | None, redact_data(mapped.details)),
            recovery=redact(mapped.recovery) if mapped.recovery else None,
        ),
    )


def protocol_result(envelope: ToolResult) -> CallToolResult:
    """Render identical redacted canonical JSON as structured and text content."""
    dumped = envelope.model_dump(mode="json")
    structured = cast(dict[str, JsonValue], redact_data(dumped))
    text = json.dumps(structured, sort_keys=True, separators=(",", ":"))
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=structured,
        is_error=not envelope.ok,
    )


def map_exception(error: BaseException) -> MCPToolError:
    """Map client, validation, transport, and local failures."""
    if isinstance(error, MCPToolError):
        return error
    if isinstance(error, MCPOutputValidationError):
        return MCPToolError(
            "internal_error", "The Kitaru response failed MCP output validation."
        )
    if isinstance(error, ConnectionConfigurationError):
        return MCPToolError(
            error.kind,
            error.message,
            details=_details(error.details),
            recovery=error.hint,
        )
    if isinstance(error, ReferenceResolutionError):
        return MCPToolError(error.code, error.message, details=error.details)
    if isinstance(error, ValidationError):
        details: dict[str, JsonValue] = {
            "issues": [
                {
                    "type": issue["type"],
                    "location": [str(part) for part in issue["loc"]],
                    "message": issue["msg"],
                }
                for issue in error.errors(include_input=False, include_url=False)[:10]
            ]
        }
        return MCPToolError(
            "invalid_arguments", "Tool arguments are invalid.", details=details
        )
    if isinstance(error, APIError):
        status = error.status_code
        code, retryable = {
            400: ("invalid_arguments", False),
            401: ("authentication_failed", False),
            403: ("permission_denied", False),
            404: ("not_found", False),
            409: ("conflict", False),
            422: ("invalid_arguments", False),
            429: ("rate_limited", True),
        }.get(status, ("remote_failed", status >= 500))
        messages = {
            "invalid_arguments": "The Kitaru server rejected the request arguments.",
            "authentication_failed": "The Kitaru server rejected the credential.",
            "permission_denied": "The Kitaru server denied this operation.",
            "not_found": "The requested Kitaru resource was not found.",
            "conflict": "The Kitaru operation conflicts with current remote state.",
            "rate_limited": "The Kitaru server rate limited the request.",
            "remote_failed": "The Kitaru server failed the request.",
        }
        return MCPToolError(code, messages[code], retryable=retryable)
    if isinstance(error, httpx.TimeoutException):
        return MCPToolError("timeout", "The Kitaru request timed out.", retryable=True)
    if isinstance(error, httpx.TransportError):
        return MCPToolError(
            "network_error", "The Kitaru server could not be reached.", retryable=True
        )
    if isinstance(error, TimeoutError):
        return MCPToolError("timeout", "The MCP tool timed out.", retryable=True)
    return MCPToolError("internal_error", "The MCP tool failed unexpectedly.")


def _details(value: object) -> dict[str, JsonValue] | None:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else None
